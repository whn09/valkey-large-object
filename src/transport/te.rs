//! The Mooncake Transfer Engine backend (`fabric-provider MooncakeTe`, cargo feature
//! `mooncake-te`). One `TransferEngine` per process carries every transfer; it picks NICs and
//! slices large transfers itself, so there is one "service" here, not one per domain.
//!
//! - `BLOB.RDMA_HELLO <hex(segment name)>`: the client's own Transfer Engine segment name (in
//!   P2PHANDSHAKE mode `ip:rpc_port`). The server opens it, which runs the TE handshake and
//!   fetches the client's buffer descriptors, and replies with its own segment name.
//! - `BLOB.RDMA_GET/SET` keep their `(rkey, addr, len)` triples. `addr` is the client's virtual
//!   address; `rkey` is ignored, since TE resolves the key from the segment's descriptors.
//! - Pool segments are registered with `remote_accessible = false`: the server is the only
//!   initiator, so its memory is never published in its segment descriptor and no peer can
//!   RMA into it.
//!
//! TE has no completion callback. `transfer` submits on the calling thread, and one poller thread
//! watches every in-flight batch, checksums reads that asked for it, and wakes the waiting task.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread::JoinHandle;

use crossbeam_channel::{Receiver, Sender};
use dma_libfabric::{Direction, Outcome, TransferDone, TransferRequest};
use dma_libfabric_protocol::DmaError;
use transfer_engine_rust::{
    BatchId, SegmentId, TransferEngine, TransferRequest as TeRequest, WILDCARD_LOCATION,
};

use crate::transport::operand::PoolOperand;

/// Module args for the Transfer Engine backend.
pub struct TeConfig {
    /// This host's address for the TE handshake (`ip` or `ip:port`).
    pub local_hostname: String,
    /// TE metadata service: `P2PHANDSHAKE`, or an etcd / http metadata URI.
    pub metadata_server: String,
    /// TE transport to install: `efa`, `rdma`, `tcp`, ...
    pub protocol: String,
}

pub struct TeBackend {
    engine: Arc<TransferEngine>,
    /// This engine's segment name, what `BLOB.RDMA_HELLO` returns.
    local_name: Vec<u8>,
    /// The client's opened segment, by Valkey client id. TE caches a segment id per name, and
    /// several connections from one client process share it, so disconnect never closes it.
    peers: Mutex<HashMap<u64, SegmentId>>,
    /// Bases of the pool segments registered with the engine.
    registered: Mutex<HashSet<usize>>,
    /// `None` only while dropping: closing it lets the poller drain and exit.
    submit: Option<Sender<InFlight>>,
    poller: Option<JoinHandle<()>>,
}

impl TeBackend {
    pub fn start(config: &TeConfig) -> Result<Self, String> {
        if config.local_hostname.is_empty() {
            return Err("te-local-hostname is required with fabric-provider MooncakeTe".into());
        }
        let engine = TransferEngine::initialize(
            &config.local_hostname,
            &config.metadata_server,
            &config.protocol,
            "",
        )
        .map_err(|error| format!("transfer engine init: {error}"))?;
        let local_name = engine
            .local_ip_and_port()
            .map_err(|error| format!("transfer engine local address: {error}"))?;
        let engine = Arc::new(engine);
        let (submit, completions) = crossbeam_channel::unbounded();
        let poller_engine = Arc::clone(&engine);
        let poller = std::thread::Builder::new()
            .name("largeobj-te".into())
            .spawn(move || poll_completions(&poller_engine, &completions))
            .map_err(|error| format!("transfer engine poller: {error}"))?;
        Ok(Self {
            engine,
            local_name: local_name.into_bytes(),
            peers: Mutex::new(HashMap::new()),
            registered: Mutex::new(HashSet::new()),
            submit: Some(submit),
            poller: Some(poller),
        })
    }

    pub fn local_address(&self) -> &[u8] {
        &self.local_name
    }

    pub fn register_segment(&self, segment: &'static [u8]) -> Result<(), String> {
        let base = segment.as_ptr() as usize;
        // SAFETY: pool segments live until `release_segment`, which unregisters first.
        unsafe {
            self.engine.register_local_memory_ex(
                segment.as_ptr().cast_mut().cast(),
                segment.len(),
                WILDCARD_LOCATION,
                false,
            )
        }
        .map_err(|error| format!("transfer engine registration: {error}"))?;
        self.registered
            .lock()
            .expect("TeBackend.registered lock unavailable")
            .insert(base);
        Ok(())
    }

    pub fn release_segment(&self, base: usize) {
        let was_registered = self
            .registered
            .lock()
            .expect("TeBackend.registered lock unavailable")
            .remove(&base);
        if was_registered {
            // SAFETY: `base` was registered by `register_segment` and its memory is still live.
            if let Err(error) = unsafe { self.engine.unregister_local_memory(base as *mut _) } {
                valkey_module::logging::log_warning(format!(
                    "largeobj: transfer engine unregister of {base:#x} failed: {error}"
                ));
            }
        }
    }

    pub fn is_registered(&self, base: usize) -> bool {
        self.registered
            .lock()
            .expect("TeBackend.registered lock unavailable")
            .contains(&base)
    }

    /// Open the client's segment (the TE handshake) and bind it to the Valkey client.
    pub fn add_peer(&self, client_id: u64, address: &[u8]) -> Result<(), String> {
        let name = std::str::from_utf8(address)
            .map_err(|_| "transfer engine segment name is not utf-8".to_string())?;
        let segment = self
            .engine
            .open_segment(name)
            .map_err(|error| format!("open segment {name}: {error}"))?;
        // Establish the EFA connection now rather than on the first transfer. Best effort: the
        // first transfer connects anyway if this fails.
        if let Err(error) = self.engine.warmup_efa_segment(name) {
            valkey_module::logging::log_debug(format!("warmup of segment {name}: {error}"));
        }
        self.peers
            .lock()
            .expect("TeBackend.peers lock unavailable")
            .insert(client_id, segment);
        Ok(())
    }

    pub fn remove_peer(&self, client_id: u64) {
        self.peers
            .lock()
            .expect("TeBackend.peers lock unavailable")
            .remove(&client_id);
    }

    pub fn transfer(&self, request: TransferRequest<PoolOperand>) -> Result<TeTransfer, DmaError> {
        let segment = *self
            .peers
            .lock()
            .expect("TeBackend.peers lock unavailable")
            .get(&request.client_id)
            .ok_or_else(|| DmaError::Fabric("no transfer engine segment for client".into()))?;
        let (local, capacity) = request.caller_context.raw_parts();
        let te_request = match request.direction {
            Direction::ToPeer => TeRequest::write(
                local.cast(),
                segment,
                request.remote_address,
                capacity as u64,
            ),
            Direction::FromPeer { length } => {
                if capacity < length {
                    return Err(DmaError::Transfer("read exceeds the local buffer".into()));
                }
                TeRequest::read(local.cast(), segment, request.remote_address, length as u64)
            }
        };
        let length = te_request.length as usize;
        let batch = self
            .engine
            .allocate_batch_id(1)
            .map_err(|error| DmaError::Fabric(format!("allocate batch: {error}")))?;
        // SAFETY: the local operand is inside a registered pool segment, and the engine keeps the
        // owning buffer alive until this transfer resolves.
        if let Err(error) = unsafe { self.engine.submit_transfer(batch, &[te_request]) } {
            let _ = self.engine.free_batch_id(batch);
            return Err(DmaError::Transfer(format!("submit: {error}")));
        }
        let slot = Arc::new(Mutex::new(Slot::default()));
        let in_flight = InFlight {
            batch,
            local: local as usize,
            length,
            want_checksum: request.want_checksum,
            slot: Arc::clone(&slot),
        };
        self.submit
            .as_ref()
            .expect("transfer engine poller is gone")
            .send(in_flight)
            .map_err(|_| DmaError::Fabric("transfer engine poller is gone".into()))?;
        Ok(TeTransfer { slot })
    }
}

impl Drop for TeBackend {
    fn drop(&mut self) {
        // Closing the channel lets the poller finish what is in flight, then exit.
        drop(self.submit.take());
        if let Some(poller) = self.poller.take() {
            let _ = poller.join();
        }
    }
}

// ─── Completions ─────────────────────────────────────────────────────────────

/// A submitted single-request batch the poller watches.
struct InFlight {
    batch: BatchId,
    /// The local operand, for the checksum of a read.
    local: usize,
    length: usize,
    want_checksum: bool,
    slot: Arc<Mutex<Slot>>,
}

impl InFlight {
    /// Resolve the transfer if its batch is terminal. Returns whether it did.
    fn try_finish(&self, engine: &TransferEngine) -> bool {
        let outcome: Outcome = match engine.get_transfer_status(self.batch, 0) {
            Ok(status) if !status.is_terminal() => return false,
            Ok(status) if status.is_completed() => Ok(TransferDone {
                bytes: self.length,
                checksum: self.want_checksum.then(|| {
                    // SAFETY: the read has landed and the initiator still owns the buffer.
                    let data =
                        unsafe { std::slice::from_raw_parts(self.local as *const u8, self.length) };
                    crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32Iscsi, data) as u32
                }),
            }),
            Ok(status) => Err(DmaError::Transfer(format!(
                "transfer engine status {:?}",
                status.status
            ))),
            Err(error) => Err(DmaError::Transfer(format!(
                "transfer engine status: {error}"
            ))),
        };
        let _ = engine.free_batch_id(self.batch);
        let waker = {
            let mut slot = self.slot.lock().expect("TeTransfer slot lock unavailable");
            slot.outcome = Some(outcome);
            slot.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        true
    }
}

/// Poll every in-flight batch until it is terminal. Blocks on the channel while idle; exits once
/// the channel is closed and nothing is left in flight.
fn poll_completions(engine: &TransferEngine, submissions: &Receiver<InFlight>) {
    let mut in_flight: Vec<InFlight> = Vec::new();
    loop {
        if in_flight.is_empty() {
            match submissions.recv() {
                Ok(next) => in_flight.push(next),
                Err(_) => return,
            }
        }
        while let Ok(next) = submissions.try_recv() {
            in_flight.push(next);
        }
        let before = in_flight.len();
        in_flight.retain(|transfer| !transfer.try_finish(engine));
        if in_flight.len() == before {
            std::thread::yield_now();
        }
    }
}

#[derive(Default)]
struct Slot {
    outcome: Option<Outcome>,
    waker: Option<Waker>,
}

/// A submitted Transfer Engine transfer. Resolves once the poller sees its batch finish.
pub struct TeTransfer {
    slot: Arc<Mutex<Slot>>,
}

impl Future for TeTransfer {
    type Output = Outcome;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut slot = self.slot.lock().expect("TeTransfer slot lock unavailable");
        if let Some(outcome) = slot.outcome.take() {
            return Poll::Ready(outcome);
        }
        if !matches!(&slot.waker, Some(waker) if waker.will_wake(context.waker())) {
            slot.waker = Some(context.waker().clone());
        }
        Poll::Pending
    }
}
