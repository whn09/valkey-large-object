//! The fabric: the transport backend chosen by `fabric-provider`, started at module load and
//! dropped at shutdown. Pool segments are registered on it at load. `BLOB.RDMA_HELLO` adds the
//! client as a peer. On disconnect the client's peer entry is released.
//!
//! - libfabric (`Emulated`, `EfaDirect`): one `asynchronous::FabricService` per libfabric domain.
//! - Mooncake Transfer Engine (`MooncakeTe`, cargo feature `mooncake-te`): see `transport::te`.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context, Poll};

use dma_libfabric::asynchronous::{FabricService, Transfer};
use dma_libfabric::{
    discover_domains, Configuration, MemoryRegion, Outcome, Pool, TransferRequest,
};
use dma_libfabric_protocol::DmaError;
use rand::RngExt;

use crate::transport::config::FabricProvider;
use crate::transport::operand::PoolOperand;
#[cfg(feature = "mooncake-te")]
use crate::transport::te::{TeBackend, TeConfig, TeTransfer};

pub struct Fabric {
    backend: Backend,
}

enum Backend {
    Libfabric(Libfabric),
    #[cfg(feature = "mooncake-te")]
    MooncakeTe(TeBackend),
}

impl std::fmt::Debug for Fabric {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let backend = match &self.backend {
            Backend::Libfabric(_) => "libfabric",
            #[cfg(feature = "mooncake-te")]
            Backend::MooncakeTe(_) => "mooncake-te",
        };
        formatter
            .debug_struct("Fabric")
            .field("backend", &backend)
            .field("services", &self.service_count())
            .finish()
    }
}

impl Fabric {
    /// Open the backend `fabric-provider` names.
    pub fn open() -> Result<Self, String> {
        let backend = match crate::fabric_provider() {
            FabricProvider::Emulated | FabricProvider::EfaDirect => {
                Backend::Libfabric(Libfabric::start(&crate::transport::config::configuration())?)
            }
            #[cfg(feature = "mooncake-te")]
            FabricProvider::MooncakeTe => Backend::MooncakeTe(TeBackend::start(&TeConfig {
                local_hostname: crate::te_local_hostname(),
                metadata_server: crate::te_metadata_server(),
                protocol: crate::te_protocol(),
                locations: crate::te_locations(),
                crc_threads: crate::fabric_crc_pool_threads(),
            })?),
            #[cfg(not(feature = "mooncake-te"))]
            FabricProvider::MooncakeTe => {
                return Err(
                    "this build has no Mooncake Transfer Engine (feature mooncake-te)".into(),
                )
            }
        };
        Ok(Self { backend })
    }

    pub fn service_count(&self) -> usize {
        match &self.backend {
            Backend::Libfabric(libfabric) => libfabric.services.len(),
            #[cfg(feature = "mooncake-te")]
            Backend::MooncakeTe(_) => 1,
        }
    }

    pub fn transfer(
        &self,
        request: TransferRequest<PoolOperand>,
    ) -> Result<TransferHandle, DmaError> {
        match &self.backend {
            Backend::Libfabric(libfabric) => {
                libfabric.transfer(request).map(TransferHandle::Libfabric)
            }
            #[cfg(feature = "mooncake-te")]
            Backend::MooncakeTe(te) => te.transfer(request).map(TransferHandle::MooncakeTe),
        }
    }

    /// Register ONE segment with the backend so any transfer can use it. Fatal-on-failure policy
    /// lives with the caller (`efa_register_segment`).
    pub fn register_segment(&self, segment: &'static [u8]) -> Result<(), String> {
        match &self.backend {
            Backend::Libfabric(libfabric) => libfabric.register_segment(segment),
            #[cfg(feature = "mooncake-te")]
            Backend::MooncakeTe(te) => te.register_segment(segment),
        }
    }

    /// Drop a segment's registration by base. MUST run before the segment memory is freed.
    pub fn release_segment(&self, base: usize) {
        match &self.backend {
            Backend::Libfabric(libfabric) => libfabric.release_segment(base),
            #[cfg(feature = "mooncake-te")]
            Backend::MooncakeTe(te) => te.release_segment(base),
        }
    }

    /// Whether the segment at `base` is currently registered.
    pub fn is_registered(&self, base: usize) -> bool {
        match &self.backend {
            Backend::Libfabric(libfabric) => libfabric.is_registered(base),
            #[cfg(feature = "mooncake-te")]
            Backend::MooncakeTe(te) => te.is_registered(base),
        }
    }

    /// Make the client reachable at BLOB.RDMA_HELLO, so an unusable address fails the hello.
    pub fn add_peer(&self, client_id: u64, address: &[u8]) -> Result<(), String> {
        match &self.backend {
            Backend::Libfabric(libfabric) => libfabric.add_peer(client_id, address),
            #[cfg(feature = "mooncake-te")]
            Backend::MooncakeTe(te) => te.add_peer(client_id, address),
        }
    }

    /// One address per service, in service order; what `BLOB.RDMA_HELLO` returns.
    pub fn local_addresses(&self) -> Vec<&[u8]> {
        match &self.backend {
            Backend::Libfabric(libfabric) => libfabric.local_addresses().collect(),
            #[cfg(feature = "mooncake-te")]
            Backend::MooncakeTe(te) => vec![te.local_address()],
        }
    }

    /// Release the client's peer entry once its transfers drain.
    pub fn remove_peer(&self, client_id: u64) {
        match &self.backend {
            Backend::Libfabric(libfabric) => libfabric.remove_peer(client_id),
            #[cfg(feature = "mooncake-te")]
            Backend::MooncakeTe(te) => te.remove_peer(client_id),
        }
    }
}

/// A submitted transfer on either backend. Resolves to its outcome.
pub enum TransferHandle {
    Libfabric(Transfer<PoolOperand>),
    #[cfg(feature = "mooncake-te")]
    MooncakeTe(TeTransfer),
}

impl Future for TransferHandle {
    type Output = (Outcome, ());

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            Self::Libfabric(transfer) => Pin::new(transfer)
                .poll(context)
                .map(|(outcome, _operand)| (outcome, ())),
            #[cfg(feature = "mooncake-te")]
            Self::MooncakeTe(transfer) => Pin::new(transfer)
                .poll(context)
                .map(|outcome| (outcome, ())),
        }
    }
}

// ─── libfabric backend ───────────────────────────────────────────────────────

struct Libfabric {
    services: Vec<FabricService<PoolOperand>>,
    /// EFA registrations keyed by segment base (one `MemoryRegion` per service), retained for the
    /// segment's lifetime. `release_segment` drops them before the memory is freed. `Mutex` because
    /// segments are added/removed at runtime through the shared `Arc<Fabric>`.
    regions: Mutex<std::collections::HashMap<usize, Vec<MemoryRegion<&'static [u8]>>>>,
    /// Runs checksummed completions off the fabric workers. Held so it outlives every service.
    _pool: Arc<Pool>,
}

impl Libfabric {
    /// Open one service per domain: the configured allowlist, or every domain the provider
    /// exposes. Blocks until each endpoint is up, and one failing fails the lot.
    pub fn start(configuration: &Configuration) -> Result<Self, String> {
        let domains = if configuration.interfaces.is_empty() {
            discover_domains(configuration).map_err(|error| error.to_string())?
        } else {
            configuration.interfaces.clone()
        };
        if domains.is_empty() {
            return Err("no fabric domains found".to_string());
        }
        let threads = configuration.crc_pool_threads.unwrap_or(1);
        let pool = Arc::new(Pool::new(threads).map_err(|error| format!("crc pool: {error}"))?);
        let mut services = Vec::with_capacity(domains.len());
        for domain in domains {
            let pinned = Configuration {
                interfaces: vec![domain.clone()],
                ..configuration.clone()
            };
            let service = FabricService::start(&pinned, Arc::clone(&pool))
                .map_err(|error| format!("domain {domain}: {error}"))?;
            services.push(service);
        }
        Ok(Self {
            services,
            regions: Mutex::new(std::collections::HashMap::new()),
            _pool: pool,
        })
    }

    /// Submit on the best of two services picked at random, by transfers outstanding.
    pub fn transfer(
        &self,
        request: TransferRequest<PoolOperand>,
    ) -> Result<Transfer<PoolOperand>, DmaError> {
        let mut rng = rand::rng();
        let first = &self.services[rng.random_range(0..self.services.len())];
        let second = &self.services[rng.random_range(0..self.services.len())];
        let service = if second.outstanding() < first.outstanding() {
            second
        } else {
            first
        };
        service
            .transfer(request)
            .map_err(|_request| DmaError::Fabric("fabric worker is gone".into()))
    }

    /// Register ONE segment on every service so any device can carry a transfer to it. Pins it
    /// once per device against `RLIMIT_MEMLOCK`. `&self` — works at startup and at runtime (pool
    /// expansion) through the shared `Arc<Fabric>`. Handles are retained in `self.regions` keyed
    /// by base for `release_segment`. On mid-loop failure the regions taken in THIS call drop and
    /// the error propagates, so a segment is never left registered on only some services.
    pub fn register_segment(&self, segment: &'static [u8]) -> Result<(), String> {
        let base = segment.as_ptr() as usize;
        let mut new_regions = Vec::with_capacity(self.services.len());
        for (service_index, service) in self.services.iter().enumerate() {
            let region = service
                .register(segment)
                .map_err(|error| format!("segment on service {service_index}: {error}"))?;
            new_regions.push(region);
        }
        // All services registered — retain the handles keyed by base.
        self.regions
            .lock()
            .expect("Fabric.regions lock unavailable")
            .insert(base, new_regions);
        Ok(())
    }

    /// Drop a segment's EFA registration by base. MUST run before the segment memory is freed:
    /// dropping the `MemoryRegion` handles tears the registration down in the DMA library's order
    /// (invalidate the cache entry, then `fi_close`), so no registration outlives its pages. No-op
    /// if the base was never registered (fabric down at expand, or already released).
    pub fn release_segment(&self, base: usize) {
        let regions = self
            .regions
            .lock()
            .expect("Fabric.regions lock unavailable")
            .remove(&base);
        // Drop outside the lock: MemoryRegion::drop calls region_cache::invalidate → fi_close,
        // and deregistration must not run while the regions map lock is held.
        drop(regions);
    }

    /// Whether the segment at `base` is currently EFA-registered.
    pub fn is_registered(&self, base: usize) -> bool {
        self.regions
            .lock()
            .expect("Fabric.regions lock unavailable")
            .contains_key(&base)
    }

    /// Insert the client's address into every service's address vector at BLOB.RDMA_HELLO, so its
    /// transfers post against a known peer and an unusable address fails the hello instead.
    pub fn add_peer(&self, client_id: u64, address: &[u8]) -> Result<(), String> {
        for (index, service) in self.services.iter().enumerate() {
            service
                .add_peer(client_id, address)
                .map_err(|error| format!("service {index}: {error}"))?;
        }
        Ok(())
    }

    /// One fabric address per service, in service order; what `BLOB.RDMA_HELLO` returns.
    pub fn local_addresses(&self) -> impl Iterator<Item = &[u8]> {
        self.services.iter().map(FabricService::local_address)
    }

    /// Release the client's address-vector entry on every service once its transfers drain.
    pub fn remove_peer(&self, client_id: u64) {
        for service in &self.services {
            service.remove_peer(client_id);
        }
    }
}

// ─── Global Fabric ───────────────────────────────────────────────────────────

/// `None` before `commit` and after `shutdown`. `None` at commit means this instance has no
/// fabric: the module runs TCP-only and `BLOB.RDMA_HELLO` reports RDMA unavailable.
/// RwLock: every EFA transfer reads it from tokio workers; only commit and shutdown write.
static FABRIC: RwLock<Option<Arc<Fabric>>> = RwLock::new(None);

/// Store the fabric from `initialize`, once it has been set up.
pub fn commit(fabric: Option<Fabric>) {
    let mut slot = FABRIC.write().expect("FABRIC lock unavailable");
    assert!(slot.is_none(), "fabric already committed");
    *slot = fabric.map(Arc::new);
}

/// The running fabric, or `None` when this instance has none.
pub fn fabric() -> Option<Arc<Fabric>> {
    FABRIC.read().expect("FABRIC lock unavailable").clone()
}

/// Drop the services. Each closes its channel, drains its in-flight transfers, and joins its
/// worker.
pub fn shutdown() {
    let fabric = FABRIC.write().expect("FABRIC lock unavailable").take();
    drop(fabric);
}
