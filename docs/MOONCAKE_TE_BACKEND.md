# Mooncake Transfer Engine as an RDMA backend (prototype)

**Branch:** `mooncake-te-backend` | **Status:** working prototype, tested on EFA | **Date:** 2026-10-27

## Summary

This branch adds a second data-plane backend to the module: the
[Mooncake Transfer Engine](https://github.com/kvcache-ai/Mooncake) (TE), next to the existing
libfabric/vdma path. It is selected with `fabric-provider MooncakeTe` and built behind the cargo
feature `mooncake-te`. With the feature off, the default build is unchanged and has no Mooncake
dependency.

The RESP command surface (`BLOB.RDMA_HELLO/GET/SET`) is unchanged. Storage, the engine, and
streaming are unchanged. Only `src/transport/` knows which backend is running.

## Why

- **Multi-NIC on both ends.** TE slices a transfer across every NIC and picks NICs by topology.
  Today a client session has one fabric address, so on a host with many NICs (p5/p5en) a single
  connection is limited to one NIC on the client side.
- **GPU memory.** TE registers CUDA (and Neuron) memory for EFA. Clients can transfer GPU memory
  directly instead of staging it through pinned host memory.
- **Not tied to one vendor.** The same API runs over EFA, RDMA verbs (mlx5 and others), and TCP.
  That covers the TSC's "any RDMA provider" goal, and the TCP mode gives CI a test path that
  needs no special hardware.
- **Interop with Mooncake Store and LMCache.** Their clients already own a TE instance with
  registered buffers. A Valkey tier behind them can reuse those buffers instead of registering
  memory a second time.

## How it maps

| Module concept | libfabric backend | TE backend |
|---|---|---|
| Startup | one `FabricService` per domain | one `TransferEngine` (`P2PHANDSHAKE` by default, so no etcd) |
| Pool segment registration | `fi_mr_reg` per domain | `register_local_memory(..., remote_accessible = false)` |
| `BLOB.RDMA_HELLO <hex>` | client's 32-byte EFA address, `fi_av_insert` | client's TE segment name (e.g. `10.0.0.5:16643`), `open_segment` |
| HELLO reply | one EFA address per domain | the server's TE segment name |
| GET / SET triple `(rkey, addr, len)` | used as-is | `addr` used; `rkey` ignored (TE resolves keys at handshake) |
| Completion | vdma worker wakes the future | a poller thread polls `get_transfer_status`, computes the CRC32C for SET, wakes the future |

Code: `src/transport/te.rs` is new. `fabric.rs` now dispatches between the two backends, and a
`TransferHandle` future wraps both kinds of transfer. The other changes are small: new configs in
`configs.rs` and `lib.rs`, and the HELLO address check in `commands/mod.rs`.

## Build and run

```bash
# Transfer Engine CMake build, with PIC (needed to link into the module .so)
cmake -S Mooncake -B mc-build -DUSE_EFA=ON -DCMAKE_POSITION_INDEPENDENT_CODE=ON && ninja -C mc-build transfer_engine

MOONCAKE_BUILD_DIR=$PWD/mc-build cargo build --release --features mooncake-te

LD_LIBRARY_PATH=mc-build/mooncake-common valkey-server --loadmodule target/release/libvalkey_large_object.so \
    operating-mode Dram fabric-provider MooncakeTe te-local-hostname <this-host-ip>
```

New configs, all immutable:

| Config | Default | Meaning |
|---|---|---|
| `te-local-hostname` | (required) | IP, or `ip:port`, used for the TE handshake |
| `te-metadata-server` | `P2PHANDSHAKE` | TE metadata service, or an etcd/http URI |
| `te-protocol` | `efa` | TE transport: `efa`, `rdma`, `tcp`, ... |

## Results (2× r8i.48xlarge, 1 EFA NIC each, about 75 Gbps)

- **Correctness.** Round trip SET → GET → SET → TCP_GET at 4 KB, 1 MB, 8 MB, and 64 MB + 12345
  bytes (multi-chunk, unaligned). Bytes and CRCs match, and the CRCs are identical to the
  efa-direct backend. Bad addresses and CRC mismatches return errors and the server stays healthy.
- **Performance.** Same driver for both backends, client on a different host than the server:

| Object | Connections | efa-direct GET | TE GET |
|---|---|---|---|
| 64 KB | 1 | 0.35 GB/s, p50 187 µs | 0.37 GB/s, p50 179 µs |
| 1 MB | 1 | 3.3 GB/s, p50 316 µs | 3.7 GB/s, p50 302 µs |
| 1 MB | 4 | 9.33 GB/s | 9.33 GB/s |
| 16–64 MB | 1 | ≈9.3 GB/s | ≈9.3 GB/s |

  The two backends perform the same within noise, and both reach line rate. A single NIC can't
  show TE's multi-NIC advantage, so that still needs p5/p5en numbers. On both backends, small
  objects are limited by the per-object RESP round trip (about 80 µs), not by RDMA.

- **Refactor safety.** The refactored efa-direct path passes the same round-trip test. Clippy is
  clean with and without the feature.

## Security dependency (important)

The TE backend registers pool memory with `remote_accessible = false`, because the server is the
only initiator. **TE's EFA transport currently ignores that flag:** every memory region gets
remote read/write rights and its rkey is published, so any TE peer that opens the server's segment
can read or write the cache and bypass ACLs. We reproduced this: a second TE process read server
memory at 2 GB/s.

The fix, kvcache-ai/Mooncake#4976, brings EFA in line with TE's verbs transport. With it, the
same attack is rejected and the module still works. **Don't deploy this backend until that fix
is merged.**

## Known limitations / next steps

- **Not run yet:** the module's pytest suite (`build.sh integ-test`, Emulated provider). Not
  tested yet: multi-NIC hosts, GPU memory, and the Tiered mode staging path over TE.
- **Prototype shortcuts:**
  - one TE batch per transfer;
  - CRC computed on the single poller thread;
  - `libasio.so` needs `LD_LIBRARY_PATH` (no rpath);
  - client segments are not closed on disconnect, because connections from one client process
    share a segment.
- **Build:** TE's Rust `build.rs` fails to link on hosts that have the NVIDIA driver but no CUDA
  runtime. Upstream PR kvcache-ai/Mooncake#4961 fixes it; until it lands we use a local `[patch]`.
- **Bigger win:** batching. With per-object RESP round trips, the control plane, not RDMA, limits
  KV-cache-sized objects (100s of KB to a few MB). A multi-key `BLOB.RDMA_MGET/MSET` would help
  both backends.

## Open questions for the team

1. Is a pluggable TE backend acceptable in this repo (optional feature), or should it live
   outside as a separate crate or module?
2. Should the HELLO protocol for TE carry the segment name as it does here, or should the
   commands become modal per provider (as discussed for Option 3 in `DMA_COMMAND_SYNTAX.md`)?
3. Which client should we prove this with first: an LMCache L2 adapter that reuses LMCache's TE
   instance, or GLIDE?
