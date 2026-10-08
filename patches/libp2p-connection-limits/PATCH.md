# Carried patch: libp2p-connection-limits

This is the production source of `libp2p-connection-limits` 0.7.0 with a closure-accounting
fix. The root `[patch.crates-io]` selects this copy for the existing libp2p 0.57.0 stack.
The crate is excluded from the workspace, like the carried libp2p-quic patch, to preserve
upstream lints and style. The `connection-limits-patch` CI job explicitly runs its tests
and is required by `CI Success`, including for maintainer PRs.

## Provenance

- Published crate: `libp2p-connection-limits` 0.7.0.
- crates.io archive checksum from the original root `Cargo.lock`:
  `11b98b22c89fc70113a8988db523b0715e299c804e08da7eb20b8440b8249833`.
- Published VCS revision: `7171dce2f90c05ba7892d4ba926abb1881db27c7`, path
  `misc/connection-limits`, from the archive's `.cargo_vcs_info.json`.
- Original complete `src/lib.rs` SHA-256:
  `9ea73d34ff7a50fae7104a8c687d5f2a77f472d4b684e7a144786413d5eae901`.
- The production source is the original file's lines 1 through 400. Its MIT copyright
  and permission notice are retained. Upstream's following swarm integration tests need
  rust-libp2p workspace-only `libp2p-swarm-test` dependencies and are not carried here.
  `src/accounting_tests.rs` instead drives the actual admission callbacks and swarm
  notifications directly, without sockets, randomness or sleeps.
- Runtime dependencies, versions and features match the published manifest:
  libp2p-core 0.44.0, libp2p-identity 0.3.0 (`peerid`), libp2p-swarm 0.48.0.
  The root lockfile changes only this crate's source and checksum, selecting the path
  package without moving any dependency version. There is no separate patch lockfile.

## Behavior and ownership

On `ConnectionClosed`, look up an existing per-peer connection set, remove the closed
ID and remove the peer entry only when that set becomes empty. A closure for an absent
peer cannot insert a key. Multiple connections keep the entry until the last closes.
Inbound and outbound sets still remove only the closed ID, and admission callbacks,
pending accounting, bypass rules and all six configurable limits remain unchanged.
Neither event ordering nor the network's configured caps move to the peer manager.
Cleanup applies even when the per-peer ceiling is disabled.

Telcoin's network maintainers own this carried patch with the `network-libp2p` dependency
stack. Public tracking is [telcoin-network #1468](https://github.com/Telcoin-Association/telcoin-network/issues/1468),
related to [#1010](https://github.com/Telcoin-Association/telcoin-network/issues/1010).
Keep the provenance and regression evidence current when updating this dependency.

## Upstream resolution and retirement

As of 2026-10-01, upstream master at
[`fd65f8a53400af888c73dd9973582a5d13284c61`](https://github.com/libp2p/rust-libp2p/blob/fd65f8a53400af888c73dd9973582a5d13284c61/misc/connection-limits/src/lib.rs)
still uses `entry(peer_id).or_default().remove(&connection_id)` on closure. No public
upstream issue or PR for this cleanup, or released version containing it, was found.
Upstream submission and its released version remain tracked in #1468; this patch does
not assign an unannounced release number or claim an upstream resolution.

Retire the carried patch only after a published libp2p-connection-limits release contains
both last-connection cleanup and non-inserting unknown-closure handling and is compatible
with the resolved libp2p stack. Record its public upstream issue/PR, release and exact
lockfile revision here. Run these regressions against that release, then remove the path
override, exclusion and carried source together. Preserve lifecycle coverage in the
network tests or upstream tests when removing the dedicated CI job.

## Regression command

From the repository root, using its lockfile and pinned toolchain:

```sh
cargo +1.94 test --locked -p libp2p-connection-limits --lib
```

The tests cover distinct-identity churn around a live-peer baseline, mixed inbound and
outbound connections, duplicate and unknown closures, rejected and failed connections,
pending budgets, configured directional/per-peer/aggregate ceilings, slot reuse and
disabled per-peer ceilings. Restoring the original closure arm must fail the lifecycle
assertions, while leaving every admission callback unchanged.
