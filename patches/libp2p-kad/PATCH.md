# Carried patch: libp2p-kad

This directory vendors libp2p-kad 0.49.0 from crates.io. The root manifest selects it through
`[patch.crates-io]` and excludes it from the workspace, following the QUIC patch convention.
The upstream checksum is `973caa45045e53f3cf1cf3d596888b82602b30640fd75485836c81aa66e7c38a`.
`CHANGELOG.md`, source files, and upstream tests are copied from the published crate. The published
`Cargo.toml` restores upstream's omitted test-only dependencies and swarm executor/derive features,
so the retained unit and integration tests compile standalone. The 46-line quickcheck helper and
443-line swarm helper are copied from `misc/quickcheck-ext/src/lib.rs` and `swarm-test/src/lib.rs`
at the crate's release commit `7171dce2f90c05ba7892d4ba926abb1881db27c7`, with standalone manifests.
A nested test workspace and formatter configuration preserve upstream formatting and isolate
test dependencies.
The root lockfile changes only the source/checksum for this package; its dependencies are unchanged.

## Purpose

The resolved library's `Config::set_periodic_bootstrap_interval(None)` disables only periodic
bootstrap. Its automatic-bootstrap throttle setter is private and compiled only in tests.
Neither API supports changing a live swarm's policy. Closed validators need to suspend both
bootstrap sources without losing their routing table, signed records, or committee lookups.

## Changes

- `Behaviour::set_public_discovery_enabled` suspends periodic and automatic bootstrap together.
  Closure removes active bootstrap and closest-peer queries, their buffered RPCs, queued public
  query results, and queued dials that are not needed by a surviving record query. The application
  admission gate rechecks already emitted and record-query dials against configured and committee
  identities before allowing a connection.
- `bootstrap::Status` retains its configured intervals and running-query accounting, clears queued
  automatic work on closure, and wakes its owner on recovery. Reopening resets the original timers.
- `QueryPool::remove` removes canceled work outright, preventing a finished bootstrap from spawning
  its next bucket-refresh phase after a later reopening.
- `Behaviour::cancel_query` also removes queued record requests when the application's updated
  committee/configured-peer policy no longer permits their key. Closure discards stale public
  requests even if their query has already finished.
- `discovery_tests.rs` and `bootstrap_policy_tests.rs` exercise closure, retained record resolution,
  queued work, timer readiness, new contacts, and recovery without fixed sleeps.

## Updating

Compare this directory with the crates.io release before updating the dependency. Carry the runtime
switch and cancellation semantics forward, or replace them with upstream APIs that provide the same
behavior. Run the new policy tests as well as the upstream library suite. The original copyright and
license notices remain in the source.

Standalone validation:

```sh
cargo +1.94 test --manifest-path patches/libp2p-kad/Cargo.toml --all-targets --all-features
```
