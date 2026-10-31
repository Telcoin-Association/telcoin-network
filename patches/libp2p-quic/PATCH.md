# Carried patch: libp2p-quic

This directory holds a vendored copy of `libp2p-quic` with a small additive change. The root
`Cargo.toml` selects it with `[patch.crates-io]` and keeps it out of the workspace with
`exclude`, so the upstream code keeps upstream style and lints.

## Upstream

- Crate: `libp2p-quic` 0.14.0 from crates.io.
- crates.io checksum (from the base `Cargo.lock`):
  `4f78ca359466657b380e469fe8c04df2f4447d1430838e6c3cef4a1c51ccb2ee`.
- Resolved stack: libp2p 0.57.0, libp2p-tls 0.7.0, quinn 0.11.9, quinn-proto 0.11.18,
  rustls 0.23.46, AWS-LC 1.18.1 / 0.45.0 and rustls-webpki 0.103.15.
- Dependencies, features and crypto provider are the same as upstream (tokio feature, quinn
  `rustls-aws-lc-rs` and `futures-io`, ring). `Cargo.toml` is the upstream
  `Cargo.toml.orig` with registry versions in place of workspace references, so the crate
  builds outside the rust-libp2p workspace. The override changes the `source` and `checksum`
  lines of `libp2p-quic`; the maintenance qualification also updates the registry crypto
  pins to address RUSTSEC-2026-0285. The active record retains source and feature evidence.
- The upstream `tests/stream_compliance.rs` is not carried: it needs a rust-libp2p
  workspace crate that is not published at a matching version.
- The upstream `Cargo.lock` is not carried. The crate builds only as a dependency of the
  node, so the node `Cargo.lock` resolves it.

## Why the patch exists

The patch closes a private advisory. In upstream 0.14.0 the listener accepts every incoming
attempt, and quinn then creates connection state and does handshake work before the remote
shows that it can receive packets at its source address. The patch adds:

- `Config::retry_unvalidated_incoming` (default `false`, upstream behavior): the listener
  answers an attempt from an address that is not validated with a QUIC Retry
  (RFC 9000 section 8.1) before any connection state exists. The listener refuses an
  attempt that may not get a Retry, and ignores an attempt when the Retry fails.
- `Config::max_incoming`, `incoming_buffer_size`, `incoming_buffer_size_total` and
  `retry_token_lifetime` (default `None`, quinn defaults): the quinn `ServerConfig` bounds
  of the queue that holds attempts before the listener decides.
- `Config::max_incoming_outcomes_per_poll` (default 128): the number of Retry, Refuse or
  Ignore outcomes one listener poll handles. At the cap the listener wakes its task and
  yields, so a stream of attempts cannot keep the swarm task from yielding. A value below 1
  acts as 1. The cap binds only when Retry is on, because an accepted attempt returns an
  event at once.
- `IncomingStats` (shared atomic counters: retried, accepted, refused, ignored, budget
  yields) through `Config::incoming_stats`. `refused` counts both an attempt the listener
  refuses and an attempt that fails in quinn `Incoming::accept`. A stale Initial can be
  discarded without a close response; the listener still returns the upstream `ListenerError`.

The new code is in `src/incoming.rs`. `src/transport.rs` gets `Listener::poll_accept`, a
bounded `try_fold` over the accept future. The fold form (not a `loop`) keeps the diff small
and matches the house rules of the node repository.

`Config::inbound_accept_task` (default `None`) is an opt-in measurement prototype for
telcoin-network issue #1741. With `None` the listener decides on attempts and accepts them
inline, inside `Transport::poll` on the swarm task, exactly as before. With `Some(capacity)`
each listener spawns one task on the provider runtime (`Provider::spawn`) that waits for
attempts, applies the same Retry policy and per-poll outcome cap (it yields to the executor
at the cap), runs `Incoming::accept`, and queues the accepted handshakes in a bounded channel
of `capacity` entries until the swarm takes them. The bound is explicit: at most `capacity`
accepted handshakes wait for the swarm, and `max_incoming` bounds the attempts ahead of them.
The task ends when the listener closes or is dropped. The node sets the field from
`quic_config.inbound_accept_task` (`mode: inline`, or `mode: spawned` with a `capacity`).

Public tracking: telcoin-network issues #1431 (maintenance and advisory coverage of carried
transport patches) and #1432 (QUIC listener and cross-release interoperability coverage).

## Diff against upstream

Issue #1680 extends `IncomingStats` with fixed stage/decision/reason outcomes and shares an
optional observer from the pinned [quinn-proto companion](../quinn-proto/PATCH.md). This exposes
token failures, actual incoming occupancy, effective ceilings and cap drops before the application
hook. Accepted upgrades carry a guard until completion, timeout or cancellation. Legacy aggregate
counters remain available. See [the observations contract](../../docs/quic-incoming-metrics.md)
for the exact meanings and queries. Root and candidate manifests select both carried sources;
independent interoperability peers retain their registry sources.

`upstream.diff` is the unified diff against the registry copy, with relative paths and no
file times. The upstream side uses `Cargo.toml.orig` as `Cargo.toml` and leaves out the
registry files. Regenerate it from the repository root (`git diff` exits 1 when the trees
differ, which is expected):

```sh
UP=$(ls -d ~/.cargo/registry/src/index.crates.io-*/libp2p-quic-0.14.0 | head -n 1)
TMP=$(mktemp -d)
cp -R "$UP" "$TMP/upstream"
cp -R patches/libp2p-quic "$TMP/patched"
mv "$TMP/upstream/Cargo.toml.orig" "$TMP/upstream/Cargo.toml"
rm -f "$TMP/upstream/Cargo.lock" "$TMP/upstream/.cargo-ok" "$TMP/upstream/.cargo_vcs_info.json"
rm -f "$TMP/patched/PATCH.md" "$TMP/patched/upstream.diff"
(cd "$TMP" && git diff --no-index --full-index --no-color --no-ext-diff --no-prefix upstream patched) \
  > patches/libp2p-quic/upstream.diff
rm -rf "$TMP"
```

## Advisory coverage

The [active maintenance record](../../docs/transport-patches/libp2p-quic/README.md)
names @MavenRain as the accepted maintenance and advisory owner, with weekly review
and review on every transport dependency update. It records upstream provenance,
submission and removal plans, crypto features and the production key logging policy.

Required transport CI runs cargo-audit 0.22.2 and cargo-deny 0.20.2 against a real
published advisory control. Both detect vulnerable registry `quinn-proto` 0.11.6
and miss the identical path source. The record documents that gap, a source-neutral
advisory match, separate Dependabot observations and the owned monitoring procedure.
The gate also audits a registry-form copy of the lockfile, so advisories against the
carried versions are still matched. `quinn-proto` is carried by path as well and has its
own record (`patches/quinn-proto/PATCH.md`); `quinn`, `libp2p-tls` and the TLS crates
remain registry crates. Extend the same record before carrying another crate from a
different source form. Passing a scanner alone does not qualify this copy.

## Removal condition

Remove this copy and the `[patch.crates-io]` entry when upstream `libp2p-quic` exposes a
decision hook on incoming attempts (Retry, Refuse or Ignore before accept) and the quinn
queue bounds. Then set the same values through the upstream API.
