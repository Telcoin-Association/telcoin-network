# feat(exex): add ExEx manager for notification fan-out

## Summary

Implement `TnExExManager`, a long-running async task that subscribes to reth's `CanonStateNotificationStream` and fans out `TnExExNotification::ChainCommitted` to all registered ExExes. It also collects `TnExExEvent::FinishedHeight` from each ExEx to track the minimum finished height across all extensions for pruning coordination.

This is substantially simpler than [reth's ExExManager](https://github.com/paradigmxyz/reth/blob/main/crates/exex/exex/src/manager.rs) because TN's BFT consensus provides immediate finality — no WAL, no reorg handling, no backfill pipeline coordination.

## Background

TN already emits `CanonStateNotification::Commit` after each block is finalized (see `crates/tn-reth/src/lib.rs` — `finish_executing_output()` calls `notify_canon_state()`). The transaction pool already subscribes to this stream at `crates/tn-reth/src/txn_pool.rs:127`. The ExEx manager subscribes to the same stream and distributes to ExExes.

## Scope

### New files
- `crates/exex/src/manager.rs`

### Modified files
- `crates/exex/src/lib.rs` — update exports

### Implementation

**`TnExExManager`** struct:
```rust
pub struct TnExExManager {
    /// Subscription to canonical state notifications from reth's BlockchainProvider.
    canon_state_stream: CanonStateNotificationStream,
    /// Per-ExEx notification senders (one per registered ExEx).
    exex_txs: Vec<(String, mpsc::Sender<TnExExNotification>)>,
    /// Per-ExEx event receivers for FinishedHeight reporting.
    event_rxs: Vec<mpsc::UnboundedReceiver<TnExExEvent>>,
    /// Minimum finished height across all ExExes.
    finished_heights: Vec<Option<BlockNumber>>,
}
```

**`TnExExManager` as a `Future`** (following `ExecutorEngine` pattern at `crates/engine/src/lib.rs:187`):

`poll()` implementation:
1. Poll `canon_state_stream` for new notifications
2. On `CanonStateNotification::Commit { new }` → wrap in `TnExExNotification::ChainCommitted { new }` and send to all ExExes
3. On any other variant → `unreachable!("TN reorgs are impossible")` (matching existing pattern in txn_pool.rs)
4. Poll all `event_rxs` for `FinishedHeight` updates and track per-ExEx heights
5. If an ExEx's send channel is full/closed, log a warning but do NOT block other ExExes

**`TnExExManagerHandle`** — cheaply cloneable handle for querying state:
```rust
pub struct TnExExManagerHandle {
    /// The minimum finished height across all ExExes.
    /// None if any ExEx hasn't reported yet.
    min_finished_height: watch::Receiver<Option<BlockNumber>>,
}
```

### Design decisions

- **Non-blocking fan-out**: If one ExEx is slow, others still receive notifications. Use `try_send` and log warnings on channel full.
- **Channel capacity**: 64 notifications per ExEx (configurable). This provides ~3 minutes of buffer at 3 blocks/second.
- **Graceful degradation**: If an ExEx's channel is closed (ExEx crashed), log an error but continue running other ExExes. Since ExExes are spawned as critical tasks, this typically means the node is shutting down anyway.

## Acceptance criteria

- [ ] Manager correctly fans out `Commit` notifications to all registered ExExes
- [ ] Manager tracks minimum finished height across all ExExes
- [ ] Slow/failed ExExes don't block notification delivery to other ExExes
- [ ] `TnExExManagerHandle` exposes current `min_finished_height`
- [ ] Unit tests: register 2 mock ExExes, send notifications, verify both receive them
- [ ] Unit test: verify `min_finished_height` tracking with multiple ExExes reporting different heights
- [ ] `cargo check --workspace` passes

## References

- [reth ExExManager](https://github.com/paradigmxyz/reth/blob/main/crates/exex/exex/src/manager.rs) — full implementation with WAL/reorg (our version is much simpler)
- `crates/engine/src/lib.rs:187` — `ExecutorEngine` Future pattern to follow
- `crates/tn-reth/src/txn_pool.rs:127` — existing `CanonStateNotificationStream` subscriber

## Dependencies

- Issue 0 (core ExEx types)
