# feat(exex): add historical block replay for ExEx catch-up

## Summary

Implement a mechanism for ExExes to replay historical finalized blocks from the database when they start (or restart after a crash) and need to catch up to the current chain tip. Since TN has no reorgs, this is simpler than [reth's backfill system](https://github.com/paradigmxyz/reth/tree/main/crates/exex/exex/src/backfill) — just read finalized blocks from the DB in ascending order.

## Background

reth's backfill system re-executes blocks through the EVM in batches using `StreamBackfillJob` with configurable thresholds (block count, memory, gas, time). This is necessary because reth needs to handle reorgs during backfill.

TN's BFT finality means all blocks in the DB are permanently final, so we can simply read them and construct `TnExExNotification::ChainCommitted` notifications identical to live ones. No EVM re-execution needed — we read the already-stored execution results.

## Scope

### New files
- `crates/exex/src/replay.rs`

### Modified files
- `crates/exex/src/context.rs` — add replay helper methods
- `crates/exex/src/lib.rs` — update exports

### Implementation

**Replay stream** (in `replay.rs`):
```rust
/// Stream that replays historical blocks from the database as ExEx notifications.
///
/// Reads finalized blocks from `start_block` to `end_block` (inclusive) and yields
/// `TnExExNotification::ChainCommitted` for each block.
pub struct ReplayStream {
    reth_env: RethEnv,
    current_block: BlockNumber,
    end_block: BlockNumber,
}

impl Stream for ReplayStream {
    type Item = eyre::Result<TnExExNotification>;
    // Reads block + receipts + state changes from DB, wraps in ChainCommitted
}
```

**Context helpers** (in `context.rs`):
```rust
impl TnExExContext {
    /// Replay historical blocks from `start_block` to the current chain tip.
    ///
    /// Returns a stream of `TnExExNotification::ChainCommitted` for each historical block.
    /// After the stream is exhausted, the ExEx should switch to `self.notifications` for live data.
    pub fn replay_from(&self, start_block: BlockNumber) -> ReplayStream { /* ... */ }

    /// Convenience: replay from `start_block` then seamlessly chain with live notifications.
    ///
    /// This is the recommended way for stateful ExExes to initialize:
    /// ```rust
    /// let mut stream = ctx.replay_and_subscribe(last_indexed_block + 1);
    /// while let Some(notification) = stream.next().await {
    ///     // Process identically — no distinction between replay and live
    /// }
    /// ```
    pub fn replay_and_subscribe(
        self,
        start_block: BlockNumber,
    ) -> impl Stream<Item = eyre::Result<TnExExNotification>> { /* ... */ }
}
```

### Key details

- **Block reading**: Use `RethEnv`'s provider methods (`BlockReader`, `ReceiptProvider`, etc.) to read block headers, bodies, receipts, and state changes from the database
- **Chain construction**: Build `reth_execution_types::Chain` from the DB data, matching the same `Arc<Chain>` format that live notifications use
- **Batch size**: Read blocks one at a time to keep memory usage bounded. The `Chain` type already supports single-block chains.
- **Edge cases**:
  - `start_block > tip` → empty stream (no replay needed)
  - `start_block = 0` → replay from genesis
  - DB missing expected blocks → return error in stream item

## Acceptance criteria

- [ ] `ReplayStream` correctly reads blocks from DB and yields `ChainCommitted` notifications
- [ ] `replay_and_subscribe()` seamlessly chains replay with live notifications
- [ ] Replay produces notifications identical in structure to live notifications
- [ ] Handles edge case: `start_block` beyond current tip returns empty stream
- [ ] Unit test: insert N blocks to DB, replay from block M, verify blocks M..N are yielded
- [ ] `cargo check --workspace` passes

## References

- [reth BackfillJob](https://github.com/paradigmxyz/reth/blob/main/crates/exex/exex/src/backfill/job.rs) — reth's version (more complex due to reorg handling)
- [reth ExEx notifications with head](https://github.com/paradigmxyz/reth/blob/main/crates/exex/exex/src/notifications.rs) — backfill + live transition
- `crates/tn-reth/src/lib.rs` — `RethEnv` provider methods for reading blocks

## Dependencies

- Issue 0 (core ExEx types)
- Issue 1 (ExEx manager)
