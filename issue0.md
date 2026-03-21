# feat(exex): add core ExEx types crate

## Summary

Create a new `crates/exex/` crate with the foundational types for TN's Execution Extensions (ExEx) system. ExEx is a plugin system that allows external modules to react to chain state changes in real-time — enabling use cases like block explorers, node operator dashboards, indexers, and bridges.

These types are simplified versions of [reth's ExEx types](https://github.com/paradigmxyz/reth/tree/main/crates/exex/types/src), adapted for TN's Bullshark BFT consensus which provides **immediate finality with no reorgs**.

## Background

reth's ExEx system has three notification variants (`ChainCommitted`, `ChainReorged`, `ChainReverted`) plus a WAL for crash recovery. TN's BFT consensus makes reorgs impossible , so we only need the `ChainCommitted` variant and no WAL.

## Scope

### New files

- `crates/exex/Cargo.toml`
- `crates/exex/src/lib.rs`
- `crates/exex/src/notification.rs`
- `crates/exex/src/event.rs`
- `crates/exex/src/context.rs`

### Modified files

- `Cargo.toml` (workspace) — add `tn-exex` to members and workspace dependencies

### Type definitions

**`TnExExNotification`** (in `notification.rs`):

```rust
/// Notification sent to ExExes when new blocks are committed to the canonical chain.
///
/// TN uses Bullshark BFT consensus with immediate finality, so only `ChainCommitted`
/// is needed. There are no reorgs — all committed blocks are immediately final.
pub enum TnExExNotification {
    /// New blocks added to the canonical chain.
    ChainCommitted {
        /// The new chain segment containing blocks, receipts, and state changes.
        new: Arc<Chain>,
    },
}
```

**`TnExExEvent`** (in `event.rs`):

```rust
/// Event sent from an ExEx back to the manager.
pub enum TnExExEvent {
    /// Signal that the ExEx has durably processed all blocks up to this height.
    /// Used for pruning coordination — the node won't prune data below the
    /// minimum finished height across all ExExes.
    FinishedHeight(BlockNumber),
}
```

**`TnExExContext`** (in `context.rs`):

```rust
/// Everything an ExEx needs to operate, provided at launch.
pub struct TnExExContext {
    /// Async stream of committed chain notifications.
    pub notifications: mpsc::Receiver<TnExExNotification>,
    /// Channel to send events (e.g., FinishedHeight) back to the manager.
    pub events: mpsc::UnboundedSender<TnExExEvent>,
    /// Access to reth's blockchain provider for querying chain state/history.
    pub reth_env: RethEnv,
}
```

**Install function type:**

```rust
/// Type for ExEx install functions. The outer future handles initialization,
/// the returned inner future is the long-running ExEx task.
///
/// Reference: reth's LaunchExEx trait uses the same nested-future pattern.
/// See: https://github.com/paradigmxyz/reth/blob/main/crates/node/builder/src/exex.rs
pub type ExExInstallFn = Box<
    dyn FnOnce(TnExExContext) -> Pin<Box<dyn Future<Output = eyre::Result<()>> + Send>>
        + Send
>;
```

### Dependencies

```toml
[dependencies]
tn-reth = { path = "../tn-reth" }
tn-types = { path = "../types" }
reth-execution-types = { workspace = true }
reth-primitives-traits = { workspace = true }
tokio = { workspace = true }
eyre = { workspace = true }
futures = { workspace = true }
```

## Acceptance criteria

- [ ] `crates/exex/` compiles as part of the workspace
- [ ] `TnExExNotification` only has `ChainCommitted` (no reorg variants)
- [ ] `TnExExContext` provides notifications receiver, events sender, and `RethEnv`
- [ ] All public types have doc comments referencing TN's BFT finality guarantee
- [ ] `cargo check --workspace` passes

## References

- [reth ExEx types crate](https://github.com/paradigmxyz/reth/tree/main/crates/exex/types/src)
- [reth LaunchExEx trait](https://github.com/paradigmxyz/reth/blob/main/crates/node/builder/src/exex.rs)
- [reth ExExContext](https://github.com/paradigmxyz/reth/blob/main/crates/exex/exex/src/context.rs)

## Dependencies

None — this is the foundational issue.
