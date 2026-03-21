# feat(exex): integrate ExEx registration into TnBuilder and node lifecycle

## Summary

Add the ability to register ExExes during node construction and wire the ExEx manager into the node lifecycle. ExExes are registered via `TnBuilder::install_exex()` before the node starts, and both the manager and ExEx tasks are spawned on the **node-level `TaskManager`** so they survive epoch boundaries.

## Background

TN's `EpochManager::run()` creates two task managers (see `crates/node/src/manager.rs`):
- `node_task_manager` (line 261) — lives for the entire node lifetime
- `epoch_task_manager` — torn down and recreated each epoch

ExExes must survive epoch boundaries to maintain continuous notification streams, so they are spawned on `node_task_manager`.

The notification source is `RethEnv::canonical_block_stream()` (`crates/tn-reth/src/lib.rs:586`), which provides a `CanonStateNotificationStream` from reth's `BlockchainProvider`.

## Scope

### Modified files

**`crates/node/src/engine/mod.rs`** — add ExEx registration to `TnBuilder`:
```rust
impl TnBuilder {
    /// Register an Execution Extension (ExEx) plugin.
    ///
    /// ExExes are long-running tasks that receive notifications about committed
    /// blocks and can process them for indexing, analytics, dashboards, etc.
    ///
    /// Reference: https://github.com/paradigmxyz/reth/blob/main/crates/node/builder/src/exex.rs
    pub fn install_exex(
        &mut self,
        name: impl Into<String>,
        install_fn: impl FnOnce(TnExExContext) -> Pin<Box<dyn Future<Output = eyre::Result<()>> + Send>> + Send + 'static,
    ) -> &mut Self {
        self.exex_fns.push((name.into(), Box::new(install_fn)));
        self
    }
}
```

Add `exex_fns: Vec<(String, ExExInstallFn)>` field to `TnBuilder` (default empty).

**`crates/node/src/manager.rs`** — spawn ExExes in `EpochManager::run()`:

After the engine is created and started (around line 284), before the epoch loop:
1. If `self.builder.exex_fns` is non-empty:
   a. Get `canon_state_stream` from `engine.canonical_block_stream().await`
   b. Create channel pairs for each registered ExEx
   c. Build `TnExExContext` for each and call its install function to get the `ExExFut`
   d. Spawn each `ExExFut` as a critical task on `node_task_manager`
   e. Build `TnExExManager` with channel pairs and `canon_state_stream`
   f. Spawn the manager as a critical task on `node_task_manager`
2. If no ExExes registered, skip entirely (zero-cost when unused)

**`crates/node/Cargo.toml`** — add `tn-exex` dependency

### Integration pattern

```rust
// In EpochManager::run(), after engine.start_engine():
if !self.builder.exex_fns.is_empty() {
    let reth_env = engine.get_reth_env().await;
    let canon_stream = reth_env.canonical_block_stream();

    let mut exex_txs = Vec::new();
    let mut event_rxs = Vec::new();

    for (name, install_fn) in self.builder.exex_fns.drain(..) {
        let (notif_tx, notif_rx) = mpsc::channel(64);
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        let ctx = TnExExContext {
            notifications: notif_rx,
            events: event_tx,
            reth_env: reth_env.clone(),
        };

        let exex_fut = install_fn(ctx);
        node_task_spawner.spawn_critical_task(&name, exex_fut);

        exex_txs.push((name, notif_tx));
        event_rxs.push(event_rx);
    }

    let manager = TnExExManager::new(canon_stream, exex_txs, event_rxs);
    node_task_spawner.spawn_critical_task("exex-manager", manager);
}
```

### User-facing API

```rust
// In the node binary or CLI setup:
let mut builder = TnBuilder { /* ... */ };

builder.install_exex("my-indexer", |ctx| Box::pin(async move {
    while let Some(notification) = ctx.notifications.recv().await {
        match notification {
            TnExExNotification::ChainCommitted { new } => {
                // Process committed blocks
            }
        }
        ctx.events.send(TnExExEvent::FinishedHeight(block_num))?;
    }
    Ok(())
}));
```

## Acceptance criteria

- [ ] `TnBuilder` has `install_exex()` method
- [ ] ExExes and manager are spawned on `node_task_manager` (survive epoch boundaries)
- [ ] Existing node startup is unaffected when no ExExes are installed
- [ ] ExEx tasks are spawned as critical tasks (node panics if ExEx crashes)
- [ ] Integration test: install a no-op ExEx via `TnBuilder`, verify it receives notifications
- [ ] `cargo check --workspace` passes

## References

- [reth ExEx registration](https://github.com/paradigmxyz/reth/blob/main/crates/node/builder/src/exex.rs) — `LaunchExEx` trait
- [reth ExEx launcher](https://github.com/paradigmxyz/reth/blob/main/crates/node/builder/src/launch/exex.rs) — wiring manager + ExExes
- `crates/node/src/manager.rs:261` — `node_task_manager` creation point
- `crates/node/src/engine/mod.rs:163` — `canonical_block_stream()` accessor

## Dependencies

- Issue 0 (core ExEx types)
- Issue 1 (ExEx manager)
