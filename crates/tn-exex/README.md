# TN ExEx - Telcoin Network Execution Extensions

A simplified execution extension system for Telcoin Network that enables real-time consumption of chain state transitions on observer and validator nodes.

## Overview

TN ExEx provides a clean API for building blockchain indexers, bridges, analytics engines, and other extensions that need to process every executed block as it's committed. Unlike traditional indexers that crawl historical data, ExExes receive blocks in real-time directly from the execution engine.

## Key Features

- **Real-time notifications**: Receive every committed block immediately after execution
- **No reorgs**: Bullshark consensus provides finality (no Write-Ahead Log or reorg handling needed)
- **Backpressure support**: ExExes can signal processing bottlenecks to prevent memory overflow
- **Observer-first design**: Run resource-intensive tasks on observer nodes without affecting consensus
- **Type-safe API**: Strongly-typed contexts, notifications, and events

## Architecture

```
Validator Committee          Observer Nodes
┌──────────────────┐         ┌──────────────────┐
│   Consensus      │────────▶│   State-Sync     │
│   + Execution    │         │   + Execution    │
│   (no ExExes)    │         │   + ExExes       │
└──────────────────┘         └──────────────────┘
                                      │
                                      ▼
                              ┌──────────────────┐
                              │  ExEx Tasks      │
                              │  - Indexers      │
                              │  - Bridges       │
                              │  - Analytics     │
                              └──────────────────┘
```

## Observer Nodes - Recommended Deployment

**Observer nodes are the primary and recommended deployment target for ExExes.**

### Why Observers?

1. **Consensus isolation**: ExEx performance issues don't affect network consensus
2. **Identical execution**: Observers execute blocks through the same `ExecutorEngine` path as validators
3. **Resource flexibility**: Run computationally expensive tasks without validator hardware constraints
4. **Dedicated infrastructure**: Separate scaling for application-layer tasks

### Observer Execution Path

Observers receive the exact same chain state transitions as validators:

```rust
// Validators:
Consensus → ExecutorEngine → finish_executing_output() → ExEx Manager → ExExes

// Observers (identical execution path):
State-Sync → ExecutorEngine → finish_executing_output() → ExEx Manager → ExExes
```

Both paths:
1. Execute blocks through `ExecutorEngine`
2. Commit canonical state to Reth database
3. Send `CanonStateNotification::Commit` to ExEx manager
4. Deliver `TnExExNotification::ChainCommitted` to registered ExExes

### Configuration

Launch an observer node:
```bash
telcoin-network node --observer --datadir /path/to/observer-data
```

The `--observer` flag ensures the node:
- Never participates in consensus committees
- Follows consensus output via state-sync
- Executes all blocks identically to validators
- Delivers ExEx notifications for every committed block

## Basic Usage

### Registering an ExEx

```rust
use tn_exex::{TnExExContext, TnExExEvent, TnExExLauncher, TnExExNotification};
use reth_provider::BlockchainProvider;

// Create launcher before node start
let mut launcher = TnExExLauncher::new();

// Install ExEx with unique ID
launcher.install("my-indexer", |mut ctx: TnExExContext<BlockchainProvider>| {
    Box::pin(async move {
        // Process notifications
        while let Some(notification) = ctx.notifications.recv().await {
            match notification {
                TnExExNotification::ChainCommitted { new } => {
                    // Access committed chain data
                    let tip = new.tip();
                    println!("New block: {} at height {}", tip.hash, tip.number);
                    
                    // Process transactions
                    for block in new.blocks() {
                        for tx in &block.body {
                            // Index transaction...
                        }
                    }
                    
                    // Signal processing complete (enables pruning)
                    ctx.events.send(TnExExEvent::FinishedHeight(tip.num_hash()))?;
                }
            }
        }
        Ok(())
    })
});

// Launch creates the manager (called by node startup code)
let (manager, handle) = launcher.launch(
    node_head,
    config,
    provider,
    task_spawner,
    None,
    None,
);
```

### Backpressure & Performance

ExExes run asynchronously with backpressure support:

```rust
// ExEx processes blocks slower than execution
while let Some(notification) = ctx.notifications.recv().await {
    // Slow processing (e.g., database writes, external API calls)
    heavy_indexing_work(&notification).await?;
    
    // Manager buffers up to 64 notifications by default
    // Engine won't block if ExEx falls behind
    
    ctx.events.send(TnExExEvent::FinishedHeight(tip.num_hash()))?;
}
```

**On validators**: Slow ExExes could delay epoch transitions (not recommended)
**On observers**: Slow ExExes only affect the observer node (recommended)

### Accessing Chain State

The `TnExExContext` provides a Reth `BlockchainProvider`:

```rust
use reth_provider::{BlockNumReader, StateProviderFactory};

// Read current block height
let current_height = ctx.provider.last_block_number()?;

// Access state at specific block
let state = ctx.provider.state_by_block_number(block_number)?;
let account = state.basic_account(address)?;
```

## Example: Transaction Indexer

```rust
launcher.install("tx-indexer", |mut ctx| {
    Box::pin(async move {
        let mut db = open_indexer_database()?;
        
        while let Some(notification) = ctx.notifications.recv().await {
            if let TnExExNotification::ChainCommitted { new } = notification {
                for block in new.blocks() {
                    for tx in &block.body {
                        // Store transaction with metadata
                        db.insert_transaction(IndexedTx {
                            hash: tx.hash(),
                            block_number: block.number,
                            from: tx.recover_signer()?,
                            to: tx.to(),
                            value: tx.value(),
                            timestamp: block.timestamp,
                        }).await?;
                    }
                }
                
                ctx.events.send(TnExExEvent::FinishedHeight(new.tip().num_hash()))?;
            }
        }
        Ok(())
    })
});
```

## Example: Bridge Relayer

```rust
launcher.install("bridge-relayer", |mut ctx| {
    Box::pin(async move {
        let relayer = BridgeRelayer::connect("https://other-chain-rpc").await?;
        
        while let Some(notification) = ctx.notifications.recv().await {
            if let TnExExNotification::ChainCommitted { new } = notification {
                // Look for bridge events in logs
                for block in new.blocks() {
                    let receipts = ctx.provider
                        .receipts_by_block(block.number.into())?
                        .unwrap_or_default();
                        
                    for receipt in receipts {
                        for log in &receipt.logs {
                            if log.address == BRIDGE_ADDRESS {
                                // Relay cross-chain message
                                relayer.relay_message(log).await?;
                            }
                        }
                    }
                }
                
                ctx.events.send(TnExExEvent::FinishedHeight(new.tip().num_hash()))?;
            }
        }
        Ok(())
    })
});
```

## Testing

ExExes can be tested with mock providers or with the integration tests in
`crates/tn-exex/tests/it/main.rs`:

```rust
#[cfg(test)]
mod tests {
    use tn_exex::{TnExExContext, TnExExNotification};
    use reth_provider::test_utils::MockEthProvider;
    
    #[tokio::test]
    async fn test_exex_processing() {
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
        
        let ctx = TnExExContext {
            head: BlockNumHash::default(),
            config: Config::default_for_test(),
            events: event_tx,
            notifications: rx,
            provider: MockEthProvider::default(),
            task_executor: TaskSpawner::default(),
        };
        
        // Send mock notification
        tx.send(TnExExNotification::ChainCommitted { 
            new: Arc::new(mock_chain()) 
        }).await?;
        
        // Verify ExEx processes it
        let event = event_rx.recv().await.unwrap();
        assert!(matches!(event, TnExExEvent::FinishedHeight(_)));
    }
}
```

## Performance Considerations

### On Validators
- **Minimize ExEx usage**: Validators should focus on consensus and execution
- **Use simple ExExes only**: Lightweight metrics or monitoring
- **Prefer observers**: Move heavy processing to dedicated observer nodes

### On Observers
- **Full ExEx capability**: Run complex indexing, bridges, analytics
- **Independent scaling**: Add more observer nodes for redundancy
- **Backpressure graceful**: Slow ExExes don't block consensus
- **State access**: Same Reth state database as validators

## Empty Handle Pattern

When no ExExes are installed, the system uses zero-overhead empty handles:

```rust
// In production code (no ExExes):
let handle = TnExExManagerHandle::empty();

// In tests (mock execution):
let handle = TnExExManagerHandle::empty();
engine.new(/* ... */, handle);
```

This ensures zero runtime overhead when ExExs aren't used.

## Integration with Node Lifecycle

ExEx manager lifecycle (in `EpochManager::run()`):

```rust
// 1. Query current chain head
let head = get_chain_head(&reth_env).await?;

// 2. Launch ExExs (if any installed)
let launcher = TnExExLauncher::new();
// ... install ExExs ...
let (manager, handle) = launcher.launch(head, provider, task_spawner);

// 3. Spawn manager as critical task (lives for node lifetime)
node_task_manager.spawn_critical("exex-manager", manager);

// 4. Thread handle through engine
let engine = ExecutorEngine::new(/* ... */, handle);
```

The ExEx manager:
- Persists across epoch transitions (like consensus_bus)
- Runs as a critical task (node exits if manager fails)
- Receives notifications from every block execution
- Manages backpressure and finished height tracking

## See Also

- [`TnExExContext`](src/context.rs) - ExEx execution context
- [`TnExExNotification`](src/notification.rs) - Chain state notification types
- [`TnExExEvent`](src/event.rs) - ExEx → Manager event types
- [`TnExExManager`](src/manager.rs) - Multi-ExEx coordination with backpressure
- [`TnExExLauncher`](src/launcher.rs) - ExEx registration and installation

## Future Extensions

Potential future enhancements:

- **EpochClosed notifications**: Signal epoch boundaries to ExExes
- **Custom RPC methods**: Allow ExExes to register RPC endpoints
- **Dynamic installation**: Hot-reload ExExes without node restart
- **ExEx-to-ExEx messaging**: Direct communication between ExExes
- **Metrics integration**: Standardized prometheus metrics for ExExes
