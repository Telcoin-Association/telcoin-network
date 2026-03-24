## Feedback on `sync_status` implementation

Thanks for the work on this — the structure of `SyncProgress` with both block-level and epoch-level tracking is a good direction. However, there's a fundamental issue with how `network_epoch` is derived that would cause `sync_status` to report incorrect results for syncing nodes.

### The problem

In `sync_status()`, you're using `current_committee` from the `ConsensusBus` as the source of truth for the network's epoch:

```rust
let network_epoch =
    self.consensus_bus.current_committee().borrow().as_ref().map(|c| c.epoch());
```

But `current_committee` is set in `EpochManager::create_committee_from_state()` (manager.rs:1366), which builds the committee from the **local node's execution state** (`epoch_state_from_canonical_tip()`). This means `network_epoch` actually reflects the **local node's current epoch**, not the network's.

Meanwhile, `local_epoch` comes from:

```rust
let local_epoch =
    self.db.last_record::<EpochRecords>().map(|(epoch, _)| epoch).unwrap_or(0);
```

This is the highest epoch record in the consensus DB. As described in SYNC.md, when a node joins the network, the `epoch_record_collector` downloads all available epoch records from peers *before* execution catches up. So `local_epoch` (from downloaded records) will often be **ahead** of the execution state.

This creates a situation where both values are local, and the comparison is inverted:

- `network_epoch` = the epoch the local execution is on (e.g., epoch 3)
- `local_epoch` = the highest epoch record downloaded from peers (e.g., epoch 10)
- `local_epoch >= network_epoch` → `10 >= 3` → `true`

Combined with `execution_synced` (which only compares block numbers within the current epoch's consensus), a node that is 7 epochs behind the network could report itself as `Synced`.

### What needs to change

The core issue is that there's no source for the **actual network epoch** available to the RPC layer. The `current_committee` watch channel reflects the local node's epoch, not the network's.

To get the true network epoch, consider:

1. **Use the downloaded epoch records as the network epoch indicator.** The epoch record collector downloads all available records from peers. The highest downloaded epoch record represents the last *completed* network epoch, so the current network epoch is approximately `last_downloaded_epoch_record + 1`. This is already in the DB via `last_record::<EpochRecords>()` — you just have the variable names swapped in meaning.

2. **Derive the local execution epoch from on-chain state**, not from downloaded records. The `current_committee` epoch (or the epoch extracted from the canonical tip) represents where the node has actually executed to.

So the logic should conceptually be:

```
network_epoch  = highest downloaded epoch record (from DB) + 1  (or similar)
local_epoch    = epoch from the node's execution state (current_committee epoch, or canonical tip)
```

And the synced condition becomes: the node's execution epoch has caught up to the network epoch, **and** execution within that epoch is caught up to the latest known consensus block.

3. **Consider adding a dedicated watch channel or field** that tracks the network's highest known epoch (updated by the epoch record collector as it downloads records from peers). This would give `sync_status` a clean separation between "where the network is" vs "where I am."

### The `current_committee` RPC method

The same concern applies to `tn_currentCommittee`. Right now it returns the local node's committee, which for a syncing node could be many epochs behind the actual network committee. This may be fine depending on the intended semantics — but it should be documented whether `tn_currentCommittee` means "the committee this node is currently participating in" vs "the network's current committee." If the latter, the data source needs to change.

### `current_epoch_info` has a similar issue

`current_epoch_info()` uses `self.db.last_record::<EpochRecords>()` which returns the highest *downloaded* epoch record. For a syncing node this could be far ahead of what the node has executed. This is arguably useful (it tells you what the network's latest completed epoch looks like), but the naming and semantics should be intentional and consistent with `sync_status`.

### Summary

The root issue is that `current_committee` reflects the local node's epoch (set from execution state), not the network's epoch. For a syncing node that has downloaded epoch records from peers but hasn't executed up to them yet, the sync comparison is inverted. The fix requires clearly separating "network's known epoch" (from downloaded epoch records) from "node's executed epoch" (from execution state / current committee), and comparing them in the right direction.
