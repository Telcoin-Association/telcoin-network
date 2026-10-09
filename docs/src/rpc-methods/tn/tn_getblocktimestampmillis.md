# tn\_getBlockTimestampMillis

Returns the consensus commit time, in milliseconds, of an execution block. An execution block keeps a whole-second `timestamp`; the millisecond commit time lives in the consensus header the block was executed from, which the block references through its `parentBeaconBlockRoot`. This method pairs the two.

Returns `null` for a block this node does not know: an unknown number or hash, `safe` and `finalized` before the first block is finalized, and heights below a snapshot-restored node's restored header window. A known block whose consensus header is missing from local storage, for example because the node does not have that epoch's consensus pack, returns the error `-32001` "Not Found." rather than `null`. A known block whose epoch's consensus pack is on disk but cannot be read returns `-32603` "internal error"; the server then answers "internal error" for 30 seconds for any block of that epoch it has not already resolved, without reading the pack again, so a repaired pack is picked up within that time.

Validators should not expose the `tn` namespace publicly. A request for a block from a sealed epoch can open that epoch's consensus pack, and the storage layer opens packs synchronously into a small cache it shares with state sync and with serving epochs to peers. The lookup runs off the async runtime and under a tight concurrency bound, but public callers can still churn that cache until storage opens and evicts packs without blocking. Serve the namespace from non-validating nodes instead, and leave `tn` out of the validator's `--http.api` and `--ws.api`; see [Enabling Namespaces](../enabling-namespaces.md).

#### Parameters

`QUANTITY|TAG|DATA` - Hexadecimal block number, a 32-byte block hash, or one of the string tags `latest`, `earliest`, `pending`, `safe` or `finalized`, as in the [default block parameter](https://ethereum.org/en/developers/docs/apis/json-rpc/#default-block). `pending` resolves as `latest`.

#### Returns

`Object` - The block's timestamps, or `null` when the node does not know the block:

* `blockNumber`: `QUANTITY` - The execution block's number.
* `blockHash`: `DATA`, 32 Bytes - The execution block's hash.
* `timestamp`: `QUANTITY` - The execution block's `timestamp` in whole seconds, as the EVM reports it.
* `timestampMillis`: `QUANTITY` - The commit time of the block's consensus header, in milliseconds since the Unix epoch, read from the header's committed sub-DAG rather than computed from `timestamp`. For genesis, which has no consensus header, `timestamp` times 1000.
* `subSecond`: `Boolean` - Whether the consensus header's leader epoch commits with millisecond resolution. `false` for leader epochs before the sub-second timestamp fork, whose commit times are whole seconds (`timestampMillis` is then a multiple of 1000), and for genesis.
* `consensusNumber`: `QUANTITY` - The number of the consensus header the block was executed from. Absent for genesis.
* `consensusDigest`: `DATA`, 32 Bytes - The digest of that consensus header, which is the block's `parentBeaconBlockRoot`. Absent for genesis. It is hex here; consensus-layer methods such as [tn\_latestConsensusHeader](tn_latestconsensusheader.md) print digests in base58.

Normally `timestamp` equals `timestampMillis / 1000` rounded down. It does not when execution raised the block's `timestamp` to its parent's, which the node counts in the `evm_timestamp_clamped_total` metric: `timestampMillis` then stays the consensus commit time and is below `timestamp` times 1000. Execution raises a block's `timestamp` only from the sub-second timestamp fork on, and then only after a consensus regression or for an epoch-0 commit made while the validators' clocks lag the genesis timestamp.

`timestampMillis` does not decrease within an epoch, except from genesis to block 1 if the validators' clocks lagged the genesis timestamp at launch: genesis reports `timestamp` times 1000 and block 1 its earlier commit time. Blocks executed from one consensus output share a value. Across an epoch boundary only `timestamp` is guaranteed not to decrease: the blocks of an epoch's first commit can report up to 998 ms less than the previous epoch's last block, within the same whole second.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"tn_getBlockTimestampMillis","params":["0x7a44a"],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "blockNumber": "0x7a44a",
    "blockHash": "0x785da075f575faf3701e75214315e1aa7806dcc434b009673a3e76f7d21eec1d",
    "timestamp": "0x6ac5af2b",
    "timestampMillis": "0x1a114343ff8",
    "subSecond": false,
    "consensusNumber": "0x6badf4",
    "consensusDigest": "0x132b90286336db32aec099b223b4343155f25487e286a8d4185a5c681e71c141"
  }
}
```
