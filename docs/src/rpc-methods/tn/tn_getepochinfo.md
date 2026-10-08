# tn\_getEpochInfo

Returns the `ConsensusRegistry` contract's entry for an epoch: its committee, issuance, starting block and duration, read from the contract state at this node's canonical tip, the newest block it has executed.

The contract keeps these entries only for a ring buffer of epochs around the current one: the three epochs before it, the current epoch, and the next two. A request for an epoch outside that window reverts with the registry's `InvalidEpoch(uint32)` error, reported as `eth_call` reports a revert: error code `3`, a message that starts with `execution reverted`, and the ABI-encoded revert data in `data`. An entry for an epoch that has not started yet reports `blockHeight` 0. For epochs that have ended, [tn\_epochRecord](tn_epochrecord.md) serves the consensus layer's epoch records from the node's database, signed by each epoch's committee and not limited to recent epochs.

#### Parameters

`Number` - The epoch number, as a JSON number. A hex string is rejected with `-32602` "Invalid params".

#### Returns

`Object` - The epoch info object, with the fields described on [tn\_getCurrentEpochInfo](tn_getcurrentepochinfo.md): `committee`, `epochIssuance`, `blockHeight`, `epochId`, `epochDuration` and `stakeVersion`.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"tn_getEpochInfo","params":[600],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "committee": [
      "0x0033a370616805b1fd275b7ffab83fc41d665ccb",
      "0x3518b301b86ceb53b5a3dff62e55cd43ef59d024",
      "0x7489025dfbaad94f2366d88a62989147d9c8b5d3",
      "0x89dab9f6fdc569c1bcdbd6493f25b7040b55dc79",
      "0xefaacf04b92298a88200aa50aa6bb7bfce587b17"
    ],
    "epochIssuance": "0x576f23131f3c2780000",
    "blockHeight": 500659,
    "epochId": 600,
    "epochDuration": 21600,
    "stakeVersion": 0
  }
}
```
