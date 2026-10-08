# tn\_epochRecord

Returns the epoch record for an epoch together with the certificate that signs it. Epoch records form the epoch chain: when an epoch ends, the record of that epoch fixes its committee, the committee chosen for the next epoch, and the last execution block and consensus header of the epoch, and the outgoing committee signs it at the start of the next epoch. Syncing nodes use the chain to learn each epoch's committee without execution state; see [Canonical Updates](../../canonical-updates.md).

The record is read from the node's consensus database, not from contract state. [tn\_getEpochInfo](tn_getepochinfo.md) serves a different view of an epoch: the `ConsensusRegistry` contract's entry, which the contract keeps only for a short ring buffer of recent epochs. A record exists only once its epoch has ended, so the current epoch has none. The method returns the error `-32001` "Not Found." when the node has no record for the epoch, or has the record but not yet a certificate for it. [tn\_epochRecordByHash](tn_epochrecordbyhash.md) looks a record up by its digest instead.

#### Parameters

`Number` - The epoch number, as a JSON number. A hex string is rejected with `-32602` "Invalid params".

#### Returns

`Array` - A two-element array: the epoch record object, then the epoch certificate object. Field names are snake\_case, and consensus-layer keys, digests and signatures are base58 strings.

The epoch record:

* `epoch`: `Number` - The epoch the record is for.
* `committee`: `Array` - The BLS public keys of the epoch's committee, base58-encoded (96-byte compressed form).
* `next_committee`: `Array` - The BLS public keys of the committee chosen for the next epoch, in the same encoding.
* `parent_hash`: `String` - Digest of the previous epoch's record, 32 bytes, base58-encoded. All zero bytes (`11111111111111111111111111111111`) for epoch 0.
* `final_state`: `Object` - The last execution block of the epoch: `number` (`Number`) and `hash` (`DATA`, 32 Bytes).
* `final_consensus`: `Object` - The last consensus header of the epoch: `number` (`Number`) and `hash` (`String`, base58).

The epoch certificate:

* `epoch_hash`: `String` - The digest of the record above, 32 bytes, base58-encoded. This is the key [tn\_epochRecordByHash](tn_epochrecordbyhash.md) takes.
* `signature`: `String` - The aggregate BLS signature of the committee members that signed the record, base58-encoded.
* `signed_authorities`: `String` - Which members of `committee` signed, as a bitmap of their positions in that list: a serialized roaring bitmap, base58-encoded. A valid certificate carries signatures from more than two thirds of the committee.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"tn_epochRecord","params":[600],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": [
    {
      "epoch": 600,
      "committee": [
        "pDmpE29YEhr93MPVPCGkWx3BsCcow4oExmxv5viJz2GTjsaXwc3YwKxE1CFdVuTVSudsFDutFSVmtgF98Abs56JZPfGQs6GzbqXDFkGA1eZx2edkwfP2Q6eRLo8coLhvTNj",
        "24JaKrebALhvhXxsAabjd5ZK22k1W4zKwGepKD5pcobQwBkZyiruYyhRM3hkLap8Ssc4qUdcRUJSrs2389hUoV7G83HKYrmkz2pTzCJiQURV5vtkrh8gFRKBQWKFTWLWah9s",
        "26L3gmkBzxAxEwvVuDrxEmKf2XwYGnYj1EQSiZSX3PBAvRa6Z5tegfmDAGHTsBwdtjFuAd7ayQEemE1AUE5ajSSK8wn3uz1MY5vjbGdeh3RoUv1TKLfbNTTULq9qdtaSTHhL",
        "rZdhyVGJAtz7kyFtFARWVVcw5DB3o9E4GvPN6zuKfUhUBGFRvEK2PgEV44CJt9wu9GMyssFQBFXRknrenuhJixo9KCADrZzHdtcvAwAYDmcLc7Ugh9epGc5LRZC5TK2hnYA",
        "26L1XwWTr6bBNnxJmDJXJgTqMAyD4ExkqwPYYJqDD3gob3jzejypQoewqRmqb9D2Yudk7zjcPqaTzFcEtXhrYXUtNx2ZYeLKC8V77LL2J136Hvw8eCGXpadGAbymThMhWYAa"
      ],
      "next_committee": [
        "pDmpE29YEhr93MPVPCGkWx3BsCcow4oExmxv5viJz2GTjsaXwc3YwKxE1CFdVuTVSudsFDutFSVmtgF98Abs56JZPfGQs6GzbqXDFkGA1eZx2edkwfP2Q6eRLo8coLhvTNj",
        "24JaKrebALhvhXxsAabjd5ZK22k1W4zKwGepKD5pcobQwBkZyiruYyhRM3hkLap8Ssc4qUdcRUJSrs2389hUoV7G83HKYrmkz2pTzCJiQURV5vtkrh8gFRKBQWKFTWLWah9s",
        "26L3gmkBzxAxEwvVuDrxEmKf2XwYGnYj1EQSiZSX3PBAvRa6Z5tegfmDAGHTsBwdtjFuAd7ayQEemE1AUE5ajSSK8wn3uz1MY5vjbGdeh3RoUv1TKLfbNTTULq9qdtaSTHhL",
        "rZdhyVGJAtz7kyFtFARWVVcw5DB3o9E4GvPN6zuKfUhUBGFRvEK2PgEV44CJt9wu9GMyssFQBFXRknrenuhJixo9KCADrZzHdtcvAwAYDmcLc7Ugh9epGc5LRZC5TK2hnYA",
        "26L1XwWTr6bBNnxJmDJXJgTqMAyD4ExkqwPYYJqDD3gob3jzejypQoewqRmqb9D2Yudk7zjcPqaTzFcEtXhrYXUtNx2ZYeLKC8V77LL2J136Hvw8eCGXpadGAbymThMhWYAa"
      ],
      "parent_hash": "EAXPstRk52BS7ggYetfzxDsC5oei4NL6rAtKcji4acUc",
      "final_state": {
        "number": 500810,
        "hash": "0x785da075f575faf3701e75214315e1aa7806dcc434b009673a3e76f7d21eec1d"
      },
      "final_consensus": {
        "number": 7056884,
        "hash": "2HqGsiq25eKZ8qRLb9Scx29TuZWE1r9LYtjouM5hnTxU"
      }
    },
    {
      "epoch_hash": "GDEpTMo1S4W2M1NajbaA6Vbf6AUndzWXTvR9J7RhEf7m",
      "signature": "7MsdyrYbUGbUXaaC2BjjtgR4PnP3swUQAgLEasfosvs4KFeCtLTTZCUGraKeHXJE7e",
      "signed_authorities": "2nLqe24nocoseuuBygc8VW6FW3oXreb3pseX"
    }
  ]
}
```
