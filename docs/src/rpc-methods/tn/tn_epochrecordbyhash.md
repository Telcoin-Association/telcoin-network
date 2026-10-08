# tn\_epochRecordByHash

Returns the epoch record with the given digest together with the certificate that signs it. It answers like [tn\_epochRecord](tn_epochrecord.md), which looks the record up by epoch number; use this method to follow the epoch chain from a record's `parent_hash` or from a certificate's `epoch_hash`.

The record is read from the node's consensus database, not from contract state; [tn\_getEpochInfo](tn_getepochinfo.md) serves the `ConsensusRegistry` contract's entry for an epoch, which the contract keeps only for a short ring buffer of recent epochs. The method returns the error `-32001` "Not Found." when the node has no record with this digest, or has the record but not yet a certificate for it.

#### Parameters

`String` - The epoch record's digest, 32 bytes, base58-encoded, as it appears in a certificate's `epoch_hash` or the next record's `parent_hash`. A `0x` hex string is rejected with `-32602` "Invalid params".

#### Returns

`Array` - A two-element array: the epoch record object, then the epoch certificate object, with the fields described on [tn\_epochRecord](tn_epochrecord.md).

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"tn_epochRecordByHash","params":["GDEpTMo1S4W2M1NajbaA6Vbf6AUndzWXTvR9J7RhEf7m"],"id":1}'
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
        // ... four more BLS public keys
      ],
      "next_committee": [
        "pDmpE29YEhr93MPVPCGkWx3BsCcow4oExmxv5viJz2GTjsaXwc3YwKxE1CFdVuTVSudsFDutFSVmtgF98Abs56JZPfGQs6GzbqXDFkGA1eZx2edkwfP2Q6eRLo8coLhvTNj",
        // ... four more BLS public keys
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
