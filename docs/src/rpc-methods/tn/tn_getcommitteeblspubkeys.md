# tn\_getCommitteeBlsPubkeys

Returns the BLS public keys for the committee of a given epoch, read from the `ConsensusRegistry` contract at the node's canonical tip. The keys are in the order the registry stores the committee, the same order [tn\_getCommitteeValidators](tn_getcommitteevalidators.md) returns, so the n-th key belongs to the n-th validator there.

The registry keeps committees only for a window around the current epoch: the three epochs before it, the current epoch, and the next two. An epoch outside that window reverts with the registry's `InvalidEpoch(uint32)` error.

#### Parameters

`Number` - The epoch number, as a JSON number. A hex string is rejected with error `-32602`.

#### Returns

`Array` of `DATA`, 96 bytes each - The committee members' compressed BLS12-381 public keys.

A revert is reported as `eth_call` reports one: error code `3`, a message that starts with `execution reverted`, and the ABI-encoded revert data in `data`.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"tn_getCommitteeBlsPubkeys","params":[601],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": [
    "0x89142f700382ba353262037ee9d72e43d56dfc6001618dee906b5dd2955c24955a0af1e16a65e064fb5a9602c3ba4846073fd2f422ae52f2e328a1b7d6e42a27cac5dc731ecaf3711ef2b727b226f0b29bad2fe890c453a6f3b23d5396b8483c",
    "0xb1f5f47e63566357362e2157f1b6860df78c08e98f7f5df8e0e2cf837cdea6420c0698dc123f187ed170acfe5c138bf11202b69c3ce2c4f5c2de7fa58d993931164fc3011118c62f0fb1b9cf723f1e55a410a9f77e8cd41576c9e4f8d0adf8ba",
    "0xb7d7220480a5df0d1d3d622426d667151a091b430f329546067dba93da38697f5307fd9729f79089fe1ae6179658e0230d00059cc2349f9308b8eaff0b79309cdd2f8ed0568340f504909d643c435bc2d98123e11c6e9a125547a78c69855463",
    "0x8fe0fa048858f10a610a75242de0532b1df341f7bdd11b28872ca0c128487e5422baa945926ebd3b75e7f350590288630a9c930031fdcd9b2ce76ac7833090378e05f8578a7ce437c6c0cf99496c83ef5ee1c1b5a5e580bd8c922070c2234e73",
    "0xb7d6a84c181082e78d93f8b203208e4e1b3a2b99dc66879bc86d73dcae4bc7a1162aac775b64d8c121c4318ffd2954590f81dd1f91583a3d1a3b260df8a1e08b4a6a56f6f1974901d0b5823c821214eeacce9b5780cae0856bc6a50a7599320f"
  ]
}
```
