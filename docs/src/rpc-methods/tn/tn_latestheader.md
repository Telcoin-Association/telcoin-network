# tn\_latestHeader

Deprecated alias of [tn\_latestConsensusHeader](tn_latestconsensusheader.md): it returns the same consensus header object, the newest one this node has seen. Use `tn_latestConsensusHeader` in new code.

#### Parameters

`None`

#### Returns

`Object` - The consensus header object, with the same fields as [tn\_latestConsensusHeader](tn_latestconsensusheader.md): `parent_hash`, `sub_dag`, `number` and `extra`.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"tn_latestHeader","params":[],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "parent_hash": "F7V4oK3SzU4Sh3TgjKtacJDqVvKG4xeBJoci5pWDM8cr",
    "sub_dag": { ... }, // as in tn_latestConsensusHeader
    "number": 7061244,
    "extra": "0x0000000000000000000000000000000000000000000000000000000000000000"
  }
}
```
