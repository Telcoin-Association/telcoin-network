# tn\_nodeMode

Returns the node's current consensus participation mode: whether it is a committee member voting in consensus, a committee member catching up, or an observer that follows consensus output. The mode is read when the request arrives rather than fixed at startup like [tn\_info](tn_info.md), so a caller can see a transient mode such as `CvvInactive` while a restarted validator catches up.

#### Parameters

`None`

#### Returns

`String` - One of:

* `"CvvActive"` - A fully synced validator voting in the current committee.
* `"CvvInactive"` - A staked validator in the current committee that is following consensus output to catch up after a failure during the epoch. The mode is transient: the node returns to `CvvActive` once it has synced past the consensus garbage-collection window.
* `"Observer"` - A node that follows consensus output and is not in the current committee. It may or may not be staked.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"tn_nodeMode","params":[],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": "Observer"
}
```
