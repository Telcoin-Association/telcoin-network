# rpc\_modules

Lists the namespaces the node serves. Each value is the fixed string `"1.0"`; it does not track any namespace's version.

This method answers only on a transport whose selection includes `rpc`. The default set (`eth`, `net`, `web3`, `rpc`, `tn`) includes it, so a node started without `--http.api` or `--ws.api`, or with `all`, serves it. An explicit list that leaves `rpc` out, such as `eth,net,web3`, does not, and the method then returns the `-32601` "Method not found" error. See [Enabling Namespaces](../enabling-namespaces.md).

The node builds the `rpc_modules` answer once, for the first transport whose selection includes `rpc`, in the order HTTP, WS, IPC, and every transport then returns that list. When HTTP and WS both serve `rpc` with different selections, the answer on WS and IPC is HTTP's list. Tracked in [#1588](https://github.com/Telcoin-Association/telcoin-network/issues/1588).

#### Parameters

`None`

#### Returns

`Object` - One entry per namespace: the key is the namespace name and the value is the string `"1.0"`.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"rpc_modules","params":[],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "eth": "1.0",
    "net": "1.0",
    "rpc": "1.0",
    "tn": "1.0",
    "web3": "1.0"
  }
}
```

[source](https://reth.rs/jsonrpc/rpc)
