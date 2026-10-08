# debug\_getBadBlocks

> [!NOTE]
> Served only when `debug` is named in `--http.api` or `--ws.api`; see [Enabling Namespaces](../enabling-namespaces.md).

> [!WARNING]
> **Always empty on Telcoin Network**
>
> Always returns an empty array on Telcoin Network: reth fills this list from engine events that report invalid blocks, and the Telcoin Network execution engine does not emit those events. Tracked in [#1590](https://github.com/Telcoin-Association/telcoin-network/issues/1590).

#### Parameters

`None`

#### Returns

`Array` - The recent bad blocks the node has seen, newest first. On Telcoin Network this is always `[]`. In reth each entry is an object with `block` (the block as [eth\_getBlockByHash](../eth/eth_getblockbyhash.md) returns it with full transactions), `hash` and `rlp`.

#### Example

#### Request

```
curl http://localhost:8545 \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"debug_getBadBlocks","params":[],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": []
}
```

[source](https://geth.ethereum.org/docs/interacting-with-geth/rpc/ns-debug#debug_getbadblocks)
