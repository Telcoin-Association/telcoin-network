# debug\_codeByHash

> [!NOTE]
> Served only when `debug` is named in `--http.api` or `--ws.api`; see [Enabling Namespaces](../enabling-namespaces.md).

Returns the contract bytecode whose Keccak-256 hash is the given code hash, the value [eth\_getProof](../eth/eth_getproof.md) reports as an account's `codeHash`.

#### Parameters

`DATA`, 32 Bytes - The code hash.

`block parameter`: `QUANTITY|TAG|DATA` - (optional) Hexadecimal block number, or one of the string tags `latest`, `earliest`, `safe`, or `finalized`; a 32-byte block hash is also accepted. See the [default block parameter](https://ethereum.org/en/developers/docs/apis/json-rpc/#default-block). Defaults to `latest`.

#### Returns

`DATA` - The bytecode, or `null` when the node has no code with that hash.

#### Example

#### Request

```
curl http://localhost:8545 \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"debug_codeByHash","params":["0xfe74fcea823036dfc874205a4198185eedae92b256d956cbf805c6c0dc2fd184","latest"],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": "0x60806040525f80546001600160a01b03169035632cf35bc960e11b01602657805f5260205ff35b365f80375f80365f845af490503d5f803e80603f573d5ffd5b503d5ff3fea164736f6c634300081a000a"
}
```

[source](https://reth.rs/jsonrpc/debug)
