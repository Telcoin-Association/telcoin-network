# debug\_dbGet

> [!NOTE]
> Served only when `debug` is named in `--http.api` or `--ws.api`; see [Enabling Namespaces](../enabling-namespaces.md).

Only contract-code keys are supported: the key must be `0x63` followed by the 32-byte code hash; any other key returns an invalid-params error.

The method mirrors geth's raw database lookup for the one key type above. It reads the latest state and returns what [debug\_codeByHash](debug_codebyhash.md) returns for the same code hash.

#### Parameters

`String` - The key: `0x63` followed by the 32-byte code hash, 66 hex digits after `0x` in all. A key without the `0x` prefix is read as raw bytes, not as hex. A key that is not valid hex fails with error `-32602` `Invalid hex key`, a key of the wrong length with `-32602` `Key must be 33 bytes, got <length>`, and a key with another first byte with `-32602` `Key prefix must be 0x63`.

#### Returns

`DATA` - The bytecode, or `null` when the node has no code with that hash.

#### Example

#### Request

```
curl http://localhost:8545 \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"debug_dbGet","params":["0x63fe74fcea823036dfc874205a4198185eedae92b256d956cbf805c6c0dc2fd184"],"id":1}'
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
