# debug\_chainConfig

> [!NOTE]
> Served only when `debug` is named in `--http.api` or `--ws.api`; see [Enabling Namespaces](../enabling-namespaces.md).

Returns the chain configuration from the node's genesis: the chain id and the block numbers and timestamps at which each Ethereum fork activates.

#### Parameters

`None`

#### Returns

`Object` - The `config` section of the genesis. Every number is a JSON number, not a hex string, and a fork the genesis does not configure is omitted:

* `chainId`: `Number` - The chain id.
* `homesteadBlock`, `eip150Block`, `eip155Block`, `eip158Block`, `byzantiumBlock`, `constantinopleBlock`, `petersburgBlock`, `istanbulBlock`, `berlinBlock`, `londonBlock`: `Number` - The block at which each block-numbered fork activates.
* `shanghaiTime`, `cancunTime`, `pragueTime`: `Number` - The timestamp at which each time-based fork activates.
* `daoForkSupport`: `Boolean` - Always present; `false` on Telcoin Network.
* `terminalTotalDifficulty`: `Number` - The terminal total difficulty, when configured.
* `terminalTotalDifficultyPassed`: `Boolean` - Always present.

#### Example

#### Request

```
curl http://localhost:8545 \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"debug_chainConfig","params":[],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "chainId": 2017,
    "homesteadBlock": 0,
    "daoForkSupport": false,
    "eip150Block": 0,
    "eip155Block": 0,
    "eip158Block": 0,
    "byzantiumBlock": 0,
    "constantinopleBlock": 0,
    "petersburgBlock": 0,
    "istanbulBlock": 0,
    "berlinBlock": 0,
    "londonBlock": 0,
    "shanghaiTime": 0,
    "cancunTime": 0,
    "pragueTime": 0,
    "terminalTotalDifficulty": 0,
    "terminalTotalDifficultyPassed": true
  }
}
```

[source](https://reth.rs/jsonrpc/debug)
