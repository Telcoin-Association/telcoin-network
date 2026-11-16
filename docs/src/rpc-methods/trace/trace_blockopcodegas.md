# trace\_blockOpcodeGas

> [!NOTE]
> Served only when `trace` is named in `--http.api` or `--ws.api`; see [Enabling Namespaces](../enabling-namespaces.md).

Replays every transaction in a block and returns, for each transaction, how many times each opcode ran and the gas those executions used. Each transaction's entry has the shape [trace\_transactionOpcodeGas](trace_transactionopcodegas.md) returns.

Traces cover the block's transactions only. The system calls a block runs before its transactions (EIP-4788 beacon root, EIP-2935 block hashes) and the epoch-closing system calls (`applyIncentives` and `concludeEpoch`) are not transactions, so no tracer shows them.

#### Parameters

`QUANTITY|TAG|DATA` - Hexadecimal block number, a 32-byte block hash, or one of the string tags `latest`, `earliest`, `safe` or `finalized`. See the [default block parameter](https://ethereum.org/en/developers/docs/apis/json-rpc/#default-block).

#### Returns

`Object` - The block's opcode usage, or `null` when the node does not know the block:

* `blockHash`: `DATA`, 32 Bytes - Hash of the block.
* `blockNumber`: `Number` - Number of the block, as a JSON number rather than a hex string.
* `transactions`: `Array` - One entry per transaction, in execution order:
    * `transactionHash`: `DATA`, 32 Bytes - Hash of the transaction.
    * `opcodeGas`: `Array` - One entry per opcode the transaction executed, in no particular order, each with `opcode` (`String`, the opcode name), `count` (`Number`, how many times it ran) and `gasUsed` (`Number`, the gas those executions used together).

#### Example

#### Request

```
curl http://localhost:8545 \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"trace_blockOpcodeGas","params":["0x1"],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "blockHash": "0x1e0ed1376b6f8cc5deb9749a96c1207765ae9cd09ff46fe6200ca5b3f2913e8f",
    "blockNumber": 1,
    "transactions": [
      {
        "transactionHash": "0xa794c7eb915c26fdb77242159053c1a36241bcd016b671fb07abe3fa9050805e",
        "opcodeGas": [] // a plain transfer executes no opcodes
      }
    ]
  }
}
```

[source](https://reth.rs/jsonrpc/trace)
