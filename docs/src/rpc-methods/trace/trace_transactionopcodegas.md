# trace\_transactionOpcodeGas

> [!NOTE]
> Served only when `trace` is named in `--http.api` or `--ws.api`; see [Enabling Namespaces](../enabling-namespaces.md).

Replays a mined transaction and returns, for each opcode it executed, how many times the opcode ran and the gas those executions used. The node re-executes the transaction's block up to and including the transaction. A plain transfer to an address without code runs no EVM instructions, so its list is empty.

Traces cover the block's transactions only. The system calls a block runs before its transactions (EIP-4788 beacon root, EIP-2935 block hashes) and the epoch-closing system calls (`applyIncentives` and `concludeEpoch`) are not transactions, so no tracer shows them.

#### Parameters

`DATA`, 32 Bytes - The transaction hash.

#### Returns

`Object` - The transaction's opcode usage, or `null` when the node does not know the transaction:

* `transactionHash`: `DATA`, 32 Bytes - Hash of the transaction.
* `opcodeGas`: `Array` - One entry per opcode the transaction executed, in no particular order:
    * `opcode`: `String` - The opcode name, such as `PUSH1` or `SSTORE`.
    * `count`: `Number` - How many times the opcode ran.
    * `gasUsed`: `Number` - The gas all those executions used together, as a JSON number.

#### Example

#### Request

```
curl http://localhost:8545 \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"trace_transactionOpcodeGas","params":["0xa794c7eb915c26fdb77242159053c1a36241bcd016b671fb07abe3fa9050805e"],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "transactionHash": "0xa794c7eb915c26fdb77242159053c1a36241bcd016b671fb07abe3fa9050805e",
    "opcodeGas": [] // a plain transfer executes no opcodes
  }
}
```

[source](https://reth.rs/jsonrpc/trace)
