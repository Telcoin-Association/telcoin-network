# trace\_block

> [!NOTE]
> Served only when `trace` is named in `--http.api` or `--ws.api`; see [Enabling Namespaces](../enabling-namespaces.md).

Returns the Parity-style traces of every transaction in a block, in block order, by re-executing the block. Each transaction contributes its call frames in the order [trace\_transaction](trace_transaction.md) lists them.

Traces cover the block's transactions only. The system calls a block runs before its transactions (EIP-4788 beacon root, EIP-2935 block hashes) and the epoch-closing system calls (`applyIncentives` and `concludeEpoch`) are not transactions, so no tracer shows them.

Telcoin Network has no block or uncle rewards, so these traces never include `reward` entries.

#### Parameters

`QUANTITY|TAG|DATA` - Hexadecimal block number, a 32-byte block hash, or one of the string tags `latest`, `earliest`, `safe` or `finalized`. See the [default block parameter](https://ethereum.org/en/developers/docs/apis/json-rpc/#default-block).

#### Returns

`Array` - The traces of the block's transactions, or `null` when the node does not know the block. A block without transactions, including the genesis block (`0x0`), returns `[]`. Each trace is an object:

* `action`: `Object` - What the frame did. For a `call`: `from` (`DATA`, 20 bytes, the caller), `callType` (`call`, `callcode`, `delegatecall` or `staticcall`), `gas` (`QUANTITY`, the gas available to the frame), `input` (`DATA`), `to` (`DATA`, 20 bytes) and `value` (`QUANTITY`, in wei). For a `create`: `from`, `gas`, `init` (`DATA`, the init code), `value` and `creationMethod` (`create` or `create2`). For a `suicide` (a `SELFDESTRUCT`): `address`, `balance` and `refundAddress`. The top-level frame's `gas` is the transaction's gas limit minus its intrinsic gas, so it is `0x0` for a plain transfer sent with a 21,000 gas limit.
* `blockHash`: `DATA`, 32 Bytes - Hash of the block that holds the transaction.
* `blockNumber`: `Number` - Number of that block, as a JSON number rather than a hex string.
* `error`: `String` - The error the frame failed with. Absent when the frame succeeded.
* `result`: `Object` - The frame's outcome. For a `call`: `gasUsed` (`QUANTITY`) and `output` (`DATA`). For a `create`: `address` (`DATA`, 20 bytes, the new contract), `code` (`DATA`, the deployed code) and `gasUsed`. `null` for a `suicide`, and can be `null` for a frame that failed. The top-level frame's `gasUsed` leaves out the intrinsic gas, so it is `0x0` for a plain transfer.
* `subtraces`: `Number` - How many frames this frame called directly.
* `traceAddress`: `Array` - The frame's path in the call tree, as JSON numbers: `[]` for the top-level frame, `[0]` for its first child, `[0, 1]` for that child's second child.
* `transactionHash`: `DATA`, 32 Bytes - Hash of the transaction.
* `transactionPosition`: `Number` - The transaction's index in the block, as a JSON number.
* `type`: `String` - The kind of frame: `call`, `create` or `suicide`.

#### Example

#### Request

```
curl http://localhost:8545 \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"trace_block","params":["0x1"],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": [
    {
      "action": {
        "from": "0xb14d3c4f5fbfbcfb98af2d330000d49c95b93aa7",
        "callType": "call",
        "gas": "0x0",
        "input": "0x",
        "to": "0x0000000000000000000000000000000000000000",
        "value": "0x64"
      },
      "blockHash": "0x1e0ed1376b6f8cc5deb9749a96c1207765ae9cd09ff46fe6200ca5b3f2913e8f",
      "blockNumber": 1,
      "result": {
        "gasUsed": "0x0",
        "output": "0x"
      },
      "subtraces": 0,
      "traceAddress": [],
      "transactionHash": "0xa794c7eb915c26fdb77242159053c1a36241bcd016b671fb07abe3fa9050805e",
      "transactionPosition": 0,
      "type": "call"
    }
  ]
}
```

[source](https://reth.rs/jsonrpc/trace)
