# trace\_filter

> [!NOTE]
> Served only when `trace` is named in `--http.api` or `--ws.api`; see [Enabling Namespaces](../enabling-namespaces.md).

Returns the Parity-style traces in a block range whose sender or receiver matches the filter, much as `eth_getLogs` does for logs. The node keeps no index of trace addresses: it re-executes every transaction in every block of the range and applies the address filter to the traces afterwards, so a narrow address filter does not make a request cheaper. A request holds two permits from the tracing permit pool and traces up to `--rpc.max-tracing-requests` blocks at a time.

`toBlock` minus `fromBlock` may be at most `--rpc.max-trace-filter-blocks` (default 100). A wider range fails with `-32602` and the message `Block range too large; currently limited to 100 blocks`; the message says 100 whatever the configured limit is. Because `fromBlock` defaults to `0x0` and `toBlock` to the latest block, a request that sets neither field fails this check once the latest block number is above 100.

A `fromBlock` or `toBlock` past the node's latest block fails with `-32001` and a `block not found` message, and a `fromBlock` greater than `toBlock` fails with `-32602` and the message `invalid parameters: fromBlock cannot be greater than toBlock`.

Traces cover the block's transactions only. The system calls a block runs before its transactions (EIP-4788 beacon root, EIP-2935 block hashes) and the epoch-closing system calls (`applyIncentives` and `concludeEpoch`) are not transactions, so no tracer shows them.

Telcoin Network has no block or uncle rewards, so these traces never include `reward` entries.

#### Parameters

`Object` - The filter object. A field not listed here fails the request with `-32602`.

* `fromBlock`: `QUANTITY` - (optional) The first block to trace, as a hexadecimal block number; tags such as `latest` are not accepted. Defaults to `0x0`.
* `toBlock`: `QUANTITY` - (optional) The last block to trace, inclusive, as a hexadecimal block number. Defaults to the latest block.
* `fromAddress`: `Array` - (optional) Sender addresses (`DATA`, 20 bytes each) to match: a call's `from`, a create's `from`, or the contract a `suicide` destroyed.
* `toAddress`: `Array` - (optional) Receiver addresses (`DATA`, 20 bytes each) to match: a call's `to`, the contract a create deployed, or a `suicide`'s `refundAddress`.
* `mode`: `String` - (optional) How the two address lists combine: `union` (the default) or `intersection`. With `union` a trace matches when its sender is in `fromAddress` or its receiver is in `toAddress`; when only one list is given only that list is checked, and with neither list every trace matches. With `intersection` a trace matches only when both its sender and its receiver match, and an empty or missing list matches any address.
* `after`: `Number` - (optional) How many matching traces to skip, as a JSON number rather than a hex string.
* `count`: `Number` - (optional) The most matching traces to return, as a JSON number rather than a hex string.

#### Returns

`Array` - The matching traces in block order. Each trace is an object:

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
 --data '{"jsonrpc":"2.0","method":"trace_filter","params":[{"fromBlock":"0x0","toBlock":"0x1"}],"id":1}'
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
