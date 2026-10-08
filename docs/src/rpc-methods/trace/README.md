# trace

The `trace` namespace serves Parity-style traces, the format of the Parity client (later OpenEthereum) that Erigon and reth also serve. A trace is a flat list of the call frames a transaction ran, each with its position in the call tree, its input and value, and its gas and output. The namespace also replays transactions with a VM instruction trace or a state diff, simulates calls and signed transactions without sending them, and searches a block range for traces by sender and receiver with `trace_filter`.

`trace` is off by default: a node serves it only when `trace` is named in `--http.api` or `--ws.api`, and `all` does not include it. The cost to serve it is high. Every method executes transactions in the EVM, and most of them re-execute historical blocks against archive state: `trace_block`, `trace_replayBlockTransactions` and `trace_blockOpcodeGas` replay a whole block, and `trace_filter` replays every block in its range. Serve it only from a private archive or indexer node, behind authentication or a method allowlist, and not from validators or public RPC nodes. See [Enabling Namespaces](../enabling-namespaces.md) for the flags.

Every `trace_*` request holds a permit from the node's tracing permit pool while it runs, and waits for one when none is free. The pool's size is `--rpc.max-tracing-requests`, and the pool is shared by the `debug_trace*` methods, the `trace_*` methods and `eth_callBundle`. `trace_filter` holds two permits and traces up to `--rpc.max-tracing-requests` blocks at a time. `--rpc.max-trace-filter-blocks` (default 100) caps how far apart a `trace_filter` request's `fromBlock` and `toBlock` may be.

Traces cover the block's transactions only. The system calls a block runs before its transactions (EIP-4788 beacon root, EIP-2935 block hashes) and the epoch-closing system calls (`applyIncentives` and `concludeEpoch`) are not transactions, so no tracer shows them.

Telcoin Network has no block or uncle rewards, so these traces never include `reward` entries.

`trace_call`, `trace_callMany`, `trace_rawTransaction`, `trace_replayTransaction` and `trace_replayBlockTransactions` take a list of trace types and return each type requested: `trace` (the call frames), `vmTrace` (every executed instruction with its gas cost, stack pushes, and memory and storage writes) and `stateDiff` (the balance, nonce, code and storage changes of every account the transaction touched). A type that was not requested comes back as `[]` (`trace`) or `null` (`vmTrace`, `stateDiff`).

These traces describe the same execution as [debug\_traceTransaction](../debug/debug_tracetransaction.md) with the `callTracer`, in a different shape. The `callTracer` returns one nested call frame per transaction with an upper-case `type` such as `CALL`. A Parity-style trace is a flat array in which each frame carries a `traceAddress` (its path in the call tree), a `subtraces` count, a lower-case `type` and `callType`, and the block and transaction it belongs to; `blockNumber` and `transactionPosition` are JSON numbers, not hex strings. The two also count gas differently: the `callTracer` includes the transaction's intrinsic gas and Parity-style traces do not. For a plain transfer with a 21,000 gas limit, the `callTracer` reports `gas` and `gasUsed` `0x5208`, while `trace_transaction` reports `action.gas` `0x0` and `result.gasUsed` `0x0`.

Example values are illustrative; field names and types match what the node returns.

| Method | Description |
| --- | --- |
| [trace\_block](trace_block.md) | The traces of every transaction in a block |
| [trace\_blockOpcodeGas](trace_blockopcodegas.md) | How often each opcode ran, and the gas it used, in every transaction of a block |
| [trace\_call](trace_call.md) | Executes a call without creating a transaction and returns the requested traces |
| [trace\_callMany](trace_callmany.md) | Executes a sequence of calls, each on the state the previous ones left, and returns their traces |
| [trace\_filter](trace_filter.md) | The traces in a block range whose sender or receiver matches the filter |
| [trace\_get](trace_get.md) | One trace of a transaction, selected by its index |
| [trace\_rawTransaction](trace_rawtransaction.md) | Executes a signed raw transaction without sending it and returns the requested traces |
| [trace\_replayBlockTransactions](trace_replayblocktransactions.md) | Replays every transaction in a block and returns the requested traces |
| [trace\_replayTransaction](trace_replaytransaction.md) | Replays a transaction and returns the requested traces |
| [trace\_transaction](trace_transaction.md) | All traces of a transaction |
| [trace\_transactionOpcodeGas](trace_transactionopcodegas.md) | How often each opcode ran, and the gas it used, in a transaction |
