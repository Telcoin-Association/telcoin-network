# eth

The `eth` namespace is the Ethereum-compatible core of the API: chain and block data, account state, transaction submission and receipts, call simulation, gas and fee quotes, and log filters. It is on by default and every node role should serve it; validators need it because observers forward `eth_sendRawTransaction` to a committee validator's advertised RPC endpoint.

Telcoin Network changes a few of these methods so they agree with how the network prices transactions. `eth_gasPrice`, `eth_maxPriorityFeePerGas`, the next-block entry of `eth_feeHistory`, and simulations at `latest` or `pending` (`eth_call`, `eth_estimateGas`, `eth_createAccessList`) use the worker's current epoch base fee rather than the latest header's, and `eth_sendRawTransaction` honours the operator's `--rpc.txfeecap`.

The methods with a page are listed in the sidebar, with the log filter methods under [Filter Methods](filter-methods/README.md).

## Served methods without a page yet

The node also serves these `eth` methods. They behave as in reth unless noted.

| Method | Notes |
| --- | --- |
| `eth_blobBaseFee` | |
| `eth_callBundle` | Shares the tracing request limit (`--rpc.max-tracing-requests`) |
| `eth_callMany` | |
| `eth_fillTransaction` | Missing fee fields are filled from the worker's epoch base fee |
| `eth_getAccount` | |
| `eth_getAccountInfo` | |
| `eth_getHeaderByHash` | |
| `eth_getHeaderByNumber` | |
| `eth_getRawTransactionByBlockHashAndIndex` | |
| `eth_getRawTransactionByBlockNumberAndIndex` | |
| `eth_getRawTransactionByHash` | |
| `eth_getTransactionBySenderAndNonce` | |
| `eth_sendRawTransactionSync` | Guarded by `--rpc.txfeecap` like `eth_sendRawTransaction` |
| `eth_simulateV1` | Block count capped by `--rpc.max-simulate-blocks` |
| `eth_subscribe`, `eth_unsubscribe` | WebSocket and IPC only |

These are registered but cannot do useful work on Telcoin Network:

| Method | Result |
| --- | --- |
| `eth_signTransaction`, `eth_signTypedData` | Error: the node holds no signing accounts (see [eth\_sign](eth_sign.md)) |
| `eth_coinbase`, `eth_mining`, `eth_getWork`, `eth_submitWork` | Error: `unimplemented` |
| `eth_hashrate` | Always `0x0` |
| `eth_submitHashrate` | Always `false` |
| `eth_getBlockAccessListByBlockHash`, `eth_getBlockAccessListByBlockNumber` | Error: `unimplemented` |
