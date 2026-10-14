# debug

The `debug` namespace serves raw RLP and EIP-2718 encodings of headers, blocks, transactions and receipts, geth-style EVM traces of blocks, transactions and simulated calls, execution witnesses, and a few chain-state helpers. It is off by default: a node serves it only when `debug` is named in `--http.api` or `--ws.api`, and `all` never includes it. The cost to serve it is high, because the tracing and witness methods re-execute historical blocks against archive state, and one request can take seconds of CPU time and return a large response. Serve it only from a private archive or indexer node behind authentication or a method allowlist, never from a validator or a public RPC endpoint; see [Enabling Namespaces](../enabling-namespaces.md).

`debug_traceBlock`, `debug_traceBlockByHash`, `debug_traceBlockByNumber`, `debug_traceTransaction`, `debug_traceCall`, `debug_traceCallMany`, `debug_executionWitness` and `debug_executionWitnessByBlockHash` each hold a permit from the node's tracing pool while they run. The pool size is `--rpc.max-tracing-requests`, and the same pool serves `trace_*` and `eth_callBundle`; a request that finds no free permit waits for one.

Traces cover the block's transactions only. The system calls a block runs before its transactions (EIP-4788 beacon root, EIP-2935 block hashes) and the epoch-closing system calls (`applyIncentives` and `concludeEpoch`) are not transactions, so no tracer shows them.

Example values are illustrative; field names and types match what the node returns.

| Method | Description |
| --- | --- |
| [debug\_chainConfig](debug_chainconfig.md) | The chain configuration from the node's genesis |
| [debug\_codeByHash](debug_codebyhash.md) | Contract bytecode for a code hash |
| [debug\_dbGet](debug_dbget.md) | Contract bytecode for a `0x63`-prefixed code-hash database key |
| [debug\_executionWitness](debug_executionwitness.md) | Execution witness of a block, by number or tag |
| [debug\_executionWitnessByBlockHash](debug_executionwitnessbyblockhash.md) | Execution witness of a block, by hash |
| [debug\_getBadBlocks](debug_getbadblocks.md) | Always `[]` on Telcoin Network |
| [debug\_getRawBlock](debug_getrawblock.md) | RLP-encoded block |
| [debug\_getRawHeader](debug_getrawheader.md) | RLP-encoded block header |
| [debug\_getRawReceipts](debug_getrawreceipts.md) | EIP-2718 encoded receipts of a block |
| [debug\_getRawTransaction](debug_getrawtransaction.md) | EIP-2718 encoded transaction, by hash |
| [debug\_getRawTransactions](debug_getrawtransactions.md) | EIP-2718 encoded transactions of a block |
| [debug\_stateRootWithUpdates](debug_staterootwithupdates.md) | State root and trie updates for a hashed state applied on top of a block |
| [debug\_traceBlock](debug_traceblock.md) | Trace every transaction of an RLP-encoded block |
| [debug\_traceBlockByHash](debug_traceblockbyhash.md) | Trace every transaction of a block, by hash |
| [debug\_traceBlockByNumber](debug_traceblockbynumber.md) | Trace every transaction of a block, by number or tag |
| [debug\_traceCall](debug_tracecall.md) | Trace a call simulated on a block's state |
| [debug\_traceCallMany](debug_tracecallmany.md) | Trace bundles of calls simulated on a block's state |
| [debug\_traceTransaction](debug_tracetransaction.md) | Trace one transaction |

## Not supported

The node registers more `debug_*` method names than the ones above, and none of the others does useful work on Telcoin Network. [debug\_getBadBlocks](debug_getbadblocks.md) is served but always returns `[]`, because the execution engine never reports a bad block (tracked in [#1590](https://github.com/Telcoin-Association/telcoin-network/issues/1590)).

### Methods that return an error

| Method | Response |
| --- | --- |
| `debug_getBlockAccessList` | Error `-32603` `unimplemented` |
| `debug_traceChain` | Error `-32603` `unimplemented` |

### Methods registered as no-ops

These 41 methods accept their parameters and return success without doing anything. Each returns `null`, except `debug_seedHash`, which returns the zero hash `0x0000000000000000000000000000000000000000000000000000000000000000`. A success response does not mean the action happened. `debug_setHead` in particular returns `null` and leaves the chain head where it was, so a script that calls it to rewind the node reports success while the node keeps its chain.

| Method | What geth does with it |
| --- | --- |
| `debug_accountRange` | Pages through the accounts at a block |
| `debug_backtraceAt` | Sets a logging backtrace location |
| `debug_blockProfile` | Writes a block profile to disk |
| `debug_chaindbCompact` | Compacts the key-value database |
| `debug_chaindbProperty` | Returns a property of the key-value database |
| `debug_cpuProfile` | Writes a CPU profile to disk |
| `debug_dbAncient` | Reads a blob from the ancient store |
| `debug_dbAncients` | Counts the items in the ancient store |
| `debug_dumpBlock` | Dumps the accounts at a block |
| `debug_freeOSMemory` | Forces garbage collection |
| `debug_freezeClient` | Freezes the client temporarily |
| `debug_gcStats` | Returns garbage-collection statistics |
| `debug_getAccessibleState` | Finds the first block with state on disk |
| `debug_getModifiedAccountsByHash` | Lists the accounts changed between two blocks, by hash |
| `debug_getModifiedAccountsByNumber` | Lists the accounts changed between two blocks, by number |
| `debug_goTrace` | Writes a Go runtime trace to disk |
| `debug_intermediateRoots` | Returns the state root after each transaction of a block |
| `debug_memStats` | Returns runtime memory statistics |
| `debug_mutexProfile` | Writes a mutex profile to disk |
| `debug_preimage` | Returns the preimage of a Keccak-256 hash |
| `debug_printBlock` | Pretty-prints a block |
| `debug_seedHash` | Returns the Ethash seed hash of a block (here always the zero hash) |
| `debug_setBlockProfileRate` | Sets the block profiling rate |
| `debug_setGCPercent` | Sets the garbage-collection target percentage |
| `debug_setHead` | Rewinds the local chain head to a block number |
| `debug_setMutexProfileFraction` | Sets the mutex profiling rate |
| `debug_setTrieFlushInterval` | Sets how often in-memory state tries are flushed to disk |
| `debug_stacks` | Prints the stacks of all goroutines |
| `debug_standardTraceBadBlockToFile` | Writes a trace of a bad block to a file |
| `debug_standardTraceBlockToFile` | Writes a trace of a block to a file |
| `debug_startCPUProfile` | Starts CPU profiling to a file |
| `debug_startGoTrace` | Starts a Go runtime trace to a file |
| `debug_stopCPUProfile` | Stops CPU profiling |
| `debug_stopGoTrace` | Stops the Go runtime trace |
| `debug_storageRangeAt` | Pages through a contract's storage at a transaction |
| `debug_traceBadBlock` | Traces a block from the bad-block list |
| `debug_verbosity` | Sets the log level |
| `debug_vmodule` | Sets a per-module log pattern |
| `debug_writeBlockProfile` | Writes a goroutine blocking profile to a file |
| `debug_writeMemProfile` | Writes an allocation profile to a file |
| `debug_writeMutexProfile` | Writes a mutex profile to a file |

### Not registered

| Method | Response |
| --- | --- |
| `debug_executePayload` | Error `-32601` "Method not found": reth declares it in a separate API that the node never registers |

Tracked in [#1591](https://github.com/Telcoin-Association/telcoin-network/issues/1591).
