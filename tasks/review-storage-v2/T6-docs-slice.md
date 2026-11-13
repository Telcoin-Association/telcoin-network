# T6 slice: RPC docs vs code at HEAD 4acc752fb (saved by the coordinator; the slice agent
# reported after T6 was stopped by the usage limit). T6: fold these into T6.md.

## Mismatches (doc says X, code does Y)
- M1 `docs/src/rpc-methods/eth/filter-methods/README.md:7`: says too many filters / pool
  listeners fail with -32604; code uses `ErrorCode::ServerIsBusy` = -32009
  (`vendor/reth/crates/rpc/rpc/src/eth/filter.rs:1028-1032`, `eth/pubsub.rs:267-273`;
  jsonrpsee-types 0.26.0 `SERVER_IS_BUSY_CODE = -32009`). -32604 appears nowhere in code.
- M2 `docs/src/rpc-methods/eth/eth_syncing.md:9,11`: says nodes always return `false`; code
  returns a sync-progress object while the worker's sync flag is set
  (`crates/tn-reth/src/worker.rs:182-192, 241-245, 305-308`; driven by
  `crates/node/src/engine/inner.rs:242-251`; test `crates/tn-reth/src/env/rpc.rs:1251-1299`).
- M3 `docs/src/rpc-methods/debug/debug_tracecallmany.md:13`: says the replay path with a smaller
  `transactionIndex` does not apply the block's pre-transaction system calls; code does
  (`vendor/reth/crates/rpc/rpc/src/debug.rs:528-544` calls `apply_pre_execution_changes`).
- M4 `docs/src/rpc-methods/trace/trace_filter.md:6`: says a request traces up to
  `--rpc.max-tracing-requests` blocks at a time; code replays 2 at a time
  (`trace.rs:48 TRACE_FILTER_PERMITS = 2`, `:483-490 .buffered(2)`); `max_tracing_requests`
  only sizes the fetch chunk (`:439-443`). `trace/README.md:7` says two, correctly.
- M5 (partial) `docs/src/rpc-methods/enabling-namespaces.md:15`: on a shared http+ws port,
  workers >0 get cross-filled cors/origins lists (`crates/tn-reth/src/env/rpc.rs:206-219`), not
  literally "the same lists as worker 0"; without a shared port the claim holds.
- M6 (partial) `docs/src/rpc-methods/eth/README.md:16` (`eth_callBundle`): the transaction-count
  refusal is computed from the node's `--rpc.gascap` (`bundle.rs:81-87`), not from the
  request's `gasLimit`, which only bounds the gas budget (`:117-124, 194-196`).

## Verified as matching (summary)
Batch size 100 (-32010), filters 1,000 / 5 min TTL, listeners 1,024, max-tracing-requests
default and permits (held until the blocking task ends, shared by debug/trace/bundle),
trace_filter two permits and the 100-block limit (-32602), getLogs 100,000/20,000 with 0 = no
limit, gas cap 50M, callMany budget (2,380 calls max, -32602), response/request/connection
limits, 160 MiB trace cap (-32603 "trace too large"), proof permits, pending-block `none` and
the `pending`→latest mapping, txfeecap (-32000), tn blocking reads 64, simulate blocks 256,
feeHistory 1-1024, every listed error code and message for debug_*/trace_*/eth_* methods, the
41 no-op debug methods, JS tracer timeout/limits, namespace selection rules, IPC paths,
forwarding by sender, base-fee sources.

## Not traced
- `trace/README.md:15` intrinsic gas in callTracer vs Parity traces; `debug_executionwitness*`
  (#1589) and `debug_getbadblocks` (#1590) TN-engine behaviour; `trace_filter.md:22-23`
  address-matching semantics (alloy `TraceFilterMatcher`); the WS-only CORS path.
