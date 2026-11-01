# JavaScript tracers (`js-tracer`)

The `debug_trace*` methods can run a JavaScript tracer. Instead of naming a built-in tracer such as
`callTracer`, the caller sends the source of a JavaScript object, and the node calls its functions
(`step`, `enter`, `exit`, `fault`, `result`) while it re-executes the transaction. This is geth's
custom tracer API. reth implements it with the `JsInspector` from revm-inspectors, which embeds
boa, a JavaScript engine written in Rust.

TN builds without it. JavaScript tracers sit behind the opt-in `js-tracer` Cargo feature, defined
in [`crates/tn-reth/Cargo.toml`](Cargo.toml#L153) and forwarded by
[`tn-node`](../node/Cargo.toml#L61) and the
[`telcoin-network` binary](../../bin/telcoin-network/Cargo.toml#L28). Whether to remove the
feature entirely is waiting on a maintainer review.

## What a build without it does

`debug_traceTransaction`, `debug_traceCall`, `debug_traceCallMany`, `debug_traceBlock`,
`debug_traceBlockByHash` and `debug_traceBlockByNumber` refuse a JavaScript `tracer` with JSON-RPC
error `-32603` `JS Tracer is not enabled`.

The built-in tracers are native Rust and behave the same in both builds: the struct logger (no
`tracer`), `callTracer`, `flatCallTracer`, `prestateTracer`, `4byteTracer`, `muxTracer`,
`noopTracer` and `erc7562Tracer`. The parity-style `trace_*` methods never used JavaScript.
Consensus, execution and the transaction pool never reach the tracer code.

The `debug` namespace is off by default as well. A node serves it only when `debug` is named in
`--http.api` or `--ws.api` (see
[Enabling Namespaces](../../docs/src/rpc-methods/enabling-namespaces.md)). So the feature matters
only to an operator who serves `debug` to clients that send their own tracer scripts.

The test `js_tracer_follows_the_build_feature` in `src/env/rpc/tests/namespaces.rs` checks both
builds: run it with and without `--features js-tracer`.

## Why it is off

boa and its helpers are 35 of the 865 packages the node compiled before this change: the engine,
its parser, garbage collector, string interner, regex engine and their helpers. That is the largest
single cut left after the reth reduction. Without it the default build has 830 packages.

A JavaScript tracer is code the RPC caller supplies, and the node runs it on its blocking pool at
every EVM step of the traced transaction. What bounds it:

- boa's loop and recursion limits, which revm-inspectors sets to 200,000 iterations per loop and
  10,000 nested calls. They bound the work inside one callback, not the total across a trace.
- The request's `timeout`, 5 s when unset (geth's default), for `debug_traceTransaction` and
  `debug_traceCall` only. Before TN-0017 these ignored the timeout and kept running after the
  client disconnected, until the trace finished or the node ran out of memory (upstream reth
  #27543). Now an expired or abandoned trace stops at the next EVM step.
- The tracing permits (`--rpc.max-tracing-requests`), which `trace_*` and `eth_callBundle` share.

The block traces (`debug_traceBlock`, `debug_traceBlockByHash`, `debug_traceBlockByNumber`) and
`debug_traceCallMany` did not get that fix, upstream or here. They ignore `timeout`, and once a
trace starts it runs until every transaction in the block or bundle is traced, even if the client
has disconnected. The node checks for a disconnect only before the trace starts.

No limit applies to memory. A tracer can keep data across steps, so a long trace can collect a
lot of it, bounded only by the timeout where one applies. JavaScript tracers are also much slower
than the native ones, because the node calls into the interpreter on every EVM step.

The TN-0017 fix is carried code. Upstream fixed #27543 with an interrupt added in
revm-inspectors 0.44.2, which needs revm 43, and TN is on revm-inspectors 0.34 with revm 34. So
the vendored reth-rpc carries a port (`vendor/reth/crates/rpc/rpc/src/js_tracer_interrupt.rs` and
the `JsTraceInterrupt` code in `debug.rs`) until TN moves to that revm (Phase 4 in
[the vendored reth roadmap](../../vendor/reth/ROADMAP.md)).

## Turning it on

Build the node with the feature:

```sh
cargo build --release -p telcoin-network --features js-tracer
```

For the Docker image, add it to `CARGO_FEATURES`, comma-separated with any other features (for
example `adiri,js-tracer`):

```sh
docker buildx build -f ./etc/Dockerfile --build-arg CARGO_FEATURES=js-tracer \
  -t telcoin-network:js-tracer --load .
```

Then serve the `debug` namespace by naming it, for example `--http.api eth,net,web3,rpc,tn,debug`.
The node logs a warning when it starts. Serve `debug` only from a private archive or indexer node
behind authentication or a method allowlist, never from a validator or a public endpoint.

To check a node, trace any transaction with a small script:

```sh
curl -s localhost:8545 -H 'Content-Type: application/json' --data '{"jsonrpc":"2.0","id":1,
  "method":"debug_traceTransaction","params":["<transaction hash>",
  {"tracer":"{fault: function() {}, result: function(ctx) { return ctx.gasUsed }}",
   "timeout":"10s"}]}'
```

A node built with the feature returns the gas the transaction used. One built without it returns
error `-32603` `JS Tracer is not enabled`.

## Removing it entirely

If the team decides to drop JavaScript tracers for good:

1. Delete the `js-tracer` feature from the three manifests linked above and its mentions in the
   comments of the root `Cargo.toml` and `crates/tn-reth/Cargo.toml`, the `js-tracer` row of the
   [README](README.md#feature-flags) feature table, the test's `cfg(feature = "js-tracer")` branch
   (keep its refusal check), this file, and the note about it on the
   [`debug_traceTransaction`](../../docs/src/rpc-methods/debug/debug_tracetransaction.md) page.
2. In `vendor/reth`, as a ledgered change (`CHANGES.md`): delete `js_tracer_interrupt.rs`, the
   `js-tracer` code in `crates/rpc/rpc/src/debug.rs` and
   `crates/rpc/rpc-eth-types/src/error/mod.rs`, and the `js-tracer` features of reth-rpc,
   reth-rpc-eth-api and reth-rpc-eth-types. Mark the TN-0017 entry in
   `tn/notes/upstream-backports.md` as no longer applicable.
