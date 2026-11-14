# Carried patch: revm-inspectors JavaScript recursion limit and trace size cap

This is the published revm-inspectors 0.34.3 source, including both upstream licenses. Its
registry archive SHA-256 is
`e341d9777b1903a8428bc6f8fe7a8f80671d2249837c9749566e61328ef14125` (the checksum `Cargo.lock`
recorded before the patch). The upstream revision is `5c5a3fabc2ff2cf9341aa6904f7663ca5b2fa49f`.
The normalized registry `Cargo.toml`, `Cargo.toml.orig`, dependencies and features are unchanged,
so the lockfile entry keeps its version and dependency list and only loses its registry source.

## The changes

### The JavaScript recursion limit

`RECURSION_LIMIT` in `src/tracing/js/mod.rs` goes from 10,000 to 128, the value upstream chose in
#548 (a5638ca, "Lower recursion limit to protect native stacks", released in 0.44.2). A recursive
getter or setter in a JavaScript tracer re-enters Boa natively, so every level also uses native
stack. At 10,000 the 2 MiB stack of a tokio blocking thread overflowed first: in a release build a
recursive getter aborted the node at about 1,700 levels, a setter at about 1,500. The abort
happened in the tracer's `setup`, before any EVM work, so neither the trace timeout nor TN-0017's
interrupt could stop it. Any `debug_trace*` call with a JavaScript tracer reached it on a node
built with `js-tracer` and serving `debug`.

Three tests are added to the JS tracer module: a recursive getter and a recursive setter, each
built on a thread with a 2 MiB stack, must fail with a `setup` error, and recursion below the
limit must still work. With the old limit both recursion tests abort the test process (SIGABRT,
stack overflow).

### A cap on what a trace records (TN-0079, TN-0153, TN-0172)

`TracingInspector` records a step for every opcode when steps are requested (the struct logger,
and `vmTrace`), copying the whole stack at each step unless the stack is disabled and memory when
it is enabled, and nothing counted steps or bytes; geth's `limit` option is ignored. Under a 50M
gas cap a call with a 1,000-word stack looping on cheap opcodes recorded tens of GB before the
response was built, and the node was OOM-killed. Whatever the preset, it also
keeps a trace of every call and create, with a fresh copy of the call's input, the init code and
the output, so a contract looping on `STATICCALL` with 16 KiB of calldata did the same through
`callTracer` and the parity `trace_*` methods, which record no steps.

The inspector now counts the bytes its trace holds (`TracingInspector::recorded_bytes`): each
call's and create's trace node with its input or init code and its output (TN-0153), and each
step with its stack snapshot, a fresh memory copy, return data, immediate bytes and the values
`step_end` records (TN-0079). The struct logger's result counts too, as it is built
(`TracingInspector::try_geth_traces`, TN-0172): each log's memory as hex strings, which it
repeats at every step even where the steps share one snapshot, and the copy of the contract's
storage it gets at an SLOAD or SSTORE. Past the cap that conversion stops with the same
`trace too large` error (`DebugInspectorError::TraceTooLarge`). The logs' other fields and their
stacks mirror the recorded steps, so such a trace holds at most about twice the cap at its
peak. Logs and storage changes are not counted; gas bounds them. Past
`TracingInspectorConfig::max_recorded_bytes` it ends the transaction as the JS interrupt does:
the context error becomes `EVMError::Custom("trace too large: ...")`. A step halts the
interpreter with `FatalExternalError`; a call or create that would pass the cap is not made, and
its hook returns a `FatalExternalError` outcome, which revm never hands to the calling frame
because it takes the context error first. Every preset sets the cap to
`DEFAULT_MAX_RECORDED_BYTES` (160 MiB, reth's default `--rpc.max-response-size`). The cap
counts what the trace holds, not its JSON. Calls' input and output, for the tracers that return
them, serialize in hex to twice what the trace holds, so such a trace could not be returned
anyway. Steps can hold more than their JSON (about 200 bytes against 60 to 100 for a struct log
with a small stack; for `vmTrace`, a copy of the whole memory after each write), so a
struct-logger or `vmTrace` trace can be refused although its response would fit, at about a
third of the cap or less. The struct logger and `prestateTracer`, which return no call input or
output, also refuse a transaction whose calls copy more than the cap. Raising
`--rpc.max-response-size` does not raise the cap. `merge` keeps the tighter of two caps, and `None` disables it. The counter is an
`Arc<AtomicUsize>` kept across `fuse` and shared between inspectors through
`share_recorded_bytes`, so reth bounds a request, not a transaction (see TN-0079 in
`vendor/reth/CHANGES.md` for the call sites). `DebugInspector::fuse` fuses the mux tracer's
inspector instead of rebuilding it, so the mux tracer keeps the count too (TN-0172). Tests are
in `src/tracing/mod.rs`, `src/tracing/mux.rs` and `src/tracing/config.rs`. Upstream has no equivalent as of 0.44.3.

The crate's `.gitignore` is removed, because it would hide the standalone `Cargo.lock` from git.
`upstream.diff` is the full difference from the registry archive (without
`.cargo_vcs_info.json` and `.cargo-ok`); regenerate it with
`git diff --no-index --full-index --no-color --no-ext-diff --no-prefix upstream patched` from a
directory holding the extracted archive as `upstream` and this directory as `patched`, without
`PATCH.md` and `upstream.diff`.

## Scope

revm-inspectors is used only for RPC tracing (`debug_*`, `trace_*`, `eth_simulateV1`'s
transfer tracing). Block execution never runs an inspector from it. The patch carries:

- the recursion limit (`src/tracing/js/mod.rs`), which reaches only JavaScript tracers, built
  only with the opt-in `js-tracer` feature (`crates/tn-reth/js-tracer.md`);
- the trace cap (TN-0079, TN-0153, TN-0172), which reaches every build and every
  `TracingInspector` tracer: the counter and the charges for steps and calls
  (`src/tracing/mod.rs`), the cap in every preset (`src/tracing/config.rs`), the charged
  struct-log conversion (`src/tracing/builder/geth.rs`, `TracingInspector::try_geth_traces`),
  `DebugInspector`'s use of it and its `TraceTooLarge` error, and the mux tracer's fuse that keeps
  the count (`src/tracing/{debug,mux}.rs`);
- the removed `.gitignore`.

The default build compiles the patched crate without Boa, so there it differs from the
registry's crate by the trace cap alone. Dropping the patch from any build drops the cap.

The nested reth workspace carries the same patch in `vendor/reth/.cargo/config.toml` (TN-0080),
which every cargo invocation there reads, the one cargo-nextest runs included, so reth's tests run
against the code TN ships. Until TN-0080 `tn/cargo.sh` passed the patch with `--config`, which
cargo does not forward to external subcommands, and the `test-vendor-reth` lane tested the
registry crate.

## Advisories

As a path package the patched crate is invisible to `cargo audit` and `cargo deny`, which match
advisories against registry packages. Check revm-inspectors advisories by hand until the patch
retires.

## Running its tests

CI's `revm-inspectors-patch` job runs, from the repository root:

```
cargo +1.94 test --locked --manifest-path patches/revm-inspectors/Cargo.toml --features js-tracer --lib
```

The crate is excluded from TN's workspace, so that works in a plain checkout. Inside a git
worktree nested in another checkout it does not: cargo walks up past the excluding workspace to
the outer one. There, copy the crate somewhere outside any workspace, drop the `snapbox`
dev-dependency (used only by upstream's integration tests, which the published crate does not
include) if it is not in the local registry, and run
`cargo test --features js-tracer --lib`, which runs the recursion-limit and the trace-cap tests.

## Retirement

The recursion limit retires when TN's reth moves to a revm-inspectors release that contains #548
(0.44.2 or later); check that release's `RECURSION_LIMIT`. The trace size cap has no upstream
equivalent, so the patch directory stays: on an upgrade, take the new registry source, re-apply
the cap (the `TN(TN-0079)`, `TN(TN-0153)` and `TN(TN-0172)` hunks of `upstream.diff`), and update the checksum,
revision and `upstream.diff` above. Should upstream grow a cap of its own, drop TN's and, if nothing else is
carried, delete:
- this directory, its `exclude` entry and its `[patch.crates-io]` entry in the root `Cargo.toml`;
- the `[patch.crates-io]` entry in `vendor/reth/.cargo/config.toml`;
- the `revm-inspectors-patch` job in `.github/workflows/pr.yaml` and its entries in the
  `CI Success` gate (`etc/transport-patches/test_ci_gate.py` tests the gate);
- its step in `.github/workflows/main-compiles.yaml`, and the patch-crate count in
  `.github/ACTIONS.md`.
