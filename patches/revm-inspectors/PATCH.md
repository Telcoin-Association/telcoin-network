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

### A cap on recorded trace steps (TN-0079)

`TracingInspector` records a step for every opcode when steps are requested (the struct logger,
and `vmTrace`), copying the whole stack at each step unless the stack is disabled and memory when
it is enabled, and nothing counted steps or bytes; geth's `limit` option is ignored. Under a 50M
gas cap a call with a 1,000-word stack looping on cheap opcodes recorded tens of GB before the
response was built, and the node was OOM-killed (TN review R4). The inspector now counts the
bytes its steps hold (`TracingInspector::recorded_bytes`: the step, its stack snapshot, a fresh
memory copy, return data, immediate bytes and the values `step_end` records) and, past
`TracingInspectorConfig::max_recorded_bytes`, ends the transaction as the JS interrupt does: the
context error becomes `EVMError::Custom("trace too large: ...")` and the interpreter halts with
`FatalExternalError`. Every preset sets the cap to `DEFAULT_MAX_RECORDED_BYTES` (160 MiB, reth's
default `--rpc.max-response-size`; a trace whose steps alone pass it could not be returned
anyway), `merge` keeps the tighter of two caps, and `None` disables it. The counter is an
`Arc<AtomicUsize>` kept across `fuse` and shared between inspectors through
`share_recorded_bytes`, so reth bounds a request, not a transaction (see TN-0079 in
`vendor/reth/CHANGES.md` for the call sites). Tests are in `src/tracing/mod.rs` and
`src/tracing/config.rs`. Upstream has no equivalent as of 0.44.3.

The crate's `.gitignore` is removed, because it would hide the standalone `Cargo.lock` from git.
`upstream.diff` is the full difference from the registry archive (without
`.cargo_vcs_info.json` and `.cargo-ok`); regenerate it with
`git diff --no-index --full-index --no-color --no-ext-diff --no-prefix upstream patched` from a
directory holding the extracted archive as `upstream` and this directory as `patched`, without
`PATCH.md` and `upstream.diff`.

## Scope

revm-inspectors is used only for RPC tracing (`debug_*`, `trace_*`, `eth_simulateV1`'s
transfer tracing). Block execution never runs an inspector from it. The limit applies only to
JavaScript tracers, which TN builds only with the opt-in `js-tracer` feature
(`crates/tn-reth/js-tracer.md`). The default build compiles the patched crate without Boa, and
its code there is identical to the registry's.

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
`cargo test --features js-tracer --lib tracing::js::tests`.

## Retirement

The recursion limit retires when TN's reth moves to a revm-inspectors release that contains #548
(0.44.2 or later); check that release's `RECURSION_LIMIT`. The trace size cap has no upstream
equivalent, so the patch directory stays: on an upgrade, take the new registry source, re-apply
the cap (the `TN(TN-0079)` hunks of `upstream.diff`), and update the checksum, revision and
`upstream.diff` above. Should upstream grow a cap of its own, drop TN's and, if nothing else is
carried, delete:
- this directory, its `exclude` entry and its `[patch.crates-io]` entry in the root `Cargo.toml`;
- the `[patch.crates-io]` entry in `vendor/reth/.cargo/config.toml`;
- the `revm-inspectors-patch` job in `.github/workflows/pr.yaml` and its entries in the
  `CI Success` gate (`etc/transport-patches/test_ci_gate.py` tests the gate);
- its step in `.github/workflows/main-compiles.yaml`, and the patch-crate count in
  `.github/ACTIONS.md`.
