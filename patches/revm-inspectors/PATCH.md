# Carried patch: revm-inspectors JavaScript recursion limit

This is the published revm-inspectors 0.34.3 source, including both upstream licenses. Its
registry archive SHA-256 is
`e341d9777b1903a8428bc6f8fe7a8f80671d2249837c9749566e61328ef14125` (the checksum `Cargo.lock`
recorded before the patch). The upstream revision is `5c5a3fabc2ff2cf9341aa6904f7663ca5b2fa49f`.
The normalized registry `Cargo.toml`, `Cargo.toml.orig`, dependencies and features are unchanged,
so the lockfile entry keeps its version and dependency list and only loses its registry source.

## The change

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

The nested reth workspace has no `[patch]` of its own, so `vendor/reth/tn/cargo.sh` passes this
one with `--config`, and reth's tests run against the code TN ships.

## Running its tests

The crate is excluded from TN's workspace, and cargo cannot test it in place inside a git worktree
nested in another checkout (it walks up to the outer workspace). Copy it somewhere outside any
workspace, drop the `snapbox` dev-dependency (used only by upstream's integration tests, which the
published crate does not include) if it is not in the local registry, and run
`cargo test --features js-tracer --lib tracing::js::tests`.

## Retirement

Remove this patch when TN's reth moves to a revm-inspectors release that contains #548 (0.44.2
or later). Check that release's `RECURSION_LIMIT` before deleting the directory, the
`[patch.crates-io]` entry and the `--config` line in `vendor/reth/tn/cargo.sh`.
