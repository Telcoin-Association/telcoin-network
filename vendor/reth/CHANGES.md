# CHANGES: TN's changes to vendored reth

The ledger of every difference between `vendor/reth` and the pinned upstream
(`tn/upstream.toml`). `python3 -I vendor/reth/tn/vendor.py status` lists the files that differ;
each one has to be covered by an entry here. Changes to TN's own files (`tn/`, the four TN docs,
`.gitignore`) and to the filtered workspace manifest's member list are not listed.

Entry format:

- **TN-NNNN** — short title. *Kind*: build | test | fix | feature | removal | backport | sync.
  *Files*: what changed. *Why*: the reason, in a sentence or two. *Upstream*: worth offering
  upstream? (yes / no / n/a).

Use the id in code comments (`TN(TN-NNNN): ...`) where an edit would surprise a reader. Kept
newest last.

## Vendoring (base v1.11.3)

- **TN-0001** — Drop the `reth-e2e-test-utils` dev-dependency. *Kind*: test.
  *Files*: `crates/engine/tree/Cargo.toml` (dev-dependency, its `rocksdb` feature forward, the
  `e2e_testsuite` test target), `crates/ethereum/node/Cargo.toml` (dev-dependency); deleted
  `crates/engine/tree/tests/e2e-testsuite/`, `crates/ethereum/node/tests/e2e/`,
  `crates/ethereum/node/tests/assets/`.
  *Why*: `reth-e2e-test-utils` depends on `reth-cli-commands`, so carrying it would pull reth's
  whole CLI into the vendored set only for these integration tests. The crates' unit tests and
  `reth-node-ethereum`'s `tests/it` still run. See DECISIONS.md D5. *Upstream*: n/a.
  *Superseded*: both crates were removed in TN-0005, so nothing of this entry remains.

- **TN-0002** — Reformat two files with TN's pinned nightly rustfmt. *Kind*: build.
  *Files*: `crates/engine/tree/src/tree/payload_processor/mod.rs` (a `while let ... &&`
  chain), `crates/ethereum/primitives/src/receipt.rs` (import order).
  *Why*: CI runs `cargo +nightly fmt --all -- --check`, and `--all` covers path dependencies,
  so the vendored code is checked with TN's nightly (`rust-nightly`, 2026-03-20) under reth's
  `rustfmt.toml`. That nightly formats these two spots differently from the one reth used.
  When TN bumps its nightly, rerun `make fmt` and record any vendored reformatting the same
  way. *Upstream*: no. *Partly superseded*: engine-tree was removed in TN-0005; only the
  `receipt.rs` change remains.

- **TN-0003** — Fix a reth-trie unit test that does not compile upstream. *Kind*: test.
  *Files*: `crates/trie/trie/src/proof_v2/mod.rs` (`test_proof_calculator_reuse_after_error`).
  *Why*: upstream backported the test to the 1.11 branch (#22781) still naming `ProofV2Target`,
  the v2 name of the type that 1.11 calls `Target`, so `reth-trie`'s lib tests fail to build at
  v1.11.3 and v1.11.4. Found by building the nested workspace with `--all-targets`. *Upstream*:
  moot. The v1.11 line is finished and v2 has `ProofV2Target`.

## Reduction (after tag `reth-reduction-start`)

- **TN-0004** — Drop the engine-API tests that need `reth-node-ethereum`. *Kind*: test.
  *Files*: `crates/rpc/rpc-engine-api/src/engine_api.rs` (the `#[cfg(test)] mod tests`
  module), `crates/rpc/rpc-engine-api/Cargo.toml` (dev-dependency);
  `crates/rpc/rpc-builder/tests/it/utils.rs` (`launch_auth` and its imports),
  `crates/rpc/rpc-builder/tests/it/main.rs` (`mod auth`), deleted
  `crates/rpc/rpc-builder/tests/it/auth.rs`, `crates/rpc/rpc-builder/Cargo.toml`
  (dev-dependency).
  *Why*: these tests build reth's engine API around `reth-node-ethereum`'s
  `EthereumEngineValidator`, which needs `reth-ethereum-payload-builder`, so keeping them would
  keep the whole node-builder subgraph in the nested workspace. TN does not serve the engine
  API. rpc-builder's http, ws, startup, middleware and serde tests stay. *Upstream*: n/a.

- **TN-0005** — Remove 24 crates TN no longer depends on. *Kind*: removal.
  *Files*: removed from `tn/crates.txt` and the nested manifest with `vendor.py remove`:
  consensus/debug-client, engine/{invalid-block-hooks,service,tree,util}, era,
  era-downloader, era-utils, ethereum/{consensus,node,payload}, exex/{exex,types},
  net/downloaders, node/{builder,ethstats,events,metrics}, payload/validator, prune/prune,
  stages/{api,stages}, static-file/static-file, trie/parallel.
  *Why*: TN wires reth's components itself. Once `tn-reth` stopped importing through
  `reth-node-builder` and `reth-node-ethereum` (commit "tn-reth: stop depending on
  reth-node-builder and reth-node-ethereum"), nothing reaches these crates in any feature
  configuration. Features they used to enable that matter (the revm precompile backends,
  `asm-keccak`, `optional-balance-check`, chain-state `rayon`, trie-sparse `std`/`metrics`) are
  now set by `tn-reth` itself. *Upstream*: n/a.
