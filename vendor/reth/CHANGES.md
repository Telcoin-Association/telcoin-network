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
