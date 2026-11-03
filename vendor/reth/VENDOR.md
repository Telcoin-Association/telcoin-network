# Vendored reth

This directory holds the reth crates that telcoin-network compiles, copied from
upstream instead of fetched as git dependencies.

| | |
|---|---|
| Upstream | https://github.com/paradigmxyz/reth |
| Tag | `v1.11.3` |
| Commit | `d6324d63e27ef6b7c49cdc9b1977c1b808234c7b` |
| Copied | 2026-11-02 |
| License | MIT OR Apache-2.0 (`LICENSE-MIT`, `LICENSE-APACHE`, copied from upstream) |

## Layout

Each crate keeps its upstream path, so `crates/storage/provider` here is
`crates/storage/provider` upstream. Only the crates in telcoin-network's resolved
dependency graph are here. `Cargo.toml` in this directory is a nested workspace
root: the vendored crates use `workspace = true` inheritance, so they need one.
The telcoin-network root manifest excludes `vendor/reth` from its own workspace
and takes the crates as path dependencies. `Cargo.lock` at the telcoin-network
root is the only lock file.

`rustfmt.toml` is upstream's. `cargo fmt --all` at the telcoin-network root also
formats path dependencies, and this file keeps the vendored code in upstream's
style.

## Verify the copy

```sh
git clone --branch v1.11.3 https://github.com/paradigmxyz/reth /tmp/reth-v1.11.3
git -C /tmp/reth-v1.11.3 rev-parse HEAD   # d6324d63e27ef6b7c49cdc9b1977c1b808234c7b
for m in $(find vendor/reth -name Cargo.toml ! -path vendor/reth/Cargo.toml); do
  d=${m#vendor/reth/}; d=${d%/Cargo.toml}
  diff -rq "/tmp/reth-v1.11.3/$d" "vendor/reth/$d"
done
```

Expected output: only `Only in /tmp/reth-v1.11.3/...` lines for files that the
local changes below remove, and for nested crate directories that are not
vendored.

## Local changes

`Cargo.toml` here is new: it is upstream's root manifest cut to the vendored
members, `[workspace.package]`, the workspace lints, and the
`[workspace.dependencies]` entries that the vendored crates reference.

Changes in the crate directories, in the order they were made:

1. Test-only parts removed. Telcoin-network never builds the vendored crates in
   test mode, so these parts do not change the build. They pointed at crates
   that are not vendored, and `cargo fmt --all` loads this workspace, so they
   had to go.
   - Every `[dev-dependencies]` and `[target.*.dev-dependencies]` table, every
     `[[test]]`, `[[bench]]` and `[[example]]` table, and the `tests/`,
     `benches/` and `examples/` directories.
   - Each `[features]` entry that named a dependency that was dev-only.
   - The optional `reth-testing-utils` dependency of `reth-downloaders` and
     `reth-stages`, and the `test-utils` feature entries that enabled it.
   - The `[workspace.dependencies]` entries that no vendored crate inherits now.
2. RocksDB removed. Telcoin-network does not use RocksDB, and the `rocksdb`
   features were always off in its build.
   - The `rocksdb` features, the optional `rocksdb` dependency of
     `reth-provider`, and the `rocksdb` workspace dependency. The `edge`
     features stay, but they do not enable `rocksdb` now.
   - The RocksDB backend, `crates/storage/provider/src/providers/rocksdb/`.
     `RocksDBProvider` is always the zero-size stub in
     `providers/rocksdb_stub.rs`.
   - All code under `#[cfg(... feature = "rocksdb")]`. The code under
     `#[cfg(not(... feature = "rocksdb"))]` is unconditional now. This is the
     code that the compiler used before, because the feature was off.
   - The `impl TransactionLookup` block in `reth-prune`, which became empty.

3. Crates that left the graph removed. Telcoin-network does not depend on
   reth-node-builder, reth-node-ethereum and reth-primitives. It takes the same
   items from the crates that define them. The crates below are not in the
   dependency graph of telcoin-network after this change, so their directories and
   their entries in `Cargo.toml` are removed: reth-consensus-debug-client,
   reth-downloaders, reth-engine-service, reth-engine-tree, reth-engine-util,
   reth-era, reth-era-downloader, reth-era-utils, reth-ethereum-consensus,
   reth-ethereum-payload-builder, reth-exex, reth-exex-types,
   reth-invalid-block-hooks, reth-node-builder, reth-node-ethereum,
   reth-node-ethstats, reth-node-events, reth-node-metrics, reth-payload-validator,
   reth-primitives, reth-prune, reth-stages, reth-stages-api, reth-static-file,
   reth-trie-parallel.
