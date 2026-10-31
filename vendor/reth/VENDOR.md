# Vendored reth

This directory holds a pruned copy of [paradigmxyz/reth](https://github.com/paradigmxyz/reth)
at tag `v1.11.3` (commit `d6324d63e27ef6b7c49cdc9b1977c1b808234c7b`). Upstream's
LICENSE-APACHE and LICENSE-MIT apply to every file under this directory.

## Layout

- `Cargo.toml` here is a nested workspace root. It carries upstream's `[workspace.package]`
  table, the kept crates as `members`, resolver 2, a `[workspace.lints.rust]` table that sets
  `warnings = "allow"`, and the subset of upstream's `[workspace.dependencies]` that a kept
  crate still inherits. It has no `[patch]` table and no profiles.
- The telcoin-network root workspace lists `vendor/reth` in `exclude`. The vendored crates are
  path dependencies of the root workspace, not members: `cargo test --workspace`,
  `cargo clippy --all` and `cargo fmt --all` do not visit them, and their dev-dependencies are
  not in `Cargo.lock`.
- Every crate keeps its upstream path under `crates/`, so an upstream diff applies as is.

## What was removed

The copy is pruned by four rules. Each rule is a function of the dependency graph that
telcoin-network resolves, so the result can be recomputed after any change to the root
workspace.

1. **Crates.** A reth crate is kept when a telcoin-network workspace member reaches it in
   `cargo metadata`, in the union of the default and `--all-features` graphs over all
   platforms. 101 of upstream's 113 crates are kept.
2. **Manifests.** In each kept crate: a feature is kept when some telcoin-network build enables
   it; an optional dependency is kept when some telcoin-network build enables it; every
   non-optional dependency is kept; `[dev-dependencies]`, `[[test]]`, `[[bench]]` and
   `[[example]]` tables are removed together with the `tests/`, `benches/` and `examples/`
   directories. The root `[workspace.dependencies]` keeps the keys a kept manifest still
   inherits.
3. **Feature items.** Inside a kept feature, `x?/f` items for a removed optional dependency `x`
   are removed, and `x/f` items for a dependency the manifest no longer declares are removed.
   Upstream lists dev-dependency features (for example `alloy-rlp/std`) in `[features]`, and
   cargo accepts them only while the dev-dependency exists.
4. **Sources.** A `.rs` file is kept when a chain of `mod` declarations reaches it from a crate
   root (`src/lib.rs`, `src/main.rs`, `build.rs`, `[[bin]]` paths, `src/bin/`) with every
   removed feature false and `cfg(test)` false. `#[path]`, `cfg_attr(.., path = ..)` and
   inline modules follow the Rust reference rules. `include!`, `include_str!` and
   `include_bytes!` targets are kept. Markdown files are removed unless a source file includes
   them. Build scripts, C sources (libmdbx) and other assets are kept.

A dangling `#[cfg(feature = "x")] mod x;` line whose file is gone is fine: the predicate is
false in every telcoin-network build, so rustc never looks for the file.

## RocksDB

The `rocksdb` feature and the `rocksdb` crate are gone. In `reth-provider`,
`providers/mod.rs` now loads `rocksdb_stub.rs` unconditionally, which is upstream's
non-`rocksdb` build, and `providers/rocksdb/` is removed. Every `#[cfg]`, `#[cfg_attr]` and
`cfg!` that named `feature = "rocksdb"` was evaluated with that feature false and every other
predicate unknown: a false predicate removed the attribute group and the item it decorated, a
true predicate removed the attribute, a mixed predicate was rewritten without the rocksdb term.
The stub keeps upstream's names (`RocksDBProvider`, `rocksdb_path` and the related
`StorageSettings` fields), so callers compile unchanged.

## Lints

Path dependencies get no `--cap-lints`, and `RUSTFLAGS` such as `-D warnings` apply to them.
Every crate root therefore starts with `#![allow(warnings, unused_extern_crates)]`
(`unused_extern_crates` is allow-by-default, so `allow(warnings)` does not cover a `-D` on
it). The workspace lint table also sets `warnings = "allow"` for the `[lints] workspace = true`
line in each manifest. Source attributes take precedence over command-line levels, so CI's
`-D warnings` does not fail on vendored code; that includes `unexpected_cfgs` warnings from
`cfg(feature = "x")` on a feature that rule 2 removed.

## Behaviour notes

- The build script of `reth-node-core` reads the git state of the enclosing repository through
  `vergen`. The version strings it embeds now describe the telcoin-network checkout.
- Inline `#[cfg(test)]` modules remain in the kept files. They never compile, because the
  vendored crates are not workspace members.
- `cfg` blocks for other removed features remain in the kept files. Only the `rocksdb`
  predicates were eliminated.

## Re-vendoring

1. `git clone --depth 1 --branch <tag> https://github.com/paradigmxyz/reth`.
2. Update the tag and commit in this file, and the version in `Cargo.toml` here.
3. Copy each kept crate directory over its counterpart, then apply the four rules above and
   the RocksDB elimination. `cargo metadata --locked` with and without `--all-features` must
   pass, and the resolved feature set and dependency edges of every package must equal those
   of the unpruned graph.
4. Add a crate when a telcoin-network member starts to reach it. Drop one when nothing reaches
   it.
