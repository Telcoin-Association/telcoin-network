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

None in the crate directories. `Cargo.toml` here is new: it is upstream's root
manifest cut to the vendored members, `[workspace.package]`, the workspace lints,
and the `[workspace.dependencies]` entries that the vendored crates reference.
