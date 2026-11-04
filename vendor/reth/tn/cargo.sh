#!/usr/bin/env bash
#
# Run cargo inside the nested reth workspace (vendor/reth), e.g. to run reth's own tests for a
# vendored crate:
#
#   vendor/reth/tn/cargo.sh test -p reth-db --lib
#   vendor/reth/tn/cargo.sh nextest run -p reth-provider
#   vendor/reth/tn/cargo.sh check --workspace --all-targets
#
# TN's own build never needs this: the root workspace reaches the vendored crates through path
# dependencies. This is for reth's unit and integration tests, whose dev-dependencies only
# resolve inside the nested workspace.
#
# The nested lockfile is seeded from TN's Cargo.lock on every run (it is git-ignored), so the
# dependencies the two workspaces share resolve to the versions TN ships; cargo adds what only
# reth's tests need. Output goes to target/vendor-reth so the two workspaces' feature sets do
# not evict each other's artifacts.
#
# TN's root manifest patches revm-inspectors (patches/revm-inspectors). The nested manifest does
# not, so the same patch is passed with --config, and reth's tests run against the code TN ships.

set -euo pipefail

VENDOR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="$(cd "$VENDOR/../.." && pwd)"

cp "$ROOT/Cargo.lock" "$VENDOR/Cargo.lock"
cd "$VENDOR"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}/vendor-reth"
patch=(--config "patch.crates-io.revm-inspectors.path=\"$ROOT/patches/revm-inspectors\"")
# a +toolchain argument has to stay first
if [[ "${1:-}" == +* ]]; then
    toolchain=$1
    shift
    exec cargo "$toolchain" "${patch[@]}" "$@"
fi
exec cargo "${patch[@]}" "$@"
