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
# The nested lockfile is seeded on every run (it is git-ignored): TN's Cargo.lock, so the
# dependencies the two workspaces share resolve to the versions TN ships, plus tn/test-only.lock,
# which pins the packages only reth's tests need (tn/lockfile.py). Output goes to
# target/vendor-reth so the two workspaces' feature sets do not evict each other's artifacts.
#
# TN's root manifest patches revm-inspectors (patches/revm-inspectors). The nested manifest does
# not, so the same patch is passed with --config, and reth's tests run against the code TN ships.
#
# A package neither file pins is resolved against crates.io when this runs;
# `tn/lockfile.py check`, which the test-vendor-reth lane runs, fails on one.
# `incompatible-rust-versions = fallback` keeps such additions (and `lockfile.py update`) to
# versions that support the nested workspace's `rust-version`.

set -euo pipefail

VENDOR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="$(cd "$VENDOR/../.." && pwd)"

# tn/lockfile.py sets TN_LOCKFILE_SEEDED when it has written the lockfile itself.
[[ "${TN_LOCKFILE_SEEDED:-}" == 1 ]] || python3 -I -B "$VENDOR/tn/lockfile.py" seed
cd "$VENDOR"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}/vendor-reth"
config=(--config "patch.crates-io.revm-inspectors.path=\"$ROOT/patches/revm-inspectors\""
    --config 'resolver.incompatible-rust-versions="fallback"')
# a +toolchain argument has to stay first
if [[ "${1:-}" == +* ]]; then
    toolchain=$1
    shift
    exec cargo "$toolchain" "${config[@]}" "$@"
fi
exec cargo "${config[@]}" "$@"
