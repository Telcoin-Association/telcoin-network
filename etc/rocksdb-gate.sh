#!/bin/bash
#
# RocksDB guard: fail if RocksDB comes back anywhere in the build.
#
# librocksdb-sys is the entire RocksDB C++ engine (332 translation units) and the single
# largest native compile in the tree. TN does not use RocksDB in any configuration: the
# vendored reth (vendor/reth) has its RocksDB backend, its `rocksdb`/`edge` features and the
# `rocksdb` crate removed (vendor/reth/CHANGES.md TN-0014), and no TN crate has a `rocksdb`
# feature. This guard keeps it that way. It catches a dependency bump or an upstream reth sync
# (`vendor/reth/tn/vendor.py sync`) that brings it back, before the cost lands on every build.
#
# Two checks, both reading files only (no cargo invocation, nothing compiled):
#   1. Cargo.lock holds no rocksdb or librocksdb-sys package. The lockfile resolves every
#      feature, target and dev-dependency of the workspace, so this covers optional paths too.
#   2. No manifest under vendor/reth or the TN crates mentions rocksdb outside a comment.
#      That catches a re-added optional dependency or feature before anyone enables it.
#
# Fails closed: a missing Cargo.lock is an error, not a pass.
#
# Runs in the `test` job of .github/workflows/pr.yaml and in etc/test-and-attest.sh, so the
# queue enforces what the maintainer's attested run checked.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -f Cargo.lock ]]; then
    echo "rocksdb guard FAILED: Cargo.lock not found in $ROOT_DIR."
    exit 1
fi

failed=0

locked=$(grep -E '^name = "(rocksdb|librocksdb-sys)"$' Cargo.lock || true)
if [[ -n "$locked" ]]; then
    echo "rocksdb guard FAILED: Cargo.lock contains RocksDB packages:"
    echo "$locked"
    failed=1
fi

# Comment lines are skipped: manifests may explain that RocksDB is gone.
manifests=$(grep -rIilE --include=Cargo.toml '^[^#]*rocksdb' vendor/reth bin crates examples Cargo.toml || true)
if [[ -n "$manifests" ]]; then
    echo "rocksdb guard FAILED: these manifests mention rocksdb:"
    echo "$manifests"
    failed=1
fi

if [[ "$failed" -ne 0 ]]; then
    cat <<'EOF'

RocksDB was removed from TN and from the vendored reth on purpose (a very expensive native
build that TN does not use). Find what brought it back (`cargo tree -i rocksdb` for a
dependency, the manifest diff for a feature) and cut it. If an upstream reth sync re-added
it, drop it again in the vendored crate and note it in vendor/reth/CHANGES.md.
EOF
    exit 1
fi

echo "rocksdb guard passed: no rocksdb package in Cargo.lock and no rocksdb in any manifest"
