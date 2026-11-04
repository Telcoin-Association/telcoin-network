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
#   1. Cargo.lock holds no package whose name contains "rocksdb" (rocksdb, librocksdb-sys, and
#      renamed forks such as rust-librocksdb-sys). The lockfile resolves every feature, target
#      and dev-dependency of the workspace, so this covers optional and transitive paths too.
#   2. No manifest under vendor/reth, the TN crates or the carried patches/ crates mentions
#      rocksdb outside a comment. That catches a re-added optional dependency or feature
#      before anyone enables it.
#
# Fails closed: a missing Cargo.lock or scanned directory is an error, not a pass.
#
# etc/test_rocksdb_gate.py runs this script against planted lockfiles and manifests.
#
# Runs in the `test` job of .github/workflows/pr.yaml and in etc/test-and-attest.sh, so the
# queue enforces what the maintainer's attested run checked, and in the `build-tests` job
# of .github/workflows/main-compiles.yaml, which checks pushes the queue did not make.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -f Cargo.lock ]]; then
    echo "rocksdb guard FAILED: Cargo.lock not found in $ROOT_DIR."
    exit 1
fi

# Directories and files whose Cargo.toml files are scanned. patches/ holds the crates that the
# root [patch.crates-io] puts into the build.
SCANNED=(vendor/reth bin crates examples patches Cargo.toml)
for path in "${SCANNED[@]}"; do
    if [[ ! -e "$path" ]]; then
        echo "rocksdb guard FAILED: $path not found in $ROOT_DIR; update SCANNED in $0."
        exit 1
    fi
done

failed=0

locked=$(grep -iE '^name = "[^"]*rocksdb[^"]*"$' Cargo.lock || true)
if [[ -n "$locked" ]]; then
    echo "rocksdb guard FAILED: Cargo.lock contains RocksDB packages:"
    echo "$locked"
    failed=1
fi

# Comment lines are skipped: manifests may explain that RocksDB is gone.
manifests=$(grep -rIilE --include=Cargo.toml '^[^#]*rocksdb' "${SCANNED[@]}" || true)
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
