#!/bin/bash
#
# RocksDB guard: fail if RocksDB comes back into the workspace.
#
# librocksdb-sys is the entire RocksDB C++ engine (332 translation units) and the single
# largest native compile reth can bring in. Storage here is MDBX (or tndb-b), so the
# workspace has no `rocksdb` feature, and the vendored reth under vendor/reth has its RocksDB
# backend and `rocksdb` features removed (`RocksDBProvider` is the zero-size stub). See the
# note above the reth entries in Cargo.toml and vendor/reth/VENDOR.md.
#
# Two static checks. They resolve nothing and compile nothing:
# 1. Cargo.lock has no `rocksdb` or `librocksdb-sys` package. The lock is resolved with every
#    feature of every workspace member, so this covers all feature combinations.
# 2. No Cargo.toml in the repo (TN or vendored reth) names rocksdb outside a comment: no
#    feature, no dependency, no feature forward.
#
# Runs in the `test` job of .github/workflows/pr.yaml and in etc/test-and-attest.sh, so the
# queue enforces what the maintainer's attested run checked.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

lock_hits=$(awk '/^name = "(rocksdb|librocksdb-sys)"$/ { print "Cargo.lock:" FNR ": " $0 }' Cargo.lock)

manifest_hits=$(find . \( -path ./target -o -path ./.git \) -prune -o -name Cargo.toml -print0 |
    xargs -0 awk '{
        line = $0
        sub(/#.*/, "", line)
        if (line ~ /(^|[^A-Za-z0-9_-])(lib)?rocksdb([^A-Za-z0-9_]|$)/) print FILENAME ":" FNR ": " $0
    }')

if [[ -n "$lock_hits" || -n "$manifest_hits" ]]; then
    echo "rocksdb guard FAILED: rocksdb is back in the workspace."
    echo
    [[ -n "$lock_hits" ]] && echo "$lock_hits"
    [[ -n "$manifest_hits" ]] && echo "$manifest_hits"
    echo
    cat <<'EOF'
RocksDB is not part of this workspace: no build may pay for the RocksDB C++ engine.
Remove the feature or dependency named above. If it came from vendored reth, cut it there
and record the change in vendor/reth/VENDOR.md.
EOF
    exit 1
fi

echo "rocksdb guard passed: rocksdb absent from Cargo.lock and from every Cargo.toml"
