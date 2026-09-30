#!/bin/bash
#
# RocksDB guard: fail if rocksdb enters the default feature graph.
#
# librocksdb-sys is the entire RocksDB C++ engine (332 translation units) and the single
# largest native compile in the tree. Storage here is MDBX-only, so the workspace manifest
# takes reth without its default features and leaves `rocksdb` off; see the note above the
# `reth` entry in Cargo.toml, including the warning that reth's `edge` feature implies it.
# This guard catches the bump that re-introduces it through some other default set: the
# cost would land on every build, and the cache warm on main would absorb it without a
# word.
#
# Structured check: count rocksdb packages in the feature-resolved metadata graph instead
# of scraping cargo's error wording. Resolves the graph only and compiles nothing; with the
# registry index already present (a restored cargo home in CI, any earlier build locally)
# it takes seconds.
#
# Fails closed: `set -euo pipefail` turns a cargo or jq failure into a failed assignment
# instead of a "zero hits" pass, and a missing jq is refused up front rather than reported
# as a clean graph.
#
# Runs in the `test` job of .github/workflows/pr.yaml and in etc/test-and-attest.sh, so the
# queue enforces what the maintainer's attested run checked.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if ! command -v jq > /dev/null; then
    echo "rocksdb guard FAILED: jq is required and was not found on PATH."
    exit 1
fi

hits=$(cargo metadata --locked --format-version 1 |
    jq '[.resolve.nodes[].id | select(test("[/#]rocksdb@"))] | length')

if [[ "$hits" -ne 0 ]]; then
    echo "rocksdb guard FAILED: librocksdb-sys re-entered the default dependency graph (${hits} rocksdb package(s) resolved)."
    echo
    cargo tree --locked -e features -i rocksdb || true
    echo
    cat <<'EOF'
rocksdb is gated behind an off-by-default feature so that no build in this workspace pays
for the RocksDB C++ engine. A dependency change has pulled it back into the default graph.
Find the edge in the tree above and cut it (turn the feature off on that dependency, or
take it without default features and re-enable the ones needed). The note above the `reth`
entry in Cargo.toml has the background.
EOF
    exit 1
fi

echo "rocksdb guard passed: rocksdb absent from the default feature graph"
