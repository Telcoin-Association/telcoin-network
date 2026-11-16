#!/bin/bash
#
# Backend guard: fail if tn-reth's opt-in `mdbx` feature is on where it must be off.
#
# tn-reth runs the reth execution database on tn-lmdb unless its `mdbx` feature is on; the
# feature swaps the backend to MDBX and new datadirs to storage layout v1
# (crates/tn-reth/README.md "Database backend"). A crate that turns it on, through a dependency's
# `features = [...]`, a `default = [...]` or a forwarded feature, moves every build that crate is
# in onto MDBX, and every test still passes: the backend tests check "this build's engine". So
# this guard reads the resolved features instead:
#   1. The release graphs: each shipped binary's package alone (telcoin-network,
#      tn-worker-gateway, tn-node-record-api), normal and build dependencies, as the release
#      builds it: with its default features, then with each of its other features but `mdbx`
#      on its own. The node's releases and images build with one of those (`adiri`:
#      etc/release.sh and the docker-adiri image; `faucet`: the docker-devnet image), and an
#      operator may turn on `js-tracer`. The list is read from cargo (`cargo metadata`), so a
#      feature added later is checked with no edit here. A package that does not reach tn-reth
#      passes.
#   Both checks resolve every target (`--target all`), not only this host's: the release builds
#   linux/amd64, the ad-hoc images arm64 too, and a `[target.'cfg(...)'.dependencies]` entry that
#   enables the feature on one of them only must fail here on any host.
#   2. The workspace graph with dev-dependencies (`cargo tree --workspace`), as
#      `./etc/ci-lanes.sh test-default` builds it: a workspace member enabling `tn-reth/mdbx`
#      would unify it into that lane, which then never runs tn-lmdb.
# `./etc/ci-lanes.sh test-mdbx` turns the feature on from the command line, which this guard does
# not see.
#
# It runs `cargo metadata --locked --no-deps` and `cargo tree --locked` (resolve, compile
# nothing). Fails closed: a cargo error, or a release package or the workspace graph in which
# tn-reth cannot be found, fails it.
#
# etc/test_backend_gate.py runs it against planted workspaces. `./etc/ci-lanes.sh test-default`
# runs both, so the merge queue, main-compiles and the attested run check it.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# The packages of the shipped binaries, and whether each must reach tn-reth: a binary whose
# package stops reaching it would pass vacuously, so the one that runs a node must reach it.
RELEASE=(telcoin-network:yes tn-worker-gateway:no tn-node-record-api:no)

failed=0

# Every package of `cargo tree "$@"` on every target, one per line as "<package> <features>".
# Called only as a plain assignment's substitution at the top level, so a cargo failure stops the
# guard (set -e).
tree() {
    cargo tree --locked --target all "$@" -f '{p} {f}' --prefix none
}

# The features of workspace package "$1" other than `default` and `mdbx`, one per line. Called
# only as a plain assignment's substitution at the top level, like `tree`; a package missing
# from the workspace fails it.
package_features() {
    cargo metadata --locked --no-deps --format-version 1 | python3 -B -I -c '
import json, sys
meta = json.load(sys.stdin)
members = set(meta["workspace_members"])
(features,) = [pkg["features"] for pkg in meta["packages"]
               if pkg["id"] in members and pkg["name"] == sys.argv[1]]
print("\n".join(sorted(f for f in features if f not in ("default", "mdbx"))))' "$1"
}

# The tn-reth lines of a `tree` output: empty when the graph does not reach tn-reth.
tn_reth_lines() {
    grep -E '^tn-reth v' <<<"$1" | sed -E 's/ \(\*\)$//' | sort -u || true
}

# Fail when any of `lines` (the tn-reth lines of graph `what`) has the `mdbx` feature.
check() {
    local what=$1 lines=$2
    local features
    while read -r line; do
        [[ -n $line ]] || continue
        features=${line##* }
        if [[ ",$features," == *",mdbx,"* ]]; then
            echo "backend guard FAILED: $what enables tn-reth's mdbx feature: $line"
            failed=1
        fi
    done <<<"$lines"
}

for entry in "${RELEASE[@]}"; do
    package=${entry%%:*}
    out=$(tree -e normal,build -p "$package")
    lines=$(tn_reth_lines "$out")
    if [[ -z $lines && ${entry##*:} == yes ]]; then
        echo "backend guard FAILED: $package does not reach tn-reth; update RELEASE in $0."
        failed=1
    fi
    check "the release graph of $package" "$lines"
    features=$(package_features "$package")
    for feature in $features; do
        out=$(tree -e normal,build -p "$package" --features "$feature")
        check "the release graph of $package --features $feature" "$(tn_reth_lines "$out")"
    done
done

out=$(tree --workspace)
lines=$(tn_reth_lines "$out")
if [[ -z $lines ]]; then
    echo "backend guard FAILED: the workspace graph does not reach tn-reth."
    failed=1
fi
check "the workspace graph (test-default)" "$lines"

if [[ "$failed" -ne 0 ]]; then
    cat <<'EOF'

tn-lmdb is the reth backend of every release and of the default test lane; MDBX is opt-in for
the test-mdbx lane and the migration e2e lanes' previous binary only. Find what turns the feature
on (`cargo tree -e features -i tn-reth`) and drop it; to run MDBX, pass `--features tn-reth/mdbx`
(or `telcoin-network/mdbx`) on the command line instead.
EOF
    exit 1
fi

echo "backend guard passed: tn-reth's mdbx feature is off in the release graphs (each package's" \
    "features but mdbx, one at a time) and the workspace graph"
