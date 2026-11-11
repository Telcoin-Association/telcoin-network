#!/bin/bash
#
# The lanes that gate a change into main, defined once.
#
# Two callers run these, and the point of this file is that they cannot drift apart:
#
#   etc/test-and-attest.sh     a maintainer runs every lane locally, plus the e2e suites,
#                              then attests the HEAD commit hash on adiri.
#   .github/workflows/pr.yaml  runs them on `pull_request` (for PRs no maintainer has
#                              attested) and on `merge_group` (for every PR, against
#                              main + the PR).
#
# A third caller, .github/workflows/cache-deps.yaml, compiles the lanes on main to warm the
# caches pr.yaml restores. It goes through this file so the artifacts it saves are the ones
# the queue's lanes need, flag for flag; `--build-only` below is its one concession, and it
# exists only because nextest has no other way to build a lane without running it.
#
# A fourth, .github/workflows/main-compiles.yaml, runs fmt, clippy and the test lanes with
# `--build-only` after every push to main that the merge queue did not make. A
# security-advisory merge reaches main without either pr.yaml event, so that workflow is
# what says such a commit still compiles. It reports after the fact and gates nothing.
#
# Before this file existed the two copies had already diverged: CI ran clippy only under
# --all-features and excluded a package that no longer exists, so "CI runs what the
# attested local run runs" was true only by inspection, and it was not true.
#
# The e2e suites are deliberately absent here. They run only in test-and-attest.sh, on a
# maintainer's machine, and are the reason the attestation exists -- GitHub-hosted runners
# cannot afford them. A lane added to this file lands in the merge queue too, so anything
# that takes hours does not belong in it.
#
# Usage: etc/ci-lanes.sh <fmt|clippy|test-default|test-adiri|test-mdbx|test-vendor-reth|all>
#        etc/ci-lanes.sh <test-default|test-adiri|test-mdbx|test-vendor-reth> --build-only
#
# `--build-only` compiles a test lane's binaries without running them. It is accepted for
# the four test lanes and nothing else, and it is the only option there is: no flag passes
# through to cargo, so a caller still cannot weaken a lane, only skip the run.
#
# NEXTEST_PROFILE selects a profile from .config/nextest.toml. CI sets it to `ci`
# (retries = 2), so a flaky test is less likely to eject a PR from the merge queue and
# force a rebuild of everything behind it. It is unset locally, giving profile.default and
# retries = 0: a flake on a maintainer's machine should be seen, not retried away before
# the attestation is written.

set -euo pipefail

# Everything below reads repo-relative paths (rust-nightly, Cargo.toml, .config/nextest.toml),
# so anchor at the workspace root and work from any cwd.
cd "$(dirname "$0")/.."

# Set, not defaulted. A caller cannot silently weaken a lane by exporting something
# different: these flags are part of what the attestation attests to.
export CARGO_INCREMENTAL=0
export RUSTFLAGS="-D warnings -D unused_extern_crates"
export CARGO_TERM_COLOR=always
export RUST_BACKTRACE=1
export CARGO_PROFILE_DEV_DEBUG=0
export CARGO_PROFILE_TEST_DEBUG=0

# fmt and clippy need nightly-only options (imports_granularity, wrap_comments); the
# compile and test lanes use the stable pin in rust-toolchain.toml. See
# .github/ACTIONS.md "Toolchain pins".
NIGHTLY=$(cat rust-nightly)

usage() {
    echo "usage: $0 <fmt|clippy|test-default|test-adiri|test-mdbx|test-vendor-reth|all>" >&2
    echo "       $0 <test-default|test-adiri|test-mdbx|test-vendor-reth> --build-only" >&2
    exit 2
}

run() {
    echo "+ $*"
    "$@"
}

# How the test lanes end: run every test and keep going past failures, or (`--build-only`)
# stop once the binaries exist. nextest rejects `--no-run` together with `--no-fail-fast`
# (nothing runs, so there is nothing to stop early), which is why this is a swap and not an
# extra flag.
run_mode=(--no-fail-fast)

lane_fmt() {
    # --all is explicit, not load-bearing today: the root manifest is virtual, so there is
    # no current package and bare `cargo fmt` already walks the members. It becomes
    # load-bearing the moment the root gains a [package] section.
    run cargo "+${NIGHTLY}" fmt --all -- --check
    echo "fmt passed"
}

lane_clippy() {
    # Three passes, because none compiles what the others do.
    #
    # 1. The release graph: the packages of the three shipped binaries, libraries and
    #    binaries only, under default features and with no dev-dependencies. `--workspace`
    #    cannot stand in for it. e2e-tests, tn-test-utils and tn-test-utils-committee are
    #    workspace members with normal dependencies on tn-types/test-utils and
    #    tn-reth/test-utils, so any workspace build unifies `test-utils` into tn-types,
    #    tn-reth, tn-config and telcoin-network-cli. The three packages alone turn no
    #    `test-utils` feature on (the only extras their union adds are reqwest/json and
    #    hyper-util/server-graceful), so this pass compiles each crate the way the release
    #    does, and it is the only compile of the `#[cfg(not(feature = "test-utils"))]` arms
    #    in tn-types. No lane compiled that graph before this pass (2026-10-29). It is also the
    #    shipped tn-lmdb graph: tn-lmdb is the default backend, so this pass is the one compile
    #    of the `#[cfg(not(feature = "test-utils"))]` arms of tn-reth's backend code.
    # 2. --workspace --all-targets --all-features: every lib, bin, test, bench and example
    #    target and every positive feature arm, so test code is compiled and linted too.
    #    When the lane built libraries only, test code that did not compile reached main
    #    (2026-10-29). crates/tn-reth's output_overlay bench needs `bench-internals`, and
    #    no lane compiled it before.
    # 3. The same, with every feature but `mdbx`. `--all-features` includes tn-reth's opt-in
    #    `mdbx` feature, which swaps the reth execution database to MDBX, so pass 2 lints the
    #    MDBX arms and never the default backend's (tn-lmdb's) test and test-utils code: the
    #    `#[cfg(not(feature = "mdbx"))]` test arms, the MDBX migration's tests among them.
    #    The release graph pass lints only their library code. `mdbx` adds no dependency, so
    #    this pass reuses pass 2's dependencies and re-lints only what depends on tn-reth.
    #
    # No pass lints code that compiles only under default features outside the release
    # graph, test code and its other `#[cfg(not(feature = ...))]` arms included.
    # lane_test_default compiles it under the same RUSTFLAGS wherever it runs (the merge
    # queue, and pull requests that are neither drafts nor a maintainer's), and the attested
    # run compiles it before a maintainer's PR can be queued. A pass with --all-targets under
    # default features compiled exactly the targets nextest compiles under the same feature
    # resolution, and was dropped for that reason.
    run cargo "+${NIGHTLY}" clippy --locked -p telcoin-network -p tn-worker-gateway -p tn-node-record-api -- -D warnings
    run cargo "+${NIGHTLY}" clippy --locked --workspace --all-targets --all-features -- -D warnings
    # A separate assignment, so a failed `cargo metadata` stops the lane (set -e) instead of
    # leaving an empty list, which would lint default features only.
    local features
    features=$(workspace_features_but_mdbx)
    [[ -n $features ]] || { echo "clippy: no workspace features read" >&2; exit 1; }
    run cargo "+${NIGHTLY}" clippy --locked --workspace --all-targets --features "$features" \
        -- -D warnings
    echo "clippy passed: release graph, workspace all targets with all features and with all but mdbx"
}

# Every feature of every workspace member except `default` and `mdbx`, package-qualified and
# comma-separated, from the manifests: clippy's third pass, `--all-features` without the opt-in
# MDBX backend. Read from cargo, so a feature added later joins the pass with no edit here.
workspace_features_but_mdbx() {
    cargo metadata --locked --no-deps --format-version 1 | python3 -B -I -c '
import json, sys
meta = json.load(sys.stdin)
members = set(meta["workspace_members"])
print(",".join(sorted(
    pkg["name"] + "/" + feature
    for pkg in meta["packages"] if pkg["id"] in members
    for feature in pkg["features"] if feature not in ("default", "mdbx")
)))'
}

# Rebuild the gateway image's build context from HEAD, resolve the workspace in it and check
# the gateway in it. bin/worker-gateway/Dockerfile copies only the paths it names
# (etc/Dockerfile copies everything), and cargo cannot load the workspace when an explicit
# member or a [patch.crates-io] path source is missing, even for `-p`. The metadata call
# resolves dependencies on purpose: `--no-deps` would hide a missing patch source. Takes an
# optional Dockerfile path so the check can be pointed at another copy of the file.
check_gateway_image_context() {
    local dockerfile=${1:-bin/worker-gateway/Dockerfile}
    local line word tmp
    local -a words paths=()
    # `COPY [--flag]... <src>... <dest>`: keep the sources. A `COPY --from=` line copies out
    # of another build stage, not out of the context, so it is skipped.
    while read -r line; do
        read -r -a words <<<"$line"
        [[ " ${words[*]} " == *" --from="* ]] && continue
        for word in "${words[@]:1:${#words[@]}-2}"; do
            [[ $word == --* ]] || paths+=("$word")
        done
    done < <(grep -E '^COPY[[:space:]]' "$dockerfile")
    if ((${#paths[@]} == 0)); then
        echo "check_gateway_image_context: no COPY sources in $dockerfile" >&2
        return 1
    fi
    tmp=$(mktemp -d)
    # A subshell, so its EXIT trap removes the copy when cargo fails as well as when it passes.
    (
        trap 'rm -rf -- "$tmp"' EXIT
        # The sources go to git quoted, so git, not the shell, expands a glob such as
        # `Cargo*`. The tn-contracts submodule archives as an empty directory, which is all
        # cargo metadata needs; the gateway's own dependencies (tn-types, tn-metrics) read
        # nothing from it. Without `-m`, tar would keep the commit time as every file's
        # mtime, and cargo, which keys a path crate's fingerprint on its path relative to
        # the workspace root, would then judge the copied path crates fresh against the
        # shared target dir whenever the checkout had been checked after the commit, so a
        # missing input would go unnoticed. `-m` stamps the copy with the extraction time,
        # so every path crate recompiles under the copy's path.
        git archive HEAD -- "${paths[@]}" | tar -xm -C "$tmp"
        # --offline needs every locked crate source in CARGO_HOME.
        run cargo fetch --locked
        # `run` traces to stdout, which this redirect would discard with the JSON, so
        # trace the call here: a failure then follows its own command line in the log.
        echo "+ cargo metadata --locked --offline --format-version 1 --manifest-path $tmp/Cargo.toml"
        cargo metadata --locked --offline --format-version 1 --manifest-path "$tmp/Cargo.toml" >/dev/null
        # Resolving the workspace is not enough: a compile-time input of the gateway's
        # dependencies, such as the chain-configs/ files tn-types reads with
        # include_str!, can be missing from the COPY list and still resolve, so check
        # the gateway in the copy too. This does not replace the standalone build in
        # lane_test_default: that build links the production binary with the
        # gateway's own feature set, and this check proves compile-time inputs such
        # as chain-configs/ are in the image's COPY set. The shared target dir reuses
        # the registry crates; the path crates recompile under the copy's path on every
        # run, because `tar -xm` above makes their sources newer than any output.
        CARGO_TARGET_DIR="$PWD/target" run cargo check --locked --offline \
            -p tn-worker-gateway --bin worker-gateway --manifest-path "$tmp/Cargo.toml"
    )
    echo "gateway image context resolves and compiles: ${paths[*]}"
}

lane_test_default() {
    run python3 -B -I -m unittest discover -s tools/network-budget -p 'test_*.py'
    # This lane is the tn-lmdb lane, and the release is the tn-lmdb build, only while no crate
    # turns tn-reth's `mdbx` feature on: every backend test checks "this build's engine", so a
    # crate that did would move both onto MDBX with every test green. etc/backend-gate.sh reads
    # the resolved features (compiles nothing); its planted-workspace tests run first.
    run python3 -B -I -m unittest discover -s etc -p 'test_backend_gate.py'
    run ./etc/backend-gate.sh
    # Keep this library-only: workspace tests unify dev-dependency features and can
    # hide missing dependency features in tn-types/test-utils.
    run cargo check --locked -p tn-types --features test-utils --lib
    # The gateway image copies only the paths its Dockerfile names, so a new workspace
    # member, patch source or compile-time input (such as chain-configs/) missing from
    # that COPY list fails here.
    check_gateway_image_context
    # The gateway ships as its own binary (bin/worker-gateway/Dockerfile builds it with
    # -p), but the workspace build below unifies features across every member, so a
    # feature the gateway needs and does not enable on its own dependencies still
    # compiles there, and neither `cargo check` nor the test build links the production
    # binary. Build it alone so a missing feature or a link failure fails here;
    # `check_gateway_image_context` above covers the image's COPY list.
    run cargo build --locked -p tn-worker-gateway --bin worker-gateway
    run cargo nextest run --locked --workspace "${run_mode[@]}"
    # New tn-lmdb datadirs are storage layout v2, so the run above gives every temp chain v2,
    # and layout v1 on tn-lmdb, which every testnet datadir migrated from MDBX keeps until it is
    # rebuilt, would run only in its explicit-layout tests. TN_TEST_NEW_DATADIR_LAYOUT=v1 gives
    # temp chains v1 instead (`TEST_NEW_DATADIR_LAYOUT_VAR` in tn-reth's env/genesis.rs). It is
    # test-only: tn-reth reads it only in test builds and only in its temp chain constructors,
    # never on the node's or `db load-state`'s path, and `the_default_build_gives_new_datadirs_v2`
    # checks the node's config still gives v2 under it. tn-reth's and tn-engine's suites hold
    # the commit, startup-heal, epoch and pinned-read tests that run on temp chains. The filter
    # keeps the workspace's feature resolution, so the binaries above are reused, not rebuilt.
    run env TN_TEST_NEW_DATADIR_LAYOUT=v1 cargo nextest run --locked --workspace \
        -E 'package(tn-reth) | package(tn-engine)' "${run_mode[@]}"
}

lane_test_adiri() {
    # The adiri-gated suites only compile under the adiri features, so lane_test_default
    # above never builds or runs them: the legacy header wire-format pins and the frozen
    # pre-fork committee vectors in tn-types, the consensus-registry fork tests and their
    # determinism oracles in tn-reth, the pre-fork entry-fee pin in tn-node, the frozen
    # pre-fork consensus-pack fixtures in tn-storage, and the pre-fork genesis rejection in
    # tn-config. tn-engine's integration suite compiles under either feature set, but its
    # hand-composed mix-hash oracle dispatches on the PREVRANDAO fork gate (#1247), and
    # only this lane runs the oracle's pre-fork legacy arm. Every crate that forwards
    # `adiri` and has adiri-gated tests belongs on this line: one left off silently stops
    # testing its pre-fork lane. With multiple -p flags cargo requires package-qualified
    # feature names.
    #
    # tn-storage used to be left off here, on the grounds that two of its consensus_pack
    # tests failed under the adiri features. They pass now, so it is back. tn-engine
    # joined for #1247: with the mix-hash oracle dispatching on the fork gate, this lane
    # is the only one that runs its pre-fork legacy arm.
    run cargo nextest run --locked \
        -p tn-types -p tn-reth -p tn-node -p tn-storage -p tn-engine -p tn-config \
        --features tn-types/adiri,tn-reth/adiri,tn-reth/test-utils,tn-node/adiri,tn-storage/adiri,tn-engine/adiri,tn-config/adiri \
        "${run_mode[@]}"
}

lane_test_mdbx() {
    # lane_test_default runs every reth database on tn-lmdb, the default backend, so it never
    # runs reth on MDBX. This is the same workspace run with tn-reth's opt-in `mdbx` feature,
    # the legacy backend kept until testnet has resynced: feature unification switches every
    # crate that opens a reth database (tn-reth, tn-node, the CLI and every test built on them)
    # to MDBX, so between the two lanes each of those tests runs once against each engine. The
    # feature changes only tn-reth's own code (it adds no dependency and changes no feature of
    # any other package), so a package with no dependency path to tn-reth, dev-dependencies
    # included (`cargo tree --workspace -i tn-reth -e normal,build,dev`), builds and tests the
    # same as in lane_test_default. The engine's and the adapter's suites are such packages and
    # are excluded. Every other package reaches tn-reth, if only through a dev-dependency, and
    # reruns here (tn-utils, with no path either, holds a single test).
    run cargo nextest run --locked --workspace --exclude tn-lmdb --exclude tn-lmdb-reth \
        --features tn-reth/mdbx "${run_mode[@]}"
    # The adiri-gated tn-reth suites on MDBX: the consensus-registry fork tests and their
    # determinism oracles read reth state through the database, and lane_test_adiri runs them
    # on tn-lmdb, the default backend, only. The other adiri packages have no database path of
    # their own.
    run cargo nextest run --locked -p tn-reth \
        --features tn-reth/adiri,tn-reth/test-utils,tn-reth/mdbx "${run_mode[@]}"
}

lane_test_vendor_reth() {
    # The vendoring tools (tn/vendor.py, tn/graph.py) and the RocksDB gate, against synthetic
    # repositories, workspaces and lockfiles.
    run python3 -B -I -m unittest discover -s vendor/reth/tn -p 'test_*.py'
    run python3 -B -I -m unittest discover -s etc -p 'test_rocksdb_gate.py'
    # reth's own unit and integration tests for the vendored crates (vendor/reth/README.md).
    # They resolve only inside the nested workspace, which tn/cargo.sh runs with its lockfile
    # seeded from TN's; reth's dev-dependencies add packages to it, so --locked cannot apply.
    # Neither the root workspace lanes above nor clippy reach these tests, and the regression
    # tests of every backport in vendor/reth/CHANGES.md live here. reth-db runs on its own with
    # two threads: at the default count macOS fails some MDBX opens with ENOMEM
    # (vendor/reth/README.md), and this lane also runs in etc/test-and-attest.sh.
    run vendor/reth/tn/cargo.sh nextest run --workspace --exclude reth-db "${run_mode[@]}"
    run vendor/reth/tn/cargo.sh nextest run -p reth-db --test-threads 2 "${run_mode[@]}"
    # The opt-in js-tracer build (crates/tn-reth/js-tracer.md): reth-rpc's JavaScript tracer
    # tests and tn-reth's suite with the feature on, which runs the feature's arm of
    # js_tracer_follows_the_build_feature. The clippy lane compiles these but runs nothing.
    run vendor/reth/tn/cargo.sh nextest run -p reth-rpc --features js-tracer "${run_mode[@]}"
    run cargo nextest run --locked -p tn-reth --features js-tracer "${run_mode[@]}"
}

(($# <= 2)) || usage
case "${2:-}" in
"") ;;
--build-only)
    case "${1:-}" in
    test-default | test-adiri | test-mdbx | test-vendor-reth) run_mode=(--no-run) ;;
    *) usage ;;
    esac
    ;;
*) usage ;;
esac

case "${1:-}" in
fmt) lane_fmt ;;
clippy) lane_clippy ;;
test-default) lane_test_default ;;
test-adiri) lane_test_adiri ;;
test-mdbx) lane_test_mdbx ;;
test-vendor-reth) lane_test_vendor_reth ;;
all)
    lane_fmt
    lane_clippy
    lane_test_default
    lane_test_adiri
    lane_test_mdbx
    lane_test_vendor_reth
    ;;
*) usage ;;
esac
