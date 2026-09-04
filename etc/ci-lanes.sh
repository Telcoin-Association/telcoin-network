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
# Before this file existed the two copies had already diverged: CI ran clippy only under
# --all-features and excluded a package that no longer exists, so "CI runs what the
# attested local run runs" was true only by inspection, and it was not true.
#
# The e2e suites are deliberately absent here. They run only in test-and-attest.sh, on a
# maintainer's machine, and are the reason the attestation exists -- GitHub-hosted runners
# cannot afford them. A lane added to this file lands in the merge queue too, so anything
# that takes hours does not belong in it.
#
# Usage: etc/ci-lanes.sh <fmt|clippy|test-default|test-adiri|all>
#        etc/ci-lanes.sh <test-default|test-adiri> --build-only
#
# `--build-only` compiles a test lane's binaries without running them. It is accepted for
# the two test lanes and nothing else, and it is the only option there is: no flag passes
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
    echo "usage: $0 <fmt|clippy|test-default|test-adiri|all>" >&2
    echo "       $0 <test-default|test-adiri> --build-only" >&2
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
    # Both feature sets, because neither lints what the other does: --all-features never
    # compiles a `#[cfg(not(feature = ...))]` arm, and the default graph never compiles the
    # gated code at all.
    run cargo "+${NIGHTLY}" clippy --locked --workspace -- -D warnings
    run cargo "+${NIGHTLY}" clippy --locked --workspace --all-features -- -D warnings
    echo "clippy for workspace: default and all features passed"
}

lane_test_default() {
    run cargo nextest run --locked --workspace "${run_mode[@]}"
}

lane_test_adiri() {
    # The adiri-gated suites only compile under the adiri features, so lane_test_default
    # above never builds or runs them: the legacy header wire-format pins and the frozen
    # pre-fork committee vectors in tn-types, the consensus-registry fork tests and their
    # determinism oracles in tn-reth, the pre-fork entry-fee pin in tn-node, the frozen
    # pre-fork consensus-pack fixtures in tn-storage, the pre-fork genesis rejection in
    # tn-config, and the fork-dispatching mix-hash oracle in tn-engine (its fixtures sit at
    # epoch 0, so only an adiri build exercises the pre-fork legacy XOR arm). Every crate
    # that forwards `adiri` and has adiri-gated tests belongs on this line: one left off
    # silently stops testing its pre-fork lane. With multiple -p flags cargo requires
    # package-qualified feature names.
    #
    # tn-storage used to be left off here, on the grounds that two of its consensus_pack
    # tests failed under the adiri features. They pass now, so it is back. tn-engine joined
    # later (#1310): its basefee-penalty suite hardcoded the post-fork oracle, and this lane
    # omitting the crate is what kept CI from seeing the adiri failure.
    run cargo nextest run --locked \
        -p tn-types -p tn-reth -p tn-node -p tn-storage -p tn-config -p tn-engine \
        --features tn-types/adiri,tn-reth/adiri,tn-reth/test-utils,tn-node/adiri,tn-storage/adiri,tn-config/adiri,tn-engine/adiri \
        "${run_mode[@]}"
}

(($# <= 2)) || usage
case "${2:-}" in
"") ;;
--build-only)
    case "${1:-}" in
    test-default | test-adiri) run_mode=(--no-run) ;;
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
all)
    lane_fmt
    lane_clippy
    lane_test_default
    lane_test_adiri
    ;;
*) usage ;;
esac
