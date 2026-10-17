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
        # nothing from it.
        git archive HEAD -- "${paths[@]}" | tar -x -C "$tmp"
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
        # the registry crates; only the path crates recompile under the copy's path.
        CARGO_TARGET_DIR="$PWD/target" run cargo check --locked --offline \
            -p tn-worker-gateway --bin worker-gateway --manifest-path "$tmp/Cargo.toml"
    )
    echo "gateway image context resolves and compiles: ${paths[*]}"
}

lane_test_default() {
    run python3 -B -I -m unittest discover -s tools/network-budget -p 'test_*.py'
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
