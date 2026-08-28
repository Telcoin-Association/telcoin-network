#!/bin/bash
#
# This script runs CI checks locally and submits a tx to adiri.
# for contract information:
# https://github.com/Telcoin-Association/tn-contracts/blob/master/src/CI/GitAttestationRegistry.sol
#
# This approach is adopted due to CI limitations using GitHub actions.

# Reset SECONDS to zero at the beginning of the script
SECONDS=0

set -e  # Exit immediately if a command exits with a non-zero status

# Navigate to the project root directory for workspace, .rustfmt.toml, .env, etc. This has
# to happen before anything reads a repo-relative path, so the script works from any cwd.
cd "$(dirname "$0")/.."

echo "executing bash script from $(pwd)"

# load environment variables
source .env

# Verify required variables are loaded
if [ -z "$GITHUB_ATTESTATION_PRIVATE_KEY" ]; then
    echo "Private key not set."
    exit 1
fi

# NOTE: this contract must match CI
CONTRACT_ADDRESS="0xf102928273a399cda6151b8616209af019499c84"
RPC_ENDPOINT="https://rpc.adiri.tel"
ATTEST_CALL="attestGitCommitHash(bytes20,bool)"
VERIFY_CALL="gitCommitHashAttested(bytes20)"
CHAIN_ID=2017
PRIVATE_KEY=${GITHUB_ATTESTATION_PRIVATE_KEY}
COMMIT_HASH=$(git rev-parse HEAD)
echo "attesting git hash: ${COMMIT_HASH}"

# Use cast to call the contract and return early if current HEAD attestation present
ALREADY_ATTESTED=$(cast call --rpc-url ${RPC_ENDPOINT} \
    ${CONTRACT_ADDRESS} "${VERIFY_CALL}" "${COMMIT_HASH}" )

# Check if the result is true (1) or false (0)
if [[ "${ALREADY_ATTESTED: -1}" == "1" ]]; then
    echo "Commit hash ${COMMIT_HASH} already attested on-chain."
    echo "Nothing to update."
    exit 0
fi

# Check if cargo-nextest is installed
if ! cargo nextest --version &> /dev/null; then
    echo "cargo-nextest is not installed."
    read -p "Would you like to install it? (y/Y to install): " response
    if [[ "$response" =~ ^[Yy]$ ]]; then
        cargo install cargo-nextest --locked
    else
        echo "cargo-nextest is required to run tests. Exiting."
        exit 1
    fi
fi

# set environment (exported so the cargo invocations below actually see them)
export CARGO_INCREMENTAL=0 # disable incremental compilation
export RUSTFLAGS="-D warnings -D unused_extern_crates"
export CARGO_TERM_COLOR=always
export RUST_BACKTRACE=1
export CARGO_PROFILE_DEV_DEBUG=0

# Fetch the status of the git repository and filter for lines that indicate modified tracked files
MODIFIED_TRACKED_FILES=$(git status --porcelain --untracked-files=no)

# Check the output - ignore untracked files
if [ -n "$MODIFIED_TRACKED_FILES" ]; then
    echo "Error: please commit changes before attesting HEAD commit hash."
    echo "$MODIFIED_TRACKED_FILES"
    exit 1
fi

# Refuse to attest content that is behind main.
#
# This matters more, not less, now that main uses a merge queue. The queue re-runs fmt,
# clippy and the two test lanes against `main + PR`, so a stale branch is caught there for
# everything CI can afford to run. What the queue can NOT re-run is the e2e suite below --
# it is the reason this script exists, and it only ever executes here, against whatever
# this branch happens to be based on. Attesting a branch that is behind main therefore
# produces the one thing the queue cannot backstop: an e2e result for a combination that
# is not the combination being merged.
#
# Merge or rebase onto main first. Override with ALLOW_STALE_BASE=1 when the drift is
# provably irrelevant (a docs-only main, say) and you want the hours back.
BASE_BRANCH="${ATTEST_BASE_BRANCH:-origin/main}"
git fetch --quiet origin || echo "warning: could not reach origin; ${BASE_BRANCH} may be stale"
if git rev-parse --verify --quiet "${BASE_BRANCH}" > /dev/null; then
    if ! git merge-base --is-ancestor "${BASE_BRANCH}" HEAD; then
        BEHIND=$(git rev-list --count "HEAD..${BASE_BRANCH}")
        echo "Error: HEAD is missing ${BEHIND} commit(s) from ${BASE_BRANCH}."
        echo "The e2e suites below only ever run here, so attesting now tests a combination"
        echo "that is not the one being merged. Merge or rebase onto ${BASE_BRANCH} first."
        echo "Set ALLOW_STALE_BASE=1 to attest anyway."
        [ "${ALLOW_STALE_BASE:-0}" = "1" ] || exit 1
        echo "ALLOW_STALE_BASE=1: attesting a stale base anyway."
    fi
else
    echo "warning: ${BASE_BRANCH} not found; skipping the stale-base check"
fi

# guard the archive-mode assumption every pinned consensus-registry read depends on (compiles
# nothing, so it runs first and fails fast)
./etc/archive-mode-guard.sh

# fmt, clippy and both unit-test lanes. These live in etc/ci-lanes.sh because
# .github/workflows/pr.yaml runs the very same file, on `pull_request` and again on
# `merge_group` against main + this PR. One definition, so the queue cannot end up
# enforcing something narrower than what gets attested here.
./etc/ci-lanes.sh fmt
./etc/ci-lanes.sh clippy
./etc/ci-lanes.sh test-default
./etc/ci-lanes.sh test-adiri

# The e2e suites: the part CI does not run, and the reason this script attests anything at
# all. They are split into two invocations to avoid any port/node confusion.
# Prebuild the node binary once into the shared target tree and hand it to the e2e tests via
# TN_BIN_PATH (mirroring `make test-e2e`), so the ignored suite reuses it instead of cold-building
# the binary inside the first test, which nextest capture would otherwise hide as a multi-minute hang.
# Target root for the e2e node-binary build and its consumer, honoring CARGO_TARGET_DIR: its
# value when set, cargo's default $(pwd)/target otherwise. A relative value is anchored here
# (the workspace root) so TN_BIN_PATH stays absolute; nextest runs each test from the package
# directory. The root is passed to the build as an explicit --target-dir and reused for
# TN_BIN_PATH, so the producing and the consuming path come from one expression and cannot
# diverge through any other target-dir channel.
E2E_TARGET_ROOT="${CARGO_TARGET_DIR:-$(pwd)/target}"
case "$E2E_TARGET_ROOT" in
    /*) ;;
    *) E2E_TARGET_ROOT="$(pwd)/$E2E_TARGET_ROOT" ;;
esac
cargo build --profile e2e --bin telcoin-network --features tn-storage/test-utils --target-dir "$E2E_TARGET_ROOT"
TN_BIN_PATH="$E2E_TARGET_ROOT/e2e/telcoin-network" \
    cargo nextest run -p e2e-tests --run-ignored ignored-only --all-features

echo "all checks passed - submitting attestation on-chain..."

#
# If we've reached this point, all checks have passed
#

# create and submit transaction
#
# Send the transaction using cast
output=$(cast send --private-key ${PRIVATE_KEY} \
    --rpc-url ${RPC_ENDPOINT} \
    --chain "2017" \
    --gas-limit 1000000 \
    ${CONTRACT_ADDRESS} \
    "${ATTEST_CALL}" "${COMMIT_HASH}" "true")

# Check if the cast command was successful
if [ $? -ne 0 ]; then
    echo "failed to submit tx"
    exit 1
fi

echo "\nTransaction output: ${output}\n"

# Extract transaction hash using awk
TX_HASH=$(echo "$output" | grep 'transactionHash' | grep -v 'logs' | awk '{print $NF}')

echo "https://telscan.io/tx/${TX_HASH}"
echo "Contract state update initiated with commit hash: ${COMMIT_HASH}"
echo "Script took ${SECONDS}s to complete"
echo
echo "This attestation satisfies the 'verify-on-chain' lane on the pull request. Attesting"
echo "changes nothing on GitHub, so re-run that lane on this sha yourself: request a review"
echo "on the PR, or re-run the failed job. Do not push -- a new sha needs a new attestation."
echo "When the PR is queued, the merge queue re-runs fmt, clippy and both test lanes against"
echo "main + this PR; expect that second run and do not force-push while it is queued."
