#!/bin/bash
#
# This script runs CI checks locally and submits a tx to adiri.
# for contract information:
# https://github.com/Telcoin-Association/tn-contracts/blob/master/src/CI/GitAttestationRegistry.sol
#
# This approach is adopted due to CI limitations using GitHub actions.

set -e  # Exit immediately if a command exits with a non-zero status

# set environment
CARGO_INCREMENTAL: 0 # disable incremental compilation
RUSTFLAGS: "-D warnings -D unused_extern_crates"
CARGO_TERM_COLOR: always
RUST_BACKTRACE: 1
CARGO_PROFILE_DEV_DEBUG: 0

# Step 1: compile tests
cargo test --no-run --locked

# Step 2: compile workspace
cargo build --workspace --all-features --quite

# Step 3: run all tests
cargo test --workspace --all-features --no-fail-fast -- --test-threads 4 ;

# Step 4: Check clippy
cargo +nighly clippy --workspace --all-features -- -D warnings

# Step 5: Check cargo fmt
cargo +nightly fmt -- --check

#
# If we've reached this point, all checks have passed
#

# Step 6: Get the latest commit hash
COMMIT_HASH=$(git rev-parse HEAD)
echo "attesting git hash: ${COMMIT_HASH}"

# Step 7: Load environment variables
source .env

# Step 8: create and submit transaction
#
# NOTE: this contract must match CI
CONTRACT_ADDRESS="0x1f2f25561a11762bdffd91014c6d0e49af334447"
RPC_ENDPOINT="https://rpc.adiri.tel"

# Construct the function call
FUNCTION_CALL="attestCommitHash(bytes32)"
PRIVATE_KEY=${GITHUB_ATTESTATION_PRIVATE_KEY}

# Send the transaction using cast
TX_HASH=$(cast send --private-key ${PRIVATE_KEY} \
    --rpc-url ${RPC_ENDPOINT} \
    ${CONTRACT_ADDRESS} \
    "${FUNCTION_CALL} ${COMMIT_HASH}")

echo "Transaction sent. Hash: ${TX_HASH}"
echo "https://telscan.io/tx/${TX_HASH}"
echo "Contract state update initiated with commit hash: ${COMMIT_HASH}"
