#!/bin/bash
#
# This uses the method selector for `attestCommitHash(bytes20)`
# to read the latest commit hash from contract.

set -e  # Exit immediately if a command exits with a non-zero status

# Get the current commit hash
CURRENT_COMMIT_HASH=$(git rev-parse HEAD)

# adiri contract details
#
# NOTE: this contract must match local test-and-attest.sh
CONTRACT_ADDRESS="0xde9700e89e0999854e5bfd7357a803d8fc476bb0"
RPC_ENDPOINT="https://rpc.adiri.tel"

# Function call
FUNCTION_CALL="gitCommitHashAttested(bytes20)"

# Use cast to call the contract
RESULT=$(cast call ${CONTRACT_ADDRESS} "${FUNCTION_CALL} ${CURRENT_COMMIT_HASH}" --rpc-url ${RPC_ENDPOINT})

# Check if the result is true (1) or false (0)
if [ "$RESULT" == "1" ]; then
    echo "Commit hash ${CURRENT_COMMIT_HASH} has been attested on-chain."
    exit 0
else
    echo "Commit hash ${CURRENT_COMMIT_HASH} has NOT been attested on-chain."
    exit 1
fi
