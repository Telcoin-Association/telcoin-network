#!/bin/bash
#
# This uses the method selector for `attestCommitHash(bytes20)`
# to read the latest commit hash from contract.

set -e  # Exit immediately if a command exits with a non-zero status

# Get the current commit hash
COMMIT_HASH=$(git rev-parse HEAD)

# adiri contract details
#
# NOTE: this contract must match local test-and-attest.sh
CONTRACT_ADDRESS="0x1f2f25561a11762bdffd91014c6d0e49af334447"
RPC_ENDPOINT="https://rpc.adiri.tel"

# Function call
FUNCTION_CALL="gitCommitHashAttested(bytes20)"

# Use cast to call the contract
RESULT=$(cast call --rpc-url ${RPC_ENDPOINT} \
    ${CONTRACT_ADDRESS} "${FUNCTION_CALL}" "${COMMIT_HASH}")

# Check if the result is true (1) or false (0)
if [[ "${RESULT: -1}" == "1" ]]; then
    echo "Commit hash ${COMMIT_HASH} has been attested on-chain."
    exit 0
else
    echo "Commit hash ${COMMIT_HASH} has NOT been attested on-chain."
    exit 1
fi
