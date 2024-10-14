#!/bin/bash
#
# This uses the method selector for `attestCommitHash(bytes20)`
# to read the latest commit hash from contract.

set -e  # Exit immediately if a command exits with a non-zero status

# adiri contract details
#
# NOTE: this contract must match local test-and-attest.sh
CONTRACT_ADDRESS="0xba26a2c8e1c54d5bbb96e891fd4a9d853f438eb9"
RPC_ENDPOINT="https://rpc.adiri.tel"

# Function call
FUNCTION_CALL="gitCommitHashAttested(bytes20)"

# Use cast to call the contract
#
# NOTE: COMMIT_HASH is set in .workflow through GitHub context
# other approaches return different commit hashes than the one
# that triggered this workflow
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
