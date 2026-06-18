#!/usr/bin/env bash
#
# Uses the method selector for `gitCommitHashAttested(bytes20)` to read
# the latest attested commit hash from the on-chain registry.

set -euo pipefail

# adiri contract details
# NOTE: this contract must match local test-and-attest.sh
CONTRACT_ADDRESS="0xf102928273a399cda6151b8616209af019499c84"
RPC_ENDPOINT="https://rpc.adiri.tel"
FUNCTION_CALL="gitCommitHashAttested(bytes20)"

# Validate COMMIT_HASH is a 40-char hex string (a git SHA-1).
if ! [[ "${COMMIT_HASH:-}" =~ ^[0-9a-fA-F]{40}$ ]]; then
    echo "ERROR: COMMIT_HASH is not a valid 40-char hex string: '${COMMIT_HASH:-<unset>}'" >&2
    exit 2
fi

# With pipefail, a cast failure propagates instead of being masked.
RESULT=$(cast call --rpc-url "${RPC_ENDPOINT}" \
    "${CONTRACT_ADDRESS}" "${FUNCTION_CALL}" "${COMMIT_HASH}")

# cast returns a 32-byte word: 0x000...001 for true, 0x000...000 for false.
# Compare the whole word against the canonical true value, not just the last char.
TRUE_WORD="0x0000000000000000000000000000000000000000000000000000000000000001"
if [[ "${RESULT}" == "${TRUE_WORD}" ]]; then
    echo "Commit hash ${COMMIT_HASH} has been attested on-chain."
    exit 0
else
    echo "Commit hash ${COMMIT_HASH} has NOT been attested on-chain (result: ${RESULT})."
    exit 1
fi
