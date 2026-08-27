#!/bin/bash
#
# Reads the git attestation registry on adiri and reports whether every commit hash
# passed in has been attested.
#
#   usage: verify_commit_hash.sh <sha> [<sha> ...]
#          COMMIT_HASH=<sha> verify_commit_hash.sh     # single-hash form
#
# exit codes
#   0  the registry answered and every hash queried is attested
#   1  the registry answered and at least one hash is NOT attested
#   2  the registry never answered (adiri RPC unreachable or erroring after retries)
#
# Callers must keep 1 and 2 apart. 1 is a real gate failure; 2 is an adiri outage, and
# under a merge queue an outage that fails closed ejects every queued PR. See
# verify_attestation.sh for the policy that acts on these codes.

set -uo pipefail

# NOTE: this contract must match local test-and-attest.sh
CONTRACT_ADDRESS="0xf102928273a399cda6151b8616209af019499c84"
RPC_ENDPOINT="${ATTESTATION_RPC_ENDPOINT:-https://rpc.adiri.tel}"
FUNCTION_CALL="gitCommitHashAttested(bytes20)"
ATTEMPTS="${ATTESTATION_RPC_ATTEMPTS:-5}"

# One `cast call`, retried with linear backoff. A single unlucky read used to surface as
# "not attested" because `set -e` turned an RPC error into a non-zero exit, which is
# indistinguishable from a genuine miss.
query() {
    local sha="$1" attempt=1 result
    while ((attempt <= ATTEMPTS)); do
        if result=$(cast call --rpc-url "${RPC_ENDPOINT}" \
            "${CONTRACT_ADDRESS}" "${FUNCTION_CALL}" "${sha}" 2>&1); then
            printf '%s' "${result//[[:space:]]/}"
            return 0
        fi
        echo "  rpc attempt ${attempt}/${ATTEMPTS} for ${sha} failed: ${result}" >&2
        ((attempt < ATTEMPTS)) && sleep $((attempt * 5))
        ((attempt++))
    done
    return 1
}

hashes=("$@")
if ((${#hashes[@]} == 0)); then
    if [[ -z "${COMMIT_HASH:-}" ]]; then
        echo "no commit hash supplied (pass as arguments or set COMMIT_HASH)" >&2
        exit 2
    fi
    hashes=("${COMMIT_HASH}")
fi

missing=0
for sha in "${hashes[@]}"; do
    if ! result=$(query "${sha}"); then
        echo "could not reach ${RPC_ENDPOINT} to verify ${sha}" >&2
        exit 2
    fi
    # the registry returns an abi-encoded bool: 0x00..00 or 0x00..01
    if [[ "${result: -1}" == "1" ]]; then
        echo "attested:     ${sha}"
    else
        echo "NOT attested: ${sha}"
        missing=1
    fi
done

exit ${missing}
