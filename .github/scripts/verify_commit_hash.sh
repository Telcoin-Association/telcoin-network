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
# The `verify-on-chain` lane in .github/workflows/pr.yaml runs this on the pull request
# head sha, and only there. Both non-zero codes fail that check, which is right at PR
# level: an outage keeps a PR out of the queue until someone re-runs the job, and cannot
# touch a PR already queued, because the lane does not run on `merge_group`. The codes
# stay distinct so the annotation on the failed step can say which one it was.

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
        echo "::error::no commit hash supplied (pass as arguments or set COMMIT_HASH)"
        exit 2
    fi
    hashes=("${COMMIT_HASH}")
fi

# `::error::` lines become annotations on the failed step, so the reason is visible from
# the checks tab without opening the log.
missing=0
for sha in "${hashes[@]}"; do
    if ! result=$(query "${sha}"); then
        echo "::error::adiri RPC (${RPC_ENDPOINT}) did not answer after ${ATTEMPTS} attempts, so ${sha} could not be verified. Re-run this job."
        exit 2
    fi
    # the registry returns an abi-encoded bool: 0x00..00 or 0x00..01
    if [[ "${result: -1}" == "1" ]]; then
        echo "attested:     ${sha}"
    else
        echo "::error::${sha} is not attested. A maintainer must run 'make attest' on this commit, then request a review to re-run this check."
        missing=1
    fi
done

exit ${missing}
