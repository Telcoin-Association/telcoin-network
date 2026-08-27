#!/bin/bash
#
# Required-check entry point for the on-chain attestation gate. Picks which commit
# hash(es) to verify from the event that triggered the run, then hands them to
# verify_commit_hash.sh.
#
# `pull_request`
#   Verify the PR head. This is the substantive gate: a maintainer ran the full local
#   suite (etc/test-and-attest.sh -- fmt, clippy, the default and adiri test lanes, and
#   the e2e suites that GitHub-hosted CI cannot afford) against exactly this content and
#   attested it. A miss, or an adiri outage, fails the check; the author re-runs it.
#
# `merge_group`
#   The merge queue builds a brand-new commit -- `main` plus this PR plus every PR ahead
#   of it in the batch. That hash did not exist when anyone ran the local suite, so it
#   cannot be attested and asking for it would deadlock the queue. What is verifiable is
#   that each PR *head* folded into the group is attested, so walk the group's
#   first-parent chain and check those.
#
#   This is a belt-and-braces check, not the primary one: GitHub requires the branch's
#   required checks to pass before a PR may enter the queue, and pushing to a queued PR
#   ejects it, so the PR-event verification above already ran on every head in the group.
#   That is why derivation trouble or an adiri outage warns and passes here instead of
#   failing: a fail-closed miss would eject the whole batch over a problem that is not
#   the code's, while a definitively unattested head still fails hard.

set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
verify="${here}/verify_commit_hash.sh"

warn() { echo "::warning::$*"; }
fail() { echo "::error::$*"; }

case "${EVENT_NAME:-}" in
pull_request)
    if [[ -z "${PR_HEAD_SHA:-}" ]]; then
        fail "pull_request event carried no head sha"
        exit 1
    fi
    echo "verifying attestation for PR head ${PR_HEAD_SHA}"
    "${verify}" "${PR_HEAD_SHA}"
    rc=$?
    case ${rc} in
    0) exit 0 ;;
    2)
        fail "adiri RPC did not answer; cannot verify ${PR_HEAD_SHA}. Re-run this job."
        exit 1
        ;;
    *)
        fail "${PR_HEAD_SHA} is not attested. A maintainer must run 'make attest' on this commit."
        exit 1
        ;;
    esac
    ;;

merge_group)
    base="${MERGE_GROUP_BASE_SHA:-}"
    head="${MERGE_GROUP_HEAD_SHA:-}"
    heads=()

    if [[ -n "${base}" && -n "${head}" ]]; then
        # The queue branch's first-parent chain from head back to base is exactly the
        # sequence of merge commits the queue created, one per PR in the batch. Each of
        # those commits' non-first parents are the PR heads. Restricting to
        # --first-parent is what keeps a contributor's own "merge main into my branch"
        # commit -- which sits off this chain -- from being mistaken for a PR head.
        mapfile -t heads < <(
            git rev-list --first-parent --parents "${base}..${head}" 2>/dev/null |
                awk 'NF > 2 { for (i = 3; i <= NF; i++) print $i }' | sort -u
        )
    fi

    if ((${#heads[@]} == 0)); then
        warn "could not derive PR heads from merge group ${base:-?}..${head:-?} (ref ${GITHUB_REF:-?}); attestation was already verified on the pull_request event before this PR could enter the queue"
        exit 0
    fi

    echo "merge group folds in ${#heads[@]} PR head(s):"
    printf '  %s\n' "${heads[@]}"
    "${verify}" "${heads[@]}"
    rc=$?
    case ${rc} in
    0) exit 0 ;;
    2)
        warn "adiri RPC did not answer; skipping the merge-queue attestation re-check (already enforced at queue entry)"
        exit 0
        ;;
    *)
        fail "a PR head in this merge group is not attested; remove it from the queue and run 'make attest' on it"
        exit 1
        ;;
    esac
    ;;

*)
    fail "verify_attestation.sh does not handle event '${EVENT_NAME:-<unset>}'"
    exit 1
    ;;
esac
