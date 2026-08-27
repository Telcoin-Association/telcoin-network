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
#   How a head is recovered from that chain depends on the queue's merge method, and the
#   two cases are not interchangeable:
#
#     MERGE   each queue commit is a merge commit, and its non-first parents are the PR
#             heads. Read them straight out of the graph.
#     SQUASH  what main's ruleset actually specifies. Each queue commit is a single-parent
#             squash of one PR, so there is no second parent to read, and the PR head sha
#             is not an ancestor of the queue branch at all -- it appears nowhere in this
#             history. The only surviving signal is the `(#N)` GitHub appends to a squash
#             subject, so parse those numbers and resolve each to a head sha via the API.
#
#   Both are implemented, graph first. Configuring the queue for MERGE or SQUASH therefore
#   does not silently turn this check into a no-op, which is what happened when only the
#   graph walk existed and the queue was set to SQUASH.
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
        # Graph derivation, for a MERGE-configured queue. The first-parent chain from head
        # back to base is exactly the sequence of commits the queue created, one per PR in
        # the batch; under MERGE each is a merge commit whose non-first parents are the PR
        # heads. Restricting to --first-parent is what keeps a contributor's own "merge
        # main into my branch" commit -- which sits off this chain -- from being mistaken
        # for a PR head. Under SQUASH every commit on the chain has a single parent, so
        # `NF > 2` matches nothing and this yields an empty list.
        mapfile -t heads < <(
            git rev-list --first-parent --parents "${base}..${head}" 2>/dev/null |
                awk 'NF > 2 { for (i = 3; i <= NF; i++) print $i }' | sort -u
        )
    fi

    # Subject derivation, for a SQUASH-configured queue -- which is what main uses. Parse
    # the trailing `(#N)` off each squash subject and ask the API for that PR's head sha.
    # Anonymous access is rate-limited hard enough to be unreliable, so this wants the
    # workflow's GITHUB_TOKEN with `pull-requests: read`.
    if ((${#heads[@]} == 0)) && [[ -n "${base}" && -n "${head}" ]]; then
        prs=()
        mapfile -t prs < <(
            git log --first-parent --format=%s "${base}..${head}" 2>/dev/null |
                sed -n 's/.*(#\([0-9][0-9]*\))[[:space:]]*$/\1/p' | sort -un
        )

        api=(--silent --show-error --fail --max-time 30
            -H "Accept: application/vnd.github+json"
            -H "X-GitHub-Api-Version: 2022-11-28")
        if [[ -n "${GITHUB_TOKEN:-}" ]]; then
            api+=(-H "Authorization: Bearer ${GITHUB_TOKEN}")
        fi

        for pr in ${prs[@]+"${prs[@]}"}; do
            [[ -n "${pr}" ]] || continue
            sha=$(
                curl "${api[@]}" \
                    "${GITHUB_API_URL:-https://api.github.com}/repos/${GITHUB_REPOSITORY:-}/pulls/${pr}" |
                    jq -r '.head.sha // empty'
            )
            if [[ -n "${sha}" ]]; then
                echo "  #${pr} -> ${sha}"
                heads+=("${sha}")
            else
                warn "could not resolve the head sha of #${pr}"
            fi
        done
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
