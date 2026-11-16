#!/bin/bash
#
# Deletes the superseded generations of the rust-cache entries that main writes, and
# nothing else.
#
#   usage: DRY_RUN=1 GH_REPO=<owner>/<repo> prune_caches.sh    # print the verdicts only
#          GH_REPO=<owner>/<repo> prune_caches.sh              # and delete
#
# environment
#   GH_REPO          owner/name of the repository; GITHUB_REPOSITORY, which every runner
#                    sets, stands in when it is unset
#   RUN_STARTED_AT   when the warm began, ISO-8601 UTC as the REST API prints it. The
#                    `prune-caches` job in cache-deps.yaml passes its own run's first
#                    attempt's start. Unset, it is looked up: the creation time of the
#                    latest successful cache-deps.yaml run on the branch.
#   DRY_RUN          1 prints the verdicts and deletes nothing; unset, empty or 0 deletes.
#                    Any other value is refused, so that DRY_RUN=true cannot delete.
#   PRUNE_REF        the ref whose entries are pruned; refs/heads/main when unset
#   MAX_DELETE       the most entries one run may delete; 20 when unset
#   CACHE_LIST_JSON  test hook: a file holding a saved listing (the API's JSON, one or
#                    more pages), read instead of calling the API. It forces a dry run,
#                    since the ids in a saved listing may be stale or made up, and
#                    RUN_STARTED_AT is then never looked up: unset, rule 3 below decides
#                    every family.
#
# Authentication is whatever `gh` already has: GH_TOKEN on a runner, the login locally.
# Deleting needs write access to the repository (`actions: write` for a token).
#
# exit codes
#   0  listed, and every delete selected succeeded (also: nothing to delete, a dry run)
#   1  at least one delete failed; the others were still attempted
#   2  bad configuration, or the listing or start time could not be read; nothing deleted
#   3  a guard rail refused the selection; nothing deleted
#
# A family is every entry on PRUNE_REF whose key starts `v<N>-rust-<shared-key>-`, for one
# of the shared keys in FAMILIES. The prefix-key version, the architecture and both hashes
# are deliberately not part of it: a superseded generation differs from the live one in
# exactly those. Within a family the live entries are, the first rule that matches any
# entry deciding:
#
#   1. those created at or after RUN_STARTED_AT. A warm that missed its exact key saved
#      it, so what it saved is live. Its fallback restore read the previous generation
#      during the same run, and that read must not keep it.
#   2. those accessed at or after RUN_STARTED_AT. The warm hit its key exactly and saved
#      nothing. This is what keeps the live entry when a Cargo.lock revert on main makes
#      an older generation's key live again, which "keep the newest created" would delete.
#      A PR restoring an old generation meanwhile keeps that one too, for a cycle.
#   3. the single most recently accessed entry, when nothing in the family was touched
#      since RUN_STARTED_AT: usually the durable-e2e family, which cache-deps.yaml never
#      reads. Alone, this rule would be wrong for the other two: any PR restoring an old
#      generation after the warm makes that one the most recently accessed.
#
# Every other entry of the family is deleted, so a family is never left empty. Entries
# outside the families (CodeQL's, another ref's, another shared key's) are never touched.

set -uo pipefail

# The shared keys main writes: two from cache-deps.yaml, one from durable-e2e.yaml.
FAMILIES=(clippy-cache test-cache durable-e2e-cache)
# The workflow whose last successful run dates a run by hand (see RUN_STARTED_AT).
WARM_WORKFLOW="cache-deps.yaml"
TIMESTAMP_RE='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?Z$'

repo="${GH_REPO:-${GITHUB_REPOSITORY:-}}"
ref="${PRUNE_REF:-refs/heads/main}"
started="${RUN_STARTED_AT:-}"
max_delete="${MAX_DELETE:-20}"
list_file="${CACHE_LIST_JSON:-}"

if [[ -z "${repo}" ]]; then
    echo "::error::no repository: set GH_REPO (owner/name), or GITHUB_REPOSITORY"
    exit 2
fi
if [[ ! "${max_delete}" =~ ^[0-9]+$ ]]; then
    echo "::error::MAX_DELETE must be a whole number, not '${max_delete}'"
    exit 2
fi
case "${DRY_RUN:-}" in
    1) dry_run=1 ;;
    "" | 0) dry_run=0 ;;
    *)
        echo "::error::DRY_RUN must be 1 (print only), or 0 or unset (delete), not '${DRY_RUN}'"
        exit 2
        ;;
esac
if [[ -n "${list_file}" ]]; then
    dry_run=1
fi

# Selects the verdicts, in one program so it can be read and tested as a unit. Input: the
# listing pages, slurped into an array. Output: one object per family entry, with its
# family and verdict, sorted by family and then most recently accessed first.
#
# Times are compared as numbers, not strings: the REST API prints cache times with
# microseconds and run times without, and fromdateiso8601 rejects fractional seconds in
# some jq versions, so `secs` parses the whole seconds and adds the fraction back. Rules 1
# and 2 compare whole seconds, so an access in the same second as the start counts as
# after it, the side that keeps. Rule 3 orders by the full value.
# shellcheck disable=SC2016 # $names below are jq variables, not shell ones
SELECT='
def secs:
  (capture("^(?<s>[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(?<f>[.][0-9]+)?Z$")
    // error("unexpected timestamp: \(.)"))
  | (.s + "Z" | fromdateiso8601) + ("0\(.f // "")" | tonumber);

$ARGS.positional as $families
| (if $since == "" then null else $since | secs | floor end) as $t0
| [ .[] | .actions_caches[]
    | select(.ref == $ref)
    | . as $e
    | (first(($e.key | capture("^v[0-9]+-rust-(?<rest>.+)$") | .rest) as $rest
        | $families[] | select(. as $f | $rest | startswith($f + "-"))) // null) as $family
    | select($family != null)
    | {id, key, size_in_bytes, created_at, last_accessed_at, family: $family,
       created: (.created_at | secs), accessed: (.last_accessed_at | secs)} ]
| group_by(.family)
| map(
    . as $fam
    | [$fam[] | select($t0 != null and (.created | floor) >= $t0) | .id] as $made
    | [$fam[] | select($t0 != null and (.accessed | floor) >= $t0) | .id] as $read
    | ($fam | max_by([.accessed, .created, .id]) | .id) as $latest
    | $fam[]
    | .verdict = (
        if ($made | length) > 0 then
          (if (.id | IN($made[])) then "keep (created since warm start)" else "delete" end)
        elif ($read | length) > 0 then
          (if (.id | IN($read[])) then "keep (accessed since warm start)" else "delete" end)
        elif .id == $latest then "keep (most recently accessed)"
        else "delete" end))
| sort_by([.family, -.accessed])
'

mib() { echo $((($1 + 524288) / 1048576)); }

# A run by hand has no run of its own to measure from, and rule 3 alone would keep
# whichever generation a PR restored last. The latest successful warm on the branch is the
# next best start: its creation time, which no re-run moves, precedes everything it read
# or wrote. It is the newest success on a page rather than the first row, which leans on
# no ordering. The runs are listed unfiltered and the successes picked out here: the
# API's own `status=success` filter is served from an index that has lagged by weeks,
# and although an older start only keeps more, it then keeps what should go.
since_what="RUN_STARTED_AT"
if [[ -z "${started}" && -z "${list_file}" ]]; then
    if [[ "${ref}" != refs/heads/* ]]; then
        echo "::error::${ref} is not a branch, so there is no warm to date it by: set RUN_STARTED_AT"
        exit 2
    fi
    branch_q=$(jq -rn --arg b "${ref#refs/heads/}" '$b | @uri')
    runs="repos/${repo}/actions/workflows/${WARM_WORKFLOW}/runs"
    if ! started=$(gh api "${runs}?branch=${branch_q}&per_page=50" \
        --jq '[.workflow_runs[] | select(.conclusion == "success")] | max_by(.created_at)
            // empty | "\(.created_at) \(.id)"'); then
        echo "::error::could not look up the latest successful ${WARM_WORKFLOW} run on ${ref}"
        exit 2
    fi
    if [[ -z "${started}" ]]; then
        echo "::error::no successful ${WARM_WORKFLOW} run on ${ref}: set RUN_STARTED_AT"
        exit 2
    fi
    since_what="the latest successful ${WARM_WORKFLOW} run on ${ref} (run ${started#* })"
    started="${started%% *}"
fi
if [[ -n "${started}" && ! "${started}" =~ ${TIMESTAMP_RE} ]]; then
    echo "::error::RUN_STARTED_AT must be ISO-8601 UTC (2026-09-30T09:01:22Z), not '${started}'"
    exit 2
fi

# The query parameter narrows the listing; the program filters on .ref again regardless.
if [[ -n "${list_file}" ]]; then
    if ! listing=$(cat "${list_file}"); then
        echo "::error::could not read CACHE_LIST_JSON (${list_file})"
        exit 2
    fi
else
    ref_q=$(jq -rn --arg r "${ref}" '$r | @uri')
    if ! listing=$(gh api --paginate "repos/${repo}/actions/caches?ref=${ref_q}&per_page=100"); then
        echo "::error::could not list the caches of ${repo}; nothing was deleted"
        exit 2
    fi
fi

if ! selection=$(jq -s --arg ref "${ref}" --arg since "${started}" "${SELECT}" \
    --args "${FAMILIES[@]}" <<<"${listing}"); then
    echo "::error::could not parse the cache listing; nothing was deleted"
    exit 2
fi

if ((dry_run)); then
    echo "dry run: nothing will be deleted"
fi
echo "repository ${repo}, ref ${ref}, families: ${FAMILIES[*]}"
if [[ -n "${started}" ]]; then
    echo "warm start ${started}, from ${since_what}"
else
    echo "no warm start: rule 3 decides every family"
fi
echo

row() { printf '%-17s  %-32s  %6s  %-20s  %-20s  %-10s  %s\n' "$@"; }
row family verdict MiB created "last accessed" id key
jq -r '.[] | [.family, .verdict, (.size_in_bytes / 1048576 | round),
    (.created_at | sub("[.][0-9]+Z$"; "Z")), (.last_accessed_at | sub("[.][0-9]+Z$"; "Z")),
    .id, .key] | @tsv' <<<"${selection}" |
    while IFS=$'\t' read -r family verdict size created accessed id key; do
        row "${family}" "${verdict}" "${size}" "${created}" "${accessed}" "${id}" "${key}"
    done
echo

read -r n_total n_delete delete_bytes keep_bytes < <(jq -r '[length,
    (map(select(.verdict == "delete")) | length),
    (map(select(.verdict == "delete") | .size_in_bytes) | add // 0),
    (map(select(.verdict != "delete") | .size_in_bytes) | add // 0)] | @tsv' <<<"${selection}")

# Guard rails. Both check the verdicts, not the rules that made them, so that a surprise
# in the listing or a slip in the program deletes nothing.
#
# A family with every entry marked for deletion would leave the lanes nothing to restore.
# The rules never select that; this catches a program that does.
emptied=$(jq -r 'group_by(.family)[] | select(all(.[]; .verdict == "delete")) | .[0].family' \
    <<<"${selection}")
if [[ -n "${emptied}" ]]; then
    echo "::error::refusing: the selection deletes every entry of: ${emptied//$'\n'/, }. Nothing was deleted."
    exit 3
fi
# Two or three generations per family is normal. A count far above that means the
# listing or the program is not what this script expects, and deleting on it could empty
# the cache for every lane at once.
if ((n_delete > max_delete)); then
    echo "::error::refusing: the selection deletes ${n_delete} of ${n_total} entries, more than MAX_DELETE=${max_delete}. Nothing was deleted."
    exit 3
fi

if ((n_delete == 0)); then
    echo "::notice::nothing to delete; $(mib "${keep_bytes}") MiB kept"
    exit 0
fi
if ((dry_run)); then
    echo "::notice::dry run: would delete ${n_delete} of ${n_total} entries, freeing $(mib "${delete_bytes}") MiB; $(mib "${keep_bytes}") MiB kept"
    exit 0
fi

# By id, never by key: the same key can exist on several refs, and a delete by key would
# take all of them.
failed=0
deleted=0
freed=0
while IFS=$'\t' read -r id size key; do
    if [[ ! "${id}" =~ ^[0-9]+$ ]]; then
        echo "::warning::skipping an entry with a malformed id '${id}' (${key})"
        failed=1
        continue
    fi
    if out=$(gh api -X DELETE "repos/${repo}/actions/caches/${id}" 2>&1); then
        echo "deleted ${id} ${key}"
        deleted=$((deleted + 1))
        freed=$((freed + size))
    else
        echo "::warning::could not delete cache ${id} (${key}): ${out}"
        failed=1
    fi
done < <(jq -r '.[] | select(.verdict == "delete") | [.id, .size_in_bytes, .key] | @tsv' \
    <<<"${selection}")

echo "::notice::deleted ${deleted} of the ${n_delete} entries selected, $(mib "${freed}") MiB freed; $(mib $((keep_bytes + delete_bytes - freed))) MiB kept"
exit ${failed}
