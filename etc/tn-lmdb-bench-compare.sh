#!/usr/bin/env bash
# The tn-lmdb engine bench (`crates/tn-lmdb-bench`, binary `tn-lmdb-bench`) on two revisions of
# tn-lmdb, run as interleaved rounds and compared row by row into a report with a pass/fail exit
# code: a regression check for the engine, locally or in CI (.github/workflows/tn-lmdb-bench.yaml,
# described in .github/ACTIONS.md). See crates/tn-lmdb-bench's README for the rows and tiers.
#
# Usage: etc/tn-lmdb-bench-compare.sh [options] [base] [head]
#        etc/tn-lmdb-bench-compare.sh [options] --worktree [base]
#        etc/tn-lmdb-bench-compare.sh --report-only <bench dir> [--threshold PCT]
#                                     [--size-threshold PCT] [--gate-durable]
#   base               the revision to compare against (default main); any rev git resolves to a
#                      commit. A revision without tn-lmdb (no crates/tn-lmdb/Cargo.toml) is
#                      refused: main has none until the branch that adds tn-lmdb merges.
#   head               the revision under test (default HEAD, this checkout's HEAD commit)
#   --worktree         this checkout's working tree is head, uncommitted and untracked changes
#                      included (no head argument then)
#   --tier quick|default|full, --rows PAT[,PAT..], --sync nosync,durable, --n N, --reps K
#                      passed to every bench run as they are (the bench's own defaults without
#                      them); --rows takes exact row names or prefixes ending in `*`
#   --rounds K         interleaved pairs (default 4 at the quick tier, 2 otherwise): round 1
#                      runs base then head, round 2 head then base, and so on (A B B A for 2).
#                      With 1 each side has a single run, so a load spike during one run decides
#                      its rows alone; with 2 a load change that spans the middle two runs (both
#                      head's) moves all of one side's runs, which 4 rounds of the quick tier's
#                      short runs make unlikely
#   --threshold PCT    a time row regresses when head's median over base's is above 1 + PCT/100
#                      AND head's min over base's above 1 + PCT/200 (default 10): both, so that
#                      one noisy run on either side cannot decide alone. `improved` is the mirror
#                      image (the same test with the sides swapped)
#   --size-threshold PCT  an exact row (a deterministic size or count) regresses when head's
#                      value is above base's x (1 + PCT/100) (default 1); a smaller value is
#                      `improved`, never a regression
#   --gate-durable     gate the time rows of `sync: durable` too; by default they are reported
#                      with their ratio and verdict `not gated` (a laptop drive's commit time
#                      flips between two speeds mid-run), and those past the threshold listed
#   --allow-same       run and compare even when both sides have the same tn-lmdb or build the
#                      same binary (an A/A run: the noise the thresholds must absorb); without it
#                      those exit 0 with "nothing to compare"
#   --bench-dir DIR    where everything below goes (default ${TMPDIR:-/tmp}/tn-lmdb-bench-compare);
#                      the runs' TMPDIR is its tmp/, so it picks the disk under test
#   --keep             keep the base and head worktrees after the run (removed at exit otherwise)
#   --report-only DIR  no builds, no runs: compare DIR's JSON again and rewrite DIR/report.md.
#                      DIR is a bench dir as this script leaves it, or any dir laid out the same:
#                      json/base-<round>.json and json/head-<round>.json (one bench run's JSON
#                      each, schema 1; at least one per side) and, optionally, meta.tsv (one
#                      `key<TAB>value` per line: the commits, host and settings of the run). The
#                      thresholds and --gate-durable are meta.tsv's unless given here.
#   -h, --help         this text
#   Environment: TMPDIR (the default bench dir's parent); the cargo and rustup variables
#   (RUSTFLAGS, CARGO_HOME, ...) reach both builds alike. CARGO_TARGET_DIR is ignored: each side
#   builds into its own build-<side>/target.
#
# Both sides run the same bench source, head's crates/tn-lmdb-bench (src/ and build.rs), each
# built as a standalone package against its own tn-lmdb, so the only difference between the two
# binaries is the engine. A side's build dir holds a generated Cargo.toml (the bench's manifest
# with its workspace-inherited fields and dependency versions resolved from head's root
# Cargo.toml, head's [profile.release] copied, tn-lmdb a path dependency, an empty [workspace]),
# head's Cargo.lock, head's .cargo/config.toml and rust-toolchain.toml (cargo and rustup read
# those from the build's directory, not the engine's), and `engine`, a link to the side's
# checkout: a `git worktree add --detach` of its commit under the bench dir (head's is this
# checkout with --worktree). Cargo hashes a path dependency inside the build's root by its
# relative path and gives rustc relative source paths, so the binary does not depend on where
# the side's checkout is: two checkouts of one tn-lmdb build byte-identical binaries. The
# script compares them: identical binaries mean the change does not reach the bench (comments,
# docs or tests only), and the script says so and exits 0 without running anything, as it does
# before building when tn-lmdb and the root Cargo.toml it inherits from are the same on both
# sides (--allow-same runs them anyway). A side that fails to build is named with cargo's error
# tail: the bench is head's, so a base whose tn-lmdb lacks an API the bench uses fails here.
#
# Each run is the binary run directly with the pass-through options, `--label <side>-<round>`
# and `--out json/<side>-<round>.json`, with TMPDIR set to the bench dir's tmp/ (emptied before
# each run). On Linux a tmpfs or ramfs bench dir is refused (syncs are free there; the bench
# refuses such a TMPDIR too). The comparison pools each row's samples over a side's runs (a row is
# its name and sync mode: the bench runs a row once per selected mode where the mode can change
# it): ratio is the median of head's over the median of base's, min ratio the same of the
# minimums. The report has both commits, the host, rustc, the bench config (tier, n, reps and page size, from
# the JSON), the rounds and thresholds, then one line per row: name, group, sync, base and head
# median, ratio, min ratio, each side's spread ((max - min) / median of its pooled samples) and
# the verdict: ok, improved, REGRESSION, not gated (a durable time row, or a row whose JSON says
# `gate: false`: the bench marks the commit rows so at the quick tier, where one commit is too
# short to hold still), or new and removed (a row on one side only, never gated). Lower is better
# for every row. When rustc or cargo processes were running as the runs started, the report says
# so.
#
# Needs git, a Rust toolchain, python3 3.11 or later (its tomllib; --report-only needs 3.8) and
# cmp. In the bench dir (marked as the script's own with a .tn-lmdb-bench-compare file; an
# existing, non-empty dir without it is refused) the script writes only:
#   wt-base/, wt-head/            the side checkouts (git worktrees), removed at exit unless
#                                 --keep, then `git worktree prune`
#   build-base/, build-head/      each side's standalone build; its target/ is kept for reruns
#   logs/build-<side>.log, logs/<side>-<round>.log  each build's and run's output
#   json/<side>-<round>.json      each run's JSON (emptied when the runs start)
#   meta.tsv                      the run's commits, host and settings, for --report-only
#   report.md                     the report, also printed; its path is printed last
#   tmp/                          the runs' TMPDIR, removed at exit
# Exit codes: 0 no gated regression (or nothing to compare), 1 a gated regression, 2 a usage,
# environment, build or run error.
set -euo pipefail

die() {
    echo "tn-lmdb-bench-compare: $*" >&2
    exit 2
}
script_dir="$(cd "$(dirname "$0")" && pwd -P)"
helper="$script_dir/tn-lmdb-bench-compare.py"
[ -f "$helper" ] || die "$helper is missing"
command -v python3 >/dev/null || die "python3 is needed"

tier="" rows="" sync="" n="" reps="" rounds="" threshold="" size_threshold=""
gate_durable=0 allow_same=0 worktree=0 keep=0 bench_dir="" report_only=""
run_option="" # the first option that only a run takes, to refuse it with --report-only
positional=()
while [ $# -gt 0 ]; do
    case "$1" in
        --*=*) set -- "${1%%=*}" "${1#*=}" "${@:2}" ;; # --opt=value is --opt value
    esac
    case "$1" in
        -h | --help)
            awk 'NR > 1 && /^#/ {sub(/^# ?/, ""); print; next} NR > 1 {exit}' "$0"
            exit 0
            ;;
        --tier | --rows | --sync | --n | --reps | --rounds | --threshold | --size-threshold \
            | --bench-dir | --report-only)
            [ $# -ge 2 ] || die "$1 needs a value (see --help)"
            case "$1" in
                --tier) tier="$2" ;;
                --rows) rows="$2" ;;
                --sync) sync="$2" ;;
                --n) n="$2" ;;
                --reps) reps="$2" ;;
                --rounds) rounds="$2" ;;
                --threshold) threshold="$2" ;;
                --size-threshold) size_threshold="$2" ;;
                --bench-dir) bench_dir="$2" ;;
                --report-only) report_only="$2" ;;
            esac
            case "$1" in
                --threshold | --size-threshold | --report-only) ;;
                *) run_option="${run_option:-$1}" ;;
            esac
            shift 2
            ;;
        --gate-durable) gate_durable=1 && shift ;;
        --allow-same) allow_same=1 run_option="${run_option:-$1}" && shift ;;
        --worktree) worktree=1 run_option="${run_option:-$1}" && shift ;;
        --keep) keep=1 run_option="${run_option:-$1}" && shift ;;
        --) shift && positional+=("$@") && break ;;
        -*) die "unknown option $1 (see --help)" ;;
        *) positional+=("$1") && shift ;;
    esac
done

positive() { case "$1" in '' | *[!0-9]* | 0*) return 1 ;; esac; }
percent() { printf '%s\n' "$1" | grep -Eq '^[0-9]+([.][0-9]+)?$'; }
for pair in "--threshold:$threshold" "--size-threshold:$size_threshold"; do
    value="${pair#*:}"
    [ -z "$value" ] || percent "$value" || die "${pair%%:*} takes a percentage, not '$value'"
done

# --report-only: render again, nothing else.
if [ -n "$report_only" ]; then
    [ -z "$run_option" ] && [ "${#positional[@]}" = 0 ] \
        || die "--report-only takes only --threshold, --size-threshold and --gate-durable"
    [ -d "$report_only" ] || die "--report-only: $report_only is not a directory"
    report_args=()
    [ -z "$threshold" ] || report_args+=(--threshold "$threshold")
    [ -z "$size_threshold" ] || report_args+=(--size-threshold "$size_threshold")
    [ "$gate_durable" = 0 ] || report_args+=(--gate-durable)
    status=0
    python3 -I "$helper" report "$report_only" ${report_args[@]+"${report_args[@]}"} || status=$?
    [ "$status" = 2 ] || echo "report: $(cd "$report_only" && pwd -P)/report.md"
    exit "$status"
fi

case "$tier" in '' | quick | default | full) ;; *) die "--tier takes quick, default or full, not '$tier'" ;; esac
for pair in "--n:$n" "--reps:$reps" "--rounds:$rounds"; do
    value="${pair#*:}"
    [ -z "$value" ] || positive "$value" || die "${pair%%:*} takes a positive integer, not '$value'"
done
if [ -z "$rounds" ]; then
    # A quick run is short, so four rounds cost little, and they give each side runs on both
    # sides of a load change of a busy machine: neither side's min then comes from the slow
    # stretch alone, so the min ratio does not call the change a regression.
    if [ "$tier" = quick ]; then rounds=4; else rounds=2; fi
fi
threshold="${threshold:-10}"
size_threshold="${size_threshold:-1}"
if [ "$worktree" = 1 ]; then
    [ "${#positional[@]}" -le 1 ] || die "--worktree makes the working tree head: pass a base only"
else
    [ "${#positional[@]}" -le 2 ] || die "too many arguments: [base] [head] (see --help)"
fi
base="${positional[0]:-main}"
head="${positional[1]:-HEAD}"
python3 -I -c 'import tomllib' 2>/dev/null || die "python3 3.11 or later is needed (tomllib)"

repo="$(cd "$script_dir" && git rev-parse --show-toplevel)"
engine_dir="crates/tn-lmdb"
bench_crate="crates/tn-lmdb-bench"
# What a side's tn-lmdb compiles from: its crate and the root manifest it inherits fields from.
engine_inputs=("$engine_dir" Cargo.toml)
no_engine_hint="main does not have tn-lmdb until the branch that adds it merges: pass a base that does, e.g. a451515f4 or later"
resolve() { # <rev>: prints its commit
    git -C "$repo" rev-parse --verify --quiet "$1^{commit}" || die "$1 is not a commit here"
}
short() { git -C "$repo" rev-parse --short "$1"; }
subject() { git -C "$repo" log -1 --format=%s "$1" | tr '\t' ' '; }
rev_text() { # <rev> <commit>: "rev (short sha)", or the short sha alone for a sha
    case "$2" in
        "$1"*) short "$2" ;;
        *) echo "$1 ($(short "$2"))" ;;
    esac
}
base_commit="$(resolve "$base")"
git -C "$repo" cat-file -e "$base_commit:$engine_dir/Cargo.toml" 2>/dev/null \
    || die "base $(rev_text "$base" "$base_commit") does not have tn-lmdb ($engine_dir/Cargo.toml): $no_engine_hint"
if [ "$worktree" = 1 ]; then
    head_commit="$(resolve HEAD)"
    [ -f "$repo/$engine_dir/Cargo.toml" ] || die "this working tree does not have tn-lmdb ($engine_dir)"
else
    head_commit="$(resolve "$head")"
    git -C "$repo" cat-file -e "$head_commit:$engine_dir/Cargo.toml" 2>/dev/null \
        || die "head $(rev_text "$head" "$head_commit") does not have tn-lmdb ($engine_dir/Cargo.toml): $no_engine_hint"
fi

# The working tree's changes to what a build reads, for the report's head line.
describe_changes() {
    local inputs=("$engine_dir" "$bench_crate" Cargo.toml Cargo.lock .cargo 'rust-toolchain*')
    local changed untracked
    changed="$(git -C "$repo" diff --name-only HEAD -- "${inputs[@]}" | sed -E 's#^(crates/[^/]+)/.*#\1#' \
        | sort -u | xargs)"
    untracked="$(git -C "$repo" ls-files --others --exclude-standard -- "${inputs[@]}" \
        | sed -E 's#^(crates/[^/]+)/.*#\1#' | sort -u | xargs)"
    echo "${changed:+uncommitted changes in: $changed}${changed:+${untracked:+; }}${untracked:+untracked files in: $untracked}"
}
head_changes=""
if [ "$worktree" = 1 ]; then
    head_text="the working tree (HEAD $(short "$head_commit"))"
    head_changes="$(describe_changes)"
    same_engine=0
    if git -C "$repo" diff --quiet "$base_commit" -- "${engine_inputs[@]}" \
        && [ -z "$(git -C "$repo" ls-files --others --exclude-standard -- "$engine_dir")" ]; then
        same_engine=1
    fi
else
    head_text="$(rev_text "$head" "$head_commit")"
    same_engine=0
    git -C "$repo" diff --quiet "$base_commit" "$head_commit" -- "${engine_inputs[@]}" && same_engine=1
fi
base_text="$(rev_text "$base" "$base_commit")"
if [ "$same_engine" = 1 ] && [ "$allow_same" = 0 ]; then
    echo "base $base_text and head $head_text have the same tn-lmdb ($engine_dir and the root Cargo.toml): nothing to compare"
    exit 0
fi
if [ "$worktree" = 1 ]; then
    [ -f "$repo/$bench_crate/Cargo.toml" ] \
        || die "this working tree has no $bench_crate: both sides run head's bench source"
else
    git -C "$repo" cat-file -e "$head_commit:$bench_crate/Cargo.toml" 2>/dev/null \
        || die "head $head_text has no $bench_crate: both sides run head's bench source"
fi

# The bench dir: created here, or one this script made before (its marker), never another.
marker=".tn-lmdb-bench-compare"
if [ -z "$bench_dir" ]; then
    tmp_root="${TMPDIR:-/tmp}"
    bench_dir="${tmp_root%/}/tn-lmdb-bench-compare"
fi
[ ! -e "$bench_dir" ] || [ -d "$bench_dir" ] || die "refusing $bench_dir: not a directory"
mkdir -p "$bench_dir"
bench_dir="$(cd "$bench_dir" && pwd -P)"
[ "$bench_dir" != / ] || die "refusing / as the bench dir"
if [ ! -f "$bench_dir/$marker" ]; then
    [ -z "$(ls -A "$bench_dir")" ] \
        || die "refusing $bench_dir: not empty and not made by this script (no $marker)"
    : >"$bench_dir/$marker"
fi
if [ "$(uname -s)" = Linux ]; then
    case "$(stat -f -c %T "$bench_dir")" in
        tmpfs | ramfs) die "refusing $bench_dir: it is tmpfs/ramfs (syncs are free there); pass --bench-dir on a disk" ;;
    esac
fi
tmp_dir="$bench_dir/tmp"
json_dir="$bench_dir/json"
log_dir="$bench_dir/logs"

# Worktrees this run added: removed at exit unless --keep.
added=()
cleanup() {
    rm -rf "$tmp_dir" 2>/dev/null || true
    if [ "$keep" = 0 ] && [ "${#added[@]}" -gt 0 ]; then
        for dir in "${added[@]}"; do
            git -C "$repo" worktree remove --force "$dir" >/dev/null 2>&1 || rm -rf "$dir"
        done
        git -C "$repo" worktree prune >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
mkdir -p "$log_dir" "$json_dir"
unset CARGO_TARGET_DIR

add_worktree() { # <side> <commit>: a fresh detached checkout of the commit at wt-<side>
    local dir="$bench_dir/wt-$1"
    if [ -e "$dir" ] || [ -L "$dir" ]; then # a kept or interrupted one of an earlier run
        git -C "$repo" worktree remove --force "$dir" >/dev/null 2>&1 || rm -rf "$dir"
        git -C "$repo" worktree prune
    fi
    git -C "$repo" worktree add --detach "$dir" "$2" >"$log_dir/worktree-$1.log" 2>&1 \
        || die "git worktree add of $1 ($(short "$2")) failed: $(tail -3 "$log_dir/worktree-$1.log")"
    added+=("$dir")
    [ "$(git -C "$dir" rev-parse HEAD)" = "$2" ] || die "wt-$1 is not at $(short "$2")"
}
echo "checking out base $base_text..." >&2
add_worktree base "$base_commit"
base_checkout="$bench_dir/wt-base"
if [ "$worktree" = 1 ]; then
    head_checkout="$repo"
else
    echo "checking out head $head_text..." >&2
    add_worktree head "$head_commit"
    head_checkout="$bench_dir/wt-head"
fi
bench_src="$head_checkout/$bench_crate"

prepare() { # <side> <checkout>: the side's standalone build dir, from head's bench and config
    local dir="$bench_dir/build-$1" file
    mkdir -p "$dir"
    rm -rf "$dir/src" "$dir/build.rs" "$dir/engine" "$dir/.cargo" "$dir/Cargo.toml" \
        "$dir/Cargo.lock" "$dir/rust-toolchain" "$dir/rust-toolchain.toml"
    cp -R "$bench_src/src" "$dir/src"
    [ ! -f "$bench_src/build.rs" ] || cp "$bench_src/build.rs" "$dir/build.rs"
    ln -s "$2" "$dir/engine"
    python3 -I "$helper" manifest "$bench_src/Cargo.toml" "$head_checkout/Cargo.toml" "$dir/Cargo.toml"
    cp "$head_checkout/Cargo.lock" "$dir/Cargo.lock"
    for file in .cargo/config.toml .cargo/config rust-toolchain.toml rust-toolchain; do
        if [ -f "$head_checkout/$file" ]; then
            mkdir -p "$(dirname "$dir/$file")"
            cp "$head_checkout/$file" "$dir/$file"
        fi
    done
}
build() { # <side>: builds the side's bench
    local dir="$bench_dir/build-$1" log="$log_dir/build-$1.log"
    echo "building the $1 bench..." >&2
    if ! (cd "$dir" && CARGO_TARGET_DIR="$dir/target" cargo build --release) >"$log" 2>&1; then
        tail -n 30 "$log" >&2
        die "the $1 build failed (see $log); both sides build head's bench source, so a $1 whose tn-lmdb lacks an API the bench uses fails here"
    fi
    [ -x "$dir/target/release/tn-lmdb-bench" ] || die "the $1 build has no tn-lmdb-bench binary (see $log)"
}
prepare base "$base_checkout"
prepare head "$head_checkout"
build base
build head
bin_base="$bench_dir/build-base/target/release/tn-lmdb-bench"
bin_head="$bench_dir/build-head/target/release/tn-lmdb-bench"
if cmp -s "$bin_base" "$bin_head" && [ "$allow_same" = 0 ]; then
    echo "the base $base_text and head $head_text bench binaries are identical: what differs between their tn-lmdb ($engine_dir, the root Cargo.toml) does not reach the binary: nothing to compare"
    exit 0
fi

run_args=()
[ -z "$tier" ] || run_args+=(--tier "$tier")
[ -z "$rows" ] || run_args+=(--rows "$rows")
[ -z "$sync" ] || run_args+=(--sync "$sync")
[ -z "$n" ] || run_args+=(--n "$n")
[ -z "$reps" ] || run_args+=(--reps "$reps")
busy_builds() { # the running rustc/cargo processes, as "name xN, ..."
    ps -A -o comm= 2>/dev/null | awk -F/ '{print $NF}' \
        | grep -E '^(rustc|cargo|cargo-[A-Za-z0-9-]+|clippy-driver)$' | sort | uniq -c \
        | awk '{printf "%s%s x%s", sep, $2, $1; sep = ", "}' || true
}
order="base head"
round=2
while [ "$round" -le "$rounds" ]; do
    if [ $((round % 2)) = 0 ]; then order="$order, head base"; else order="$order, base head"; fi
    round=$((round + 1))
done
{
    printf 'date\t%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'host\t%s\n' "$(uname -n | cut -d. -f1)"
    printf 'rustc\t%s\n' "$(cd "$bench_dir/build-head" && rustc -V)"
    printf 'base\t%s\n' "$base_text"
    printf 'base_subject\t%s\n' "$(subject "$base_commit")"
    printf 'head\t%s\n' "$head_text"
    printf 'head_subject\t%s\n' "$(subject "$head_commit")"
    printf 'head_changes\t%s\n' "$head_changes"
    printf 'rows\t%s\n' "$rows"
    printf 'sync\t%s\n' "$sync"
    printf 'rounds\t%s\n' "$rounds"
    printf 'order\t%s\n' "$order"
    printf 'threshold\t%s\n' "$threshold"
    printf 'size_threshold\t%s\n' "$size_threshold"
    printf 'gate_durable\t%s\n' "$gate_durable"
    printf 'busy\t%s\n' "$(busy_builds)"
} >"$bench_dir/meta.tsv"
rm -f "$json_dir"/*.json "$bench_dir/report.md"
round=1
while [ "$round" -le "$rounds" ]; do
    if [ $((round % 2)) = 1 ]; then sides="base head"; else sides="head base"; fi
    for side in $sides; do
        rm -rf "$tmp_dir"
        mkdir "$tmp_dir"
        log="$log_dir/$side-$round.log"
        if [ "$side" = base ]; then bin="$bin_base"; else bin="$bin_head"; fi
        echo "round $round/$rounds: $side..." >&2
        started=$SECONDS
        TMPDIR="$tmp_dir" "$bin" ${run_args[@]+"${run_args[@]}"} --label "$side-$round" \
            --out "$json_dir/$side-$round.json" >"$log" 2>&1 || {
            tail -n 20 "$log" >&2
            die "the $side run of round $round failed (see $log)"
        }
        echo "round $round/$rounds: $side done in $((SECONDS - started)) s" >&2
    done
    round=$((round + 1))
done

status=0
python3 -I "$helper" report "$bench_dir" || status=$?
[ "$status" = 2 ] || echo "report: $bench_dir/report.md"
exit "$status"
