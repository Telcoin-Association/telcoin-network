#!/usr/bin/env bash
# The busy-node execution bench (`tn-engine`'s `exec_bench` test binary) on two builds, run as
# interleaved pairs over several load profiles and aggregated into one report in the repo root
# (`reth-exec-<host>-<time>.txt`). See tn-reth's README, "Execution benchmark", and the bench's
# module docs (`crates/engine/tests/exec_bench/main.rs`) for its knobs and report.
#
# Usage: etc/reth-exec-bench.sh <dir on the disk under test> [before-commit]
#   Backend mode (no commit): this checkout (uncommitted changes included) built twice, MDBX (the
#                    default features) against tndb-b (`--features tn-reth/tndb-b`).
#   Commit mode (a before-commit): one backend, the commit against this checkout. The commit must
#                    have the bench (`crates/engine/tests/exec_bench/main.rs`).
#   EXEC_BACKEND=tndb-b  commit mode's backend: `mdbx` or `tndb-b` (the default)
#   EXEC_ROUNDS=2    pairs per profile; round 1 runs A then B, round 2 B then A, and so on (the
#                    default 2 is A B B A, 1 is A B), where A is mdbx or before and B tndb-b or after
#   EXEC_PROFILES="busy busy-readers peak busy-metrics"  the profiles to run, in order:
#                    busy          the bench's defaults
#                    busy-readers  TN_EXEC_BENCH_READERS=4
#                    peak          TN_EXEC_BENCH_BATCHES=20 TN_EXEC_BENCH_GAS=30000000
#                    busy-metrics  TN_EXEC_BENCH_METRICS=prom TN_EXEC_BENCH_READERS=4
#                  and two more, run only when named:
#                    busy-epoch    TN_EXEC_BENCH_EPOCH_EVERY=50 (an epoch close every 50 outputs)
#                    busy-export   TN_EXEC_BENCH_EXPORT=1 (a state-pack export beside the writer)
#   EXEC_ALLOW_CARGO=1  time the runs even while rustc/cargo processes run (the report says so)
#   Bench knobs (TN_EXEC_BENCH_*) pass through the environment to every run; a profile's own
#   settings win over them. The script sets TN_EXEC_BENCH_CACHE_DIR and TN_EXEC_BENCH_REPORT
#   itself, per run, over any in the environment.
#
# Each test binary is run directly (not through cargo), its md5 and test count recorded, and the
# run refused if the two binaries match or either lacks the ignored `exec_bench` test. Each build
# has its own target dir under the bench dir: cargo names a workspace's crates by their path within
# it and judges freshness by mtime, so two trees or two feature sets sharing one would reuse or
# rebuild each other's artifacts. The before tree is an export of the commit (`git archive`; no
# git writes) with this checkout's `tn-contracts` submodule linked in. Before the timed runs the
# script refuses to start while rustc or cargo run. `TMPDIR`, the cache dir and the JSON report
# path are set on each run's command line, never exported. A profile whose run fails is named in
# the report and its remaining runs skipped; the other profiles still run, and the script exits 1.
#
# The report has the host, the knobs each run's JSON header records, the binaries, one line per
# run, and per profile: each side's median over the rounds of the total's p50, p90, p99 and mean,
# each stage's mean, Mgas/s, tx/s and the database growth per output (allocated and logical);
# with readers on, the writer's total mean with them on over off and each read's p50 and p99; with
# epoch closes, the closing and normal outputs' total means; with an export, its duration, the
# database growth while it held its read and the writer's total mean beside it over without; each
# pair's B/A ratio of those; and the median ratio with its spread (min..max). On macOS a durable commit is
# `F_FULLFSYNC` for libmdbx but a plain fsync for tndb-b, so backend-mode durable commit times
# there are not like-for-like; the report says so.
#
# Needs a Rust toolchain, git, python3, and room in the bench dir for two release builds plus the
# bench's cached bases. In the bench dir the script writes only:
#   target-mdbx/, target-tndb-b/           this checkout's builds (commit mode's after side too)
#   target-before-<commit>-<backend>/      commit mode's before build
#   build-<label>.log                      each build's output
#   cache/ (backend mode), cache-before-<commit>/ and cache-after/ (commit mode)
#                                          the bench's cached bases, kept for reruns; remove one
#                                          when the bench refuses its base as built by other code
#   reports/<profile>-<side>-<round>.json  each run's JSON report, replaced by a rerun
#   logs/<profile>-<side>-<round>.log      each run's output, replaced by a rerun
#   src-before/ and tmp/                   the before export and the runs' TMPDIR, each marked as
#                                          the script's own and removed on exit
# It removes nothing else there. A refusal or a failed build before the first run leaves no report.
set -euo pipefail

die() { echo "$*" >&2; exit 1; }
usage="usage: $0 <dir on the disk under test> [before-commit]"
bench_dir="${1:?$usage}"
before="${2:-}"
[ $# -le 2 ] || die "$usage"
[ ! -e "$bench_dir" ] || [ -d "$bench_dir" ] || die "refusing $bench_dir: not a directory"
mkdir -p "$bench_dir"
bench_dir="$(cd "$bench_dir" && pwd -P)"
[ "$bench_dir" != / ] || die "refusing / as the bench dir"
repo="$(cd "$(dirname "$0")" && git rev-parse --show-toplevel)"
host="$(uname -n | cut -d. -f1)"
report="$repo/reth-exec-$host-$(date +%Y%m%d-%H%M%S).txt"
bench_file="crates/engine/tests/exec_bench/main.rs"

rounds="${EXEC_ROUNDS:-2}"
case "$rounds" in '' | *[!0-9]* | 0*) die "EXEC_ROUNDS must be a positive integer, not '$rounds'" ;; esac
profile_settings() { # <profile>: prints the profile's TN_EXEC_BENCH_* assignments
    case "$1" in
        busy) echo "" ;;
        busy-readers) echo "TN_EXEC_BENCH_READERS=4" ;;
        peak) echo "TN_EXEC_BENCH_BATCHES=20 TN_EXEC_BENCH_GAS=30000000" ;;
        busy-metrics) echo "TN_EXEC_BENCH_METRICS=prom TN_EXEC_BENCH_READERS=4" ;;
        busy-epoch) echo "TN_EXEC_BENCH_EPOCH_EVERY=50" ;;
        busy-export) echo "TN_EXEC_BENCH_EXPORT=1" ;;
        *) return 1 ;;
    esac
}
read -r -a profiles <<<"${EXEC_PROFILES:-busy busy-readers peak busy-metrics}"
[ "${#profiles[@]}" -gt 0 ] || die "EXEC_PROFILES is empty"
for profile in "${profiles[@]}"; do
    profile_settings "$profile" >/dev/null \
        || die "unknown profile '$profile' (busy, busy-readers, peak, busy-metrics, busy-epoch, busy-export)"
done
command -v python3 >/dev/null || die "python3 is needed for the report"

if [ "$(uname -s)" = "Linux" ]; then
    case "$(stat -f -c %T "$bench_dir")" in
        tmpfs | ramfs) die "refusing $bench_dir: it is tmpfs/ramfs (syncs are free there)" ;;
    esac
fi
[ -f "$repo/tn-contracts/artifacts/ConsensusRegistry.json" ] \
    || die "tn-contracts is not checked out (git submodule update --init)"
[ -f "$repo/$bench_file" ] || die "this checkout has no $bench_file"

# A src-before or tmp this script did not create (no marker) is refused, never removed.
marker=".reth-exec-bench"
src_before="$bench_dir/src-before"
tmp_dir="$bench_dir/tmp"
for dir in "$src_before" "$tmp_dir"; do
    if [ -e "$dir" ] || [ -L "$dir" ]; then
        [ -d "$dir" ] && [ ! -L "$dir" ] && [ -f "$dir/$marker" ] \
            || die "refusing to remove $dir: this script did not create it"
    fi
done

if [ -z "$before" ]; then
    mode="backend"
    side_a="mdbx"
    side_b="tndb-b"
    tree_a="$repo"
    tree_b="$repo"
    flags_a=""
    flags_b="--features tn-reth/tndb-b"
    target_a="$bench_dir/target-mdbx"
    target_b="$bench_dir/target-tndb-b"
    cache_a="$bench_dir/cache"
    cache_b="$bench_dir/cache"
else
    mode="commit"
    backend="${EXEC_BACKEND:-tndb-b}"
    case "$backend" in
        mdbx) flags_a="" ;;
        tndb-b) flags_a="--features tn-reth/tndb-b" ;;
        *) die "EXEC_BACKEND must be mdbx or tndb-b, not '$backend'" ;;
    esac
    git -C "$repo" cat-file -e "$before^{commit}" 2>/dev/null || die "no commit $before"
    git -C "$repo" cat-file -e "$before:$bench_file" 2>/dev/null \
        || die "$before has no $bench_file: commit mode needs a commit that has the bench"
    before_key="$(git -C "$repo" rev-parse --short "$before")"
    side_a="before"
    side_b="after"
    tree_a="$src_before"
    tree_b="$repo"
    flags_b="$flags_a"
    target_a="$bench_dir/target-before-$before_key-$backend"
    target_b="$bench_dir/target-$backend"
    cache_a="$bench_dir/cache-before-$before_key"
    cache_b="$bench_dir/cache-after"
fi

# A run refused or failed before its first timed run leaves no report.
runs_started=0
cleanup() {
    rm -rf "$src_before" "$tmp_dir" 2>/dev/null || true
    [ "$runs_started" = 1 ] || rm -f "$report"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
for dir in "$src_before" "$tmp_dir"; do
    rm -rf "$dir"
done
mkdir "$tmp_dir"
: >"$tmp_dir/$marker"
mkdir -p "$bench_dir/reports" "$bench_dir/logs" "$cache_a" "$cache_b"
if [ "$mode" = commit ]; then
    mkdir "$src_before"
    : >"$src_before/$marker"
    git -C "$repo" archive "$before" | tar -x -C "$src_before"
    rm -rf "$src_before/tn-contracts"
    ln -s "$repo/tn-contracts" "$src_before/tn-contracts"
fi
unset CARGO_TARGET_DIR

kv() { printf '%-18s %s\n' "$1:" "$2" >>"$report"; }
digest() {
    if command -v md5sum >/dev/null; then md5sum "$1" | cut -d' ' -f1; else md5 -q "$1"; fi
}
describe_checkout() {
    local changes=""
    git -C "$repo" diff --quiet HEAD -- crates || changes=" + uncommitted changes in crates/"
    [ -z "$(git -C "$repo" ls-files --others --exclude-standard -- crates | head -1)" ] \
        || changes="$changes + untracked files in crates/"
    echo "$(git -C "$repo" rev-parse --short HEAD) ($(git -C "$repo" log -1 --format=%s HEAD))$changes"
}
{
    echo "### RETH-EXEC v1"
    echo
    echo "### SYSTEM"
} >"$report"
kv date "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
kv host "$(uname -a)"
if [ "$(uname -s)" = "Linux" ]; then
    kv cpu "$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ //') x $(nproc)"
    kv memory "$(awk '/MemTotal/ {printf "%.1f GiB", $2 / 1048576}' /proc/meminfo)"
    part="$(df -P "$bench_dir" | awk 'NR == 2 {print $1}')"
    kv mount "$part $(stat -f -c %T "$bench_dir")"
    dev="$(lsblk -no PKNAME "$part" 2>/dev/null | head -1 || true)"
    [ -n "$dev" ] && kv device "$(lsblk -dno NAME,MODEL,SIZE "/dev/$dev" 2>/dev/null || echo "$dev")"
    probe="$tmp_dir/sync-probe"
    kv "dsync 4k x2000" "$(dd if=/dev/zero of="$probe" bs=4k count=2000 oflag=dsync 2>&1 | tail -1)"
    rm -f "$probe"
elif [ "$(uname -s)" = "Darwin" ]; then
    kv cpu "$(sysctl -n machdep.cpu.brand_string) x $(sysctl -n hw.ncpu)"
    kv memory "$(sysctl -n hw.memsize | awk '{printf "%.1f GiB", $1 / 1073741824}')"
    kv mount "$(df -P "$bench_dir" | awk 'NR == 2 {print $1 " on " $6}')"
fi
kv rustc "$(rustc -V)"
kv "bench dir" "$bench_dir"
{
    echo
    echo "### CONFIG"
} >>"$report"
if [ "$mode" = backend ]; then
    kv mode "backend: mdbx (A, default features) against tndb-b (B, --features tn-reth/tndb-b)"
    kv checkout "$(describe_checkout)"
else
    kv mode "commit: before (A) against after (B), backend $backend"
    kv before "$before ($(git -C "$repo" log -1 --format=%s "$before"))"
    kv after "$(describe_checkout)"
    kv change "$(git -C "$repo" diff --stat "$before" -- crates | tail -1)"
    if cmp -s "$src_before/$bench_file" "$repo/$bench_file"; then
        kv "before bench" "identical to this checkout's"
    else
        kv "before bench" "DIFFERS from this checkout's (a changed timed region reads as a change)"
    fi
fi
order="$side_a $side_b"
round=2
while [ "$round" -le "$rounds" ]; do
    if [ $((round % 2)) = 0 ]; then order="$order, $side_b $side_a"; else order="$order, $side_a $side_b"; fi
    round=$((round + 1))
done
kv rounds "$rounds ($order per profile)"
kv profiles "${profiles[*]}"
caller_knobs="$(env | grep -E '^TN_EXEC_BENCH_' | grep -vE '^TN_EXEC_BENCH_(CACHE_DIR|REPORT)=' \
    | sort | tr '\n' ' ' | sed 's/ $//' || true)"
kv "caller knobs" "${caller_knobs:-(none)}"
for profile in "${profiles[@]}"; do
    kv "profile $profile" "$(profile_settings "$profile" | sed 's/^$/(the bench defaults)/')"
done
kv "cache dirs" "$side_a $cache_a, $side_b $cache_b"
if [ "$(uname -s)" = "Darwin" ]; then
    kv "sync note" "macOS: libmdbx's durable commit is F_FULLFSYNC, tndb-b's a plain fsync; durable commit times are not like-for-like between the backends"
fi

build() { # <label> <source tree> <target dir> <cargo flags>: prints the test binary's path
    local flags log="$bench_dir/build-$1.log" bin
    read -r -a flags <<<"$4"
    echo "building the $1 test binary..." >&2
    (cd "$2" && CARGO_TARGET_DIR="$3" cargo test --locked --release -p tn-engine --test exec_bench \
        ${flags[@]+"${flags[@]}"} --no-run) >"$log" 2>&1 || {
        tail -30 "$log" >&2
        die "build of $1 failed; see $log"
    }
    bin="$(grep -oE 'Executable tests/exec_bench/main.rs \([^)]+\)' "$log" \
        | tail -1 | sed -E 's/.*\((.*)\)/\1/')"
    case "$bin" in /*) ;; *) bin="$2/$bin" ;; esac
    [ -x "$bin" ] || die "no test binary found for $1 (see $log)"
    echo "$bin"
}
if [ "$mode" = backend ]; then
    label_a="mdbx"
    label_b="tndb-b"
else
    label_a="before-$before_key-$backend"
    label_b="after-$backend"
fi
bin_a="$(build "$label_a" "$tree_a" "$target_a" "$flags_a")"
bin_b="$(build "$label_b" "$tree_b" "$target_b" "$flags_b")"
tests() { "$1" --list 2>/dev/null | grep -c ': test$' || true; }
md5_a="$(digest "$bin_a")"
md5_b="$(digest "$bin_b")"
{
    echo
    echo "### BINARIES"
} >>"$report"
kv "$side_a" "$bin_a $md5_a ($(tests "$bin_a") tests)"
kv "$side_b" "$bin_b $md5_b ($(tests "$bin_b") tests)"
[ "$md5_a" != "$md5_b" ] || die "the $side_a and $side_b test binaries are identical: refusing to compare"
# A filter that matches no test would run nothing and pass: require the bench in both binaries.
for bin in "$bin_a" "$bin_b"; do
    [ "$("$bin" exec_bench --exact --ignored --list 2>/dev/null | grep -c ': test$' || true)" = 1 ] \
        || die "$bin has no ignored test named exec_bench: refusing to compare"
done

busy_builds() { # the running rustc/cargo processes, as "name xN, ..."
    ps -A -o comm= 2>/dev/null | awk -F/ '{print $NF}' \
        | grep -E '^(rustc|cargo|cargo-[A-Za-z0-9-]+|clippy-driver)$' | sort | uniq -c \
        | awk '{printf "%s%s x%s", sep, $2, $1; sep = ", "}' || true
}
busy="$(busy_builds)"
if [ -n "$busy" ]; then
    [ "${EXEC_ALLOW_CARGO:-}" = 1 ] \
        || die "refusing to time runs while rustc/cargo run ($busy); wait, or set EXEC_ALLOW_CARGO=1"
    echo "warning: timing runs while rustc/cargo run ($busy): EXEC_ALLOW_CARGO=1" >&2
    kv "WARNING" "runs timed while rustc/cargo ran ($busy): EXEC_ALLOW_CARGO=1"
fi

runs_started=1
{
    echo
    echo "### RUNS"
} >>"$report"
manifest="$tmp_dir/runs.tsv"
: >"$manifest"
run() { # <profile> <side>: runs the side's binary once with the profile's settings
    local profile="$1" side="$2" bin tree cache settings name json log start when load status=0
    local busy_now
    if [ "$side" = "$side_a" ]; then bin="$bin_a" tree="$tree_a" cache="$cache_a"; else
        bin="$bin_b" tree="$tree_b" cache="$cache_b"
    fi
    read -r -a settings <<<"$(profile_settings "$profile")"
    name="$profile-$side-$round"
    json="$bench_dir/reports/$name.json"
    log="$bench_dir/logs/$name.log"
    rm -f "$json"
    busy_now="$(busy_builds)"
    when="$(date -u +%H:%M:%SZ)"
    load="$(uptime | sed 's/.*load average[s]*: //' | tr -d ',')"
    echo "running $profile on $side (round $round)..." >&2
    start=$SECONDS
    # From the crate dir, as cargo runs it; the settings go on this command line only.
    (cd "$tree/crates/engine" && env TMPDIR="$tmp_dir" TN_EXEC_BENCH_CACHE_DIR="$cache" \
        TN_EXEC_BENCH_REPORT="$json" ${settings[@]+"${settings[@]}"} \
        "$bin" --ignored --exact exec_bench --nocapture) >"$log" 2>&1 || status=$?
    [ "$status" = 0 ] && [ ! -f "$json" ] && status="no-json"
    printf '%-14s %-7s round %s  %s  load %-16s %5ss  %s%s\n' "$profile" "$side" "$round" \
        "$when" "$load" "$((SECONDS - start))" "$([ "$status" = 0 ] && echo ok || echo "FAILED ($status)")" \
        "${busy_now:+  (rustc/cargo running: $busy_now)}" >>"$report"
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$profile" "$side" "$round" "$status" "$json" "$log" \
        >>"$manifest"
    if [ "$status" != 0 ]; then
        echo "profile $profile: $side round $round failed ($status); see $log" >&2
        tail -5 "$log" >&2
        return 1
    fi
}
failed=""
for profile in "${profiles[@]}"; do
    round=1
    while [ "$round" -le "$rounds" ]; do
        if [ $((round % 2)) = 1 ]; then first="$side_a" second="$side_b"; else
            first="$side_b" second="$side_a"
        fi
        if ! run "$profile" "$first" || ! run "$profile" "$second"; then
            failed="$failed $profile"
            echo "profile $profile: skipping its remaining runs" >&2
            break
        fi
        round=$((round + 1))
    done
done

# Std-lib Python, read from stdin and run isolated (-I), so nothing beside it is imported.
python3 -I - "$manifest" "$side_a" "$side_b" >>"$report" <<'PY'
import json
import statistics
import sys

manifest, side_a, side_b = sys.argv[1:4]
# The bench's stage keys under `total`, in its order, with their depth in its stage tree.
STAGES = {
    "execute": 1, "signer_recovery": 2, "finish": 1, "hashed_state": 2, "state_root": 2,
    "overlay_extend": 1, "save_blocks": 1, "save_blocks_mdbx": 2, "insert_block": 3,
    "write_state": 3, "write_hashed_state": 3, "write_trie_updates": 3,
    "update_history_indices": 3, "save_blocks_sf": 2, "commit_sf": 1, "commit_mdbx": 1, "other": 1,
}

runs = []
with open(manifest) as f:
    for line in f:
        profile, side, rnd, status, path, log = line.rstrip("\n").split("\t")
        runs.append(dict(profile=profile, side=side, round=int(rnd), status=status, json=path,
                         log=log))


def load(path):
    with open(path) as f:
        return json.load(f)


def metrics(doc):
    """The report's metrics of one run, as (label, value) in order."""
    stats = doc["stats"]
    total = stats.get("total", {})
    out = [("total p50 ms", total.get("p50_ms")), ("total p90 ms", total.get("p90_ms")),
           ("total p99 ms", total.get("p99_ms")), ("total mean ms", total.get("mean_ms"))]
    keys = [k for k in STAGES if k in stats] + sorted(k for k in stats
                                                      if k not in STAGES and k != "total")
    out += [(f"{'  ' * STAGES.get(k, 1)}{k} mean ms", stats[k].get("mean_ms")) for k in keys]
    throughput = doc.get("throughput", {})
    out += [("Mgas/s", throughput.get("mgas_per_s")), ("tx/s", throughput.get("tx_per_s"))]
    sizes = doc.get("sizes", {})
    # Allocated growth moves in a backend's own steps (tndb-b's segments, MDBX's geometry); the
    # logical growth (table bytes, report v2) compares the backends
    out += [("db growth/output KiB", kib(sizes.get("db_growth_per_output_bytes"))),
            ("db logical growth/output KiB", kib(sizes.get("db_logical_growth_per_output_bytes")))]
    side = doc.get("side_load")  # report v2: readers and the submitter beside the writer
    if side:
        out.append(("writer total on/off", split(side, "mean_ratio")))
        out.append(("side errors", side.get("errors")))
        for kind, reads in side.get("readers", {}).items():
            out += [(f"  {kind} p50 ms", reads.get("p50_ms")),
                    (f"  {kind} p99 ms", reads.get("p99_ms"))]
    closes = doc.get("epoch_closes")  # report v3
    if closes:
        total = closes.get("writer", {}).get("total", {})
        out += [("closing total mean ms", (total.get("closing") or {}).get("mean_ms")),
                ("normal total mean ms", (total.get("normal") or {}).get("mean_ms"))]
    export = doc.get("export")  # report v3
    if export:
        out += [("export s", export.get("duration_s")),
                # allocated, not length: MDBX's length is its preallocated geometry
                ("db growth while pinned KiB", kib(export.get("db_allocated_growth_bytes"))),
                ("writer total export/idle", split(export, "mean_ratio"))]
    return out


def kib(value):
    return None if value is None else value / 1024


def split(section, ratio):
    """A side load's or export's ratio of the writer's total, on over off."""
    return section.get("writer", {}).get("total", {}).get(ratio)


def fmt(value):
    if value is None:
        return "-"
    magnitude = abs(value)
    if magnitude >= 1000:
        return f"{value:.0f}"
    if magnitude >= 100:
        return f"{value:.1f}"
    if magnitude >= 1:
        return f"{value:.2f}"
    return f"{value:.3f}"


def median(values):
    values = [v for v in values if v is not None]
    return statistics.median(values) if values else None


def knob_text(knobs):
    return " ".join(f"{k}={v}" for k, v in knobs.items() if k not in ("report", "cache_dir"))


profiles = []
for run in runs:
    if run["profile"] not in profiles:
        profiles.append(run["profile"])
for profile in profiles:
    print()
    print(f"### PROFILE {profile}")
    mine = [r for r in runs if r["profile"] == profile]
    for r in mine:
        if r["status"] != "0":
            print(f"FAILED: {r['side']} round {r['round']} ({r['status']}); see {r['log']}")
    docs = {}  # (side, round) -> JSON
    for r in mine:
        if r["status"] == "0":
            docs[(r["side"], r["round"])] = load(r["json"])
    for side in (side_a, side_b):
        headers = [(rnd, doc["header"]) for (s, rnd), doc in sorted(docs.items()) if s == side]
        if not headers:
            print(f"{side}: no completed run")
            continue
        first = headers[0][1]
        print(f"{side}: backend={first.get('backend')} sync={first.get('sync')} "
              f"metrics={first.get('metrics')} pacing={first.get('pacing')}")
        print(f"{side} knobs: {knob_text(first.get('knobs', {}))}")
        for rnd, header in headers[1:]:
            if knob_text(header.get("knobs", {})) != knob_text(first.get("knobs", {})):
                print(f"{side} round {rnd} KNOBS DIFFER: {knob_text(header.get('knobs', {}))}")
    sides_knobs = {knob_text(doc["header"].get("knobs", {})) for doc in docs.values()}
    if len(sides_knobs) > 1:
        print(f"KNOBS DIFFER between runs of {profile}: the ratios compare different loads")
    pairs = sorted(rnd for (s, rnd) in docs if s == side_a and (side_b, rnd) in docs)
    if not pairs:
        print("no complete pair: no aggregation")
        continue
    table = {}  # label -> {side: [values per round]}
    labels = []
    for (side, rnd), doc in sorted(docs.items(), key=lambda item: item[0][1]):
        for label, value in metrics(doc):
            if label not in table:
                table[label] = {side_a: {}, side_b: {}}
                labels.append(label)
            table[label][side][rnd] = value
    ratio_head = f"{side_b}/{side_a} median [min..max]"
    print(f"pairs: {len(pairs)} (rounds {', '.join(str(p) for p in pairs)}); "
          f"medians over each side's rounds; ratio = {side_b} / {side_a} per pair")
    print(f"{'metric':<34} {side_a + ' med':>11} {side_b + ' med':>11}  {ratio_head:<30} per pair")
    for label in labels:
        a, b = table[label][side_a], table[label][side_b]
        ratios = []
        for rnd in pairs:
            va, vb = a.get(rnd), b.get(rnd)
            ratios.append(vb / va if va and vb is not None else None)
        valid = [x for x in ratios if x is not None]
        spread = (f"{statistics.median(valid):.3f} [{min(valid):.3f}..{max(valid):.3f}]"
                  if valid else "-")
        per_pair = " ".join("-" if x is None else f"{x:.3f}" for x in ratios)
        print(f"{label:<34} {fmt(median(a.values())):>11} {fmt(median(b.values())):>11}  "
              f"{spread:<30} {per_pair}")

print()
print("### FILES")
for run in runs:
    state = "" if run["status"] == "0" else f"  (FAILED: {run['status']})"
    json_path = run["json"] if run["status"] == "0" else "-"
    print(f"{run['profile']} {run['side']} round {run['round']}: {json_path}  log {run['log']}{state}")
PY
{
    echo
    echo "### END $(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >>"$report"
echo "report: $report"
[ -z "$failed" ] || die "failed profiles:$failed (see the report)"
