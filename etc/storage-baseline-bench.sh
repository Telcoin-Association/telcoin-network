#!/usr/bin/env bash
# Run the consensus-DB baseline benches (tn-storage `workload_*`) on this machine and write one
# report file (system, disk, a raw sync probe, then each workload's tables), for comparing the
# backends on the production OS. Written for Linux; also runs on macOS.
#
# Usage: etc/storage-baseline-bench.sh <empty dir on the disk under test> [report file]
#   BASELINE_QUICK=1 ...    one pass with only the columns the backend decision needs (Split
#                           tndb, tndb group, production MDBX); skips the reference columns
#                           that sync every write, which dominate the run time on a disk whose
#                           fsync flushes its cache (most Linux setups), and runs 3 compaction-cost
#                           pairs instead of 6. About 30-40 minutes on a laptop NVMe.
#   BASELINE_ROUNDS=1 ...   runs each workload once instead of three times (default 3; 1 if
#                           BASELINE_QUICK)
#   BASELINE_ONLY="workload_mixed"
#                           runs only the named workloads (space separated)
#
# The knobs carry this script's prefix so a generic QUICK, ROUNDS or ONLY exported by another
# script is never picked up; the report records the values used.
#
# Every run also ends with `consensus_pack_workload` (tn-storage `pack_bench`): production-shaped
# consensus outputs saved and made durable at 2 a second while readers query the pack, at N=10 and
# N=50 with 16 KB and 256 KB batches (about 7 minutes). TN_BENCH_PACK_RATE / TN_BENCH_PACK_OUTPUTS /
# TN_BENCH_PACK_BATCH_KB / TN_BENCH_PACK_COMMITTEE / TN_BENCH_PACK_MAX_MB / TN_BENCH_PACK_READ_SECS
# override it (see the bench's docs); BASELINE_ONLY="consensus_pack_workload" runs it alone.
#
# The compaction-cost pairs (`workload_mixed_compaction`) run at a busy node's batch rate, 1 MB
# batches 25 times a second, for 6000 rounds a run: long enough for the batch log to reach its
# compaction threshold a few times. TN_BENCH_PAIR_BATCH_KB / TN_BENCH_PAIR_RATE /
# TN_BENCH_PAIR_ROUNDS / TN_BENCH_PAIRS override them; e.g. TN_BENCH_PAIR_RATE=0
# TN_BENCH_PAIR_BATCH_KB=256 TN_BENCH_PAIR_ROUNDS=300 for the drive-saturating burst (batches as
# fast as taken), far beyond production rates.
#
# Needs this checkout's tn-storage with the tndb work (an uncommitted tree can be copied over
# with `rsync -a --exclude target <repo>/ <host>:<repo>/`), a Rust toolchain, and a few GB free in
# the bench directory. The benches create their databases under TMPDIR, set here to that
# directory: on Linux /tmp is often tmpfs (RAM), where every sync is free, so tmpfs is refused.
# A full run takes roughly 1-2 hours; keep the machine otherwise idle.
set -euo pipefail

bench_dir="${1:?usage: $0 <dir on the disk under test> [report file]}"
mkdir -p "$bench_dir"
bench_dir="$(cd "$bench_dir" && pwd)"
repo="$(cd "$(dirname "$0")/.." && pwd)"
report="${2:-$repo/storage-baseline-$(hostname -s)-$(date +%Y%m%d-%H%M%S).txt}"
quick="${BASELINE_QUICK:-}"
# The compaction-cost pairs at a busy node's batch rate (see the usage above).
export TN_BENCH_PAIR_BATCH_KB="${TN_BENCH_PAIR_BATCH_KB:-1024}" \
    TN_BENCH_PAIR_RATE="${TN_BENCH_PAIR_RATE:-25}" \
    TN_BENCH_PAIR_ROUNDS="${TN_BENCH_PAIR_ROUNDS:-6000}"
if [ -n "$quick" ]; then
    rounds="${BASELINE_ROUNDS:-1}"
    # Fewer compaction-cost pairs.
    export TN_BENCH_PAIRS="${TN_BENCH_PAIRS:-3}"
else
    rounds="${BASELINE_ROUNDS:-3}"
fi
workloads="${BASELINE_ONLY:-workload_consensus_rounds workload_batch_cache workload_mixed workload_compaction}"
os="$(uname -s)"

if [ "$os" = "Linux" ]; then
    fstype="$(stat -f -c %T "$bench_dir")"
    if [ "$fstype" = "tmpfs" ] || [ "$fstype" = "ramfs" ]; then
        echo "error: $bench_dir is on $fstype; give a directory on the disk under test" >&2
        exit 1
    fi
fi
if [ ! -d "$repo/crates/storage/src/tndb" ]; then
    echo "error: $repo has no crates/storage/src/tndb; copy over the tree with the tndb work" >&2
    exit 1
fi

section() { printf '\n### %s\n' "$*" >>"$report"; }
kv() { printf '%-16s %s\n' "$1:" "$2" >>"$report"; }
try() { "$@" 2>/dev/null || echo "?"; }

cd "$repo"
echo "building the tn-storage bench binary (release, all features)..."
build_log="$(mktemp)"
if ! cargo test --release -p tn-storage --all-features --lib --no-run >"$build_log" 2>&1; then
    tail -30 "$build_log" >&2
    exit 1
fi
# The test binary cargo reports (under any CARGO_TARGET_DIR), made absolute.
bin="$(grep -oE 'Executable unittests src/lib.rs \([^)]+\)' "$build_log" | tail -1 |
    sed -E 's/.*\((.*)\)/\1/' || true)"
case "$bin" in /*) ;; *) bin="$repo/$bin" ;; esac
rm -f "$build_log"
if [ ! -x "$bin" ]; then
    echo "error: no tn-storage test binary found in cargo's output" >&2
    exit 1
fi
tests="$("$bin" --list --ignored 2>/dev/null || true)"
if ! grep -q 'workload_mixed_compaction' <<<"$tests" ||
    ! grep -q 'consensus_pack_workload' <<<"$tests"; then
    echo "error: $bin lacks the baseline workloads; is this the tree with the tndb work?" >&2
    exit 1
fi

: >"$report"
echo "### TN-STORAGE-BASELINE v1" >>"$report"
section SYSTEM
kv date "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
kv host "$(uname -a)"
if [ "$os" = "Linux" ]; then
    kv distro "$(. /etc/os-release 2>/dev/null && echo "$PRETTY_NAME" || echo "?")"
    kv cpu "$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ //') x $(nproc)"
    kv memory "$(awk '/MemTotal/ {printf "%.1f GiB", $2 / 1048576}' /proc/meminfo)"
else
    kv cpu "$(try sysctl -n machdep.cpu.brand_string) x $(try sysctl -n hw.ncpu)"
    kv memory "$(( $(try sysctl -n hw.memsize) / 1073741824 )) GiB"
fi
kv rustc "$(try rustc -V)"
kv load "$(uptime | sed 's/.*load average[s]*: //')"

section REPO
kv commit "$(try git rev-parse --short HEAD) on $(try git rev-parse --abbrev-ref HEAD)"
kv uncommitted "$(git status --porcelain 2>/dev/null | wc -l | tr -d ' ') paths"
kv binary "$bin"
kv md5 "$( (md5sum "$bin" 2>/dev/null || md5 -r "$bin") | cut -d' ' -f1)"
kv knobs "BASELINE_QUICK=${quick:-} BASELINE_ROUNDS=$rounds BASELINE_ONLY=${BASELINE_ONLY:-}"

section DISK
kv bench_dir "$bench_dir"
kv free "$(df -h "$bench_dir" | awk 'NR==2 {print $4}')"
if [ "$os" = "Linux" ]; then
    kv mount "$(try findmnt -n -o SOURCE,FSTYPE,OPTIONS -T "$bench_dir")"
    src="$(findmnt -n -o SOURCE -T "$bench_dir" 2>/dev/null || true)"
    src="${src%%\[*}" # btrfs reports `/dev/x[/subvolume]`
    # Not every source is a block device (ZFS, overlay): no device then, and the script goes on.
    dev="$(lsblk -n -o PKNAME "$src" 2>/dev/null | head -1 || true)"
    [ -n "$dev" ] || dev="$(basename "$src")"
    kv device "$(try lsblk -d -n -o NAME,MODEL,ROTA,SIZE,TRAN "/dev/$dev")"
    kv scheduler "$(try cat "/sys/block/$dev/queue/scheduler")"
    kv write_cache "$(try cat "/sys/block/$dev/queue/write_cache")"
    kv fua "$(try cat "/sys/block/$dev/queue/fua")"
else
    kv mount "$(df "$bench_dir" | awk 'NR==2 {print $1}') $(mount | grep " on $(df "$bench_dir" | awk 'NR==2 {print $NF}') " | head -1)"
fi

# Raw sync latency and throughput, to read the durable latencies against.
section SYNC-PROBE
probe="$bench_dir/sync-probe.$$"
if [ "$os" = "Linux" ]; then
    { dd if=/dev/zero of="$probe" bs=4k count=2000 oflag=dsync 2>&1 | tail -1 |
        sed 's/^/dsync 4k x2000:  /'; } >>"$report" || true
    { dd if=/dev/zero of="$probe" bs=1M count=256 conv=fdatasync 2>&1 | tail -1 |
        sed 's/^/fdatasync 256M:  /'; } >>"$report" || true
else
    { dd if=/dev/zero of="$probe" bs=1m count=256 2>&1 | tail -1 |
        sed 's/^/write 256M (no sync on macOS dd):  /'; } >>"$report" || true
fi
rm -f "$probe"

# The columns BASELINE_QUICK runs for each workload (TN_BENCH_COLUMNS; empty: every column).
quick_columns() {
    case "$1" in
        workload_consensus_rounds)
            echo "MemDb,TnDb-group,Split<TnDb-group>,Layered<MDBX-prod>" ;;
        workload_batch_cache)
            echo "MemDb,TnDb-group,Split<TnDb-group>,Layered-cache<MDBX-prod>" ;;
        workload_mixed)
            echo "Split-nocompact,Split<TnDb-group>,Composite<MDBX-prod>" ;;
        workload_compaction)
            echo "TnDb-group-nocompact,TnDb-group" ;;
        workload_startup_reload)
            echo "Split<TnDb-group>,Layered<MDBX-prod>" ;;
        *) echo "" ;;
    esac
}

# A bench's test path: the pack workload lives in `pack_bench`, the rest in `workload_bench`.
test_path() {
    case "$1" in
        consensus_pack_workload) echo "pack_bench::$1" ;;
        *) echo "workload_bench::$1" ;;
    esac
}

run() {
    local name="$1" label="$2" out start end status rss="" columns="" path fork_env=""
    out="$(mktemp)"
    path="$(test_path "$name")"
    [ -n "$quick" ] && columns="$(quick_columns "$name")"
    # The pack workload writes headers in the mainnet layout. This binary has every feature, so
    # `adiri`, whose epoch 0 predates the seed-signature and millisecond-timestamp forks;
    # `test-utils` lets these overrides move both forks to genesis.
    [ "$name" = consensus_pack_workload ] &&
        fork_env="TN_SEED_SIGNATURE_FORK_EPOCH=0 TN_SUBSECOND_TIMESTAMP_FORK_EPOCH=0"
    echo "running $name ($label)..."
    section "WORKLOAD $name $label"
    kv load "$(uptime | sed 's/.*load average[s]*: //')"
    [ -n "$columns" ] && kv columns "$columns"
    start="$(date +%s)"
    set +e
    if [ "$os" = "Linux" ] && [ -x /usr/bin/time ]; then
        TN_BENCH_COLUMNS="$columns" TMPDIR="$bench_dir" NO_COLOR=1 /usr/bin/time -v -o "$out.time" \
            env $fork_env "$bin" "$path" --exact --ignored --nocapture --test-threads 1 >"$out" 2>&1
        status=$?
        rss="$(awk -F: '/Maximum resident set size/ {gsub(/ /, "", $2); print $2 " KiB"}' "$out.time")"
    else
        TN_BENCH_COLUMNS="$columns" TMPDIR="$bench_dir" NO_COLOR=1 env $fork_env "$bin" \
            "$path" --exact --ignored --nocapture --test-threads 1 >"$out" 2>&1
        status=$?
    fi
    set -e
    end="$(date +%s)"
    kv seconds "$((end - start))"
    kv exit "$status"
    [ -n "$rss" ] && kv max_rss "$rss"
    echo "--- output" >>"$report"
    grep -vE '^(running [0-9]+ tests?|test result:|$)' "$out" >>"$report" || true
    grep -m1 '^test result:' "$out" >>"$report" || true
    rm -f "$out" "$out.time"
    rm -rf "${bench_dir:?}"/.tmp* 2>/dev/null || true
}

run msync_concurrency_probe "run 1"
for round in $(seq 1 "$rounds"); do
    for workload in $workloads; do
        run "$workload" "run $round"
    done
done
if [ -z "${BASELINE_ONLY:-}" ]; then
    run workload_startup_reload "run 1"
    run workload_mixed_compaction "run 1"
    run consensus_pack_workload "run 1"
fi

section END
kv finished "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo
echo "report: $report"
