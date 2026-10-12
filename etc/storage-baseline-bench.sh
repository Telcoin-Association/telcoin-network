#!/usr/bin/env bash
# Run the consensus-DB baseline benches (tn-storage `workload_*`) on this machine and write one
# report file (system, disk, a raw sync probe, then each workload's tables), for comparing the
# backends on the production OS. Written for Linux; also runs on macOS.
#
# Usage: etc/storage-baseline-bench.sh <empty dir on the disk under test> [report file]
#   ROUNDS=1 ...            runs each workload once instead of three times (default 3)
#   ONLY="workload_mixed"   runs only the named workloads (space separated)
#
# Needs this checkout's tn-storage with the tnlog work (an uncommitted tree can be copied over
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
rounds="${ROUNDS:-3}"
workloads="${ONLY:-workload_consensus_rounds workload_batch_cache workload_mixed workload_compaction}"
os="$(uname -s)"

if [ "$os" = "Linux" ]; then
    fstype="$(stat -f -c %T "$bench_dir")"
    if [ "$fstype" = "tmpfs" ] || [ "$fstype" = "ramfs" ]; then
        echo "error: $bench_dir is on $fstype; give a directory on the disk under test" >&2
        exit 1
    fi
fi
if [ ! -d "$repo/crates/storage/src/tnlog" ]; then
    echo "error: $repo has no crates/storage/src/tnlog; copy over the tree with the tnlog work" >&2
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
bin="$repo/$(grep -o 'target/release/deps/tn_storage-[0-9a-f]*' "$build_log" | head -1)"
rm -f "$build_log"
if ! "$bin" --list --ignored 2>/dev/null | grep -q 'workload_mixed_compaction'; then
    echo "error: $bin lacks the baseline workloads; is this the tree with the tnlog work?" >&2
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

section DISK
kv bench_dir "$bench_dir"
kv free "$(df -h "$bench_dir" | awk 'NR==2 {print $4}')"
if [ "$os" = "Linux" ]; then
    kv mount "$(try findmnt -n -o SOURCE,FSTYPE,OPTIONS -T "$bench_dir")"
    src="$(findmnt -n -o SOURCE -T "$bench_dir" 2>/dev/null || true)"
    dev="$(lsblk -n -o PKNAME "$src" 2>/dev/null | head -1)"
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

run() {
    local name="$1" label="$2" out start end status rss=""
    out="$(mktemp)"
    echo "running $name ($label)..."
    section "WORKLOAD $name $label"
    kv load "$(uptime | sed 's/.*load average[s]*: //')"
    start="$(date +%s)"
    set +e
    if [ "$os" = "Linux" ] && [ -x /usr/bin/time ]; then
        TMPDIR="$bench_dir" NO_COLOR=1 /usr/bin/time -v -o "$out.time" "$bin" \
            "workload_bench::$name" --exact --ignored --nocapture --test-threads 1 >"$out" 2>&1
        status=$?
        rss="$(awk -F: '/Maximum resident set size/ {gsub(/ /, "", $2); print $2 " KiB"}' "$out.time")"
    else
        TMPDIR="$bench_dir" NO_COLOR=1 "$bin" \
            "workload_bench::$name" --exact --ignored --nocapture --test-threads 1 >"$out" 2>&1
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
if [ -z "${ONLY:-}" ]; then
    run workload_startup_reload "run 1"
    run workload_mixed_compaction "run 1"
fi

section END
kv finished "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo
echo "report: $report"
