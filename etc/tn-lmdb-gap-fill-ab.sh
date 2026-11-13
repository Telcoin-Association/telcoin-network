#!/usr/bin/env bash
# tn-lmdb's gap fill (`EnvOptions::gap_fill`, `--db.gap-fill`) on a Linux disk: the persist bench
# and the execution bench, durable, storage v2, at each gap fill in GAPS, in the order GAPS then
# reversed (A B C C B A for ROUNDS=2), one build. Each run records the engine's counters (runs of
# dirty pages, gaps filled, pages written) and the disk's /proc/diskstats deltas over the run
# (writes the device completed, KiB written, ms writing; for the execution bench also over its
# timed loop alone), plus `iostat -dxk 1` beside it when installed. The engine's counters per
# durable commit are in the persist bench's `persist_bench_io` lines and the execution bench's
# JSON report (`db_writes.counters_mean_per_output`: `tn_lmdb_sync_runs`, `tn_lmdb_gaps_filled`,
# `tn_lmdb_gap_pages`, `tn_lmdb_gap_misses`, `tn_lmdb_gap_fill_ns`; under `auto` also the mean
# width it chose and its fit, `tn_lmdb_gap_auto_*`). Prints a summary and leaves every log in the
# bench dir.
#
# Usage: etc/tn-lmdb-gap-fill-ab.sh <dir on the disk under test>
#   GAPS="0 4 8"   the gap fills to compare: widths in pages (0 is off) or `auto`, as in
#                  GAPS="0 8 auto" (auto chooses each commit's width from the disk's measured
#                  costs: its summary adds the mean width, the last fit's cost per write and per
#                  page, and whether the fit is the capped form)
#   ROUNDS=2       passes over GAPS; even passes run it reversed
#   PERSIST=1      run the persist bench (TN_PERSIST_BENCH_*: 400 blocks, 200 txs; both modes,
#                  1 and 4 blocks per commit)
#   EXEC=1         run the execution bench's busy profile (TN_EXEC_BENCH_*: 300 outputs)
#   EXEC_CACHE=<dir>  the execution bench's cache of bases (default <dir>/cache): the first run
#                  builds the base and the aged base there (long), later ones clone them
#   Other TN_PERSIST_BENCH_* and TN_EXEC_BENCH_* variables pass through to every run.
set -euo pipefail

dir=${1:?usage: $0 <dir on the disk under test>}
[ "$(uname -s)" = Linux ] || { echo "Linux only: the disk counters are /proc/diskstats" >&2; exit 2; }
mkdir -p "$dir"
dir=$(cd "$dir" && pwd)
repo=$(cd "$(dirname "$0")/.." && pwd)
gaps=${GAPS:-0 4 8}
rounds=${ROUNDS:-2}
cache=${EXEC_CACHE:-$dir/cache}
out=$dir/gap-fill-ab-$(hostname)-$(date +%Y%m%d-%H%M%S)
mkdir -p "$out" "$cache" "$dir/tmp"

# The block device under the bench dir: its /proc/diskstats name (a partition's or a mapped
# device's own line).
source_dev=$(df --output=source "$dir" | tail -1)
# By the file system's device number, so a source the kernel names `/dev/root` (no such node)
# still resolves to its block device.
majmin=$(findmnt -no MAJ:MIN -T "$dir" 2>/dev/null | head -1 | tr -d ' ')
dev=
[ -n "$majmin" ] && [ -e "/sys/dev/block/$majmin" ] && dev=$(basename "$(readlink -f "/sys/dev/block/$majmin")")
[ -n "$dev" ] || dev=$(basename "$(readlink -f "$source_dev")")
grep -q " $dev " /proc/diskstats || { echo "no /proc/diskstats line for $dev ($source_dev)" >&2; exit 2; }
echo "disk: $source_dev ($dev), bench dir $dir, logs $out"
# writes completed, sectors written, ms writing, reads completed, sectors read
disk() { awk -v d="$dev" '$3 == d { print $8, $10, $11, $4, $6 }' /proc/diskstats; }

cd "$repo"
echo "== build (one binary per bench)"
cargo test --locked --release -p tn-reth --lib --no-run >"$out/build.log" 2>&1
cargo test --locked --release -p tn-engine --test exec_bench --no-run >>"$out/build.log" 2>&1

# run <name> <gap> <command...>: the command beside a sampler that writes, every second, the
# time (ms), the run log's size and the disk's writes, sectors written, ms writing and reads
# (reports/<name>.disk.tsv), and beside `iostat -dxk 1` when installed. The summary line gives the
# whole run's deltas and, for the execution bench, the timed loop's (the samples from
# `elapsed_s` before the log's "timed loop done" line to it).
run() {
    local name=$1 gap=$2
    shift 2
    local log=$out/$name.log tsv=$out/$name.disk.tsv
    : >"$log"
    (while :; do
        printf '%s %s %s\n' "$(date +%s%3N)" "$(stat -c %s "$log" 2>/dev/null || echo 0)" "$(disk)"
        sleep 1
    done) >"$tsv" &
    local sampler=$! iostat_pid=
    if command -v iostat >/dev/null; then
        iostat -dxk -y 1 "$dev" >"$out/$name.iostat" 2>&1 &
        iostat_pid=$!
    fi
    local start=$SECONDS
    TMPDIR=$dir/tmp "$@" >"$log" 2>&1 || echo "$name: exit $?" | tee -a "$log"
    sleep 1
    kill "$sampler" 2>/dev/null || true
    [ -n "$iostat_pid" ] && kill "$iostat_pid" 2>/dev/null || true
    local whole timed=
    whole=$(awk 'NR == 1 { w = $3; s = $4; m = $5; r = $6 } END {
        dw = $3 - w; printf "disk_writes=%d disk_kib=%d kib_per_write=%.1f write_ms=%d disk_reads=%d",
        dw, ($4 - s) / 2, dw ? ($4 - s) / 2 / dw : 0, $5 - m, $6 - r }' "$tsv")
    local elapsed done_at
    elapsed=$(sed -n 's/^timed loop done: elapsed_s=\([0-9.]*\).*/\1/p' "$log" | head -1)
    if [ -n "$elapsed" ]; then
        # the log's size once the line was written: the first sample at or past it
        done_at=$(grep -b -m1 '^timed loop done' "$log" | cut -d: -f1)
        timed=$(awk -v at="$done_at" -v el="$elapsed" '
            { t[NR] = $1; b[NR] = $2; w[NR] = $3; s[NR] = $4; m[NR] = $5 }
            END {
                for (e = 1; e <= NR && b[e] <= at; e++) {}
                if (e > NR) e = NR
                for (i = e; i > 1 && t[e] - t[i] < el * 1000; i--) {}
                secs = (t[e] - t[i]) / 1000; dw = w[e] - w[i]
                printf "timed_s=%.0f timed_writes=%d timed_writes_per_s=%.0f timed_kib_per_write=%.1f timed_mib_per_s=%.1f",
                    secs, dw, secs ? dw / secs : 0, dw ? (s[e] - s[i]) / 2 / dw : 0,
                    secs ? (s[e] - s[i]) / 2048 / secs : 0
            }' "$tsv")
    fi
    printf '%s gap=%s secs=%s %s %s\n' "$name" "$gap" $((SECONDS - start)) "$whole" "$timed" \
        | tee -a "$out/summary.txt"
    grep -h '^persist_bench\|^throughput:' "$log" | sed "s/^/  /" | tee -a "$out/summary.txt" || true
    local report=$out/$name.json
    if [ -f "$report" ] && command -v jq >/dev/null; then
        jq -c '.db_writes.counters_mean_per_output | {commits: .tn_lmdb_commits,
            pages: .tn_lmdb_pages_synced, runs: .tn_lmdb_sync_runs, gaps: .tn_lmdb_gaps_filled,
            gap_pages: .tn_lmdb_gap_pages, misses: .tn_lmdb_gap_misses,
            gap_fill_ms: (.tn_lmdb_gap_fill_ns / 1e6), data_sync_ms: (.tn_lmdb_data_sync_ns / 1e6),
            auto_mean_width: ((.tn_lmdb_gap_auto_width_sum // 0) / ([.tn_lmdb_gap_auto_steps // 0, 1e-9] | max)),
            auto_probes: (.tn_lmdb_gap_auto_probes // 0), auto_readapts: (.tn_lmdb_gap_auto_readapts // 0),
            auto_run_us: ((.tn_lmdb_gap_auto_run_ns // 0) / 1e3),
            auto_page_us: ((.tn_lmdb_gap_auto_page_ns // 0) / 1e3),
            auto_capped: (.tn_lmdb_gap_auto_capped // 0)}' \
            "$report" | sed 's/^/  per output: /' | tee -a "$out/summary.txt" || true
    fi
}

order=()
for ((r = 1; r <= rounds; r++)); do
    if ((r % 2)); then order+=($gaps); else order+=($(echo $gaps | tr ' ' '\n' | tac)); fi
done
echo "order: ${order[*]}"
if [ "${EXEC:-1}" = 1 ]; then
    # Build (or find) the execution bench's base and aged base before any measured run.
    echo "== exec bench bases in $cache"
    TMPDIR=$dir/tmp TN_EXEC_BENCH_LAYOUT=v2 TN_EXEC_BENCH_CACHE_DIR="$cache" TN_EXEC_BENCH_OUTPUTS=1 \
        TN_EXEC_BENCH_WARMUP=0 cargo test --locked --release -p tn-engine --test exec_bench -- \
        --ignored --exact exec_bench --nocapture >"$out/prepare.log" 2>&1
fi
n=0
for gap in "${order[@]}"; do
    n=$((n + 1))
    if [ "${PERSIST:-1}" = 1 ]; then
        run "persist-$n-g$gap" "$gap" env TN_PERSIST_BENCH_SYNC=durable TN_PERSIST_BENCH_LAYOUT=v2 \
            TN_PERSIST_BENCH_GAP_FILL="$gap" \
            cargo test --locked --release -p tn-reth --lib env::persist_bench::per_block_persist \
            -- --ignored --exact --nocapture
    fi
    if [ "${EXEC:-1}" = 1 ]; then
        run "exec-busy-$n-g$gap" "$gap" env TN_EXEC_BENCH_SYNC=durable TN_EXEC_BENCH_LAYOUT=v2 \
            TN_EXEC_BENCH_GAP_FILL="$gap" TN_EXEC_BENCH_CACHE_DIR="$cache" \
            TN_EXEC_BENCH_REPORT="$out/exec-busy-$n-g$gap.json" \
            cargo test --locked --release -p tn-engine --test exec_bench -- --ignored --exact \
            exec_bench --nocapture
    fi
done
echo "== summary ($out/summary.txt)"
cat "$out/summary.txt"
