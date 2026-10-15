#!/usr/bin/env bash
# Before/after run of one tn-storage bench: the bench on a before commit and on this checkout
# (uncommitted changes included), before/after/after/before, written to one report in the repo
# root (`storage-ab-<host>-<time>.txt`). For judging a storage change on the machine and disk that
# matter (Linux for anything durable: a macOS fsync/msync doesn't flush the drive).
#
# Usage: etc/storage-ab-bench.sh <empty dir on the disk under test> <before-commit> [bench]
#   bench            a tn-storage test path (default pack_bench::consensus_pack_workload); see the
#                    crate README's Benchmarks section. Its module must exist in the before commit.
#   AB_ROUNDS=1      one before and one after run instead of before/after/after/before
#   AB_FEATURES=...  extra cargo flags for both builds, e.g. "--features adiri"
#   AB_BENCH_FROM=checkout  run this checkout's copy of the bench on both sides, for a bench that
#                    itself changed (what its timed region covers, say); its file must build
#                    against the before commit
#   Bench knobs (TN_BENCH_*, INDEX_BENCH_N) pass through the environment; the report lists them.
#
# The before tree is an export of the commit (`git archive`; no git writes) with this checkout's
# `tn-contracts` submodule linked in. It runs its own copy of the bench when it has one (a newer
# bench may call code the commit lacks), else this checkout's. It builds into its own target dir
# under the bench dir, one per commit and bench source, kept for reruns: cargo names a workspace's
# crates by their path within it, so two trees sharing one target dir would reuse each other's
# artifacts, and it judges freshness by mtime, which the export sets to the commit's time (another
# tree's files can look older than its artifacts and be skipped). Each side's test binary
# is run directly, its md5 and test count recorded, and the run refused if the two binaries match.
# Needs a Rust toolchain, git, and room in the bench dir for a release build plus the bench's data.
set -euo pipefail

bench_dir="${1:?usage: $0 <empty dir on the disk under test> <before-commit> [bench]}"
before="${2:?usage: $0 <empty dir on the disk under test> <before-commit> [bench]}"
bench="${3:-pack_bench::consensus_pack_workload}"
mkdir -p "$bench_dir"
bench_dir="$(cd "$bench_dir" && pwd)"
repo="$(cd "$(dirname "$0")" && git rev-parse --show-toplevel)"
report="$repo/storage-ab-$(uname -n | cut -d. -f1)-$(date +%Y%m%d-%H%M%S).txt"
read -r -a features <<<"${AB_FEATURES:-}"

die() { echo "$*" >&2; exit 1; }
if [ "$(uname -s)" = "Linux" ]; then
    case "$(stat -f -c %T "$bench_dir")" in
        tmpfs | ramfs) die "refusing $bench_dir: it is tmpfs/ramfs (syncs are free there)" ;;
    esac
fi
git -C "$repo" cat-file -e "$before^{commit}" 2>/dev/null || die "no commit $before"
[ -f "$repo/tn-contracts/artifacts/ConsensusRegistry.json" ] \
    || die "tn-contracts is not checked out (git submodule update --init)"

# The bench's source file, from its module path: `pack_bench::x` is src/pack_bench.rs,
# `archive::index_bench::x` is src/archive/index_bench.rs.
module="${bench%::*}"
bench_fn="${bench##*::}"
bench_file="crates/storage/src/${module//:://}.rs"
[ -f "$repo/$bench_file" ] || die "no $bench_file for bench $bench"
grep -q "fn $bench_fn()" "$repo/$bench_file" || die "no fn $bench_fn in $bench_file"

src_before="$bench_dir/src-before"
rm -rf "$src_before"
mkdir -p "$src_before"
cleanup() { rm -rf "$src_before" "$bench_dir"/.tmp* 2>/dev/null || true; }
trap cleanup EXIT
git -C "$repo" archive "$before" | tar -x -C "$src_before"
rm -rf "$src_before/tn-contracts"
ln -s "$repo/tn-contracts" "$src_before/tn-contracts"
[ -f "$src_before/$bench_file" ] || die "$before has no $bench_file"
if [ "${AB_BENCH_FROM:-}" != checkout ] && grep -q "fn $bench_fn()" "$src_before/$bench_file"; then
    bench_before="the commit's own"
    target_key="$(git -C "$repo" rev-parse --short "$before")"
else
    # Keeping the file's own mtime: a rerun with the same bench finds its build fresh, and an
    # edited bench is newer than the artifacts, so it is rebuilt.
    cp -p "$repo/$bench_file" "$src_before/$bench_file"
    bench_before="copied from this checkout"
    target_key="$(git -C "$repo" rev-parse --short "$before")-checkout-bench"
fi

target_before="$bench_dir/target-before-$target_key"
target_after="${CARGO_TARGET_DIR:-$repo/target}"
unset CARGO_TARGET_DIR
export TMPDIR="$bench_dir"

kv() { printf '%-18s %s\n' "$1:" "$2" >>"$report"; }
digest() {
    if command -v md5sum >/dev/null; then md5sum "$1" | cut -d' ' -f1; else md5 -q "$1"; fi
}
{
    echo "### STORAGE-AB v1"
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
    probe="$bench_dir/sync-probe"
    kv "dsync 4k x2000" "$(dd if=/dev/zero of="$probe" bs=4k count=2000 oflag=dsync 2>&1 | tail -1)"
    rm -f "$probe"
fi
kv rustc "$(rustc -V)"
kv bench "$bench"
kv before "$before ($(git -C "$repo" log -1 --format=%s "$before"))"
kv after "$(git -C "$repo" rev-parse --short HEAD) ($(git -C "$repo" log -1 --format=%s HEAD))$(git -C "$repo" diff --quiet HEAD -- crates || echo ' + uncommitted changes in crates/')"
kv change "$(git -C "$repo" diff --stat "$before" -- crates/storage | tail -1)"
kv "before bench" "$bench_before"
kv mode "$([ "${AB_ROUNDS:-2}" = 1 ] && echo 'before, after' || echo 'before, after, after, before')"
kv features "${AB_FEATURES:-(default)}"
kv knobs "$(env | grep -E '^(TN_BENCH_|INDEX_BENCH_)' | tr '\n' ' ')"

build() { # <label> <source tree> <target dir>: prints the test binary's path
    echo "building the $1 test binary..." >&2
    (cd "$2" && CARGO_TARGET_DIR="$3" cargo test --release -p tn-storage --lib \
        ${features[@]+"${features[@]}"} --no-run) >"$bench_dir/build-$1.log" 2>&1 || {
        tail -30 "$bench_dir/build-$1.log" >&2
        die "build of $1 failed; see $bench_dir/build-$1.log"
    }
    local bin
    bin="$(grep -oE 'Executable unittests src/lib.rs \([^)]+\)' "$bench_dir/build-$1.log" \
        | tail -1 | sed -E 's/.*\((.*)\)/\1/')"
    case "$bin" in /*) ;; *) bin="$2/$bin" ;; esac
    [ -x "$bin" ] || die "no test binary found for $1 (see $bench_dir/build-$1.log)"
    echo "$bin"
}
bin_before="$(build before "$src_before" "$target_before")"
bin_after="$(build after "$repo" "$target_after")"
tests() { "$1" --list 2>/dev/null | grep -c ': test$' || true; }
kv "before binary" "$bin_before $(digest "$bin_before") ($(tests "$bin_before") tests)"
kv "after binary" "$bin_after $(digest "$bin_after") ($(tests "$bin_after") tests)"
[ "$(digest "$bin_before")" != "$(digest "$bin_after")" ] \
    || die "the before and after test binaries are identical: refusing to compare"

run() { # <label> <test binary> <source tree> <round>
    echo "running $1 (round $4)..." >&2
    {
        echo
        echo "### RUN $1 round $4 ($(date -u +%H:%M:%SZ), load $(uptime | sed 's/.*load average[s]*: //'))"
    } >>"$report"
    # From the crate dir, as cargo runs it.
    (cd "$3/crates/storage" && "$2" "$bench" --exact --ignored --nocapture --test-threads 1) 2>&1 \
        | grep -vE '^running [0-9]+ test' >>"$report"
}
run before "$bin_before" "$src_before" 1
run after "$bin_after" "$repo" 1
if [ "${AB_ROUNDS:-2}" != 1 ]; then
    run after "$bin_after" "$repo" 2
    run before "$bin_before" "$src_before" 2
fi
{
    echo
    echo "### END $(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >>"$report"
echo "report: $report"
