#!/usr/bin/env bash
# The busy-node execution bench (`tn-engine`'s `exec_bench` test binary) on two builds, run as
# interleaved pairs over several load profiles and aggregated into one report in the repo root
# (`reth-exec-<host>-<time>.txt`). See tn-reth's README, "Execution benchmark", and the bench's
# module docs (`crates/engine/tests/exec_bench/main.rs`) for its knobs and report.
#
# Usage: etc/reth-exec-bench.sh <dir on the disk under test> [before-commit]
#        etc/reth-exec-bench.sh --report-only <bench dir>
#   Report-only mode: no builds, no runs; renders the report again from the bench dir's
#                    reports/ into reth-exec-report-<host>-<time>.txt in the repo root, with the
#                    runs' own SYSTEM, CONFIG, BINARIES and RUNS sections when reports/head.txt
#                    is there. The runs are reports/runs.tsv's (the script's last runs in that
#                    dir); a dir without it (a bench dir of an older script) has its runs read
#                    from the report names, <profile>-<side>-<round>.json: every report there
#                    counts as a completed run, a stale one from an earlier invocation included,
#                    and a failed run is not known.
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
#   EXEC_SYS_SAMPLE=1  Linux only (refused elsewhere): a sampler beside each run, started just
#                    before the binary and killed right after it (and on any exit), which never
#                    changes a run's status. Every second it writes reports/<run>.sys.tsv: the
#                    time, the run log's size, /proc/meminfo's Dirty and Writeback, the drive's
#                    temperature (hwmon temp1_input, when readable) and the /proc/diskstats line
#                    of the whole disk under the bench dir (writes, sectors written, ms writing,
#                    ms doing I/O, from Linux 5.5 flushes and ms flushing, then reads, sectors
#                    read and ms reading), and the file system's own line when that is a
#                    partition or a mapped device, each line ending with /proc/meminfo's
#                    AnonPages, Cached and MemAvailable and /proc/vmstat's pgmajfault and
#                    workingset_refault_file (a dropped file page read again). To
#                    reports/<run>.frag.tsv it writes filefrag's extent count of the run's data
#                    file (tndb.dat or mdbx.dat under the run's TMPDIR) as soon as the file exists
#                    and then every 30 s, when filefrag is installed.
#                    The devices, the temperature file and filefrag are found once, before the
#                    builds, and named in CONFIG. The disk is sampled whole: any other writer on
#                    it shows up in the rows.
#   EXEC_PAGE_SIZE_MDBX=<bytes>, EXEC_PAGE_SIZE_TNDB=<bytes>  the database page size of every run
#                    on that backend (TN_EXEC_BENCH_PAGE_SIZE on its command line: a power of two
#                    from 4096 to 65536); unset, a backend's runs take TN_EXEC_BENCH_PAGE_SIZE from
#                    the environment, else its own default (MDBX the OS page size, tndb-b 4096).
#                    Each page size has its own cached bases. In commit mode both sides run
#                    EXEC_BACKEND, so both take its variable, and the before commit's bench must
#                    read TN_EXEC_BENCH_PAGE_SIZE (an older bench refuses it as an unknown knob)
#   Bench knobs (TN_EXEC_BENCH_*) pass through the environment to every run; a profile's own
#   settings win over them. The script sets TN_EXEC_BENCH_CACHE_DIR and TN_EXEC_BENCH_REPORT
#   itself, per run, over any in the environment, and TN_EXEC_BENCH_PAGE_SIZE when the run's
#   backend has an EXEC_PAGE_SIZE_* set.
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
# run, and per profile: each side's page size (as requested and as its database has it), each
# side's median over the rounds of the total's p50, p90, p99 and mean,
# each stage's mean, Mgas/s, tx/s and the database growth per output (allocated and logical);
# with readers on, the writer's total mean with them on over off and each read's p50 and p99; with
# epoch closes, the closing and normal outputs' total means; with an export, its duration, the
# database growth while it held its read and the writer's total mean beside it over without; each
# pair's B/A ratio of those; and the median ratio with its spread (min..max). On macOS a durable commit is
# `F_FULLFSYNC` for libmdbx but a plain fsync for tndb-b, so backend-mode durable commit times
# there are not like-for-like; the report says so.
#
# From each run's per-output rows the table also has the gate's rows: the total's p90, p95 and
# p99 pooled over every output of a side's paired rounds (one percentile of all of them, linear
# between the closest ranks as numpy's default; the ratio is of the two pooled values, so no
# spread), and the database commit's (the `commit_mdbx` stage, on either backend) last-third over
# first-third mean per run (the bench's `thirds`, else from the rows), each side's runs listed and
# the ratio of the medians. Below it,
# one line per run of the commit's mean in s over 10 consecutive equal groups of the timed
# outputs (its drift over the run). Report v4 adds a block per side of the database's writes per
# output: the commit's ms, the process's disk MiB written, the data file's growth in KiB and each
# of the backend's counters that moved (mdbx_* or tndb_*, in the bench's order; `_ns` shown as
# ms, a maximum as its max), then the commit's ms per page written (tndb-b: per
# tndb_pages_synced; MDBX: per mdbx_newly + mdbx_cow + mdbx_split), which tells a commit that
# writes more pages from one that writes the same pages slower. Each row there is a run's mean
# over its timed outputs, its first third and its last third (and last/first), the median over a
# side's rounds; a v3 report has no counters and says n/a. With EXEC_SYS_SAMPLE=1 a block per side
# has the samples over each run's timed loop, its first third and its last third (medians over a
# side's rounds): the disk's write MiB/s, ms per write (ms writing over writes completed), busy %
# (ms doing I/O over wall), flushes/s and ms per flush, the page cache's Dirty max and mean and
# Writeback max, the drive's temperature max, and the data file's extents at the first and last
# extent sample. The bench prints nothing between its header and its report, so the first sample
# at which the run log holds the report's stage table ends the timed loop and the loop's own
# wall time (`throughput.elapsed_s`) places its start; a run whose log does not place it uses all
# its samples, and the report says so.
#
# Needs a Rust toolchain, git, python3, and room in the bench dir for two release builds plus the
# bench's cached bases (--report-only needs only git and python3); EXEC_SYS_SAMPLE=1 also needs
# awk, stat, and findmnt or df, and uses lsblk and filefrag when installed (it installs nothing).
# In the bench dir the script writes only:
#   target-mdbx/, target-tndb-b/           this checkout's builds (commit mode's after side too)
#   target-before-<commit>-<backend>/      commit mode's before build
#   build-<label>.log                      each build's output
#   cache/ (backend mode), cache-before-<commit>/ and cache-after/ (commit mode)
#                                          the bench's cached bases, kept for reruns; remove one
#                                          when the bench refuses its base as built by other code
#   reports/<profile>-<side>-<round>.json  each run's JSON report, replaced by a rerun
#   reports/<profile>-<side>-<round>.sys.tsv and .frag.tsv
#                                          each run's samples (EXEC_SYS_SAMPLE=1), replaced by a
#                                          rerun
#   reports/runs.tsv, reports/head.txt     the runs (emptied when they start, a line added as
#                                          each ends) and the report's head (written when they
#                                          are done), for --report-only
#   logs/<profile>-<side>-<round>.log      each run's output, replaced by a rerun
#   src-before/ and tmp/                   the before export and the runs' TMPDIR, each marked as
#                                          the script's own and removed on exit
# It removes nothing else there. A refusal or a failed build before the first run leaves no report.
set -euo pipefail

die() { echo "$*" >&2; exit 1; }
# The report's per-profile sections. Std-lib Python, read from stdin and run isolated (-I), so
# nothing beside it is imported. `render_report run <manifest> <side a> <side b>` renders the runs
# of a manifest; `render_report dir <bench dir>` those of a bench dir (--report-only).
render_report() {
    python3 -I - "$@" <<'PY'
import json
import os
import re
import statistics
import sys

# `run <manifest> <side a> <side b>`, or `dir <bench dir>`: re-render a bench dir's reports from
# its reports/runs.tsv, else from the report names (<profile>-<side>-<round>.json).
mode = sys.argv[1]
# The bench's stage keys under `total`, in its order, with their depth in its stage tree.
STAGES = {
    "execute": 1, "signer_recovery": 2, "finish": 1, "hashed_state": 2, "state_root": 2,
    "overlay_extend": 1, "save_blocks": 1, "save_blocks_mdbx": 2, "insert_block": 3,
    "write_state": 3, "write_hashed_state": 3, "write_trie_updates": 3,
    "update_history_indices": 3, "save_blocks_sf": 2, "commit_sf": 1, "commit_mdbx": 1, "other": 1,
}
SIDE_PAIRS = (("mdbx", "tndb-b"), ("before", "after"))
REPORT_NAME = re.compile(
    r"^(?P<profile>.+)-(?P<side>mdbx|tndb-b|before|after)-(?P<round>[1-9][0-9]*)\.json$")


def read_manifest(path, bench=None):
    """The runs of a manifest; with `bench`, each run's files are looked up under that dir."""
    runs = []
    with open(path) as f:
        for line in f:
            if not line.strip():
                continue
            profile, side, rnd, status, json_path, log = line.rstrip("\n").split("\t")
            if bench is not None:
                json_path = os.path.join(bench, "reports", f"{profile}-{side}-{rnd}.json")
                log = os.path.join(bench, "logs", f"{profile}-{side}-{rnd}.log")
            runs.append(dict(profile=profile, side=side, round=int(rnd), status=status,
                             json=json_path, log=log))
    return runs


def infer_runs(bench):
    """Every report named <profile>-<side>-<round>.json, oldest first, as completed runs."""
    reports = os.path.join(bench, "reports")
    found = []
    for entry in os.listdir(reports):
        match = REPORT_NAME.match(entry)
        if match:
            path = os.path.join(reports, entry)
            found.append((os.path.getmtime(path), entry, match, path))
    runs = []
    for _, _, match, path in sorted(found):
        profile, side, rnd = match.group("profile"), match.group("side"), match.group("round")
        runs.append(dict(profile=profile, side=side, round=int(rnd), status="0", json=path,
                         log=os.path.join(bench, "logs", f"{profile}-{side}-{rnd}.log")))
    return runs


if mode == "run":
    manifest, side_a, side_b = sys.argv[2:5]
    runs = read_manifest(manifest)
else:
    bench = sys.argv[2]
    manifest = os.path.join(bench, "reports", "runs.tsv")
    runs = read_manifest(manifest, bench) if os.path.exists(manifest) else infer_runs(bench)
    if not runs:
        sys.exit(f"no runs in {bench}/reports (no runs.tsv, no <profile>-<side>-<round>.json)")
    seen = {run["side"] for run in runs}
    for side_a, side_b in SIDE_PAIRS:
        if seen <= {side_a, side_b}:
            break
    else:
        sys.exit(f"reports of both modes in {bench}/reports ({', '.join(sorted(seen))})")


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


def sig(value):
    """Three significant digits, for the compact rows (deciles, counters, samples)."""
    if value is None:
        return "-"
    if value == 0:
        return "0"
    if abs(value) >= 100:
        return f"{value:.0f}"
    return f"{value:#.3g}"


def median(values):
    values = [v for v in values if v is not None]
    return statistics.median(values) if values else None


def mean(values):
    values = [v for v in values if v is not None]
    return sum(values) / len(values) if values else None


def ratio(over, under):
    return over / under if over is not None and under else None


def knob_text(knobs):
    # the page size is each backend's own (EXEC_PAGE_SIZE_*), printed per side: not a load knob
    return " ".join(f"{k}={v}" for k, v in knobs.items()
                    if k not in ("report", "cache_dir", "page_size"))


def page_text(header):
    requested = header.get("knobs", {}).get("page_size")
    return (f"{header.get('page_size_bytes', '-')} bytes "
            f"(requested {'default' if requested is None else requested})")


def percentile(values, p):
    """The p-th percentile, linear between the closest ranks (numpy's default method)."""
    values = sorted(v for v in values if v is not None)
    if not values:
        return None
    pos = (len(values) - 1) * p / 100
    low = int(pos)
    high = min(low + 1, len(values) - 1)
    return values[low] + (values[high] - values[low]) * (pos - low)


def rows_of(doc):
    return doc.get("rows") or []


def thirds_of(doc):
    """The run's first and last third of its rows, as the bench's `thirds` splits them."""
    _, first, last = windows(rows_of(doc), third_len(doc))
    return first, last


def commit_thirds(doc):
    """The commit's last-third over first-third mean: the bench's own, else from the rows."""
    stage = ((doc.get("thirds") or {}).get("stages") or {}).get("commit_mdbx") or {}
    if stage.get("mean_ratio") is not None:
        return stage["mean_ratio"]
    first, last = thirds_of(doc)
    return ratio(mean(r.get("commit_mdbx_ms") for r in last),
                 mean(r.get("commit_mdbx_ms") for r in first))


def deciles(doc, key="commit_mdbx_ms"):
    """The mean of `key` over 10 consecutive equal groups of outputs (None under 10 outputs)."""
    rows = rows_of(doc)
    n = len(rows)
    if n < 10:
        return None
    return [mean(r.get(key) for r in rows[i * n // 10:(i + 1) * n // 10]) for i in range(10)]


def gate_rows(docs, pairs, rounds):
    """The tail and drift rows, in the table's columns: (label, a, b, ratio text, per run)."""
    out = []
    pooled = {side: [r.get("total_ms") for rnd in pairs for r in rows_of(docs[(side, rnd)])]
              for side in (side_a, side_b)}
    for p in (90, 95, 99):
        a, b = percentile(pooled[side_a], p), percentile(pooled[side_b], p)
        text = "-" if ratio(b, a) is None else f"{ratio(b, a):.3f} (pooled)"
        out.append((f"total p{p} ms (pooled)", a, b, text, ""))
    per_run = {side: [commit_thirds(docs[(side, rnd)]) for rnd in rounds[side]]
               for side in (side_a, side_b)}
    a, b = median(per_run[side_a]), median(per_run[side_b])
    text = "-" if ratio(b, a) is None else f"{ratio(b, a):.3f} (of the medians)"
    runs_text = "; ".join(f"{side} {' '.join(fmt(v) for v in per_run[side])}"
                          for side in (side_a, side_b))
    out.append(("commit last/first third", a, b, text, runs_text))
    return out


def print_deciles(docs, rounds):
    lines = []
    for side in (side_a, side_b):
        for rnd in rounds[side]:
            values = deciles(docs[(side, rnd)])
            text = "- (under 10 outputs)" if values is None else " ".join(sig(v / 1000)
                                                                           for v in values)
            lines.append(f"  {side} round {rnd}: {text}")
    print("commit deciles s (mean commit_mdbx per tenth of the timed outputs, in order)")
    for line in lines:
        print(line)


def third_len(doc):
    """The outputs in a third, as the bench's `thirds` counts them."""
    n = len(rows_of(doc))
    k = (doc.get("thirds") or {}).get("outputs_per_third")
    return k if isinstance(k, int) and 0 < k <= n else n // 3


def windows(values, k):
    """A run's per-output values, its first third and its last third."""
    return values, values[:k], values[len(values) - k:]


def counter_runs(doc):
    """One v4 run's counter rows: [(label, (timed, first, last))] in db_counter_names order."""
    rows = rows_of(doc)
    k = third_len(doc)
    writes = doc.get("db_writes") or {}
    maxima = set((writes.get("counters_max") or {}).keys())

    def row(label, values, agg=mean):
        return label, tuple(agg(w) for w in windows(values, k))

    def counter(name):
        return [(r.get("db_counters") or {}).get(name) for r in rows]

    out = [row("commit_mdbx ms", [r.get("commit_mdbx_ms") for r in rows]),
           row("disk MiB", [None if r.get("disk_bytes_written") is None
                            else r["disk_bytes_written"] / 2**20 for r in rows])]
    # the file's growth per output: each row's length over the one before it (the first row's
    # over the file before the timed outputs)
    previous = writes.get("db_file_len_before_bytes")
    if previous is None:
        previous = ((doc.get("sizes") or {}).get("before_timed") or {}).get("db_len_bytes")
    growth = []
    for r in rows:
        length = r.get("db_file_len_bytes")
        growth.append(None if length is None or previous is None else (length - previous) / 1024)
        previous = length
    out.append(row("file growth KiB", growth))
    names = doc["header"].get("db_counter_names") or sorted(
        {name for r in rows for name in r.get("db_counters") or {}})
    for name in names:
        values = counter(name)
        if not any(values):
            continue  # never moved
        if name in maxima:
            out.append(row(f"{name} (max)", values,
                           lambda w: max((v for v in w if v is not None), default=None)))
        elif name.endswith("_ns"):
            out.append(row(f"{name} (ms)", [None if v is None else v / 1e6 for v in values]))
        else:
            out.append(row(name, values))
    # the engine-versus-device discriminator: commit time per page the commit wrote
    if "tndb_pages_synced" in names:
        label, pages = "commit ms per page synced", ("tndb_pages_synced",)
    elif "mdbx_newly" in names:
        label, pages = "commit ms per page (newly+cow+split)", ("mdbx_newly", "mdbx_cow",
                                                               "mdbx_split")
    else:
        return out
    written = [sum(v or 0 for v in values) for values in zip(*(counter(k) for k in pages))]
    commits = [r.get("commit_mdbx_ms") or 0 for r in rows]
    pairs = list(zip(commits, written))
    out.append(row(label, pairs, lambda w: ratio(sum(c for c, _ in w), sum(p for _, p in w))))
    return out


def print_windows(title, head, blocks):
    """Rows of (label, (timed, first, last)) per side, medians over the side's rounds."""
    print(title)
    print(f"{'':<46} {head[0]:>11} {head[1]:>11} {head[2]:>11} {'last/first':>11}")
    for side, rows in blocks:
        for label, (whole, first, last) in rows:
            print(f"{side + '  ' + label:<46} {sig(whole):>11} {sig(first):>11} {sig(last):>11} "
                  f"{sig(ratio(last, first)):>11}")


def merge_runs(per_run):
    """Per-run rows -> rows of medians over the runs, in the first run's label order."""
    labels = []
    values = {}
    for rows in per_run:
        for label, triple in rows:
            if label not in values:
                labels.append(label)
                values[label] = []
            values[label].append(triple)
    return [(label, tuple(median(t[i] for t in values[label]) for i in range(3)))
            for label in labels]


def print_counters(docs, rounds):
    blocks = []
    notes = []
    for side in (side_a, side_b):
        v4 = [docs[(side, rnd)] for rnd in rounds[side] if "db_writes" in docs[(side, rnd)]]
        if not v4:
            notes.append(f"{side}: n/a (report v3: no per-output database counters)")
            continue
        header = v4[0]["header"]
        error = header.get("disk_bytes_error")
        notes.append(f"{side}: {len(v4)} run(s); disk bytes from "
                     f"{header.get('disk_bytes_source') or '-'}"
                     + (f" (a read FAILED: {error})" if error else ""))
        blocks.append((side, merge_runs([counter_runs(doc) for doc in v4])))
    print()
    if not blocks:
        print("db writes per output: n/a (report v3: no per-output database counters)")
        return
    print_windows("db writes per output (report v4; per run the mean over its timed outputs, "
                  "its first and last third; medians over a side's rounds; counters that never "
                  "moved left out; _ns as ms)", ("timed", "first 1/3", "last 1/3"), blocks)
    for note in notes:
        print(f"  {note}")


SAMPLE_FIELDS = ("t", "log", "dirty", "writeback", "temp", "dev", "writes", "sectors",
                 "ms_writing", "ms_io", "flushes", "ms_flushing", "reads", "sectors_read",
                 "ms_reading", "anon", "cached", "available", "majflt", "refault_file")
# a sampler before the read and memory columns wrote the first 12 fields only
OLD_SAMPLE_FIELDS = 12


def number(text):
    try:
        return float(text.replace(",", "."))
    except ValueError:
        return None


def read_samples(path):
    """The sampler's 1 s samples of the whole disk: [{field: value}] in time order."""
    disk = None
    samples = []
    with open(path) as f:
        for line in f:
            fields = line.split()
            if not fields:
                continue
            if fields[0] == "#":
                if len(fields) >= 3 and fields[1] == "disk":
                    disk = fields[2]
                continue
            if len(fields) not in (OLD_SAMPLE_FIELDS, len(SAMPLE_FIELDS)):
                continue
            sample = dict(zip(SAMPLE_FIELDS, fields))
            if disk is not None and sample["dev"] != disk:
                continue
            for key in SAMPLE_FIELDS:
                if key != "dev":
                    sample[key] = number(sample[key]) if key in sample else None
            if sample["t"] is not None:
                samples.append(sample)
    return samples


def read_extents(path):
    out = []
    with open(path) as f:
        for line in f:
            fields = line.split()
            if len(fields) >= 2 and fields[0] != "#":
                t, extents = number(fields[0]), number(fields[1])
                if t is not None and extents is not None:
                    out.append((t, extents))
    return out


def timed_span(samples, log, elapsed):
    """The timed loop's (start, end) in the samples' clock, and whether the log placed it.

    The bench prints its header, then nothing until its report, just after the timed loop: the
    first sample at which the log holds the stage table ends the loop, and the loop's own wall
    time (throughput.elapsed_s) places its start."""
    start, end = samples[0]["t"], samples[-1]["t"]
    try:
        with open(log, "rb") as f:
            found = re.search(rb"(?m)^stage +mean ", f.read())
    except OSError:
        found = None
    if found is None or not elapsed:
        return start, end, False
    done = next((s["t"] for s in samples if s["log"] is not None and s["log"] > found.start()),
                end)
    return max(start, done - elapsed), done, True


def sample_stats(samples, start, end):
    """The rows of one window of samples, as {label: value}."""
    inside = [s for s in samples if start - 0.5 <= s["t"] <= end + 0.5]
    out = {}
    if len(inside) < 2:
        return out
    s0, s1 = inside[0], inside[-1]
    dt = s1["t"] - s0["t"]

    def delta(key):
        if s0[key] is None or s1[key] is None or s1[key] < s0[key]:
            return None
        return s1[key] - s0[key]

    writes, sectors, flushes = delta("writes"), delta("sectors"), delta("flushes")
    out["disk write MiB/s"] = None if sectors is None or not dt else sectors * 512 / 2**20 / dt
    out["disk ms per write"] = ratio(delta("ms_writing"), writes)
    out["disk busy %"] = (None if delta("ms_io") is None or not dt
                          else delta("ms_io") / (dt * 1000) * 100)
    out["disk flushes/s"] = None if flushes is None or not dt else flushes / dt
    out["disk ms per flush"] = ratio(delta("ms_flushing"), flushes)
    reads, sectors_read = delta("reads"), delta("sectors_read")
    out["disk read MiB/s"] = (None if sectors_read is None or not dt
                              else sectors_read * 512 / 2**20 / dt)
    out["disk ms per read"] = ratio(delta("ms_reading"), reads)
    majflt, refaults = delta("majflt"), delta("refault_file")
    out["major faults/s"] = None if majflt is None or not dt else majflt / dt
    out["file refaults/s"] = None if refaults is None or not dt else refaults / dt

    def series(key):
        return [s[key] for s in inside if s[key] is not None]

    dirty, writeback, temp = series("dirty"), series("writeback"), series("temp")
    out["Dirty max MiB"] = max(dirty) / 1024 if dirty else None
    out["Dirty mean MiB"] = mean(dirty) / 1024 if dirty else None
    out["Writeback max MiB"] = max(writeback) / 1024 if writeback else None
    out["temperature max C"] = max(temp) / 1000 if temp else None
    anon, cached, available = series("anon"), series("cached"), series("available")
    out["AnonPages max MiB"] = max(anon) / 1024 if anon else None
    out["Cached min MiB"] = min(cached) / 1024 if cached else None
    out["MemAvailable min MiB"] = min(available) / 1024 if available else None
    return out


SAMPLE_ROWS = ("disk write MiB/s", "disk ms per write", "disk busy %", "disk flushes/s",
               "disk ms per flush", "disk read MiB/s", "disk ms per read", "major faults/s",
               "file refaults/s", "Dirty max MiB", "Dirty mean MiB", "Writeback max MiB",
               "AnonPages max MiB", "Cached min MiB", "MemAvailable min MiB",
               "temperature max C")


def sample_runs(run, doc):
    """One sampled run's rows, [(label, (timed, first, last))], and whether the log placed the
    timed loop; None when the run has no samples."""
    base = run["json"][:-len(".json")] if run["json"].endswith(".json") else run["json"]
    try:
        samples = read_samples(base + ".sys.tsv")
    except OSError:
        return None
    if len(samples) < 2:
        return None
    elapsed = (doc.get("throughput") or {}).get("elapsed_s")
    start, end, placed = timed_span(samples, run["log"], elapsed)
    third = (end - start) / 3
    windows = [sample_stats(samples, start, end), sample_stats(samples, start, start + third),
               sample_stats(samples, end - third, end)]
    rows = [(label, tuple(w.get(label) for w in windows)) for label in SAMPLE_ROWS]
    try:
        extents = read_extents(base + ".frag.tsv")
    except OSError:
        extents = []
    if extents:
        rows.append(("data file extents (first/last sample)",
                     (None, extents[0][1], extents[-1][1])))
    return rows, placed


def print_samples(mine, docs, rounds):
    blocks = []
    notes = []
    for side in (side_a, side_b):
        found = []
        for rnd in rounds[side]:
            run = next(r for r in mine if r["side"] == side and r["round"] == rnd
                       and r["status"] == "0")
            got = sample_runs(run, docs[(side, rnd)])
            if got is not None:
                found.append(got)
                if not got[1]:
                    notes.append(f"{side} round {rnd}: timed loop not found in the log, "
                                 "the whole run's samples used")
        if found:
            blocks.append((side, merge_runs([rows for rows, _ in found])))
    if not blocks:
        return
    print()
    print_windows("system (EXEC_SYS_SAMPLE: 1 s samples of the whole disk and the page cache "
                  "over each run's timed loop, its first and last third; medians over a side's "
                  "rounds)", ("timed", "first 1/3", "last 1/3"), blocks)
    for note in notes:
        print(f"  {note}")


def section(name, fn, *args):
    # a new row block that fails says so and the report goes on: the runs took hours
    try:
        fn(*args)
    except Exception as error:
        print(f"{name}: not rendered ({type(error).__name__}: {error})")


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
        print(f"{side} page size: {page_text(first)}")
        for rnd, header in headers[1:]:
            if knob_text(header.get("knobs", {})) != knob_text(first.get("knobs", {})):
                print(f"{side} round {rnd} KNOBS DIFFER: {knob_text(header.get('knobs', {}))}")
            if page_text(header) != page_text(first):
                print(f"{side} round {rnd} PAGE SIZE DIFFERS: {page_text(header)}")
    sides_knobs = {knob_text(doc["header"].get("knobs", {})) for doc in docs.values()}
    if len(sides_knobs) > 1:
        print(f"KNOBS DIFFER between runs of {profile}: the ratios compare different loads")
    pairs = sorted(rnd for (s, rnd) in docs if s == side_a and (side_b, rnd) in docs)
    if not pairs:
        print("no complete pair: no aggregation")
        continue
    rounds = {side: sorted(rnd for (s, rnd) in docs if s == side) for side in (side_a, side_b)}
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

    def print_gate():
        # from the per-output rows: the pooled tail over the paired rounds, the commit's drift
        for label, a, b, text, per_run in gate_rows(docs, pairs, rounds):
            print(f"{label:<34} {fmt(a):>11} {fmt(b):>11}  {text:<30} {per_run}".rstrip())

    section("gate rows", print_gate)
    section("commit deciles", print_deciles, docs, rounds)
    section("db writes", print_counters, docs, rounds)
    section("system samples", print_samples, mine, docs, rounds)

print()
print("### FILES")
for run in runs:
    state = "" if run["status"] == "0" else f"  (FAILED: {run['status']})"
    json_path = run["json"] if run["status"] == "0" else "-"
    print(f"{run['profile']} {run['side']} round {run['round']}: {json_path}  log {run['log']}{state}")
PY
}

if [ "${1:-}" = --report-only ]; then # re-render a bench dir's reports: no builds, no runs
    [ $# = 2 ] || die "usage: $0 --report-only <bench dir>"
    [ -d "$2/reports" ] || die "$2 has no reports/ dir"
    command -v python3 >/dev/null || die "python3 is needed for the report"
    bench_dir="$(cd "$2" && pwd -P)"
    repo="$(cd "$(dirname "$0")" && git rev-parse --show-toplevel)"
    report="$repo/reth-exec-report-$(uname -n | cut -d. -f1)-$(date +%Y%m%d-%H%M%S).txt"
    {
        echo "### RETH-EXEC v1"
        echo
        echo "### REPORT-ONLY"
        printf '%-18s %s\n' "date:" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "bench dir:" "$bench_dir"
        if [ -f "$bench_dir/reports/runs.tsv" ]; then
            printf '%-18s %s\n' "runs:" "reports/runs.tsv (the script's last runs there)"
        else
            printf '%-18s %s\n' "runs:" "from the report names, <profile>-<side>-<round>.json (no reports/runs.tsv: a failed run is not known, every report counts as completed)"
        fi
        if [ -f "$bench_dir/reports/head.txt" ]; then # the runs' own SYSTEM, CONFIG, BINARIES, RUNS
            echo
            sed -n '/^### SYSTEM/,$p' "$bench_dir/reports/head.txt"
        fi
    } >"$report"
    render_report dir "$bench_dir" >>"$report" || { rm -f "$report"; die "the report failed to render"; }
    printf '\n### END %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$report"
    echo "report: $report"
    exit 0
fi

usage="usage: $0 <dir on the disk under test> [before-commit] | --report-only <bench dir>"
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

page_size_of() { # <backend>: prints EXEC_PAGE_SIZE_<backend>, or nothing when unset
    case "$1" in
        mdbx) echo "${EXEC_PAGE_SIZE_MDBX:-}" ;;
        tndb-b) echo "${EXEC_PAGE_SIZE_TNDB:-}" ;;
    esac
}
for var in EXEC_PAGE_SIZE_MDBX EXEC_PAGE_SIZE_TNDB; do # the bench checks the range
    case "${!var:-}" in
        '') ;;
        *[!0-9]* | 0*) die "$var must be a page size in bytes, not '${!var}'" ;;
    esac
done

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
sys_sample="${EXEC_SYS_SAMPLE:-0}"
case "$sys_sample" in
    0) ;;
    1) [ "$(uname -s)" = "Linux" ] \
        || die "EXEC_SYS_SAMPLE=1 samples /proc and /sys: Linux only (this host is $(uname -s))" ;;
    *) die "EXEC_SYS_SAMPLE must be 0 or 1, not '$sys_sample'" ;;
esac

if [ "$(uname -s)" = "Linux" ]; then
    case "$(stat -f -c %T "$bench_dir")" in
        tmpfs | ramfs) die "refusing $bench_dir: it is tmpfs/ramfs (syncs are free there)" ;;
    esac
fi
# EXEC_SYS_SAMPLE=1: the /proc/diskstats names of the bench dir's file system (sys_dev) and of
# the whole disk under it (sys_disk; the file system's own device when it spans several disks),
# the drive's temperature file and filefrag, each found once, here.
sys_dev="" sys_disk="" sys_temp="" sys_frag="" sys_flush=""
if [ "$sys_sample" = 1 ]; then
    has_stats() { awk -v d="$1" '$3 == d {found = 1} END {exit !found}' /proc/diskstats; }
    sys_source="$(findmnt -no SOURCE -T "$bench_dir" 2>/dev/null | head -1 || true)"
    [ -n "$sys_source" ] || sys_source="$(df -P "$bench_dir" | awk 'NR == 2 {print $1}')"
    sys_source="${sys_source%%\[*}" # btrfs names a subvolume: /dev/sda2[/@home]
    sys_dev="$(basename "$(readlink -f "$sys_source" 2>/dev/null || echo "$sys_source")")"
    has_stats "$sys_dev" \
        || die "EXEC_SYS_SAMPLE=1: $bench_dir is on $sys_source, which has no /proc/diskstats line"
    sys_disk="$(lsblk -nrso KNAME,TYPE "/dev/$sys_dev" 2>/dev/null \
        | awk '$2 == "disk" {print $1}' | sort -u || true)"
    if [ "$(printf '%s\n' "$sys_disk" | grep -c .)" != 1 ]; then # no lsblk, or several disks
        sys_disk=""
        if [ -e "/sys/class/block/$sys_dev/partition" ]; then
            sys_disk="$(basename "$(dirname "$(readlink -f "/sys/class/block/$sys_dev")")")"
        fi
    fi
    if [ -z "$sys_disk" ] || ! has_stats "$sys_disk"; then sys_disk="$sys_dev"; fi
    for file in /sys/block/"$sys_disk"/device/hwmon*/temp1_input \
        /sys/class/nvme/"${sys_disk%n[0-9]*}"/hwmon*/temp1_input; do
        if [ -r "$file" ] && cat "$file" >/dev/null 2>&1; then
            sys_temp="$file"
            break
        fi
    done
    sys_frag="$(command -v filefrag || true)"
    # flush requests completed and ms flushing are diskstats fields 19 and 20 (Linux 5.5)
    sys_flush="$(awk -v d="$sys_disk" '$3 == d {print (NF >= 20 ? "yes" : "no")}' /proc/diskstats)"
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
    page_a="$(page_size_of mdbx)"
    page_b="$(page_size_of tndb-b)"
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
    page_a="$(page_size_of "$backend")"
    page_b="$page_a"
    target_a="$bench_dir/target-before-$before_key-$backend"
    target_b="$bench_dir/target-$backend"
    cache_a="$bench_dir/cache-before-$before_key"
    cache_b="$bench_dir/cache-after"
fi

# A run refused or failed before its first timed run leaves no report.
runs_started=0
sampler_pid=""
stop_sampler() { # never fails: the sampler cannot change a run's status
    [ -n "$sampler_pid" ] || return 0
    kill "$sampler_pid" 2>/dev/null || true
    wait "$sampler_pid" 2>/dev/null || true
    sampler_pid=""
}
cleanup() {
    stop_sampler
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
unset_page="(TN_EXEC_BENCH_PAGE_SIZE or the default)"
kv "page sizes" "$side_a ${page_a:-$unset_page}, $side_b ${page_b:-$unset_page}"
if [ "$(uname -s)" = "Darwin" ]; then
    kv "sync note" "macOS: libmdbx's durable commit is F_FULLFSYNC, tndb-b's a plain fsync; durable commit times are not like-for-like between the backends"
fi
if [ "$sys_sample" = 1 ]; then
    disk_text="$sys_disk"
    [ "$sys_dev" = "$sys_disk" ] || disk_text="$sys_disk (the bench dir's file system: $sys_dev)"
    kv "sys sample" "EXEC_SYS_SAMPLE=1: every 1 s /proc/meminfo Dirty, Writeback, AnonPages, Cached, MemAvailable, /proc/vmstat pgmajfault, workingset_refault_file and /proc/diskstats $disk_text (writes and reads), flush fields ${sys_flush:-no}; temperature ${sys_temp:-not readable}; every 30 s ${sys_frag:-no filefrag (not installed): no extent samples}; reports/<run>.sys.tsv and .frag.tsv; the whole disk: other writers on it show up"
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
manifest="$bench_dir/reports/runs.tsv" # kept for --report-only
: >"$manifest"
rm -f "$bench_dir/reports/head.txt" # rewritten just before the report renders
# EXEC_SYS_SAMPLE=1, in the background beside one run until killed: every second the time, the
# run log's size (the report places the timed loop by it), /proc/meminfo's Dirty and Writeback,
# the drive's temperature, the disk's (and the file system's) /proc/diskstats line, then
# /proc/meminfo's AnonPages, Cached and MemAvailable and /proc/vmstat's two fault counters; filefrag's
# extent count of the run's data file as soon as it exists, then every 30 s. No python here: one
# awk per sample.
sampler() { # <samples file> <extents file> <run log>
    trap - EXIT TERM
    set +e
    local devs="$sys_disk" tick=0 next_frag=0 now file extents
    [ "$sys_dev" = "$sys_disk" ] || devs="$sys_disk $sys_dev"
    {
        echo "# disk $sys_disk"
        echo "# fs $sys_dev"
        echo "# t log_bytes dirty_kib writeback_kib temp_mc dev writes sectors_written ms_writing ms_io flushes ms_flushing reads sectors_read ms_reading anon_kib cached_kib available_kib pgmajfault workingset_refault_file"
    } >"$1"
    [ -z "$sys_frag" ] || echo "# t extents file" >"$2"
    while :; do
        now="${EPOCHREALTIME:-$(date +%s.%N)}"
        awk -v t="$now" -v log_bytes="$(stat -c %s "$3" 2>/dev/null || echo -)" \
            -v devs=" $devs " -v temp_file="$sys_temp" '
            BEGIN {
                dirty = "-"; writeback = "-"; anon = "-"; cached = "-"; available = "-"
                majflt = "-"; refault = "-"
            }
            FILENAME == "/proc/meminfo" {
                if ($1 == "Dirty:") dirty = $2
                else if ($1 == "Writeback:") writeback = $2
                else if ($1 == "AnonPages:") anon = $2
                else if ($1 == "Cached:") cached = $2
                else if ($1 == "MemAvailable:") available = $2
                next
            }
            FILENAME == "/proc/vmstat" {
                if ($1 == "pgmajfault") majflt = $2
                else if ($1 == "workingset_refault_file") refault = $2
                next
            }
            index(devs, " " $3 " ") {
                line[$3] = $8 " " $10 " " $11 " " $13 " " (NF >= 20 ? $19 " " $20 : "- -") \
                    " " $4 " " $6 " " $7
            }
            END {
                temp = "-"
                if (temp_file != "" && (getline value < temp_file) > 0) temp = value
                n = split(devs, dev, " ")
                for (i = 1; i <= n; i++)
                    if (dev[i] in line)
                        print t, log_bytes, dirty, writeback, temp, dev[i], line[dev[i]], anon,
                            cached, available, majflt, refault
            }' /proc/meminfo /proc/vmstat /proc/diskstats >>"$1" 2>/dev/null
        if [ -n "$sys_frag" ] && [ "$tick" -ge "$next_frag" ]; then
            file="$(find "$tmp_dir" -maxdepth 4 -type f \( -name tndb.dat -o -name mdbx.dat \) \
                -print -quit 2>/dev/null)"
            next_frag=$((tick + 1))
            if [ -n "$file" ]; then # "<file>: <n> extents found"
                extents="$("$sys_frag" "$file" 2>/dev/null | awk 'END {print $(NF - 2)}')"
                echo "$now ${extents:--} ${file##*/}" >>"$2"
                next_frag=$((tick + 30))
            fi
        fi
        tick=$((tick + 1))
        sleep 1
    done
}
run() { # <profile> <side>: runs the side's binary once with the profile's settings
    local profile="$1" side="$2" bin tree cache page settings name json log start when load status=0
    local busy_now
    if [ "$side" = "$side_a" ]; then bin="$bin_a" tree="$tree_a" cache="$cache_a" page="$page_a"; else
        bin="$bin_b" tree="$tree_b" cache="$cache_b" page="$page_b"
    fi
    read -r -a settings <<<"$(profile_settings "$profile")"
    # the side's page size, after the caller's environment and before the profile's settings
    [ -z "$page" ] || settings=("TN_EXEC_BENCH_PAGE_SIZE=$page" ${settings[@]+"${settings[@]}"})
    name="$profile-$side-$round"
    json="$bench_dir/reports/$name.json"
    log="$bench_dir/logs/$name.log"
    rm -f "$json" "${json%.json}.sys.tsv" "${json%.json}.frag.tsv"
    busy_now="$(busy_builds)"
    when="$(date -u +%H:%M:%SZ)"
    load="$(uptime | sed 's/.*load average[s]*: //' | tr -d ',')"
    echo "running $profile on $side (round $round)..." >&2
    : >"$log" # the sampler's first sample must not see the previous run's log
    if [ "$sys_sample" = 1 ]; then
        sampler "${json%.json}.sys.tsv" "${json%.json}.frag.tsv" "$log" </dev/null >/dev/null 2>&1 &
        sampler_pid=$!
    fi
    start=$SECONDS
    # From the crate dir, as cargo runs it; the settings go on this command line only.
    (cd "$tree/crates/engine" && env TMPDIR="$tmp_dir" TN_EXEC_BENCH_CACHE_DIR="$cache" \
        TN_EXEC_BENCH_REPORT="$json" ${settings[@]+"${settings[@]}"} \
        "$bin" --ignored --exact exec_bench --nocapture) >"$log" 2>&1 || status=$?
    stop_sampler
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

cp "$report" "$bench_dir/reports/head.txt"
render_report run "$manifest" "$side_a" "$side_b" >>"$report" \
    || die "the report failed to render; after a fix: $0 --report-only $bench_dir"
{
    echo
    echo "### END $(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >>"$report"
echo "report: $report"
[ -z "$failed" ] || die "failed profiles:$failed (see the report)"
