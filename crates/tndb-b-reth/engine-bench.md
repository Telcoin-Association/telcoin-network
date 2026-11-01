# The tndb-b engine against MDBX

`src/engine_bench.rs` (`tndb_b_vs_mdbx`, `#[ignore]`d) runs the [`tndb-b`](../tndb-b/README.md)
engine through its own API (`Env`, not this crate's adapter) against MDBX (`reth-libmdbx`), each
column in a fresh temp dir on the same seeded streams: 1M entries per bulk table, 8-byte BE
sequential keys and 32-byte random keys, 100-byte values. This crate's
[README](README.md#benchmark-reths-db-benches-against-mdbx) has the other bench, reth's database
benches through the adapter.

```text
cargo test --release -p tndb-b-reth tndb_b_vs_mdbx -- --ignored --nocapture --test-threads 1
```

## Run tip (Linux)

Pin the run to performance cores, and put the temp dir on a real disk (never a tmpfs: on Linux
the run stops if it is one), scoped to the one command. On a CPU with P and E cores (`lscpu -e`
shows which CPUs are which), for example:

```text
TMPDIR=/path/on/disk taskset -c 0-7 cargo test --release -p tndb-b-reth tndb_b_vs_mdbx -- --ignored --nocapture --test-threads 1
```

For the durable rows, also turn off the drive's autonomous power states and set the CPU governor
and energy preference to `performance` (this crate's README gives the commands under its Linux
gate). Even then a consumer NVMe drive syncs a commit at one of two speeds and switches between
them during a run, for every engine at once; judge a durable row against MDBX's time for it in the
same run.

## Selecting what runs

Environment variables select what runs (the `src/engine_bench.rs` module docs have the details):

- `TNDB_B_BENCH_N`: entries per bulk table, and rows per `changesets` shape (default 1,000,000).
- `TNDB_B_BENCH_ENGINES`: the columns, comma-separated (default all): `t4d`, `t16d`, `t4n`,
  `md`, `md4`, `mn`.
- `TNDB_B_BENCH_ROWS`: the row groups, comma-separated (default all): `seq`, `rand`, `get-hit`,
  `get-miss`, `scan-fwd`, `scan-rev`, `seek`, `seek-fwd` (the seek row's targets sorted
  ascending), `small`, `mixed`, `clear`, `append` (the API rows),
  `big` (the large-value rows, see [Large values](#large-values-overflow-runs)), `delete` (the
  delete rows, see [Deletes](#deletes)), `changesets` (tndb-b columns only, see
  [Changesets: composite or packed](#changesets-composite-or-packed)), `rewrite` (`t4d` and
  `md4` only, see [Rewrites (reth's write stages)](#rewrites-reths-write-stages)), and the
  aliases `bulk`, `get` and `scan`.
- `TNDB_B_BENCH_REPEAT=k`: each selected read row runs k times and reports the median.

## The columns and rows

- `t4d` and `t16d`: tndb-b `Durable` with 4 KiB and 16 KiB pages; `t4n`: 4 KiB `NoSync`.
- `md`: MDBX (`reth-libmdbx`) `Durable`, opened as `tn-storage` opens it: a `write_map`
  environment with the OS page size and a generous geometry. `md4`: the same with 4 KiB pages.
  `mn`: MDBX `SafeNoSync`. On x86_64 the OS page is 4 KiB, so `md` and `md4` are the same
  configuration there.

**Like-for-like pairs:** `t4d` with `md4` everywhere, and `t16d` with `md` on Apple Silicon,
where the OS page is 16 KiB. On x86_64 `t16d` has no MDBX twin. `t4n` has none either: a
`SafeNoSync` commit costs far more than a tndb-b NoSync commit on both platforms (see
[NoSync against SafeNoSync](#nosync-against-safenosync)).

A bulk row's "put loop" runs from `begin` to just before `commit`, and its "commit" is the rest.
The API rows time the put loop into a fresh environment: tndb-b's `RwTxn::append`,
`RwCursor::put` and `RwCursor::append` against MDBX's `put` with `WriteFlags::APPEND`,
`Cursor::put`, and `Cursor::put` with `WriteFlags::APPEND`. The "txn cursor" rows run the same
puts through `TxnCursor::put` and `TxnCursor::append` (tndb-b only): the cost of a cursor that
shares its transaction, against the `RwCursor` rows.

The "fdatasync" rows in the tables below reran the small-commit workload with commit step 4
forced to one `fdatasync` of the whole file for every commit (a test-only switch,
`Writer::data_by_fdatasync`). Step 4 picks the `fdatasync` itself whenever the dirty pages form two
or more ranges or more than 16 runs, so the two modes differ only on one-range commits, which step
4 syncs by one `msync`. The two modes alternated in blocks of 100 transactions (A B B A order) on
the same table, so neither gained from running second.

**Where these tables ran.** Every table here was taken while this bench lived in the engine crate
(`cargo test -p tndb-b`), built with the engine's test configuration. It moved here unchanged but
for the rows that need the engine's test-only switches: the "fdatasync" rows and the churn with
merging off are now the engine's own bench, `tndb_b_sync_and_merge` (the engine README's
[Step 4](../tndb-b/README.md#step-4-one-msync-or-one-fdatasync) has its numbers), and the durable
small-commit rows here no longer alternate with a forced `fdatasync`. Those rows stay in the tables
below as measured. The engine is now a plain dependency of the bench, without its test-only code.

**Which code the tables measured.** The macOS and Linux tables below were both taken on
2026-10-20 with M1 and its performance work, before step 4 became one sync: each dirty range was
synced by its own `msync` (one `fdatasync` past 16 runs), and there the "fdatasync" rows replaced
those msyncs on every commit. They also predate every M2 change (format v2 and its page counters,
overflow runs, merging, the one-descent delete, the owned and detached cursors). The large-value
and delete tables further down were taken during M2, each with its code state given. None has
been rerun on the final M2 code; this crate's Linux gate ran reth's benches
(`tndb_reth_vs_mdbx`), not these.

## macOS run

**Run:** Apple M1 Max, 64 GiB RAM, internal SSD (APFS), macOS (Darwin 24.6), 2026-10-20; code: M1
with its performance work, per-range msyncs at step 4, before M2. Each cell is the median of
three full runs (each about 45 s). The Mac had background load, and the
MDBX cells moved by up to 20% between runs (`md` random insert: 779–894 ms), so a difference
under about 10% is noise. How step 4 syncs matters mainly to the small-commit rows: a bulk
commit is one range, synced by one `msync` before and after, and nearly every mixed-row commit
already took the `fdatasync` past 16 runs. Since step 4 became one sync, the Mac's small-commit
median is 73 us at 4K and 90 us at 16K, against 114 and 130 us here (see
[Step 4](../tndb-b/README.md#step-4-one-msync-or-one-fdatasync)).

| | `t4d` | `t16d` | `t4n` | `md` | `md4` | `mn` |
|---|--:|--:|--:|--:|--:|--:|
| seq insert 1M, 1 txn | 96.7 ms | 81.8 ms | 62.1 ms | 140.4 ms | 147.3 ms | 127.2 ms |
| seq: put loop | 59.8 ms | 47.3 ms | 57.0 ms | 110.5 ms | 115.4 ms | 109.6 ms |
| seq: commit | 36.9 ms | 34.5 ms | 5.1 ms | 29.9 ms | 31.5 ms | 16.1 ms |
| random insert 1M, 1 txn | 892.1 ms | 829.9 ms | 801.6 ms | 820.3 ms | 773.0 ms | 830.7 ms |
| random: put loop | 800.9 ms | 741.4 ms | 792.9 ms | 754.0 ms | 683.4 ms | 795.1 ms |
| random: commit | 91.2 ms | 91.1 ms | 8.6 ms | 66.3 ms | 61.1 ms | 30.8 ms |
| file size (st_size) | 520.0 MiB | 528.0 MiB | 520.0 MiB | 512.0 MiB | 512.0 MiB | 512.0 MiB |
| file used (pages × P) | 318.1 MiB | 311.0 MiB | 318.1 MiB | 312.3 MiB | 324.6 MiB | 312.3 MiB |
| get hit (per op) | 717 ns | 668 ns | 724 ns | 696 ns | 681 ns | 713 ns |
| get miss (per op) | 752 ns | 746 ns | 698 ns | 742 ns | 712 ns | 768 ns |
| scan forward 1M | 46.6 ms | 27.1 ms | 43.0 ms | 30.2 ms | 55.8 ms | 31.7 ms |
| scan reverse 1M | 38.8 ms | 24.9 ms | 36.6 ms | 30.0 ms | 37.1 ms | 29.2 ms |
| seek + 10 entries (per seek) | 1.2 us | 1.1 us | 1.1 us | 1.1 us | 1.1 us | 1.1 us |
| small txn (3 puts) median | 114.4 us | 129.9 us | 5.9 us | 1.3 ms | 1.3 ms | 2.3 ms |
| small txn p99 | 189.5 us | 192.0 us | 14.1 us | 3.8 ms | 3.8 ms | 6.6 ms |
| small txns total (2000) | 234.7 ms | 262.7 ms | 14.0 ms | 2.98 s | 2.95 s | 5.42 s |
| fdatasync: median | 75.3 us | 83.9 us | - | - | - | - |
| fdatasync: p99 | 125.8 us | 117.2 us | - | - | - | - |
| fdatasync: total | 155.7 ms | 171.9 ms | - | - | - | - |
| mixed 100k upd/del (1000/txn) | 3.34 s | 2.63 s | 256.5 ms | 3.27 s | 4.26 s | 1.62 s |
| clear_table 1M + commit | 1.4 ms | 569.0 us | 1.4 ms | 2.2 ms | 2.7 ms | 3.6 ms |
| append put 1M (put loop) | 53.9 ms | 48.3 ms | 59.4 ms | 64.6 ms | 69.2 ms | 67.7 ms |
| cursor put 1M (put loop) | 58.5 ms | 52.3 ms | 61.4 ms | 50.1 ms | 51.2 ms | 50.7 ms |
| cursor put APPEND 1M (put loop) | 57.8 ms | 51.5 ms | 60.2 ms | 57.9 ms | 61.6 ms | 59.2 ms |

**What M1's performance work changed** (this table replaced a run of the first M1 code). Every
guarantee and damage check is kept; the checks moved to where they are needed.

- **Provenance trust.** The writer's descent skips the per-fetch and separator-range checks on
  pages this transaction built, and computes the separator bounds only when it first meets a
  published page (item 11 of the engine README's
  [do-not-flag list](../tndb-b/README.md#intentional-design-decisions-please-do-not-flag-these)).
- **The path cache.** A table put that lands in the leaf of the table's last put goes straight
  into it, with no descent (item 12).
- **Append and cursor put.** `RwTxn::append`, and `RwCursor::put`/`append` placed from the path
  the write took. An append split leaves the left leaf as it is and refills the path cache.
- **One header parse per page, cold errors.** The parsed views (`View`, `LeafView`,
  `BranchView`, `LeafMut`) check a page's header once for many cell reads. The hot accessors are
  `#[inline]` and build their errors in `#[cold]` functions.
- **Search prefetch.** Each binary-search probe prefetches the two cells the next probe may read.
- **Cheaper splits.** `SplitBuf` plans a split in reused buffers and writes the right half
  straight into the new page, with no heap allocation per split.

At 16K, against the run this table replaces: the sequential insert went from 391 to 82 ms (its
put loop from about 367 to 47 ms), the random insert from 1097 to 830 ms, the forward scan from
38.8 to 27.1 ms, and a seek from 1.3 to 1.1 us. Small commits, the mixed workload and
`clear_table` stayed level in back-to-back comparisons.

## Linux run

**Run:** Linux x86_64 laptop (i7-1360P, NVMe, ext4), 2026-10-20, three full runs; code: M1 with
its performance work, per-range msyncs at step 4 (before the one-range rule), before M2. Each run
took 132–163 s.

**CPU-bound rows.** Each cell is the median of the three runs. tndb-b's cells repeated within a
few percent, and the median drops a few one-run outliers in `mn`. The NoSync bulk totals are
shown for `t4n` and `mn` only; the durable totals follow the drive (below).

| | `t4d` | `t16d` | `t4n` | `md` | `md4` | `mn` |
|---|--:|--:|--:|--:|--:|--:|
| seq: put loop | 53.5 ms | 46.0 ms | 53.6 ms | 115.7 ms | 115.8 ms | 115.8 ms |
| random: put loop | 512.9 ms | 557.8 ms | 517.4 ms | 544.2 ms | 546.8 ms | 563.3 ms |
| seq insert 1M, 1 txn (NoSync) | - | - | 60.3 ms | - | - | 116.1 ms |
| random insert 1M, 1 txn (NoSync) | - | - | 531.3 ms | - | - | 563.8 ms |
| file size (st_size) | 516.0 MiB | 528.0 MiB | 516.0 MiB | 512.0 MiB | 512.0 MiB | 512.0 MiB |
| file used (pages × P) | 318.1 MiB | 311.0 MiB | 318.1 MiB | 324.6 MiB | 324.6 MiB | 324.6 MiB |
| get hit (per op) | 497 ns | 545 ns | 494 ns | 587 ns | 602 ns | 603 ns |
| get miss (per op) | 505 ns | 569 ns | 504 ns | 619 ns | 626 ns | 632 ns |
| scan forward 1M | 31.9 ms | 20.8 ms | 31.9 ms | 42.7 ms | 42.5 ms | 35.2 ms |
| scan reverse 1M | 32.6 ms | 21.2 ms | 32.6 ms | 30.2 ms | 30.3 ms | 30.4 ms |
| seek + 10 entries (per seek) | 1.1 us | 855 ns | 1.1 us | 1.0 us | 1.0 us | 1.1 us |
| append put 1M (put loop) | 52.2 ms | 46.0 ms | 52.8 ms | 56.2 ms | 56.2 ms | 55.6 ms |
| cursor put 1M (put loop) | 55.4 ms | 48.6 ms | 55.7 ms | 48.3 ms | 47.5 ms | 47.2 ms |
| cursor put APPEND 1M (put loop) | 54.4 ms | 48.2 ms | 54.8 ms | 52.6 ms | 52.9 ms | 52.1 ms |

The MDBX forward-scan medians hide a split: each MDBX column had runs of about 32 ms and runs of
about 42.7 ms, while tndb-b's scans moved by at most 0.2 ms. The cause is unconfirmed; the core
type the run landed on is one guess (see the [run tip](#run-tip-linux)).

**Durable rows.** Each cell is the range (min–max) of the three runs.

| | `t4d` | `t16d` | `md` | `md4` |
|---|--:|--:|--:|--:|
| seq: commit | 32.7–119.3 ms | 28.6–127.2 ms | 33.0–143.4 ms | 104.0–170.9 ms |
| random: commit | 51.9–214.6 ms | 63.0–223.8 ms | 50.2–406.3 ms | 191.7–280.6 ms |
| small txn (3 puts) median | 904.3–920.2 us | 703.2–931.3 us | 543.6 us–3.5 ms | 3.4 ms (all three) |
| small txn p99 | 4.9–16.9 ms | 5.6–7.1 ms | 848.6 us–7.0 ms | 3.9–7.3 ms |
| small txns total (2000) | 2.15–5.45 s | 1.85–4.29 s | 1.13–8.19 s | 5.72–6.48 s |
| fdatasync: median | 514.5–515.9 us | 531.8–568.1 us | - | - |
| fdatasync: p99 | 3.4–7.2 ms | 3.5–3.9 ms | - | - |
| fdatasync: total | 1.21–3.17 s | 1.54–3.38 s | - | - |
| mixed 100k upd/del (1000/txn) | 7.90–57.80 s | 3.11–10.62 s | 33.33–43.74 s | 3.47–27.54 s |
| clear_table 1M + commit | 4.3–9.1 ms | 900.8 us–4.0 ms | 1.6–11.6 ms | 1.6–7.2 ms |

**These rows measure the drive's state, not the engine.** On x86_64 `md` and `md4` are the same
configuration, yet within one run they differed by up to 12.6× (mixed 43.7 s vs 3.5 s;
sequential commit 33 vs 171 ms). By estimate, each column writes 1–3 GB (two bulk loads, the
mixed commits and three fresh 1M-entry files), so a later column meets a busier drive. The
fastest runs are level:
sequential commit 32.7 ms (`t4d`) vs 33.0 ms (`md`), random commit 51.9 vs 50.2 ms.

**The small-commit A/B rows do not drift with the drive,** because the two modes alternate on one
table. They are why step 4 changed (see
[Step 4](../tndb-b/README.md#step-4-one-msync-or-one-fdatasync)). Under the one-range rule, about
3 in 4 small commits have two or more ranges and take the `fdatasync` path, so the next Linux run
should show a small-commit median near the "fdatasync" rows. That is not yet measured.

## Where tndb-b wins and loses

From the macOS and Linux tables above, so for the code before M2.

**On Apple Silicon, where tndb-b loses, and why** (the macOS table):

- **The commit of a bulk load costs more:** 91 ms against MDBX's 61–66 ms after the random
  insert, and 35–37 ms against 30–32 ms after the sequential one. A tndb-b commit stamps a CRC on
  every page it wrote, msyncs the dirty pages (one range for a bulk load), and fsyncs the file's
  growth; in a profile of the sequential commit, msync took about 77%, CRCs 15% and the growth
  fsync 8%.
- **Random inserts at 4K:** the put loop takes 801 ms against `md4`'s 683 ms (+17%). Random keys
  rarely land in the cached leaf, so each put descends. MDBX's put loop gets faster at 4K than at
  16K while tndb-b's gets slower, which points at tndb-b's per-page costs: each level of the
  descent, and each split (about four times as frequent per entry at 4K). At 16K the put loop is
  level with MDBX (741 vs 754 ms).
- **A plain cursor put at 4K** takes 58.5 ms against `md4`'s 51.2 ms (+14%); at 16K it is level
  (52.3 vs 50.1 ms).
- **Point reads at 4K** are about 5% slower than `md4` (717 vs 681 ns hit, 752 vs 712 ns miss),
  which is at the noise level. A fetch from the published file keeps its range, trailer, page
  number and txn checks, and a miss checks the leaf against its separators. At 16K reads are level
  with MDBX.

**On Apple Silicon, where tndb-b wins:**

- **Sequential loads:** the put loop is 2.3× faster than MDBX's at 16K (47 vs 111 ms) and 1.9×
  at 4K (60 vs 115 ms), because almost every put lands in the cached leaf. A plain put even beats
  MDBX's `APPEND` put (65–69 ms). `RwTxn::append` takes 48–54 ms, and `RwCursor::append` is a
  little faster than MDBX's cursor `APPEND` (51.5 vs 57.9 ms at 16K, 57.8 vs 61.6 ms at 4K).
- **Small durable commits are about 10× cheaper** in the table (130 us vs 1.3 ms median at 16K,
  114 us vs 1.3 ms at 4K), and more since step 4 became one sync (90 and 73 us; see
  [Step 4](../tndb-b/README.md#step-4-one-msync-or-one-fdatasync)). tndb-b syncs only the pages
  the commit dirtied, by one `msync` or one `fdatasync`, plus one meta page. MDBX msyncs the whole
  used span of the map on every commit (about 312 MB here), and on macOS that walk is the cost.
- **Scans** are 10–17% faster than MDBX's at 16K (27.1 vs 30.2 ms forward, 24.9 vs 30.0 ms
  reverse). At 4K the forward scan beats `md4` (46.6 vs 55.8 ms), and the reverse one is level.
- **`clear_table`** frees a 1M-entry tree 2–4× faster (569 us vs 2.2 ms at 16K).
- **The durable mixed workload** matches MDBX or beats it (2.63 vs 3.27 s at 16K, 3.34 vs 4.26 s
  at 4K). MDBX's own cell moved most between runs here (2.72–3.33 s at 16K), so read this row as
  parity.
- **File sizes match:** the used pages within 2%, the file lengths within 3%.

**On Linux x86_64** (the Linux tables; "MDBX" is the median of the six `md` and `md4` runs, the
same configuration there):

- **At 4K, tndb-b wins** the sequential put loop by 2.2× (53.5 vs 115.7 ms), the random put loop
  by 6% (512.9 vs 545.5 ms), `RwTxn::append` by 7% (52.2 vs 56.2 ms), and gets by 16–19% (497 vs
  589 ns hit, 505 vs 622 ns miss). Small durable commits took 0.90–0.92 ms with per-range
  msyncs, against 3.4–3.5 ms in 5 of the 6 MDBX runs; the sixth, an `md` run, took 0.54 ms.
- **At 4K, level:** the forward scan against MDBX's fast runs (31.8–31.9 ms both; its slow runs
  took about 42.7 ms), and the bulk commits at the fastest runs (above).
- **At 4K, tndb-b loses** on a plain cursor put (+15%: 55.4 vs 48.2 ms), the reverse scan (+8%:
  32.6 vs 30.3 ms) and the cursor `APPEND` (+3%: 54.4 vs 52.6 ms). A seek is about level (1.0–1.1
  vs 1.0 us, at the table's 0.1 us resolution).
- **At 16K, tndb-b beats MDBX on every CPU row but two:** the random put loop (557.8 vs 545.5 ms,
  +2%) and the plain cursor put, which is level (48.6 vs 48.2 ms).
- **4K against 16K:** 16K wins the scans by 35%, a seek by about 20% and the sequential puts by
  11–14%. 4K wins gets by 9–11% and random puts by 8%.
- **The Mac's 4K losses do not occur here.** The random put loop (+17% on the Mac) and point
  reads (+5%) both beat MDBX at 4K, and tndb-b's random put loop is faster at 4K than at 16K, so
  the per-page explanation above is an Apple Silicon finding.

## NoSync against SafeNoSync

**On macOS**, MDBX under `SafeNoSync` still issues an `MS_ASYNC` msync of the whole span every
commit (`MDBX_MMAP_NEEDS_JOLT` is set off Linux). That makes its small commits slower than its
own Durable mode (2.3 vs 1.3 ms median), and it is most of why tndb-b's NoSync mixed workload is
about 6× faster (256.5 ms vs 1.62 s).

**On Linux**, `mn` took a median of 0.58–3.5 ms per small commit (`t4n` 3.8 us) and 4.3–5.4 s on
the mixed row (`t4n` 0.25 s). The cause was not investigated.

On both platforms, a tndb-b NoSync commit does no I/O, and its deferred `Env::sync` is not timed.

## Large values (overflow runs)

The `big` rows (`TNDB_B_BENCH_ROWS=big`) put values of 2, 8 and 32 KiB: 20k, 10k and 4k entries
under 8-byte keys in a scrambled order, in one transaction. They time the put loop (per put),
its commit, and then a get of every key in another scrambled order (per get). At 4 KiB pages
the values take runs of 1, 3 and 9 pages; at 16 KiB the 2 KiB value is inline and the others take
runs of 1 and 3 pages. Each value size runs in its own fresh environment, so no row writes into
file space another row grew and synced (see the macOS note in the engine README's
[Platform notes](../tndb-b/README.md#platform-notes); MDBX grows its file sparse on macOS, so it
does not pay that cost). The engine's inline limit is about 1 KB at 4 KiB pages, against MDBX's
about 2 KB. `TNDB_B_BENCH_N` does not scale these rows.

**Run:** the Mac above, 2026-10-20, medians of three runs; code: M2 with overflow runs and the
one-sync step 4, before run allocation loaded records for a fit, merging and the later M2
changes. Not rerun since.

| | `t4d` | `t16d` | `md` | `md4` |
|---|--:|--:|--:|--:|
| put 2 KiB (per op) | 918 ns | 1.60 us | 1.30 us | 671 ns |
| commit | 29.9 ms | 19.0 ms | 16.6 ms | 22.9 ms |
| get 2 KiB (per op) | 302 ns ¹ | 419 ns | 361 ns | 293 ns |
| put 8 KiB (per op) | 2.00 us | 2.30 us | 2.00 us | 1.70 us |
| commit | 36.5 ms | 61.3 ms | 38.0 ms | 32.4 ms |
| get 8 KiB (per op) | 1.10 us | 271 ns ¹ | 286 ns | 286 ns |
| put 32 KiB (per op) | 4.30 us | 8.00 us | 4.90 us | 4.20 us |
| commit | 57.9 ms | 65.0 ms | 46.3 ms | 36.5 ms |
| get 32 KiB (per op) | 2.20 us | 2.60 us | 282 ns | 274 ns |

¹ A 1-page run, read with its header checks only (the engine README's do-not-flag item 13). These
two cells come from a later set of three interleaved runs, after that change; before it the same
gets took 724 ns and 1.30 us, and the multi-page gets did not move.

- **Puts** of 2 and 8 KiB at 4 KiB pages are level with MDBX or within about 0.3 us of `md4`, and
  32 KiB puts are level with `md4` (4.3 vs 4.2 us). The `t16d` 32 KiB put varied most (5.4–8.0 us
  across the three runs).
- **Gets** of a multi-page run cost the CRC pass over the value (item 13): 1.1 and 2.2 us at 4 KiB
  pages against MDBX's 0.3 us, which hands back a pointer. A 1-page run reads like an inline
  value.
- **Commits** stamp one CRC per run and sync every run page, so they grow with the bytes written,
  as MDBX's do, but faster: at 32 KiB, 57.9 ms for `t4d` against 36.5 ms for `md4`. The cause is
  not investigated.

## Deletes

The `delete` rows (`TNDB_B_BENCH_ROWS=delete`) time deletes, which merge pages in tndb-b (see
[Deletes and merging](../tndb-b/README.md#deletes-and-merging)) and rebalance them in MDBX. Each
runs in a fresh environment, so it changes no table another row reads:

- **delete 90% + commit:** a random table of N entries is filled (untimed), then 90% of its keys
  are deleted in a random order in one transaction, timed as a whole and split into the delete
  loop and the commit, as a bulk insert is.
- **scan after:** a forward scan of the tenth left, which shows how well the deletes left the
  pages filled.
- **mixed 50/50:** operations over a random table of N entries in transactions of a fixed size:
  half delete a random present key, and half put, re-inserting the key deleted longest ago. The
  table stays within a few hundred entries of N, so the row measures deletes and inserts at a
  steady size.
- **churn file size:** ten rounds over a table of N/4 random keys, each a fill and a
  delete of 90% of the keys at random, timed as a whole; then the file's used span (pages times
  the page size) and its size. When this table was taken, tndb-b ran the churn twice, with merging
  and without (the "churn (no merge)" rows, through the test-only `Writer::no_merge`), to show what
  merging saves; that run is now the engine's `tndb_b_sync_and_merge`.

**Run:** Apple M1 Max, macOS, 2026-10-21, N = 300k, medians of three runs; code: M2 with merging
and the one-descent delete, before the owned and detached cursors, the MDBX-aligned state after a
delete and the adapter. Not rerun since. The "merging off" column reruns tndb-b's rows with
merging turned off; MDBX's two columns are `md` (16 KiB pages here) and `md4`.

| | `t4d` | `t16d` | `t4d`, merging off | `md` | `md4` |
|---|--:|--:|--:|--:|--:|
| delete 90% + commit | 232.0 ms | 268.8 ms | 235.7 ms | 220.3 ms | 213.9 ms |
| delete loop | 207.2 ms | 245.7 ms | 210.6 ms | 204.6 ms | 197.4 ms |
| commit | 24.5 ms | 22.8 ms | 25.9 ms | 16.3 ms | 16.9 ms |
| scan of the tenth left | 2.5 ms | 1.0 ms | 7.6 ms | 0.9 ms | 1.8 ms |
| mixed 50/50, 100k ops | 923.3 ms | 1009.5 ms | 940.8 ms | 1291.6 ms | 1603.9 ms |
| churn, 10 rounds of 75k | 729.2 ms | 761.4 ms | 742.5 ms | 811.4 ms | 743.2 ms |
| churn: table pages × P | 2.3 MiB | 2.4 MiB | 13.1 MiB | 3.4 MiB | 3.3 MiB |
| churn: file used | 45.6 MiB | 44.9 MiB | 45.7 MiB | 30.2 MiB | 31.1 MiB |

A single run at N = 1M agrees: the delete loop takes 892 ms (`t4d`) and 1046 ms (`t16d`) against
MDBX's 908 ms (`md`) and 1021 ms (`md4`), and after the churn tndb-b's table holds 7.8 MiB
(43.8 MiB without merging) against MDBX's 11.2 MiB, in a used span of 152 MiB against
101–104 MiB.

What the rows show:
- **Merging works.** After the churn the table takes 5–6 times fewer pages than without merging
  (2.3 against 13.1 MiB at 4K), and is smaller than MDBX's. After deleting 90% of a table, the scan
  of what is left is three times faster at 4K.
- **Merging costs almost nothing.** Delete, mixed and churn times are within a few percent of
  merging off. The other rows (puts, small commits, mixed updates, `clear_table`) are unchanged.
- **Random deletes are level with MDBX's.** At 300k the 4K delete loop is within 5% of `md4`
  (207 vs 197 ms, the noise level here), and at 1M it is level or faster (892 vs 908–1021 ms).
  They were 44% slower while `write::delete` descended twice, once to look the key up, so a miss
  changed nothing, and again to plan the change (304 vs 200 ms in the same session). Now one
  descent does both: it defers the verification of the published pages it passes to a hit
  (`Verify::Later`, `verify_path`), so a miss still changes and verifies nothing. That cut the
  delete loop by 32% at 4K and 30% at 16K, and the churn by 21–25%. What is left of the delete
  loop is that one descent and the leaf search (about 60% of it in a profile), the cell's removal
  (12%), and the copies of the path (6% zero-filling the page `alloc` returned, then the copy;
  `alloc` no longer zero-fills, tndb-b's README do-not-flag item 17).
- **The mixed 50/50 workload is faster than MDBX's** at both page sizes.
- **After the churn, the file's used span is about 1.5× MDBX's,** even though tndb-b's tree is
  smaller. Each round is a fill transaction followed by a delete transaction, and a page freed by
  one transaction is reused only from the second transaction after it, once no meta slot names a
  state that reaches it (the reuse gate). So each fill grows the file instead of reusing the pages
  the delete just freed. The gate is deliberate: it keeps the previous state recoverable if the
  newest meta is lost.

## Changesets: composite or packed

The `changesets` rows (`TNDB_B_BENCH_ROWS=changesets`, tndb-b columns only) ask whether reth's
changeset tables, composite keys here (`DUP_LAYOUTS`), would do better as dup tables of
`DupOptions::packed(K)`, the state tables' form. Each shape has reth's widths and about N rows,
appended in ascending order with a commit every 10 blocks:

- **StorageChangeSets:** K 28 (block ‖ address), subkey 32, values of 1..=32 bytes; 200 addresses
  a block, 1..=4 slots each (2000 blocks at N = 1M), so every key's rows fit a packed set.
- **AccountChangeSets:** K 8 (block), subkey 20, values of 0..=100 bytes; 200 accounts a block
  (5000 blocks), so every key nests at any bound.

Per form, the rows are the load per row (commits included), a `walk_range` per row read (a seek to
a block's first row, then steps across keys through 100 blocks; ten such walks spread over the
blocks), a point get per op, the file's used span and the table's pages. Each column runs the forms
in ABBA order (composite, packed, packed, composite), each pass in a fresh environment, and a cell
is the mean of its form's two passes.

**Run:** Apple M1 Max, macOS, 2026-10-31, N = 1M, code: the packed dup sets (23e55c6f6). Times are
the median of three runs, with the spread (max − min) in brackets; the pages were the same in every
run.

| | `t4d` composite | `t4d` packed | `t4n` composite | `t4n` packed |
|---|--:|--:|--:|--:|
| StorageChangeSets: append (per row) | 396 ns (22) | 561 ns (31) | 60 ns (1) | 293 ns (19) |
| walk_range (per row) | 29 ns (1) | 46 ns (4) | 20 ns (1) | 38 ns (2) |
| get (per op) | 714 ns (92) | 751 ns (139) | 723 ns (51) | 685 ns (44) |
| file used | 85.6 MiB | 68.1 MiB | 90.5 MiB | 72.7 MiB |
| table pages × P | 85.5 MiB | 68.1 MiB | 85.5 MiB | 68.1 MiB |
| AccountChangeSets: append (per row) | 542 ns (17) | 637 ns (34) | 60 ns (4) | 145 ns (6) |
| walk_range (per row) | 31 ns (4) | 35 ns (0) | 20 ns (1) | 27 ns (3) |
| get (per op) | 683 ns (25) | 745 ns (107) | 684 ns (81) | 736 ns (132) |
| file used | 86.4 MiB | 105.3 MiB | 98.8 MiB | 114.6 MiB |
| table pages × P | 86.4 MiB | 105.2 MiB | 86.4 MiB | 105.2 MiB |

The rule: a changeset table switches to a packed dup table only if its append and `walk_range`
rows are within their spread of composite and its file shrinks by 15% or more. Both stay
composite:

- **StorageChangeSets:** packing shrinks the table by 20.4% (a key's 28 bytes are stored once for
  its set, not once a row), but the append is 42% slower at `t4d` and 4.9× at `t4n` (each row
  appended to a packed key rewrites the key's set), and the walk 1.6× and 1.9×, all far past their
  spread.
- **AccountChangeSets:** every key nests (a nested tree a block), the table grows by 22%, and the
  append and walk are slower than composite by more than their spread.

The point gets are level within their spread in both shapes. `t4n`'s used span is larger than
`t4d`'s at the same table pages: with no sync a freed page is never reused (the reuse gate).

## Rewrites (reth's write stages)

The `rewrite` rows (`TNDB_B_BENCH_ROWS=rewrite`, columns `t4d` and `md4` only) price reth's write
stages per call: a positioning seek through a write cursor, then a write at its row, in ascending
key order, as reth writes. Each table is N rows, loaded in a random order (untimed), then
rewritten by 20 transactions of 10k present rows each, picked at random (no row twice) and sorted,
through one write cursor a transaction:

- **history** (reth's `append_history_index`, the open shard): a plain table of 28-byte keys (20
  random bytes ‖ `u64::MAX`) and values of 16..=160 bytes. Per key the adapter's calls for
  `seek_exact` and `upsert`: `TxnCursor::seek_exact`, then `TxnCursor::put` of the value grown by
  8 bytes. MDBX as reth-db calls it: `Cursor::set_key`, `Cursor::put` with `WriteFlags::UPSERT`.
- **dup** (reth's `write_hashed_state`, the storage loop): a dup table as this crate stores
  HashedStorages (32-byte keys, subkeys and values), all N rows under 3 keys, so each key is a
  nested tree of N/3 rows (as on TN, where two stablecoins and a router hold all the slots). Per
  pair the adapter's engine calls for `seek_by_key_subkey`, `delete_current` and `upsert`:
  `DupTxnCursor::seek_dup_with_key_tail`, then `delete_current`, `follower_shares_prefix` and
  `insert` of key ‖ subkey with a new value. MDBX, a `DUP_SORT` table of subkey ‖ value:
  `get_both_range`, `del` with `WriteFlags::CURRENT`, `put` with `WriteFlags::UPSERT`.

Each table also runs **seek only**, the same transactions' seeks without the writes (aborted), so
a write's own cost is the rewrite's loop less the seek-only loop. Each pass is a fresh environment.
A loop cell is the median transaction's loop per key; a commit cell the median commit.

**Run:** Apple M1 Max, macOS 15.8, 2026-11-01, N = 1M, code 08a4c3ee3 plus these rows, the median
of three runs (1-minute load average 2.6-3.3, no build running). Per key (per pair):

| | `t4d` | `md4` | ratio |
|---|--:|--:|--:|
| history: `seek_exact` + `upsert` | 2984 ns | 1894 ns | 1.58 |
| commit (median transaction) | 104.2 ms | 90.4 ms | 1.15 |
| `seek_exact` only | 601 ns | 523 ns | 1.15 |
| the write's own (rewrite − seek only) | 2383 ns | 1371 ns | 1.74 |
| dup: `seek_by_key_subkey` + `delete_current` + `upsert` | 2773 ns | 1805 ns | 1.54 |
| commit (median transaction) | 73.1 ms | 56.0 ms | 1.31 |
| `seek_by_key_subkey` only | 587 ns | 517 ns | 1.14 |
| the write's own (rewrite − seek only) | 2186 ns | 1288 ns | 1.70 |

What it shows (macOS `sample` of the `t4d` rewrite loops, `tndb_hist_txn` and `tndb_dup_txn`):

- The writes, not the seeks, carry the gap: a seek alone is within 15% of MDBX's, the write after
  it 1.7× MDBX's.
- Every history `put` after a seek descends from the root (`put_cached` -> `put_from_root`, no
  cache hit), and almost every dup delete does (`delete_cached` -> `delete_from_root`); the dup
  insert that follows hits the path cache the delete left. The re-descent itself
  (`descend_write` and its leaf search) is about 9% of the history loop and 6% of the dup loop.
- The larger cost is the copy-on-write of the path (`make_path_writable`, almost all of it the
  page copies' `memmove`, and the pages' `Writer::alloc`): 52% of the history loop and 45% of the
  dup loop. MDBX's copy-on-write (`page_touch_unmodifable`) takes about 0.7 of tndb-b's samples
  per write.
- A seek inside a transaction that has written costs more than a seek alone (about 900 ns against
  601 in the history loop), a quarter of it in `Writer::child`.

