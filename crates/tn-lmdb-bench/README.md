# tn-lmdb-bench

A regression benchmark of the `tn-lmdb` engine (`crates/tn-lmdb`), driven only through its public
API, so the same source builds against two revisions of the engine and their results compare row
by row. It depends on nothing else in the workspace (`tn-lmdb`, `serde`, `serde_json`, `tempfile`),
and its source is `src/` alone, so it also builds as a standalone package whose `tn-lmdb` is a path
to another checkout.

Every row is a fixed, seeded workload with a stable name. It runs `reps` times, each rep in a fresh
environment in a fresh temp directory (under `$TMPDIR`), and records one sample per rep. Data is
generated before timing; a timer wraps the measured loop only. Lower is better for every row.

A time row's sample is the fastest of its passes in the rep. A pass is the row's measured loop: run
again over the same state for a read row, on a fresh environment (its state set up untimed) for a
row that writes. A row runs passes until they add up to 30 ms of measured time and number 3 (or add
up to 100 ms), at most 16: a scan of 100k entries takes about 3 ms, a commit well under one, and
a sample that short lands whole in one scheduler hiccup or load spike. A pass of 100 ms or more
runs once.

- **Time rows** (`ns_per_op`): nanoseconds per operation (the "op" column below). Noisy.
- **Exact rows** (`bytes`, `pages_per_op`, `exact: true`): deterministic sizes, the same on every
  run of a revision: any change is real. The smoke test checks that two runs agree.

Each row runs under the sync modes its result depends on (`--sync`). A row of one transaction's
put or delete loop, of the file one commit leaves, or of reads (on any state: under `nosync` the
settles below make it the durable state) does the same work under either mode, so it runs under
`nosync` only (`n` below); a time row that includes commits or is timed between them (the rewrite
rows), and a size row of a state built by several commits, runs under every selected mode (`*`);
`reth.block.pages_synced` runs under `durable` only (`d`).

Under `nosync` no timed region calls `Env::sync`, so a commit row times the in-memory commit. A
`nosync` environment reuses a freed page only once a sync has made the freeing commit durable, so
a row of several commits settles after each one (an untimed `Env::sync`, a no-op under `durable`):
its commits reuse pages as a durable run's do, and its file sizes are the durable run's. A
`nosync` time row is then the durable workload without its syncs' I/O (it still pays the faults
of writing pages again after a sync), which is why it is the one to gate on.

## Rows

`n` is the bulk table size (`--n`); the other counts follow it up to the counts in parentheses.

| row | op | sync | what it models |
|---|---|---|---|
| `put.seq` | put | n | `n` 8-byte big-endian sequential keys, 100-byte values, one transaction: the put loop |
| `put.seq.commit` | commit | * | that transaction's commit (not gated in a run under 1M entries, see below) |
| `put.rand` | put | n | `n` random 32-byte keys, one transaction: the put loop |
| `put.rand.commit` | commit | * | its commit (not gated in a run under 1M entries) |
| `put.rand.file_bytes` | bytes | n | the file's used span (pages times page size) after it |
| `put.append` | append | n | `put.seq` through `RwTxn::append` |
| `put.cursor` | put | n | `put.seq` through `RwCursor::put` |
| `put.txn_cursor` | put | n | `put.seq` through a detached `TxnCursor::put` |
| `get.hit`, `get.miss` | get | n | point gets of every key of the random table, and of `n` absent keys |
| `scan.fwd`, `scan.rev` | entry | n | full cursor scans of the random table |
| `seek.range` | seek | n | a seek to a random absent key, then 9 `next` (100k seeks) |
| `seek.range.sorted` | seek | n | the same targets sorted ascending, as a walker seeks |
| `ro.txn` | transaction | n | short read transactions on one thread, each `begin_ro`, 2 point gets of random hits and its drop (20k), over a table of random entries small enough to stay in the caches (10k), so the gets do not bury the transaction's own cost under memory misses |
| `ro.txn.mt` | transaction a thread | n | the same on 4 threads at once over one environment, each its own gets, started together: the span from the first thread's start to the last one's end over a thread's transactions. Equal to `ro.txn` when the threads share nothing they write, but for the threads' start skew (the last thread to reach the barrier starts at once, the others once woken) and any wait for a core: it wants 4 idle cores, and on a shared runner it is noisy; the ratio is what readers on several threads (an RPC node) pay for the state they share. The thread count is fixed so runs compare across machines; 4 stay on performance cores (an M1 Max has 8) |
| `commit.small` | transaction | * | transactions of 3 puts committed one by one into a fresh environment (2000) |
| `mixed` | operation | * | 50/50 deletes and re-inserts over a full random table, 1000 a transaction (100k) |
| `delete.rand90` | delete | n | 90% of a random table deleted in random order, one transaction |
| `churn` | round | * | 10 rounds of fill and 90% delete over `n / 4` random keys |
| `churn.file_bytes`, `churn.table_bytes` | bytes | * | the file's used span and the table's pages (`Env::check`) after it |
| `big.2k`, `big.8k`, `big.32k` | put | n | values of 2, 8, 32 KiB (overflow runs), one transaction (20k, 10k, 4k) |
| `big.2k.get`, ... | get | n | a get of every key in a scrambled order (multi-page runs pass their CRC) |
| `reth.state.load` | row | * | a PlainStorageState-shaped dup table (key 20, subkey 32, values 1..=32 B, `DupOptions::packed(20)`), `n` rows put in random order, 10k a transaction: 40% of rows under keys of 1..=4 rows (packed), 60% under keys of thousands (nested) |
| `reth.state.get` | get | n | `DupRoCursor::seek_dup` of a random row through a reset cursor (100k) |
| `reth.state.walk` | row | n | every key's rows by `seek_key` then `next_in_prefix` |
| `reth.state.file_bytes` | bytes | * | the file's used span after the load |
| `reth.changesets.append` | row | * | StorageChangeSets' shape as the adapter stores it (a plain table keyed block ‖ address ‖ slot, 8 + 20 + 32), 200 addresses a block, appended with a commit every 10 blocks |
| `reth.changesets.walk` | row | n | 10 `walk_range`s of 100 blocks: a seek, then steps across keys |
| `reth.changesets.file_bytes` | bytes | * | the file's used span after the load |
| `reth.history.rewrite` | key | * | reth's `append_history_index`: `TxnCursor::seek_exact` then `put` of the value grown 8 bytes, 20 sorted transactions over a table of `n` 28-byte keys (10k keys each) |
| `reth.dup.rewrite` | pair | * | reth's `write_hashed_state` storage loop as engine calls, the ones the adapter issued before it made reth's dup rewrite one put: `DupTxnCursor::seek_dup_with_key_tail`, `delete_current`, `insert`, over a HashedStorages-shaped dup table (32/32) of `n` rows under 3 nested keys. The adapter's dup rewrite is now one `DupTxnCursor::put`, which `reth.block`'s slot upserts time |
| `reth.block` | block | * | an execution output's writes, one transaction a block, commit included: a header (600 B), 50 transactions (200 B) and receipts (150 B) appended, 100 account upserts (32-byte keys, 80 B) over an `n / 10` account base (1k to 100k), about 300 storage slot upserts through a dup cursor, both changesets appended, 200 history index extensions (`n / 1000` blocks, 4 to 1000) |
| `reth.block.file_bytes` | bytes | * | the file's used span after the blocks |
| `reth.block.pages_synced` | pages | d | pages synced per block commit (`Env::io_stats`) |

## Tiers

| tier | `n` | reps | sync | wall time (M1 Max) |
|---|---|---|---|---|
| `quick` | 100k | 3 | nosync | about 15 s (durable rows: 6 s more); a comparison of 4 rounds, 2 min |
| `default` | 1M | 3 | nosync | about 2 min |
| `full` | 1M | 7 | nosync, durable | several minutes; durable time depends on the drive |

`--n` and `--reps` override a tier's.

## CLI

```text
tn-lmdb-bench [--tier quick|default|full] [--rows <pat,..>] [--groups generic,reth]
              [--sync nosync,durable] [--n N] [--reps K] [--page-size P] [--out FILE]
              [--list] [--label S]
```

- `--rows` takes exact names or prefixes ending in `*` (`reth.*`); a pattern matching no row is an
  error.
- `--list` prints the (row, group, sync) triples the other arguments would run, tab-separated.
- Progress goes to stderr, the summary table to stdout, the JSON to `--out`.
- Exit 0 on success; 2 on a usage or environment error, refused before any work (a `$TMPDIR` on
  tmpfs or ramfs on Linux, where syncs cost nothing, or one a temp dir cannot be made in; an
  `--out` that cannot be written); 101 when the run itself fails (an engine error such as a full
  disk, or a failed check of a workload's own results: a panic, its message on stderr).
- An interrupted run (a signal) leaves its rep's `tn-lmdb-bench-*` dir under `$TMPDIR`; the
  compare script removes its own.

```text
cargo build --release -p tn-lmdb-bench
./target/release/tn-lmdb-bench --tier quick --out quick.json
```

## JSON (schema 1)

```json
{ "schema": 1, "bench": "tn-lmdb-bench", "label": "", "tier": "quick", "n": 100000, "reps": 3,
  "page_size": 4096, "os": "macos", "arch": "aarch64",
  "rows": [ { "name": "put.seq", "group": "generic", "sync": "nosync", "metric": "ns_per_op",
              "unit": "ns", "samples": [51.9, 51.7, 49.9], "median": 51.7, "min": 49.9,
              "max": 51.9, "exact": false, "gate": true } ] }
```

`metric` is `ns_per_op` (unit `ns`), `bytes` (`B`) or `pages_per_op` (`pages`); the last two are
exact. A row is keyed by `name` and `sync`. `median` of an even count is the mean of the middle
two. `gate` is false for a row the comparison reports but does not gate at the run's size: the two
single-commit rows (`put.seq.commit`, `put.rand.commit`) in a run of fewer than 1M entries a bulk
table (`n`; the quick tier's 100k), where a commit of 100k entries takes a fraction of a
millisecond, mostly writing fresh file pages, and its cost moves by 10% and more from one run to
the next with the machine's memory state, however many passes it takes. From 1M entries on (the
default and full tiers, or any `--n` of 1M or more; a commit of milliseconds) they are gated.

## Comparing two revisions

`etc/tn-lmdb-bench-compare.sh` runs this bench on two revisions of the engine and fails on a
regression:

```sh
etc/tn-lmdb-bench-compare.sh --tier quick HEAD~1 HEAD                  # two commits
etc/tn-lmdb-bench-compare.sh --tier quick --worktree HEAD              # the working tree as head
etc/tn-lmdb-bench-compare.sh --report-only <bench dir> --threshold 5   # the same runs, judged again
```

Both sides run head's bench source, each built as a standalone package against its own side's
`crates/tn-lmdb` (a `git worktree` of the side's commit), so the engine is the only difference
between the two binaries. The bench uses only public API that every revision with `crates/tn-lmdb`
has. An unchanged tn-lmdb, or a change that does not reach the binary, exits 0 with "nothing to
compare", and a one-line report says why (`--allow-same` runs it anyway: an A/A run that shows the
noise). The runs alternate, base head then head base (`--rounds`, default 4 at the quick tier and 2
otherwise), and each row's samples are pooled per side. A time row regresses when head's median
over base's is above 1 + `--threshold` (default 10%) and head's min over base's is above 1 + half
of it. An exact row regresses when it grows past `--size-threshold` (default 1%). Durable time rows
are reported, and gated only with `--gate-durable`; a row whose JSON says `gate: false`, time or
exact, is reported only. Exit codes: 0 no gated regression (or nothing to compare), 1 a gated
regression, 2 a usage, environment, build or run error, or any failure the script does not expect
(never 1). A base or head without tn-lmdb (no `crates/tn-lmdb/Cargo.toml`) is refused, and so is a
second run on a bench dir another run holds (its `.lock`, removed at exit; a run killed by SIGKILL
leaves it). The report is `<bench dir>/report.md` (default bench dir
`$TMPDIR/tn-lmdb-bench-compare`). The script's header comment is the full reference. CI runs it on
pull requests labeled `bench`, and by hand (`.github/ACTIONS.md`, "tn-lmdb regression bench").

Within one run a quick-tier time row holds to a few percent. On a busy machine whole runs still
move: when other work starts, every sample of the next run can sit 10 to 20% above the run before.
The pooled comparison absorbs that when each side has runs on both sides of the change (neither
side's min then comes from the slow stretch alone), which is why the quick tier runs four rounds:
two (A B B A) put both of head's runs in the middle, where one load change can cover them. A row
flagged once and gone on a rerun with more rounds (`--rounds 6 --rows <row>`) is noise. Trust a
regression that holds over repeated runs.

Durable rows time real syncs, and consumer NVMe drives commit at two speeds that flip mid-run, so a
durable time row is noisy: judge the `nosync` time rows and the exact rows, and a durable row only
over repeated runs. On macOS the engine's syncs are plain `fsync`/`msync`, which do not flush the
drive's cache, so durable rows there cost far less than on Linux.
