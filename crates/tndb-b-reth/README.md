# `tndb-b-reth`

reth's database API (`reth-db-api`, reth v1.11.3) on the [`tndb-b`](../tndb-b/README.md) engine.
`TndbEnv` is a reth `Database` whose transactions and cursors behave as reth's MDBX ones do and
report MDBX's error codes, so reth's code and reth's MDBX tests run on it unchanged. reth's five
dup-sort tables are stored as composite keys; the engine has no dup-sort feature.

**Status: pre-production.** The crate is a workspace member, but nothing in TN uses it yet: TN's
reth database is still MDBX. The benchmark below passed its Linux gate on 2026-10-26. Next comes TN
integration behind a feature. On a reth upgrade (storage v2 included), follow the checklist in
`CLAUDE.md` ("On a reth upgrade").

The crate docs (`src/lib.rs`) are the reference: the type mapping, the dup layout and the mapping
of every dup call, the error codes, every difference from MDBX, and the differential and crash
tests. `CLAUDE.md` is the guide for changing the crate.

## Opening

```rust,ignore
use reth_db_api::{database::Database, transaction::DbTxMut};
use tndb_b_reth::{TndbArgs, TndbEnv, TndbEnvKind};

let mut db = TndbEnv::open(dir, TndbEnvKind::RW, TndbArgs::default())?;
db.create_tables()?; // every one of reth's tables the file lacks
let tx = db.tx_mut()?;
// ... reth's DbTxMut and cursor calls, as on MDBX ...
tx.commit()?;
```

- **One file,** `<dir>/tndb.dat` (`DATA_FILE`), locked with the engine's `flock`: a second open of
  a file another handle holds is `MDBX_BUSY`.
- **`TndbArgs`:** 4 KiB pages by default (any power of two from 4 to 64 KiB, fixed when the file
  is created), the engine's map reserve, and `SyncMode::Durable` (or `NoSync`, made durable by the
  engine's `sync` or the drop).
- **`TndbEnvKind::RO`** opens through the engine's read-only open: an existing file no writer
  holds (the node stopped), a shared lock, never written or synced; `tx_mut` is `MDBX_EACCESS`.
- **Limits:** a value up to 16 MiB. One up to the inline limit (about a quarter page: 999 bytes
  with an 8-byte key at 4 KiB pages, where MDBX keeps about 2 KB in the leaf) stays in its leaf;
  a larger one goes to an overflow run of contiguous pages, transparently.

## Dup-sort tables

Each value of a dup-sort table is a row of its own. reth's dup values start with their encoded
subkey (`DupSort::SubKey`), so a row's stored key is the encoded key followed by the value's first
W bytes, and its stored value is the rest. Every key of these tables has one width, so the rows
sort by key, then by subkey, as MDBX sorts a key's values.

| table | key | subkey (W) |
|---|---|---|
| `PlainStorageState` | `Address` (20 bytes) | `B256` slot (32) |
| `StorageChangeSets` | `BlockNumberAddress` (28) | `B256` slot (32) |
| `HashedStorages` | `B256` (32) | `B256` slot (32) |
| `AccountChangeSets` | `BlockNumber` (8) | `Address` (20) |
| `StoragesTrie` | `B256` (32) | `StoredNibblesSubKey` (65) |

A read returns the value (subkey, then the rest) as one slice of the page, through the engine's
`current_with_key_tail`, with no copy. `seek_by_key_subkey` reads its row in the seek itself, from
the leaf the seek fetched (the engine's `seek_with_key_tail`), with `key ‖ subkey` built on the
stack. A row this layout cannot hold is `MDBX_BAD_VALSIZE`.

## Differences from MDBX, in brief

`lib.rs` lists each one. reth's code reaches none of them.

- **Environment:** no read transaction times out: a long reader keeps the pages freed since its
  snapshot from reuse, so the file grows until it ends, and a write commit made once a reader has
  held a superseded snapshot for over 60 s logs a `warn!` with its age, at most once a minute
  (from the engine's lock-free `Env::oldest_reader_age`: readers pay nothing, a commit one atomic
  load while no reader holds a superseded snapshot, plus a clock read while one does); one file with an `flock`; the metrics skip a table
  the file lacks.
- **Data:** a dup-sort subkey holds one value: a second value for an existing (key, subkey) is
  refused with `MDBX_KEYEXIST` and a message saying so, changing nothing, where MDBX stores both
  (writing the value the row holds again succeeds, as on MDBX; reth deletes before it writes, so
  it never meets the refusal, and a future reth change that writes real duplicates fails loudly
  instead of losing a row); dup values may be up to 16 MiB; a range walk may start
  past an excluded key; a raw subkey wider than the table's subkey, which starts with a row's
  subkey, can make `seek_by_key_subkey` take the row after the one MDBX takes (no reth subkey type
  encodes that wide).
- **Cursor positions:** past either end the cursor is past the end where MDBX's stays on the end
  row; after another writer's delete and put the cursor finds its place again lazily, at its next
  call; and a few dup-sort positions differ (`last_dup`, or the dup moves after a
  `delete_current`, right after a key's only row is deleted; `append_dup` with the subkey of a
  key's last row; `last_dup` before the first row; a dup key of one row). After `last` a cursor,
  read-only or write, ends the table as MDBX's does.
- **Where MDBX is no oracle:** when another writer in the same transaction changes the rows of the
  dup key a cursor is in, MDBX's cursor works from a stale copy of the key's rows. The
  differential test found it skipping rows, leaving the table's `entries` count one off, and
  aborting in libmdbx assertions (`tree_rebalance`). Here the cursor finds its place again by
  key.

## Tests

- **reth's MDBX tests,** ported with only the constructor swapped (`tests/mdbx_port.rs`); three
  dup-sort tests assert the one-value-per-subkey refusal instead.
- **The adapter's own** (`tests/adapter.rs`, `tests/dup.rs`): MDBX's error codes, reth's error
  variants, the metrics, every dup cursor call and the cursor steps after a delete or `last`, each
  run on MDBX too, the layouts against reth's encodings, and the differences pinned beside MDBX's
  results.
- **A differential test** (`tests/differential.rs`): random scripts of transaction and cursor
  calls (two write cursors and a read cursor, commits, aborts, a held read transaction) run on
  both databases, whose outcome lists must be equal; it skips the documented differences, about
  4% of ops, and fails if a tenth or more are skipped. A second value for a dup row's subkey runs on tndb-b only, which must refuse it and
  change nothing (the ops after it still match MDBX's). `random_scripts_match_mdbx` runs 32 scripts in the default suite, and its `_heavy`
  twin 2000 in release.
- **Crash cycles** (`tests/crash.rs`, Unix only): a child process writing reth-style rounds is
  killed with `SIGKILL` in its open, its writes or its commit, and the reopened environment must
  hold its last committed round exactly and pass the engine's `check` (5 cycles; 200 in the
  heavy twin).

```text
cargo nextest run -p tndb-b-reth                                              # default suite
cargo test --release -p tndb-b-reth -- --ignored --skip tndb_reth_vs_mdbx --skip tndb_b_vs_mdbx  # heavy variants
```

## Benchmark: reth's db benches against MDBX

`tndb_reth_vs_mdbx` (`src/bench.rs`, `#[ignore]`d) runs the workloads of reth's
`crates/storage/db/benches` (`criterion.rs`, `hash_keys.rs`, `get.rs`, `put.rs`) on a `TndbEnv`
(4 KiB pages, `Durable`) and on reth's MDBX `DatabaseEnv` (opened as reth's benches open it), with
the same seeded data, and prints each workload's time per operation and the ratio:

```text
cargo test --release -p tndb-b-reth tndb_reth_vs_mdbx -- --ignored --nocapture --test-threads 1
```

On Apple Silicon add `TNDB_RETH_BENCH_MDBX_PAGE=4096` for MDBX pages the size of tndb's (reth's
default is the OS page, 16 KiB there; on Linux x86_64 it is 4 KiB already). `TNDB_RETH_BENCH_N` sets
the `hash_keys` batch sizes (default `10000,100000`; reth's add `1000000`), `TNDB_RETH_BENCH_ROWS`
picks rows by group (`table`, `dupsort`, `hash`, `get`, `put`) or by part of a label,
`TNDB_RETH_BENCH_REPEAT` the minimum samples per cell, `TNDB_RETH_BENCH_ENGINES` one side for
profiling, and `TNDB_RETH_BENCH_SYNC=nosync` skips every commit sync on both sides (tndb `NoSync`,
MDBX `UtterlyNoSync`), so the write rows time the CPU alone: the table's own header row then reads
`bench (nosync: CPU only, not the gate)`, so the mark survives the gate loop's `grep '^|'`. The
module docs give each workload, the data, the timing and where they differ from reth's.

**The Linux gate** decides: each bench at most 1.10× MDBX, with at most one explained row up to
1.25×. Run it three times at reth's sizes, on performance cores with the temp dir on a real disk
(see the [run tip](engine-bench.md#run-tip-linux); the run stops if the temp dir is on a tmpfs),
and take each cell's median. Pin the power settings first: the drive's autonomous power states off,
and the CPU governor and energy preference at `performance` (all three reset on a reboot or a
sleep). From the repository root, with the performance cores in `taskset` (`lscpu -e` shows which
CPUs they are):

```text
sudo nvme set-feature /dev/nvme0 -f 0x0c -v 0
echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/energy_performance_preference
mkdir -p tmp; for i in 1 2 3; do TMPDIR=$PWD/tmp TNDB_RETH_BENCH_N=10000,100000,1000000 taskset -c 0-7 cargo test --release -p tndb-b-reth tndb_reth_vs_mdbx -- --ignored --nocapture --test-threads 1 | grep '^|' > reth-bench-$i.txt; done
```

**Durable write rows on a consumer drive.** A laptop NVMe drive syncs a commit at one of two speeds
(on the gate laptop about 0.7 ms or about 5 ms), and it switches between them during a run, for
both engines at once: MDBX's own time for a write row can move 5× from one run to the next, with
the power settings pinned. tndb and MDBX issue the same syncs per commit (a data `msync`, a size
sync when the file grew, a meta `msync`), so the drive's state, not the engine, decides which speed
a row lands on. A durable write row over 1.10× is the engine's only if it stays over with the
drive at its fast speed (the runs where MDBX's time for that row is lowest) or with
`TNDB_RETH_BENCH_SYNC=nosync`.

### Linux run (the gate)

**Run:** Linux x86_64 laptop (i7-1360P, NVMe, ext4), 2026-10-22, the gate command above (three full
runs at reth's sizes, the temp dir on the NVMe drive, the power settings pinned, the performance
cores in `taskset`). Each cell is the median of its three runs, so a ratio need not be the quotient
of the two times beside it. **Code:** `87f7c81fc`, after the gate's tuning round and the append
tuning (below); "before tuning" is the same command on `26cfa37cc`. **Verdict (2026-10-26):
passed.** The one row over 1.10× that is the engine's is the explained dup read; the six durable
write rows over 1.10× are the drive's (see "Over 1.10× on Linux").

| bench | ops | tndb | MDBX | tndb/MDBX | before tuning |
|---|--:|--:|--:|--:|--:|
| CanonicalHeaders.SeqWrite | 1000 | 832.0 ns | 853.0 ns | 0.97 | 0.94 |
| CanonicalHeaders.RandomWrite | 10 | 72.25 us | 74.27 us | 0.96 | 0.89 |
| CanonicalHeaders.SeqRead | 1000 | 33.0 ns | 41.0 ns | 0.80 | 0.83 |
| CanonicalHeaders.RandomRead | 10 | 147.0 ns | 143.0 ns | 1.03 | 1.38 |
| HeaderNumbers.SeqWrite | 1000 | 842.0 ns | 864.0 ns | 0.98 | 0.97 |
| HeaderNumbers.RandomWrite | 10 | 172.75 us | 163.00 us | 0.97 | 0.98 |
| HeaderNumbers.SeqRead | 1000 | 35.0 ns | 44.0 ns | 0.80 | 0.82 |
| HeaderNumbers.RandomRead | 10 | 147.0 ns | 144.0 ns | 1.02 | 1.30 |
| Headers.SeqWrite | 1000 | 5.58 us | 5.79 us | 0.95 | 0.95 |
| Headers.RandomWrite | 10 | 493.36 us | 502.65 us | 0.98 | 0.96 |
| Headers.SeqRead | 1000 | 154.0 ns | 163.0 ns | 0.95 | 0.98 |
| Headers.RandomRead | 10 | 251.0 ns | 270.0 ns | 0.93 | 1.05 |
| BlockBodyIndices.SeqWrite | 1000 | 4.92 us | 4.95 us | 1.13 | 0.97 |
| BlockBodyIndices.RandomWrite | 10 | 469.48 us | 479.88 us | 0.98 | 0.90 |
| BlockBodyIndices.SeqRead | 1000 | 57.0 ns | 63.0 ns | 0.90 | 0.87 |
| BlockBodyIndices.RandomRead | 10 | 157.0 ns | 164.0 ns | 0.96 | 1.25 |
| BlockOmmers.SeqWrite | 100 | 60.06 us | 56.91 us | 1.06 | 1.00 |
| BlockOmmers.RandomWrite | 10 | 486.90 us | 490.51 us | 0.95 | 0.97 |
| BlockOmmers.SeqRead | 100 | 168.0 ns | 164.0 ns | 1.02 | 1.02 |
| BlockOmmers.RandomRead | 10 | 244.0 ns | 241.0 ns | 1.01 | 1.17 |
| TransactionHashNumbers.SeqWrite | 1000 | 5.00 us | 3.93 us | 1.20 | 0.92 |
| TransactionHashNumbers.RandomWrite | 10 | 478.60 us | 497.67 us | 0.96 | 0.88 |
| TransactionHashNumbers.SeqRead | 1000 | 36.0 ns | 45.0 ns | 0.80 | 0.80 |
| TransactionHashNumbers.RandomRead | 10 | 145.0 ns | 147.0 ns | 0.99 | 1.30 |
| Transactions.SeqWrite | 100 | 59.13 us | 59.95 us | 0.99 | 0.92 |
| Transactions.RandomWrite | 10 | 550.25 us | 590.79 us | 1.03 | 0.90 |
| Transactions.SeqRead | 100 | 155.0 ns | 184.0 ns | 0.84 | 1.01 |
| Transactions.RandomRead | 10 | 269.0 ns | 272.0 ns | 0.98 | 1.10 |
| PlainStorageState.SeqWrite | 1095 | 3.39 us | 4.48 us | 0.85 | 0.93 |
| PlainStorageState.RandomWrite | 10 | 464.42 us | 458.76 us | 0.97 | 0.92 |
| PlainStorageState.SeqRead | 268 | 65.0 ns | 65.0 ns | 1.00 | 1.03 |
| PlainStorageState.RandomRead | 10 | 191.0 ns | 171.0 ns | 1.12 | 1.63 |
| PlainAccountState.SeqWrite | 1000 | 1.03 us | 944.0 ns | 0.98 | 0.95 |
| PlainAccountState.RandomWrite | 10 | 74.03 us | 75.43 us | 0.94 | 0.97 |
| PlainAccountState.SeqRead | 1000 | 59.0 ns | 69.0 ns | 0.86 | 0.88 |
| PlainAccountState.RandomRead | 10 | 177.0 ns | 168.0 ns | 1.05 | 1.24 |
| hash append_all (pre 0, write 20000) | 20000 | 105.0 ns | 109.0 ns | 0.96 | 0.93 |
| hash append_input (pre 0, write 10000) | 10000 | 150.0 ns | 150.0 ns | 1.00 | 0.96 |
| hash insert_unsorted (pre 10000, write 10000) | 10000 | 847.0 ns | 644.0 ns | 1.15 | 1.05 |
| hash insert_sorted (pre 10000, write 10000) | 10000 | 756.0 ns | 615.0 ns | 1.13 | 1.01 |
| hash put_unsorted (pre 10000, write 10000) | 10000 | 877.0 ns | 674.0 ns | 1.19 | 0.99 |
| hash put_sorted (pre 10000, write 10000) | 10000 | 767.0 ns | 827.0 ns | 0.94 | 0.89 |
| hash append_all (pre 0, write 200000) | 200000 | 139.0 ns | 147.0 ns | 0.94 | 0.92 |
| hash append_input (pre 0, write 100000) | 100000 | 178.0 ns | 191.0 ns | 0.97 | 0.99 |
| hash insert_unsorted (pre 100000, write 100000) | 100000 | 609.0 ns | 595.0 ns | 1.03 | 1.06 |
| hash insert_sorted (pre 100000, write 100000) | 100000 | 449.0 ns | 440.0 ns | 1.02 | 1.04 |
| hash put_unsorted (pre 100000, write 100000) | 100000 | 591.0 ns | 583.0 ns | 1.02 | 0.99 |
| hash put_sorted (pre 100000, write 100000) | 100000 | 445.0 ns | 492.0 ns | 0.89 | 0.87 |
| hash append_all (pre 0, write 2000000) | 2000000 | 139.0 ns | 121.0 ns | 1.16 | 1.00 |
| hash append_input (pre 0, write 1000000) | 1000000 | 103.0 ns | 98.0 ns | 1.05 | 1.23 |
| hash insert_unsorted (pre 1000000, write 1000000) | 1000000 | 701.0 ns | 779.0 ns | 0.90 | 0.84 |
| hash insert_sorted (pre 1000000, write 1000000) | 1000000 | 250.0 ns | 233.0 ns | 1.06 | 1.02 |
| hash put_unsorted (pre 1000000, write 1000000) | 1000000 | 659.0 ns | 695.0 ns | 0.97 | 0.99 |
| hash put_sorted (pre 1000000, write 1000000) | 1000000 | 238.0 ns | 301.0 ns | 0.79 | 0.93 |
| Get.get | 1000000 | 23.0 ns | 31.0 ns | 0.74 | 0.59 |
| Get.get_by_encoded_key | 1000000 | 23.0 ns | 29.0 ns | 0.79 | 0.55 |
| Put.put | 1000000 | 48.0 ns | 131.0 ns | 0.37 | 0.42 |
| Put.append | 1000000 | 48.0 ns | 76.0 ns | 0.63 | 0.71 |

**The tuning round** took every plain `RandomRead` from 1.10 to 1.38 down to 0.93 to 1.05, and the
dup `RandomRead` from 1.63 to 1.12. It made five changes:

- a snapshot resolves each table handle once, for all its read transactions (the engine's
  `TableRoots`), so a `RoTxn` is two `Arc`s;
- the cursor descent parses each page's header once per level and keeps the search's own result;
- the cursor path is 6 bytes a level;
- `seek_by_key_subkey` builds its key on the stack and reads its row in the seek (the engine's
  `seek_with_key_tail`);
- copy-on-write copies a page once.

**The append tuning** that followed copies each row's small pieces without a libc call and splits
the rightmost leaf of an append without a second descent. It took `hash append_input` at 1M from
1.11 to 1.05 (CPU only, from 1.10 to 1.02).

`Get.get` went from 17 to 23 ns (0.59 to 0.77 of MDBX) on this machine but not on the Mac, where the
same path does less work than before; it is not explained.

**Over 1.10× on Linux:**

- `PlainStorageState.RandomRead` (1.12): reth's storage-slot read, `seek_by_key_subkey`. tndb seeks
  a 52-byte composite key (`address ‖ slot`) in one tree, where MDBX seeks the 20-byte key, then the
  slot in the key's own sub-tree.
- Six durable write rows (1.13 to 1.20): `BlockBodyIndices.SeqWrite`,
  `TransactionHashNumbers.SeqWrite`, `hash insert_unsorted`, `insert_sorted` and `put_unsorted` at
  10K, and `hash append_all` at 2M. These are the drive's two commit speeds (see "Durable write rows
  on a consumer drive"). In two more full runs of the same code on 2026-10-26, under the same
  settings, the six rows read 0.99 to 1.09. Run alone (one row, one engine per process),
  `append_all` at 2M reads 1.00 and `insert_sorted` at 10K 1.03, with the commits equal in a system
  call trace. Reserving the file's blocks as it grows, which MDBX does not do, is not the cause: a
  build with sparse growth, alternated with those two runs, read no better (`append_all` at 2M 1.22
  and 0.90).

**CPU only:** with the commit syncs off (`TNDB_RETH_BENCH_SYNC=nosync`, three runs at 1M),
`append_input` is 1.02 (54 against 53 ns) and `append_all` at 2M 1.00 (54 against 54 ns). The
others are under MDBX: `insert_unsorted` 0.79, `insert_sorted` 0.92, `put_unsorted` 0.91,
`put_sorted` 0.64.

### Mac run

**Run:** Apple M1 Max, macOS, 2026-10-21, MDBX with 4 KiB pages, the median of 3 full runs (each
cell itself the median of its samples). **Code:** after the two tuning changes below and the
zero-copy dup reads, before the gate's tuning round (see the Linux run); "before tuning" is the
ratio before the two tuning changes. The last fixes (a write cursor's state after a refused write,
the engine's free walk and merge checks, a dup `insert`'s presence check) came after these runs and
are not re-measured; of them only the write cursor's state update is on a timed path, one branch per
write. Neither are four later changes:
- the cursor end after `last` now lives in this crate's cursor state, not the engine's;
- read-only cursors end the table too;
- dup-sort writes refuse a second value for a subkey;
- the long-reader check runs after each commit.

An A/B on the Mac (2026-10-26, syncs off, tndb only, five alternating runs of each build) found
them flat: a geomean of 1.01, and every row within the runs' own spread. On macOS `msync` does not
flush the drive's cache, so the write rows, which are mostly the commit, flatter tndb here and say
little about Linux.

| bench | ops | tndb | MDBX | tndb/MDBX | before tuning |
|---|--:|--:|--:|--:|--:|
| CanonicalHeaders.SeqWrite | 1000 | 246.0 ns | 872.0 ns | 0.28 | 0.29 |
| CanonicalHeaders.RandomWrite | 10 | 8.95 us | 81.47 us | 0.11 | 0.11 |
| CanonicalHeaders.SeqRead | 1000 | 26.0 ns | 27.0 ns | 0.96 | 0.96 |
| CanonicalHeaders.RandomRead | 10 | 220.0 ns | 195.0 ns | 1.13 | 1.35 |
| HeaderNumbers.SeqWrite | 1000 | 247.0 ns | 868.0 ns | 0.29 | 0.29 |
| HeaderNumbers.RandomWrite | 10 | 8.93 us | 81.36 us | 0.11 | 0.11 |
| HeaderNumbers.SeqRead | 1000 | 28.0 ns | 29.0 ns | 0.93 | 0.97 |
| HeaderNumbers.RandomRead | 10 | 225.0 ns | 195.0 ns | 1.15 | 1.36 |
| Headers.SeqWrite | 1000 | 1.24 us | 1.47 us | 0.84 | 0.84 |
| Headers.RandomWrite | 10 | 10.17 us | 82.56 us | 0.12 | 0.12 |
| Headers.SeqRead | 1000 | 244.0 ns | 251.0 ns | 0.97 | 0.92 |
| Headers.RandomRead | 10 | 387.0 ns | 395.0 ns | 0.98 | 1.13 |
| BlockBodyIndices.SeqWrite | 1000 | 246.0 ns | 910.0 ns | 0.27 | 0.27 |
| BlockBodyIndices.RandomWrite | 10 | 9.05 us | 81.48 us | 0.11 | 0.11 |
| BlockBodyIndices.SeqRead | 1000 | 34.0 ns | 36.0 ns | 0.94 | 0.94 |
| BlockBodyIndices.RandomRead | 10 | 237.0 ns | 195.0 ns | 1.22 | 1.45 |
| BlockOmmers.SeqWrite | 100 | 3.89 us | 9.14 us | 0.43 | 0.43 |
| BlockOmmers.RandomWrite | 10 | 13.04 us | 82.45 us | 0.16 | 0.16 |
| BlockOmmers.SeqRead | 100 | 231.0 ns | 233.0 ns | 0.99 | 1.00 |
| BlockOmmers.RandomRead | 10 | 366.0 ns | 354.0 ns | 1.03 | 1.16 |
| TransactionHashNumbers.SeqWrite | 1000 | 249.0 ns | 867.0 ns | 0.28 | 0.29 |
| TransactionHashNumbers.RandomWrite | 10 | 8.98 us | 81.39 us | 0.11 | 0.11 |
| TransactionHashNumbers.SeqRead | 1000 | 28.0 ns | 29.0 ns | 0.97 | 0.97 |
| TransactionHashNumbers.RandomRead | 10 | 225.0 ns | 200.0 ns | 1.12 | 1.37 |
| Transactions.SeqWrite | 100 | 3.64 us | 10.24 us | 0.36 | 0.36 |
| Transactions.RandomWrite | 10 | 11.13 us | 83.47 us | 0.13 | 0.13 |
| Transactions.SeqRead | 100 | 193.0 ns | 204.0 ns | 0.95 | 0.94 |
| Transactions.RandomRead | 10 | 350.0 ns | 341.0 ns | 1.03 | 1.16 |
| PlainStorageState.SeqWrite | 1095 | 279.0 ns | 891.0 ns | 0.31 | 0.31 |
| PlainStorageState.RandomWrite | 10 | 9.07 us | 81.42 us | 0.11 | 0.11 |
| PlainStorageState.SeqRead | 268 | 75.0 ns | 52.0 ns | 1.43 | 1.40 |
| PlainStorageState.RandomRead | 10 | 333.0 ns | 245.0 ns | 1.36 | 1.70 |
| PlainAccountState.SeqWrite | 1000 | 328.0 ns | 951.0 ns | 0.34 | 0.35 |
| PlainAccountState.RandomWrite | 10 | 9.04 us | 81.50 us | 0.11 | 0.11 |
| PlainAccountState.SeqRead | 1000 | 48.0 ns | 54.0 ns | 0.89 | 0.89 |
| PlainAccountState.RandomRead | 10 | 241.0 ns | 216.0 ns | 1.12 | 1.33 |
| hash append_all (pre 0, write 20000) | 20000 | 90.0 ns | 96.0 ns | 0.95 | 0.96 |
| hash append_input (pre 0, write 10000) | 10000 | 105.0 ns | 136.0 ns | 0.77 | 0.77 |
| hash insert_unsorted (pre 10000, write 10000) | 10000 | 436.0 ns | 737.0 ns | 0.59 | 0.61 |
| hash insert_sorted (pre 10000, write 10000) | 10000 | 291.0 ns | 612.0 ns | 0.48 | 0.45 |
| hash put_unsorted (pre 10000, write 10000) | 10000 | 460.0 ns | 749.0 ns | 0.61 | 0.61 |
| hash put_sorted (pre 10000, write 10000) | 10000 | 305.0 ns | 725.0 ns | 0.42 | 0.43 |
| hash append_all (pre 0, write 200000) | 200000 | 77.0 ns | 69.0 ns | 1.12 | 1.13 |
| hash append_input (pre 0, write 100000) | 100000 | 78.0 ns | 73.0 ns | 1.07 | 1.08 |
| hash insert_unsorted (pre 100000, write 100000) | 100000 | 472.0 ns | 497.0 ns | 0.95 | 0.96 |
| hash insert_sorted (pre 100000, write 100000) | 100000 | 262.0 ns | 292.0 ns | 0.92 | 0.97 |
| hash put_unsorted (pre 100000, write 100000) | 100000 | 496.0 ns | 539.0 ns | 0.92 | 0.95 |
| hash put_sorted (pre 100000, write 100000) | 100000 | 276.0 ns | 360.0 ns | 0.77 | 0.81 |
| Get.get | 1000000 | 17.0 ns | 41.0 ns | 0.41 | 0.93 |
| Get.get_by_encoded_key | 1000000 | 17.0 ns | 41.0 ns | 0.41 | 0.95 |
| Put.put | 1000000 | 49.0 ns | 151.0 ns | 0.33 | 0.44 |
| Put.append | 1000000 | 49.0 ns | 106.0 ns | 0.46 | 0.62 |

The `TransactionHashNumbers` (`hash`) rows at reth's third size, 1M (one run, 3 samples): append_all
0.97, append_input 1.01, insert_unsorted 0.63, insert_sorted 0.41, put_unsorted 0.63, put_sorted
0.38.

Table sizes after the fill (pages times the page size; the `hash` rows' sizes are within 1% of
MDBX's at every size):

| table (filled by `tx.put`) | tndb | MDBX | tndb/MDBX |
|---|--:|--:|--:|
| CanonicalHeaders | 0.05 MiB | 0.05 MiB | 1.00 |
| HeaderNumbers | 0.05 MiB | 0.05 MiB | 1.00 |
| Headers | 0.66 MiB | 0.66 MiB | 1.00 |
| BlockBodyIndices | 0.03 MiB | 0.03 MiB | 1.00 |
| BlockOmmers | 0.18 MiB | 0.08 MiB | 2.25 |
| TransactionHashNumbers | 0.05 MiB | 0.05 MiB | 1.00 |
| Transactions | 0.04 MiB | 0.04 MiB | 1.00 |
| PlainStorageState | 0.09 MiB | 0.10 MiB | 0.88 |
| PlainAccountState | 0.05 MiB | 0.05 MiB | 0.93 |

`BlockOmmers` holds values of 0 to 2 headers (about 0.6 KB each): a 2-header value is past tndb's
inline limit at 4 KiB pages (about 1 KB) and takes a 1-page overflow run, where MDBX keeps values up
to about 2 KB in the leaf. TN blocks carry no ommers.

**The tuning:** two changes, from samply profiles of the `RandomRead` rows (a read transaction, then
10 times a new cursor and its `seek_exact`). A new engine cursor zeroed and moved a 784-byte path
(`[(u32, usize); 48]`, 96 stores) on every creation; its steps became `(u32, u32)`, which took about
45 ns off each `RandomRead` operation (the path is now 288 bytes, `u32` pages and `u16` slots:
`tree_cursor_stays_small`). The table map hashed each table name with SipHash on every call; it now
hashes with `NameHasher` (FxHash's step), which halved `Get.get` (an empty table, so the lookup path
alone) and took 17 ns off each `Put` row operation.

**Zero-copy dup reads:** a dup-sort row's value used to be rebuilt, subkey then rest, in the
cursor's buffer. The cursors now read the row again once a move is done through the engine's
`current_with_key_tail` (see [Dup-sort tables](#dup-sort-tables)); only a value in an overflow run,
which no reth dup table's values reach, is still joined in the buffer. The two `PlainStorageState`
rows above are from targeted runs (`TNDB_RETH_BENCH_ROWS=PlainStorageState`,
`TNDB_RETH_BENCH_REPEAT=20`, the median of 3, alternating with a build without the change):
`RandomRead` went from 366 to 333 ns, as a fresh cursor no longer allocates its buffer on its first
row, and `SeqRead` from 73 to 75 ns (under the noise, though in each pair): re-reading the cell
after each step costs about what the copy did.

**Over 1.10× on the Mac, and why:**
- `RandomRead` of the small-value tables (1.12 to 1.22): the descent itself. tndb's `seek_exact` on a
  two-level tree costs about twice MDBX's, in the per-page checks every fetch makes (the separator
  range, the page kind and the child's txn: the engine's damage rules) and a binary search whose
  every probe bounds-checks its cell; what is left of the cursor's creation is about MDBX's (its
  cursor is `malloc`ed and freed). Rows with larger values (`Headers`, `Transactions`, `BlockOmmers`)
  are at 0.98 to 1.03, as the shared decode dominates.
- `PlainStorageState` `SeqRead` (1.43) and `RandomRead` (1.36): the copy that a profile showed as a
  fifth of `SeqRead`'s time is mostly reth's `StorageEntry` decode (`U256::from_compact` copies
  1 to 32 bytes and mispredicts on the length), which MDBX's side runs too. tndb's step through a
  key's rows reads each row's cell up to three times (the prefix step's key comparison, its entry,
  then the zero-copy re-read above); `RandomRead` seeks a 52-byte composite key where MDBX seeks
  the 20-byte key, then the subkey in the key's own sub-tree.
- `hash append_all` at 100k (1.12; 0.95 at 10k and 0.97 at 1M): not profiled.

## Benchmark: the engine against MDBX

`tndb_b_vs_mdbx` (`src/engine_bench.rs`, `#[ignore]`d) runs the `tndb-b` engine through its own API
(`Env`, not the adapter) against MDBX (`reth-libmdbx`, opened as `tn-storage` opens it): bulk
inserts, gets, scans, seeks, small commits, deletes and large values, on the same seeded data.
[`engine-bench.md`](engine-bench.md) gives the command, the selectors, the runs and what they show.

```text
cargo test --release -p tndb-b-reth tndb_b_vs_mdbx -- --ignored --nocapture --test-threads 1
```
