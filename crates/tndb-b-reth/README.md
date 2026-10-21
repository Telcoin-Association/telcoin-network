# `tndb-b-reth`

reth's database API (`reth-db-api`, reth v1.11.3) on the [`tndb-b`](../tndb-b/README.md) engine.
`TndbEnv` is a reth `Database` whose transactions and cursors behave as reth's MDBX ones do and
report MDBX's error codes, so reth's code and reth's MDBX tests run on it unchanged. reth's five
dup-sort tables are stored as composite keys; the engine has no dup-sort feature.

**Status: pre-production.** The crate is a workspace member, but nothing in TN uses it yet: TN's
reth database is still MDBX. Next come the Linux gate run of the benchmark below, then TN
integration behind a feature.

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
`current_with_key_tail`, with no copy. A row this layout cannot hold is `MDBX_BAD_VALSIZE`.

## Differences from MDBX, in brief

`lib.rs` lists each one. reth's code reaches none of them.

- **Environment:** no read transaction times out; one file with an `flock`; the metrics skip a
  table the file lacks.
- **Data:** a dup-sort subkey holds one value (a second write with the same subkey replaces the
  first; reth deletes before it writes); dup values may be up to 16 MiB; a range walk may start
  past an excluded key.
- **Cursor positions:** past either end the cursor is past the end where MDBX's stays on the end
  row; after another writer's delete and put the cursor finds its place again lazily, at its next
  call; and a few dup-sort positions differ (`last_dup` after a key's only row is deleted,
  `append_dup` of a key's last value, `last_dup` before the first row, a dup key of one row).
- **Where MDBX is no oracle:** when another writer in the same transaction changes the rows of the
  dup key a cursor is in, MDBX's cursor works from a stale copy of the key's rows. The
  differential test found it skipping rows, leaving the table's `entries` count one off, and
  aborting in libmdbx assertions (`tree_rebalance`). Here the cursor finds its place again by
  key.

## Tests

- **reth's MDBX tests,** ported with only the constructor swapped (`tests/mdbx_port.rs`); three
  dup-sort tests assert the one-value-per-subkey difference instead.
- **The adapter's own** (`tests/adapter.rs`, `tests/dup.rs`): MDBX's error codes, reth's error
  variants, the metrics, every dup cursor call and the cursor steps after a delete or `last`, each
  run on MDBX too, the layouts against reth's encodings, and the differences pinned beside MDBX's
  results.
- **A differential test** (`tests/differential.rs`): random scripts of transaction and cursor
  calls (two write cursors and a read cursor, commits, aborts, a held read transaction) run on
  both databases, whose outcome lists must be equal; it skips the documented differences, under
  5% of ops. `random_scripts_match_mdbx` runs 32 scripts in the default suite, and its `_heavy`
  twin 2000 in release.
- **Crash cycles** (`tests/crash.rs`, Unix only): a child process writing reth-style rounds is
  killed with `SIGKILL` in its open, its writes or its commit, and the reopened environment must
  hold its last committed round exactly and pass the engine's `check` (5 cycles; 200 in the
  heavy twin).

```text
cargo nextest run -p tndb-b-reth                                              # default suite
cargo test --release -p tndb-b-reth -- --ignored --skip tndb_reth_vs_mdbx    # heavy variants
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
`TNDB_RETH_BENCH_REPEAT` the minimum samples per cell, and `TNDB_RETH_BENCH_ENGINES` one side for
profiling. The module docs give each workload, the data, the timing and where they differ from
reth's.

**The Linux gate** decides: each bench at most 1.10× MDBX, with at most one explained row up to
1.25×. Run it three times at reth's sizes, on performance cores with the temp dir on a real disk
(see the engine README's run tip), and take each cell's median:

```text
for i in 1 2 3; do TNDB_RETH_BENCH_N=10000,100000,1000000 cargo test --release -p tndb-b-reth tndb_reth_vs_mdbx -- --ignored --nocapture --test-threads 1 | grep '^|' > reth-bench-$i.txt; done
```

### Mac run

**Run:** Apple M1 Max, macOS, 2026-10-21, MDBX with 4 KiB pages, the median of 3 full runs (each
cell itself the median of its samples). **Code:** after the two tuning changes below and the
zero-copy dup reads; "before tuning" is the ratio before the tuning. The last fixes (a write
cursor's state after a refused write, the engine's free walk and merge checks, a dup `insert`'s
presence check) came after these runs and are not re-measured; of them only the write cursor's
state update is on a timed path, one branch per write. On macOS `msync` does not flush the drive's
cache, so the write rows, which are mostly the commit, flatter tndb here and say little about
Linux.

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
(`[(u32, usize); 48]`, 96 stores) on every creation; its steps are now `(u32, u32)`
(`tree_cursor_stays_small`), which took about 45 ns off each `RandomRead` operation. The table map
hashed each table name with SipHash on every call; it now hashes with `NameHasher` (FxHash's step),
which halved `Get.get` (an empty table, so the lookup path alone) and took 17 ns off each `Put` row
operation.

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
