# tndb-b-reth: a guide for working in this crate

`tndb-b-reth` implements reth's database API (`reth-db-api`, reth v1.11.3) on the
[`tndb-b`](../tndb-b) engine. `TndbEnv` is a reth `Database` whose transactions and cursors behave
as reth's MDBX ones do and report MDBX's error codes, so reth's code and its MDBX tests run
unchanged. reth's five dup-sort tables are stored as composite keys (`key ‖ subkey`); the engine
has no dup-sort feature. It is pre-production and not wired into TN. This is the only crate that
depends on both reth and the engine; the engine depends on neither reth nor `tn-storage`.

**Read in this order:**
1. this file;
2. `README.md` (purpose, status, the bench table);
3. the crate docs in `src/lib.rs`: the type mapping, the dup layout and call mapping, the error
   table, every difference from MDBX, and the differential and crash tests;
4. the module docs at the top of the file you change;
5. for engine behaviour, `crates/tndb-b/CLAUDE.md` and the engine README's cursor sections
   ("Public API reference", "Detached cursors").

reth's contract is in the checkout at
`~/.cargo/git/checkouts/reth-e231042ee7db3fb7/d6324d6/crates/storage/db-api/src`; its MDBX
implementation and tests are in `crates/storage/db/src/implementation/mdbx/` there.

## Crate map

| file | role |
|---|---|
| `lib.rs` | crate docs (the reference: mappings, differences from MDBX, the tests), re-exports (`TndbEnv`, `TndbEnvKind`, `TndbArgs`, `DATA_FILE`, `TndbTx`, `TndbTxMut`, `TndbCursor`, `TndbCursorMut`, and the engine's `SyncMode`) |
| `env.rs` | `TndbEnv`: `open` (`RW` creates `<dir>/tndb.dat` through `Env::open`, `RO` uses `Env::open_read_only`), `create_tables`/`create_tables_for`, `failure`, reth's `Database` and `DatabaseMetrics` (the gauges MDBX reports, from `table_stat` and `Env::free_pages`); `TndbArgs` (page size, default 4096; map reserve; `SyncMode`); the table map `TableMap` (name to `TableEntry`: the engine `Table` and its `DupLayout`, hashed by `NameHasher`, FxHash's step), `resolve`; the long-reader warning `ReaderWatch` (one per `TndbEnv`, shared with each `TndbTxMut`: `after_commit`, run after every successful commit, `create_tables_for`'s included, warns from `Env::oldest_reader_age` past `LONG_READER_WARN_AFTER`, 60 s, at most once per `LONG_READER_WARN_INTERVAL`, 60 s, through a compare-exchange of `last_warned`; `warning_due` is the pure rate-limit rule; the test-only `TndbEnv::set_long_reader_limits` shortens both) |
| `tx.rs` | `TndbTx` (an `Arc` of the engine's `RoTxn`, so its cursors own a share), `TndbTxMut` (`SharedTxn` = `Arc<Mutex<Option<RwTxn>>>`: every method takes `&self`, holds the guard for one call, and `commit` takes the transaction out, after which calls are `MDBX_BAD_TXN`, and after a successful commit runs the environment's `ReaderWatch::after_commit`); reth's `DbTx`, `DbTxMut`, `TableImporter`; `delete_rows_of` (a dup key's rows); `insert_dup` (every dup row write is the engine's `insert`: on `KeyExists` the held value is compared, the same value is a no-op success as on MDBX, another is a second value for the subkey, refused) |
| `cursor.rs` | `TndbCursor` (over `RoCursor`) and `TndbCursorMut` (over `TxnCursor`, the transaction locked per call as `Locked`); `Moves`, the engine moves both implement, `seek_with_key_tail` among them (both forward it to the engine's); `Rows`, the adapter's moves written once over `Moves`: the table's `DupLayout`, the value buffer, and `Pos` (`Fresh`, `Missed`, `Engine`, `End`, `KeyDeleted`, `DupEnd { soft }`, MDBX's cursor states the engine's cursors do not hold: `End` is a cursor, read-only or write, ending a plain table after `last`, a fresh `prev`, or an `append` refused for its order or of the last key, its row's key kept in `end_key` so another writer's delete of the row ends it); `Rows::wrote` (the state after a write: placed by a success or a handled refusal, else unchanged; a refused second value for a dup subkey returns before it, so changes nothing); `insert_dup_at` (`insert_dup` through the cursor: the same value moves the cursor to the row; the cursor `append`'s last-key check uses a separate engine cursor so a refusal leaves the cursor where it was); `Rows::seek_by_key_subkey_row` (builds `key ‖ subkey` in a 128-byte stack buffer, `SEEK_KEY_STACK`, which a compile-time check holds every `DUP_LAYOUTS` row key to, a `Vec` only for a longer raw input, and returns the row from one `Moves::seek_with_key_tail`, kept only if it is the key's, `is_row_of`); `dup_decoded` and `dup_value_of`, free fns, `#[inline(always)]`; `range_start` for the walkers; reth's cursor traits, plain and dup |
| `dup.rs` | `DupLayout` (`key` width K, `subkey` width W), `DUP_LAYOUTS` (the five tables), `dup_layout`; `join` and `split` (a row to its stored key and value and back), `is_row_of`, `past` (the seek key past a key's rows, `k ‖ 0xFF × (W + 1)`), `joined` and `rebuild` (a read's value from the engine's `KeyTail`: one slice, or joined in the cursor's buffer for a value in a run) |
| `error.rs` | engine errors as reth's `DatabaseError` with MDBX's codes and messages: `Mdbx` (the codes, from `mdbx.h` and `libc`), `info` (the exhaustive map), the per-operation constructors (`read`, `write`, `delete`), `finished`, `dup_sort_unsupported`, `bad_size`, `second_dup_value` (`MDBX_KEYEXIST` with this crate's message: one value per subkey), `table_not_found` |
| `bench.rs` | test-only: `tndb_reth_vs_mdbx`, reth's db benches run on both databases (module docs: workloads, data, timing, selectors; `TNDB_RETH_BENCH_SYNC=nosync` opens tndb `NoSync` and MDBX `UtterlyNoSync`, and the table's header row, which `grep '^|'` keeps, says it is CPU only, not the gate) |
| `engine_bench.rs` | test-only: `tndb_b_vs_mdbx`, the engine through its own API (`Env`) against `reth-libmdbx` (module docs: rows, caveats, the `TNDB_B_BENCH_*` selectors; runs and tables in `engine-bench.md`); it uses `bench.rs`'s `announce_temp_dir` and `page_size` |
| `tests/mod.rs` | `SPAWN_LOCK`, `open_env`, `spawn` (below) |
| `tests/mdbx_port.rs` | reth's MDBX tests with only the constructor swapped; three dup tests assert the one-value-per-subkey refusal instead |
| `tests/adapter.rs` | error codes against `reth_libmdbx`, reth's error variants per operation, the metrics over every table, a blocked second writer, shared write cursors and cursor steps after a delete or `last` (both run on MDBX too), a read-only cursor's steps after `last` (`read_only_cursor_steps_after_last_match_mdbx`), `known_cursor_deviations_from_mdbx`, the long-read tests, the long-reader warning (`a_long_reader_warning_is_due_first_then_once_per_interval`, and `a_long_reader_warns_after_a_commit_at_most_once_per_interval` with shortened limits, catching the `warn!` through the hand-rolled `WarnCapture` subscriber) |
| `tests/dup.rs` | every dup call against MDBX (`dup_cursor_calls_match_mdbx`, `dup_cursor_steps_after_a_delete_or_last_match_mdbx`), `known_dup_cursor_deviations_from_mdbx` (the raw-subkey difference among them), `dup_layouts_match_reths_encodings`, `seek_by_key_subkey` against MDBX at every layout's full widths (`seek_by_key_subkey_matches_mdbx_at_every_layouts_widths`) and with a seek key past the stack buffer (`seek_by_key_subkey_longer_than_the_stack_buffer_matches_mdbx`, which pins the raw-subkey difference there too), import both ways, refused rows and tables, dup values in runs (`dup_values_in_overflow_runs_read_back_whole`, `seek_by_key_subkey` hits and misses among them), a dup write refused for its size, the one-value-per-subkey rule on every write path (`a_dup_row_written_again_with_its_value_matches_mdbx`, `a_second_value_for_a_subkey_is_refused_and_changes_nothing`, `a_second_value_refusal_is_a_write_error_and_plain_tables_still_overwrite`; `second_value_refused` checks the refusal's shape) |
| `tests/differential.rs` | `random_scripts_match_mdbx` (32 scripts of up to 64 ops) and `_heavy` (2000 of 256): proptest scripts on both databases, outcome lists equal, with a per-cursor model that skips the excluded classes; a second value for a dup subkey (class 6) runs on tndb-b only and must be refused |
| `tests/crash.rs` | `killed_child_reopens_to_its_last_commit` (5 cycles) and `_heavy` (200, which must kill one in a commit); `crash_cycle_child` is the child, a no-op unless `CHILD_ENV` is set |

## Rules of the crate

- **Never change MDBX-visible behaviour without the differential test.** A change to what a call
  returns, or where it leaves a cursor, runs `random_scripts_match_mdbx` (and the heavy variant
  before handing back). MDBX is the oracle: a new mismatch is fixed, or becomes a documented
  difference in `lib.rs`, pinned beside MDBX's result in `known_cursor_deviations_from_mdbx` or
  `known_dup_cursor_deviations_from_mdbx`, with an exclusion class in the runner.
- **The dup layout is fixed per table** (`DUP_LAYOUTS`; `dup_layouts_match_reths_encodings` checks
  it against reth's encodings). Stored key = encoded key ‖ the value's first W bytes, stored value
  = the rest:

  | table | key (K) | subkey (W) |
  |---|---|---|
  | `PlainStorageState` | 20 | 32 |
  | `StorageChangeSets` | 28 | 32 |
  | `HashedStorages` | 32 | 32 |
  | `AccountChangeSets` | 8 | 20 |
  | `StoragesTrie` | 32 | 65 |

  A row this layout cannot hold is `MDBX_BAD_VALSIZE`; any other dup-sort table is refused
  (`DatabaseError::Other`). A layout change is a format change for every file holding that table.
- **The differential test's exclusion classes** (`tests/differential.rs` module docs) are: past
  either end; another writer's delete and put; another writer changes the cursor's dup key;
  `last_dup` or `delete_current` after an outer delete; `append_dup` of the key's last value; one
  value per subkey (a second value runs on tndb-b only, which must refuse it); `last_dup` before the first row; a dup key of one row. Widening a class hides
  behaviour from the oracle: do it only for a difference documented in `lib.rs`. About 4% of ops
  are skipped (the run prints the counts by class), and the test fails if a tenth or more are; a
  change that raises the rate needs a reason.
- **MDBX is no oracle when another writer changes a cursor's dup key.** MDBX's cursor keeps its own
  copy of the key's nested rows, which may go stale by page layout: its next step can skip a row
  or change key, `delete_current_duplicates` can leave `entries` one off, and `delete_current` can
  abort the process in libmdbx's `tree_rebalance`. reth never does this. Here the cursor finds its
  place again by key; the engine's model tests cover it, and class 3 taints such cursors.
- **Errors map to MDBX's codes** (`error::info`, an exhaustive match, and the table in `lib.rs`),
  each code checked against `reth_libmdbx::Error::X.into()` by
  `engine_errors_carry_mdbx_codes_and_messages`. A new engine error variant needs a row in both
  and in that test. `Corrupt` carries only `MDBX_CORRUPTED`; the detail is in `TndbEnv::failure`.
- **Tests open through `open_env` and spawn through `spawn`** (`tests/mod.rs`). `SPAWN_LOCK` is an
  `RwLock`: opens share it, a spawn takes it alone. A child forked while another thread's open is
  between its create and its lock holds that open's file description until it execs, and the
  open then fails with `MDBX_BUSY`. Never call `TndbEnv::open` or `Command::spawn` directly in a
  test.
- **Default tests stay small** (CI runs nextest in debug); heavy variants are
  `#[ignore = "heavy: run with --release"]` with a `_heavy` suffix. **Mutation-check every new
  test.** Durable anchors only in comments and docs: function, test and type names, real issue
  numbers.
- **reth's tests are ported, not rewritten:** a port changes only the constructor (and random
  values to fixed ones); a deviation asserts this crate's behaviour and says so in the module docs.

## Checks before handing back

```text
cargo nextest run -p tndb-b-reth
cargo +nightly-2026-03-20 clippy -p tndb-b-reth --all-targets -- -D warnings
cargo +nightly-2026-03-20 clippy -p tndb-b-reth --all-targets --all-features -- -D warnings
cargo +nightly-2026-03-20 fmt --all -- --check
RUSTDOCFLAGS="-D warnings" cargo doc -p tndb-b-reth --no-deps
cargo test --release -p tndb-b-reth -- --ignored --skip tndb_reth_vs_mdbx --skip tndb_b_vs_mdbx   # heavy variants, about 1.5 min
./etc/ci-lanes.sh clippy
```

A change in the engine runs the engine's checks too (`crates/tndb-b/CLAUDE.md`).

## The benchmarks and the Linux gate

`tndb_b_vs_mdbx` (`src/engine_bench.rs`) runs the engine itself against MDBX, on demand
(selectors `TNDB_B_BENCH_N`, `TNDB_B_BENCH_ENGINES`, `TNDB_B_BENCH_ROWS`, `TNDB_B_BENCH_REPEAT`;
`engine-bench.md` holds its runs):

```text
cargo test --release -p tndb-b-reth tndb_b_vs_mdbx -- --ignored --nocapture --test-threads 1
```

`tndb_reth_vs_mdbx` (`src/bench.rs`) runs on demand:

```text
cargo test --release -p tndb-b-reth tndb_reth_vs_mdbx -- --ignored --nocapture --test-threads 1
```

Selectors: `TNDB_RETH_BENCH_N` (the `hash_keys` sizes, default `10000,100000`),
`TNDB_RETH_BENCH_ROWS` (a group, `table`, `dupsort`, `hash`, `get`, `put`, or part of a label),
`TNDB_RETH_BENCH_REPEAT` (minimum samples per cell), `TNDB_RETH_BENCH_ENGINES` (`tndb` or `mdbx`,
to profile one side), `TNDB_RETH_BENCH_SYNC` (`durable`, the default, or `nosync`: tndb `NoSync`
and MDBX `UtterlyNoSync`, so the write rows time the CPU alone; never the gate, and the table's
header row says so), and on Apple Silicon `TNDB_RETH_BENCH_MDBX_PAGE=4096` for MDBX pages the size
of tndb's.

**The Linux gate** decides M2's performance: each bench at most 1.10× MDBX, with at most one
explained row up to 1.25×. Run it three times at reth's sizes, pinned to performance cores with
the temp dir on a real disk (as the run tip in `engine-bench.md` says; the run stops if the temp
dir is on a tmpfs), with the drive's autonomous power states off and the CPU governor and energy
preference at `performance` (the README gives the commands; all three reset on a reboot or a
sleep). From the repository root:

```text
mkdir -p tmp; for i in 1 2 3; do TMPDIR=$PWD/tmp TNDB_RETH_BENCH_N=10000,100000,1000000 taskset -c 0-7 cargo test --release -p tndb-b-reth tndb_reth_vs_mdbx -- --ignored --nocapture --test-threads 1 | grep '^|' > reth-bench-$i.txt; done
```

Report each cell as the median of the three. A consumer NVMe drive syncs a commit at one of two
speeds and switches during a run, for both engines at once (the README's "Durable write rows on a
consumer drive"), so a durable write row over 1.10× is the engine's only if it stays over at the
drive's fast speed (the runs where MDBX's time for that row is lowest) or under
`TNDB_RETH_BENCH_SYNC=nosync`. Both engines issue the same syncs per commit; a change to tndb's
commit path that adds one is the engine's cost, whatever the drive does.

## On a reth upgrade (including storage v2)

reth's database interface leaks MDBX's behaviour, so a new reth version is checked against it,
not assumed:

1. **Re-port reth's database tests** from the new version into `src/tests/mdbx_port.rs`, organised
   as now (its module docs: the tests of `reth-db`'s `implementation/mdbx/mod.rs` and the named
   one of its `cursor.rs`, only the constructor swapped, random values fixed, a documented
   deviation asserting this crate's behaviour), and run them. Update the reth version in the
   crate docs' first line, the README's and this file's, and the checkout path above.
2. **Re-run the differential test,** default and heavy, and read the skip counts by class:
   ```text
   cargo test --release -p tndb-b-reth random_scripts_match_mdbx -- --nocapture
   cargo test --release -p tndb-b-reth random_scripts_match_mdbx_heavy -- --ignored --nocapture
   ```
   The test fails at a skip rate of a tenth (about 4% now). A new mismatch, or a class whose
   share moved, means libmdbx's behaviour changed under reth's interface; find out how before
   widening a class.
3. **Re-check the leaky-interface list** (`src/lib.rs`, "Differences from MDBX") against the new
   reth code:
   - cursor end rows (class 1): new reth code that reads `current` or calls `delete_current`
     after stepping past an end, or after a `seek` past the end, would see this crate's
     difference;
   - the dup write pattern: reth deletes a (key, subkey) before it writes it
     (`seek_by_key_subkey`, `delete_current`, `upsert`). A real duplicate fails loudly with
     `second_dup_value`'s refusal, so a new failure there means reth changed its pattern, which
     needs a decision (a layout change, or real dup-sort support), never a quiet overwrite;
   - the error codes: `engine_errors_carry_mdbx_codes_and_messages` against the new
     `reth_libmdbx`, and the error variants reth expects per operation (`tests/adapter.rs`).
4. **Diff reth's table set** (`reth_db_api::tables`) against the adapter's: plain tables come
   from `Tables::ALL` on their own, but a new dup-sort table needs a `DUP_LAYOUTS` row (else its
   rows are refused), a removed one leaves its row unused, and a changed key or subkey width, or
   a dup-sort flag, is a format change (`dup_layouts_match_reths_encodings` catches an encoding
   change).
5. **Storage v2** moves some tables out of MDBX. Those go to the tn-storage toolkit (such as
   `HdxIndex` for the transaction-hash index), never RocksDB: a project decision. If v2 lands
   while TN integration is under way, port the adapter to v2's database API once, not to v1 and
   then again.
6. **If a hot path changed** (reth's cursor or transaction code, or this crate's), re-run
   `tndb_reth_vs_mdbx` under the Linux gate rule above (and the README's "Linux run (the gate)").
