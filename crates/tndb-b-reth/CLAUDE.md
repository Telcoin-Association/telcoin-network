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
| `env.rs` | `TndbEnv`: `open` (`RW` creates `<dir>/tndb.dat` through `Env::open`, `RO` uses `Env::open_read_only`), `create_tables`/`create_tables_for`, `failure`, reth's `Database` and `DatabaseMetrics` (the gauges MDBX reports, from `table_stat` and `Env::free_pages`); `TndbArgs` (page size, default 4096; map reserve; `SyncMode`); the table map `TableMap` (name to `TableEntry`: the engine `Table` and its `DupLayout`, hashed by `NameHasher`, FxHash's step), `resolve` |
| `tx.rs` | `TndbTx` (an `Arc` of the engine's `RoTxn`, so its cursors own a share), `TndbTxMut` (`SharedTxn` = `Arc<Mutex<Option<RwTxn>>>`: every method takes `&self`, holds the guard for one call, and `commit` takes the transaction out, after which calls are `MDBX_BAD_TXN`); reth's `DbTx`, `DbTxMut`, `TableImporter`; `delete_rows_of` (a dup key's rows) |
| `cursor.rs` | `TndbCursor` (over `RoCursor`) and `TndbCursorMut` (over `TxnCursor`, the transaction locked per call as `Locked`); `Moves`, the engine moves both implement, `seek_with_key_tail` among them (both forward it to the engine's); `Rows`, the adapter's moves written once over `Moves`: the table's `DupLayout`, the value buffer, and `Pos` (`Fresh`, `Missed`, `Engine`, `KeyDeleted`, `DupEnd { soft }`, MDBX's cursor states the engine's cursors do not hold); `Rows::wrote` (the state after a write: placed by a success or a handled refusal, else unchanged); `Rows::seek_by_key_subkey_row` (builds `key ‖ subkey` in a 128-byte stack buffer, `SEEK_KEY_STACK`, which a compile-time check holds every `DUP_LAYOUTS` row key to, a `Vec` only for a longer raw input, and returns the row from one `Moves::seek_with_key_tail`, kept only if it is the key's, `is_row_of`); `dup_decoded` and `dup_value_of`, free fns, `#[inline(always)]`; `range_start` for the walkers; reth's cursor traits, plain and dup |
| `dup.rs` | `DupLayout` (`key` width K, `subkey` width W), `DUP_LAYOUTS` (the five tables), `dup_layout`; `join` and `split` (a row to its stored key and value and back), `is_row_of`, `past` (the seek key past a key's rows, `k ‖ 0xFF × (W + 1)`), `joined` and `rebuild` (a read's value from the engine's `KeyTail`: one slice, or joined in the cursor's buffer for a value in a run) |
| `error.rs` | engine errors as reth's `DatabaseError` with MDBX's codes and messages: `Mdbx` (the codes, from `mdbx.h` and `libc`), `info` (the exhaustive map), the per-operation constructors (`read`, `write`, `delete`), `finished`, `dup_sort_unsupported`, `bad_size`, `table_not_found` |
| `bench.rs` | test-only: `tndb_reth_vs_mdbx`, reth's db benches run on both databases (module docs: workloads, data, timing, selectors; `TNDB_RETH_BENCH_SYNC=nosync` opens tndb `NoSync` and MDBX `UtterlyNoSync`, and the table's header row, which `grep '^|'` keeps, says it is CPU only, not the gate) |
| `tests/mod.rs` | `SPAWN_LOCK`, `open_env`, `spawn` (below) |
| `tests/mdbx_port.rs` | reth's MDBX tests with only the constructor swapped; three dup tests assert the one-value-per-subkey deviation instead |
| `tests/adapter.rs` | error codes against `reth_libmdbx`, reth's error variants per operation, the metrics over every table, a blocked second writer, shared write cursors and cursor steps after a delete or `last` (both run on MDBX too), `known_cursor_deviations_from_mdbx`, the long-read tests |
| `tests/dup.rs` | every dup call against MDBX (`dup_cursor_calls_match_mdbx`, `dup_cursor_steps_after_a_delete_or_last_match_mdbx`), `known_dup_cursor_deviations_from_mdbx` (the raw-subkey difference among them), `dup_layouts_match_reths_encodings`, `seek_by_key_subkey` against MDBX at every layout's full widths (`seek_by_key_subkey_matches_mdbx_at_every_layouts_widths`) and with a seek key past the stack buffer (`seek_by_key_subkey_longer_than_the_stack_buffer_matches_mdbx`, which pins the raw-subkey difference there too), import both ways, refused rows and tables, dup values in runs (`dup_values_in_overflow_runs_read_back_whole`, `seek_by_key_subkey` hits and misses among them), a dup write refused for its size |
| `tests/differential.rs` | `random_scripts_match_mdbx` (32 scripts of up to 64 ops) and `_heavy` (2000 of 256): proptest scripts on both databases, outcome lists equal, with a per-cursor model that skips the excluded classes |
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
  value per subkey; `last_dup` before the first row; a dup key of one row. Widening a class hides
  behaviour from the oracle: do it only for a difference documented in `lib.rs`. Under 5% of ops
  are skipped (the run prints the counts by class); a change that raises the rate needs a reason.
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
cargo test --release -p tndb-b-reth -- --ignored --skip tndb_reth_vs_mdbx   # heavy variants, about 1.5 min
./etc/ci-lanes.sh clippy
```

A change in the engine runs the engine's checks too (`crates/tndb-b/CLAUDE.md`).

## The benchmark and the Linux gate

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
the temp dir on a real disk (as the engine README's run tip says; the run stops if the temp
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
