# tn-lmdb-reth: a guide for working in this crate

`tn-lmdb-reth` implements reth's database API (`reth-db-api`, reth v1.11.3) on the
[`tn-lmdb`](../tn-lmdb) engine. `TnLmdbEnv` is a reth `Database` whose transactions and cursors behave
as reth's MDBX ones do and report MDBX's error codes, so reth's code and its MDBX tests run
unchanged. Of reth's five dup-sort tables, the state tables (`PlainStorageState`,
`HashedStorages`, `StoragesTrie`) are the engine's dup tables, each key's few rows packed in its
outer cell (`DupOptions::packed`), more in a nested tree, and the changesets are composite keys
(`key ‖ subkey`) in plain engine tables. It is pre-production: TN uses it behind the
off-by-default `tn-lmdb` feature of `tn-reth` (`crates/tn-reth/src/env/database.rs` holds every
backend difference). This is the only crate that depends on both reth and the engine; the engine
depends on neither reth nor `tn-storage`.

**Read in this order:**
1. this file;
2. `README.md` (purpose, status, the dup layouts and why the changesets stay composite, the
   reth-bench runs against MDBX);
3. the crate docs in `src/lib.rs`: the type mapping, the dup layout and call mapping, the error
   table, every difference from MDBX, and the differential and crash tests;
4. the module docs at the top of the file you change;
5. for engine behaviour, `crates/tn-lmdb/CLAUDE.md` and the engine README's cursor sections
   ("Public API reference", "Detached cursors").

reth's contract is in the checkout at
`~/.cargo/git/checkouts/reth-e231042ee7db3fb7/d6324d6/crates/storage/db-api/src`; its MDBX
implementation and tests are in `crates/storage/db/src/implementation/mdbx/` there.

## Crate map

| file | role |
|---|---|
| `lib.rs` | crate docs (the reference: mappings, differences from MDBX, the tests), re-exports (`TnLmdbEnv`, `TnLmdbEnvKind`, `TnLmdbArgs`, `RethTableStat`, `DATA_FILE`, `TnLmdbTx`, `TnLmdbTxMut`, `TnLmdbCursor`, `TnLmdbCursorMut`, and the engine's `SyncMode`, `CheckReport`, `TableCheck`, `IoStats` and `MIN_GROWTH`) |
| `env.rs` | `TnLmdbEnv`, reth's `Database` and `DatabaseMetrics`: `open` (`RW` creates `<dir>/tn-lmdb.dat`, `RO` is the engine's read-only open), `create_tables`/`create_tables_for`, `sync`, `check`, `io_stats`, `table_stats`; `TnLmdbArgs` (page size, map reserve, `max_growth`, `SyncMode` with its automatic syncs `sync_period` and `sync_bytes`, and the test-only form knobs `composite_dup_tables` and `packed_dup_max`); the table map (`TableMap`, `TableEntry`, `NameHasher`); the long-reader warning (`ReaderWatch`). Invariants: `TableEntry::open` refuses a table in another form than its layout's, or a dup table with other options (`error::form_mismatch`, `MDBX_INCOMPATIBLE`); a `sync` failure, or damage `check` finds, latches the environment |
| `tx.rs` | `TnLmdbTx` (an `Arc<RoTxn>` its cursors share, and the `ReadPool` of their engine cursors) and `TnLmdbTxMut` (`SharedTxn`: the engine transaction and the cursors' held dup delete, `Deferred`, behind one mutex each call holds); reth's `DbTx`, `DbTxMut`, `TableImporter`; `insert_dup`. Invariants: every call first runs a held delete (a second delete held while one is held is refused, never an overwrite); `commit` refuses, aborting, if one failed, and runs `ReaderWatch::after_commit` after a success; a second value for a dup subkey is refused, the same value is a no-op success |
| `cursor.rs` | `TnLmdbCursor` (over the engine's `RoCursor` or `DupRoCursor`, pooled per transaction by `ReadPool`) and `TnLmdbCursorMut` (over `TxnCursor` or `DupTxnCursor`, the transaction locked per call); `Rows`, reth's moves written once over `Moves` for plain and dup tables, with the MDBX cursor states the engine's cursors do not hold (`Pos`); the held dup `delete_current` (`Deferred`, `Parked`), which makes reth's dup rewrite one engine `put`. The module docs give each move's MDBX rule; a change to one runs the differential test |
| `dup.rs` | `DupLayout` (`key` width K, `subkey` width W, `nested`: the form, an engine dup table or composite keys; `options`, the engine's `DupOptions` of a nested layout), `DUP_LAYOUTS` (the five tables), `dup_layout`; `join` and `split` (a row to its stored key and value and back), `is_row_of`, `past` (the seek key past a key's rows, `k ‖ 0xFF × (W + 1)`), `joined` and `rebuild` (a read's value from the engine's `KeyTail`: one slice, or joined in the cursor's buffer for a value in a run) |
| `error.rs` | engine errors as reth's `DatabaseError` with MDBX's codes and messages: `Mdbx` (the codes, from `mdbx.h` and `libc`), `info` (the exhaustive map), the per-operation constructors (`read`, `write`, `delete`), `finished`, `dup_sort_unsupported`, `bad_size`, `second_dup_value` (`MDBX_KEYEXIST` with this crate's message: one value per subkey), `form_mismatch` (`Open` with `MDBX_INCOMPATIBLE`: a table in another form than its layout's), `table_not_found` |
| `bench.rs` | test-only: `tn_lmdb_reth_vs_mdbx`, reth's db benches run on both databases (module docs: workloads, data, timing, selectors; `TN_LMDB_RETH_BENCH_SYNC=nosync` opens tn-lmdb `NoSync` and MDBX `UtterlyNoSync`, and the table's header row, which `grep '^|'` keeps, says it is CPU only, not the gate) |
| `engine_bench.rs` | test-only: `tn_lmdb_vs_mdbx`, the engine through its own API (`Env`) against `reth-libmdbx` (module docs: rows, caveats, the `TN_LMDB_BENCH_*` selectors; `engine-bench.md` has the how-to and the historical runs); it uses `bench.rs`'s `announce_temp_dir` and `page_size` |
| `tests/mod.rs` | `SPAWN_LOCK`, `open_env`, `spawn` (below) |
| `tests/mdbx_port.rs` | reth's MDBX tests with only the constructor swapped (`create_test_db`, the production forms; `create_test_db_with(kind, Form)` for any form); three dup tests assert the one-value-per-subkey refusal instead |
| `tests/adapter.rs` | the adapter's surface against MDBX: error codes (`engine_errors_carry_mdbx_codes_and_messages`) and reth's error variants per operation, the metrics and table stats, `sync`, `io_stats`, `check`, cursor steps after a delete or `last` (run on MDBX too), `known_cursor_deviations_from_mdbx`, the form check at open (`a_table_in_another_form_than_its_layout_is_refused`, `a_dup_table_with_other_packing_is_refused_at_open`), the read-cursor pool, the cursors' sizes (`tn_lmdb_cursors_stay_small`), and the long-reader warning (caught by `WarnCapture`) |
| `tests/dup.rs` | every dup call against MDBX on every form (`on_every_form`: a new database per `Form`, MDBX's beside it); `known_dup_cursor_deviations_from_mdbx`; `dup_layouts_match_reths_encodings`; `seek_by_key_subkey` at every layout's widths; values in overflow runs; the one-value-per-subkey rule on every write path (`a_second_value_for_a_subkey_is_refused_and_changes_nothing`); the held delete of reth's dup rewrite (`the_dup_rewrite_matches_mdbx_and_is_one_engine_put`, `a_held_delete_runs_before_any_other_call_as_on_mdbx`, `a_commit_runs_a_held_delete_as_on_mdbx`) and its error timing (`a_held_delete_that_meets_damage_fails_the_call_that_runs_it`, `a_held_delete_that_fails_unlatched_still_fails_the_commit`, `a_second_held_delete_is_refused_and_fails_the_commit`, `a_rewrite_put_on_a_latched_environment_skips_the_held_delete`); a nested wipe that removes no row or leaves one is refused and fails the commit (`a_wipe_that_breaks_its_invariant_is_refused_and_fails_the_commit`) |
| `tests/differential.rs` | `random_scripts_match_mdbx` (32 scripts of up to 64 ops) and `_heavy` (2000 of 256): proptest scripts on MDBX once and on tn-lmdb in every form, each form's outcomes and skips by class equal to MDBX's. The scripts drive the write transaction's cursors and the read transaction's own cursor (every read call and walk over the engine's read-only cursors, kept across the write transaction's commits, reopened from the pool); a second value for a dup subkey (class 6) runs on tn-lmdb only and must be refused. A new seed each run, printed (`PROPTEST_RNG_SEED` reruns it); `random_scripts_skip_counts_are_pinned_at_seed_1` and `_heavy` pin seed 1's counts by class (`DEFAULT_TOTALS`, `HEAVY_TOTALS`) |
| `tests/crash.rs` | `killed_child_reopens_to_its_last_commit` (5 cycles), `_composite` (5, the composite form, told to the child by `COMPOSITE_ENV`) and `_heavy` (200, which must kill one in a commit); a round writes one account of `BIG_SLOTS` (600) rows, over several nested leaves, and wipes it now and then, `SMALL_ACCOUNTS` of a few rows (packed sets rewritten) and a growing account (packed, then promoted in a later commit); `crash_cycle_child` is the child, a no-op unless `CHILD_ENV` is set |

## Rules of the crate

- **Never change MDBX-visible behaviour without the differential test.** A change to what a call
  returns, or where it leaves a cursor, runs `random_scripts_match_mdbx` (and the heavy variant
  before handing back). MDBX is the oracle: a new mismatch is fixed, or becomes a documented
  difference in `lib.rs`, pinned beside MDBX's result in `known_cursor_deviations_from_mdbx` or
  `known_dup_cursor_deviations_from_mdbx`, with an exclusion class in the runner.
- **The dup layout is fixed per table** (`DUP_LAYOUTS`; `dup_layouts_match_reths_encodings` checks
  it against reth's encodings, the form included). Logical key = encoded key ‖ the value's first
  W bytes, stored value = the rest; a nested table is an engine dup table (`DupOptions::packed(K)`,
  `packed_max` resolved to the page size's most, 987 for K 20 at 4 KiB), a composite one a plain engine table keyed by the logical key:

  | table | key (K) | subkey (W) | form |
  |---|---|---|---|
  | `PlainStorageState` | 20 | 32 | nested (dup, packed) |
  | `StorageChangeSets` | 28 | 32 | composite |
  | `HashedStorages` | 32 | 32 | nested (dup, packed) |
  | `AccountChangeSets` | 8 | 20 | composite |
  | `StoragesTrie` | 32 | 65 | nested (dup, packed) |

  A row this layout cannot hold is `MDBX_BAD_VALSIZE`; any other dup-sort table is refused
  (`DatabaseError::Other`). A layout change, the form included, is a format change for every file
  holding that table: the open refuses a table in the other form, or a dup table whose recorded
  options (key width, `packed_max` resolved at the file's page size) differ (`TableEntry::open`,
  `error::form_mismatch`, `MDBX_INCOMPATIBLE`), and `tn-reth`'s exec-bench bases need a new `LAYOUT_VERSION`
  (`crates/tn-reth/src/test_utils/load/base.rs`). Every call has a nested arm and a composite one:
  a change to either runs the tests on every form (they do by default: `tests::Form` — packed,
  packed-small via the test-only `TnLmdbArgs::packed_dup_max(PACKED_SMALL_RETH = 128)`, composite —
  in `on_every_form` and the differential runner; the crash test's `_composite` twin).
- **The differential test's exclusion classes** (`tests/differential.rs` module docs) are: past
  either end; another writer's delete and put; another writer changes the cursor's dup key;
  `last_dup` or `delete_current` after an outer delete; `append_dup` of the key's last value; one
  value per subkey (a second value runs on tn-lmdb only, which must refuse it); `last_dup` before the first row; a dup key of one row. Widening a class hides
  behaviour from the oracle: do it only for a difference documented in `lib.rs`. About 4.5% of ops
  are skipped (the run prints the counts by class), and the test fails if a tenth or more are; a
  change that raises the rate needs a reason.
- **The differential test's seed:** the random tests take a new seed each run (CI keeps finding new
  scripts), printed with the counts and in a failure's message; `PROPTEST_RNG_SEED=<seed>` reruns
  a failure's scripts. The pinned tests (`random_scripts_skip_counts_are_pinned_at_seed_1`,
  `_heavy`) always run seed 1: a change to the scripts' strategy (`op()`) or to the skip rules
  moves their counts, and the new counts are pinned only once the change is understood (a class
  whose share moved is a behaviour change until shown otherwise).
- **MDBX is no oracle when another writer changes a cursor's dup key.** MDBX's cursor keeps its own
  copy of the key's nested rows, which may go stale by page layout: its next step can skip a row
  or change key, `delete_current_duplicates` can leave `entries` one off, and `delete_current` can
  abort the process in libmdbx's `tree_rebalance`. reth never does this. Here the cursor finds its
  place again by key; the engine's model tests cover it, and class 3 taints such cursors.
- **Errors map to MDBX's codes** (`error::info`, an exhaustive match, and the table in `lib.rs`),
  each code checked against `reth_libmdbx::Error::X.into()` by
  `engine_errors_carry_mdbx_codes_and_messages`. A new engine error variant needs a row in both
  and in that test. `Corrupt` carries only `MDBX_CORRUPTED`; the detail is in `TnLmdbEnv::failure`.
- **Tests open through `open_env` and spawn through `spawn`** (`tests/mod.rs`). `SPAWN_LOCK` is an
  `RwLock`: opens share it, a spawn takes it alone. A child forked while another thread's open is
  between its create and its lock holds that open's file description until it execs, and the
  open then fails with `MDBX_BUSY` (the engine's `Locked`). Never call `TnLmdbEnv::open` or
  `Command::spawn` directly in a test. The hazard is any open of a database in the test binary,
  the engine's own `Env::open` included: a test that calls it holds `SPAWN_LOCK.read()` across
  its opens (as `changesets_forms_hold_the_same_rows` does).
- **Default tests stay small** (CI runs nextest in debug); heavy variants are
  `#[ignore = "heavy: run with --release"]` with a `_heavy` suffix. **Mutation-check every new
  test.** Durable anchors only in comments and docs: function, test and type names, real issue
  numbers.
- **reth's tests are ported, not rewritten:** a port changes only the constructor (and random
  values to fixed ones); a deviation asserts this crate's behaviour and says so in the module docs.

## Checks before handing back

```text
cargo nextest run -p tn-lmdb-reth
cargo +nightly-2026-03-20 clippy -p tn-lmdb-reth --all-targets -- -D warnings
cargo +nightly-2026-03-20 clippy -p tn-lmdb-reth --all-targets --all-features -- -D warnings
cargo +nightly-2026-03-20 fmt --all -- --check
RUSTDOCFLAGS="-D warnings" cargo doc -p tn-lmdb-reth --no-deps
cargo test --release -p tn-lmdb-reth -- --ignored --skip tn_lmdb_reth_vs_mdbx --skip tn_lmdb_vs_mdbx   # heavy variants, about 2.5 min
./etc/ci-lanes.sh clippy
```

A change in the engine runs the engine's checks too (`crates/tn-lmdb/CLAUDE.md`).

## The benchmarks and the Linux gate

`tn_lmdb_vs_mdbx` (`src/engine_bench.rs`) runs the engine itself against MDBX, on demand
(selectors `TN_LMDB_BENCH_N`, `TN_LMDB_BENCH_ENGINES`, `TN_LMDB_BENCH_ROWS`, `TN_LMDB_BENCH_REPEAT`;
`engine-bench.md` has its how-to and its historical runs):

```text
cargo test --release -p tn-lmdb-reth tn_lmdb_vs_mdbx -- --ignored --nocapture --test-threads 1
```

`tn_lmdb_reth_vs_mdbx` (`src/bench.rs`), the adapter's comparison against MDBX on reth's database
benches, runs on demand (the README holds its runs):

```text
cargo test --release -p tn-lmdb-reth tn_lmdb_reth_vs_mdbx -- --ignored --nocapture --test-threads 1
```

Selectors: `TN_LMDB_RETH_BENCH_N` (the `hash_keys` sizes, default `10000,100000`),
`TN_LMDB_RETH_BENCH_ROWS` (a group, `table`, `dupsort`, `hash`, `get`, `put`, or part of a label),
`TN_LMDB_RETH_BENCH_REPEAT` (minimum samples per cell), `TN_LMDB_RETH_BENCH_ENGINES` (`tn-lmdb` or `mdbx`,
to profile one side), `TN_LMDB_RETH_BENCH_SYNC` (`durable`, the default, or `nosync`: tn-lmdb `NoSync`
and MDBX `UtterlyNoSync`, so the write rows time the CPU alone; never the gate, and the table's
header row says so), and on Apple Silicon `TN_LMDB_RETH_BENCH_MDBX_PAGE=4096` for MDBX pages the size
of tn-lmdb's.

**The Linux gate**, the rule for a change on the adapter's hot path (this crate's cursors and
transactions, or reth's cursor or transaction code; an engine change runs the engine's regression
check, `crates/tn-lmdb/CLAUDE.md`): each bench at most 1.10× MDBX, with at most one explained row up
to 1.25×. Run it three times at reth's sizes, pinned to performance cores with the temp dir on a
real disk (as the run tip in `engine-bench.md` says; the run stops if the temp dir is on a tmpfs),
with the drive's autonomous power states off and the CPU governor and energy preference at
`performance` (the README gives the commands; all three reset on a reboot or a sleep). From the
repository root:

```text
mkdir -p tmp; for i in 1 2 3; do TMPDIR=$PWD/tmp TN_LMDB_RETH_BENCH_N=10000,100000,1000000 taskset -c 0-7 cargo test --release -p tn-lmdb-reth tn_lmdb_reth_vs_mdbx -- --ignored --nocapture --test-threads 1 | grep '^|' > reth-bench-$i.txt; done
```

Report each cell as the median of the three. A consumer NVMe drive syncs a commit at one of two
speeds and switches during a run, for both engines at once (the README's "Durable write rows on a
consumer drive"), so a durable write row over 1.10× is the engine's only if it stays over at the
drive's fast speed (the runs where MDBX's time for that row is lowest) or under
`TN_LMDB_RETH_BENCH_SYNC=nosync`. Both engines sync a commit alike: one data sync (tn-lmdb an
`msync` of one dirty range, else an `fdatasync`; MDBX an `msync` of the used span), a size sync when
the file grew, and a meta `msync`; a change to tn-lmdb's commit path that adds one is the engine's
cost, whatever the drive does.

## On a reth upgrade (including storage v2)

reth's database interface leaks MDBX's behaviour, so a new reth version is checked against it,
not assumed:

1. **Re-port reth's database tests** from the new version into `src/tests/mdbx_port.rs`, organised
   as now (its module docs: the tests of `reth-db`'s `implementation/mdbx/mod.rs` and the named
   one of its `cursor.rs`, only the constructor swapped, random values fixed, a documented
   deviation asserting this crate's behaviour), and run them. Update the reth version in the
   crate docs' first line, the README's and this file's, and the checkout path above. Every reth
   crate the adapter uses (`reth-trie-common` included) comes from the root `Cargo.toml`'s
   `[workspace.dependencies]`, so the workspace's tag bump moves them all.
2. **Re-run the differential test,** default and heavy, random and pinned, and read the skip
   counts by class:
   ```text
   cargo test --release -p tn-lmdb-reth random_scripts_ -- --include-ignored --nocapture
   ```
   The test fails at a skip rate of a tenth (about 4.5% now), and the pinned tests fail if a count
   at seed 1 moved. A new mismatch, or a class whose share moved, means libmdbx's behaviour
   changed under reth's interface; find out how before widening a class or pinning new counts.
3. **Re-check the leaky-interface list** (`src/lib.rs`, "Differences from MDBX") against the new
   reth code:
   - cursor end rows (class 1): new reth code that reads `current` or calls `delete_current`
     after stepping past an end, or after a `seek` past the end, would see this crate's
     difference;
   - the dup write pattern: reth deletes a (key, subkey) before it writes it
     (`seek_by_key_subkey`, `delete_current`, `upsert`). A real duplicate fails loudly with
     `second_dup_value`'s refusal, so a new failure there means reth changed its pattern, which
     needs a decision (a layout change, or real dup-sort support), never a quiet overwrite. The
     adapter makes the pattern one engine put (the dup `delete_current` is held until the cursor's
     next call, `Deferred`): check that reth's three writers (`write_state_changes`,
     `write_hashed_state`, `write_storage_trie_updates_sorted`) still upsert through the same
     cursor right after the delete; another order stays correct but loses the saving;
   - the error codes: `engine_errors_carry_mdbx_codes_and_messages` against the new
     `reth_libmdbx`, and the error variants reth expects per operation (`tests/adapter.rs`).
4. **Diff reth's table set** (`reth_db_api::tables`) against the adapter's: plain tables come
   from `Tables::ALL` on their own, but a new dup-sort table needs a `DUP_LAYOUTS` row (else its
   rows are refused), a removed one leaves its row unused, and a changed key or subkey width, or
   a dup-sort flag, is a format change (`dup_layouts_match_reths_encodings` catches an encoding
   change).
5. **Storage v2** moves some tables out of MDBX. Those go to the tn-storage toolkit (such as
   `HdxIndex` for the transaction-hash index), never RocksDB: a project decision. When TN moves to
   storage v2, port the adapter to v2's database API as part of that move.
6. **If a hot path changed** (reth's cursor or transaction code, or this crate's), re-run
   `tn_lmdb_reth_vs_mdbx` under the Linux gate rule above (and the README's "Linux run (the gate)").
