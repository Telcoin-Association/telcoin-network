# tn-lmdb: a guide for working in this crate

`tn-lmdb` is an embedded, memory-mapped key-value engine. It has one page file, copy-on-write
B+trees with the values in their leaves (a value too large for its leaf goes to an overflow run of
contiguous pages, up to 16 MiB), snapshot readers, a single writer, two meta pages, and a
persisted free list. There is no log: the meta pages, the gated page reuse and the free list carry
all crash safety. The design is LMDB-style, but this is a separate engine with its own format, not
LMDB or a binding to it, and it reads no LMDB or MDBX file (README, intro). It depends on nothing
in the workspace. Its primitives are copied from `tn-storage`, so never add a `tn-storage`
dependency, nor a reth one: reth's database API lives in the adapter crate `crates/tn-lmdb-reth`,
which builds on this crate's public API (its own `CLAUDE.md` covers it).

**Read in this order:**
1. this file;
2. `README.md`, the section for your topic: on-disk format (overflow runs included), dup tables,
   deletes and merging, commit protocol, free list and page reuse, readers, open and recovery,
   failure latch, concurrency, and the "please do not flag" list;
3. the module docs at the top of the file you change, then the item docs of what you touch.

`tree/write.rs`, `page.rs` and `txn.rs` are large, the first two about half tests: grep for the
symbol and read with an offset.

## Crate map

Each row: the file's role, its entry points, what a change must keep; item docs hold the rest.

| file | role |
|---|---|
| `lib.rs` | Crate docs, `compile_error!` off unix and on every unix but Linux and macOS (the syncs are verified on those two only: README, Platform notes, "Other Unixes"), the re-exports (the whole public API). |
| `error.rs` | `Error` (`thiserror`) and `Result`; `Error::latches`, the one list of the errors that latch (the latch, the write cursors' unset arms and the adapter read it). |
| `env.rs` | `Env`: open and recovery, steps 1–9 (`choose_state` 4–6, `checked_snapshot` 9; README "Open and recovery"), `open_read_only` (README "Read-only open"), the writer hand-off, snapshots (`Snapshot::page_again`, do-not-flag 14; `Snapshot` aligned to 128 bytes, its `Arc` counts on a line of their own), the reader queue (`Published`, `reuse_gate`, `Env::oldest_reader_age`), commit steps 3–6 (`stamp_private`, `make_durable`, `data_sync`), NoSync's deferral and automatic syncs (`defer_sync`; `AutoSync`, `sync_due` the one rule, `sync_with`; README "Commit protocol"), `alloc`, `copy_published_with` (a private source refused; `copy_unpublished`), `io_stats`, the test-only `CrashAt`, the close (`EnvInner::drop`, `Writer::trim_tail`; README "Close"). |
| `stats.rs` | `IoStats` (public, each counter documented; `since`), `elapsed_ns` and `lap`, the commit steps' clock reads. |
| `txn.rs` | Transactions and handles: `RoTxn`, `RwTxn`, `ReadTxn`; `Table` resolution through the snapshot's `TableRoots` (`RoTxn::root`, `resolve_root`; do-not-flag 15); commit steps 1–3 and 7 (`Work::commit`, `Work::flush_nested` first). Cursors: `RoCursor`, `RwCursor`, `TxnCursor` (detached; `Tracker` checks `Work::set_record`'s stamp, `Work::schema`, `Work::serial`), `DupRoCursor`, `DupTxnCursor` (`WorkRoots`). Dup tables: `dup_put`, `dup_delete` (the record's totals counted before the write changes anything the transaction reads, but for a sum past a counter's type: `OtherTrees`), `dup_delete_prefix`, `dup_get`, `packed_put`, `packed_delete`, `promote`, the working map (`Working::nested`, `dup_entry`, `held_cell`: do-not-flag 29), the free walks (`free_dup_table`; `WalkRoots`, their `write::NestedRoots`). See the dup rules in Rules of the crate. |
| `meta.rs` | Meta record codec (`FORMAT_VERSION` 5), `choose_meta`, `slot_to_write`. Any other version is `UnsupportedVersion`, checked before the CRC, never damage; no migration. |
| `page.rs` | Slotted page codec: `Geometry` (limits; `entry_limit` is the one source of the leaf `U / 2` and branch `U / 4` limits), `validate`, `Val`, `leaf_cell`/`leaf_val`, `CellVal`, `View`/`LeafView`/`BranchView`, `LeafMut`, `leaf_find`, `branch_route`, `SplitBuf` (splits and merges), `copy_insert`/`copy_replace`/`copy_delete`, the page CRC (`free_gap`, `crc`, `stamp`, `crc_state`; do-not-flag 28). A nested cell (`CELL_NESTED`, `NESTED_CELL` bytes) is accepted inside `leaf_cell`, still inline, by `nested_shape` once the inline and overflow tests fail, and decoded by the `#[cold]` `nested_val`. `prefetch` is one of the five `unsafe` places (do-not-flag 2). |
| `packed.rs` | The packed-set codec, a dup table's inline outer cell (README "Packed sets"): `PACKED_MIN`, `PackedView` (`new`'s O(1) header checks; `row`, `row_span`, `find`), `validate` (the whole set), the rebuilds `build_one`/`build_with`/`build_without` and their sizes `encoded_len_*`. Reads check lazily (do-not-flag 23); callers name the table through `packed_error`. |
| `overflow.rs` | Overflow runs: `Ovf` (the 12-byte cell reference), `MAX_VALUE` (16 MiB), the run codec (`encode_run`, `run_pages`, `value`), `checked_run` (every check a run read makes, behind every store's `PageRead::run`), `RunCrc` (`Skip`, `MultiPage`, `Always`). |
| `tree/mod.rs` | `TreeRoot` (the 32-byte handle and its page counters; `add_entry`/`remove_entry`, the checked entry count every put and delete changes through, `Corrupt` past its bounds), the `PageRead` store (`page`, `child`, `run`; the provided `value`, a get's value read, refuses a nested cell; its body is `read_value`, which no store overrides and the plain cursors' `Values::read` in `tree/cursor.rs` calls directly, so a trait-object store is called only for a run) and the `PageWrite` store (`alloc`, not zeroed; `free`, `page_mut`, `copy_published_with`, runs, `count_verify`); `POISON`, debug builds' fill of what `alloc` and `alloc_run` hand out. |
| `tree/read.rs` | The readers' descents and checks: `checked_page` (the per-fetch checks), `checked_child` (plus the identity check, behind `PageRead::child`; `check_identity`), `descend`, `get`, `check_miss` (a miss's neighbours inside the leaf's separators; a delete's miss too), `last_key`, `tighten` (shared with `write.rs`); the dup lookups through `find_in`: `find` (the catalog's and the free list's unread `Val`), `find_outer` (a miss's neighbours held to the table's width), `find_nested` (subkeys of 1 to `max_subkey` bytes). |
| `tree/write.rs` | Copy-on-write `put`/`delete` (`put_from_root`, `delete_from_root`: plan, reserve, apply; each computes the handle it leaves, checked, before anything changes, a delete's in `handle_after`, where an emptied tree's must count exactly its pages), splits, unlink and collapse, merging (`plan_delete`, `plan_merges`, `merge_due` the one merge rule, `check_sibling`, `apply_merges`), the checks of what a delete widens or moves (`check_widened_neighbour`, an unlink's or a collapse's neighbour; `check_edge_chain`, its edge or a branch sibling's child-0 chain; `collapse_to` only navigates), `descend_write` (provenance trust, do-not-flag 11), overflow puts (`reserve_put`, `write_run`: a run written whole before a leaf names it, the old one freed once the change applies), `tree_pages`, `free_subtree`, `clear`, the path cache (`PathCache`, `put_cached`, `delete_cached`; do-not-flag 12), the routing hint (`Hint`, `route_by_hint`; do-not-flag 27), the cell policy `Cells { Plain, Outer, Packing }`, `put_outer_cell`/`delete_outer_cell` (an outer tree's only writers), `copy_page` (do-not-flag 17, 28); `free_dup_tree` and `write::NestedRoots`, the free walks' source of a dup table's nested roots (`txn::WalkRoots`). |
| `tree/cursor.rs` | `TreeCursor` (a detached path, `tree_cursor_stays_small`): `descend` (an edge descent checks every key's range, a key descent only the keys it compared, do-not-flag 29; the deferred `check_leaf` runs before any read past the landing entry), the seeks (from the held leaf only for a key inside it: `start_near`, `find_near`), steps, prefix steps, `place`/`place_after_delete`, `hint`; the public `Cursor`; `KeyTail`; `CellRead` (`Values`, the plain cursors' instance of each `*_in` generic; `Cells`; `KeyJoined`). |
| `tree/dup_cursor.rs` | The dup cursors' walker `DupWalk` (the outer `TreeCursor` through `Cells`, then the key's nested cursor or packed set; `dup_walk_stays_small`), its moves returning the landed `Row`, projected by `Pick` (`PickValue`, `PickTail`); `dup_cursor::NestedRoots`, the cursors' source of a key's nested root (its sources `SnapshotRoots`, `WorkRoots` are in `txn.rs`); `nested_cell`; `DupHint`, `DupWalk::hints`; `place_in_key`, `place_after_nested_delete`, `place_after_outer_delete`. Every outer landing checks the key's width (`check_key`) and maps the cell (`enter`, else `refused_cell`); every nested landing the subkey's length (`set_sub`). |
| `alloc.rs` | `Allocator`: `private`, `stamped`, `pool`, `freed`, consumed records, `next_pgno`, `abort`, and the `epoch` every allocate, free, stamp, `begin_txn` and abort moves (the path cache's validity); the run policy (`Writer::alloc_run`, `first_fit`/`find_fit` and `Fit`, what the search knows of the pool: every push resets it, `merge_into_pool` keeps it, `allocate_run`), `run_privacy`, `PrivatePages` (step 3's units); `reusable` (`sort_pages`, `first_duplicate`), `merge_sorted`. |
| `stamp.rs` | Commit step 3's CRCs on several threads, for `Writer::stamp_private`: `threads`, `crcs`, `Unit::{Page, Run}`, `stamp`, `put_crc`. It must give the serial loop's bytes, order and errors; the threads only read, no `unsafe`; `run_crc` duplicates `crc.rs`'s private 0 to all-ones mapping. |
| `freelist.rs` | Record codec, `next_record`, `decode_pages`, the fixed-point `save` (README "Free list and page reuse"). |
| `catalog.rs` | `DupOptions` (public; `PACKED_DEFAULT`, `resolved`, `resolve`: `InvalidDupOptions`), `TableRecord` (`RECORD_LEN`; `decode` refuses unknown flags, non-zero reserved bytes, `DupRecord::broken_rule`), `usable` (a record held to this build's limits at every use: `catalog::get`, `Env::check`), the catalog ops. |
| `check.rs` | `Env::check`: structure plus exact page accounting; `claim_run` (a run read's checks, full CRC included, plus its zero pad); `Checker::walk` (`Leaves::{Plain, DupOuter}`); `Checker::walk_dup` (width, nested trees walked `Plain`, packed sets validated whole, an inline cell in a table that never packs refused (`NOT_NESTED`), totals against the record); `TableCheck`. |
| `latch.rs` | `Latch` (first cause wins; `fail` vs `fail_damaged`), `UnwindLatch` (a guarded step that ends without a panic of its own calls `disarm`, so an unrelated unwind never latches). |
| `file.rs` | `PageFile`: the reserved mapping (each page slice made from the raw pointer over its own bytes, never a reference to the whole mapping), `grow` (capped by `EnvOptions::max_growth`, at least `MIN_GROWTH`), `trim_to`, `distrust_tail` (open's inherited tail: cut and regrown at the first growth), the syncs (`msync_range`, `msync_page`, `fdatasync_all`, the size sync; every `msync` goes through `msync_flags`; on macOS `msync_ranges`, and `fdatasync_all`'s msync of the mapping: do-not-flag 30), poison; the test-only `dirty_os_pages` (macOS, `mincore`'s modified pages); `ReadOnlyFile`; `advise_access` (do-not-flag 16); `MapView` (the readers' and the writer's, updated in place by a remap). |
| `sys.rs` | `fsync_file`, `fdatasync_file`, `fsync_directory`, `allocate_range`, `flock_exclusive` (a writer's open), `flock_shared` (a read-only open's). |
| `crc.rs` | A copy of `crates/storage/src/archive/crc.rs`; keep the code a copy (its module doc, two `dead_code` allows and `#[inline]` on `crc_is_zero` differ, as the module doc says; a sync keeps all three). |
| `page_set.rs` | A copy of `crates/storage/src/archive/page_set.rs` plus a touched-word list (`clear` and `drain` cost the words inserts touched); keep the rest a copy. |
| `disk_model.rs` | Test-only: the page cache and power-loss model. |
| `tree/test_harness.rs` | Test-only: `MemStore` (one flat buffer, so runs span pages; `assert_accounting`, `counted`, `reader_at` → `ReadView`), the walkers (`walk_tree`, which panics on a nested cell; `walk_outer_tree`, a dup outer tree of nested cells only; `walk_packing_tree`, one that packs: nested or inline cells), the merge-closed check (`merge_open_pages`, `level_keys`), `leaf_fill`/`page_fill`, the seeded `Rng`, `ReportSeed`. |
| `bench.rs` | The `#[ignore]`d benchmark `tn_lmdb_sync_and_merge` (step 4's choice against a forced `fdatasync`, merging on and off) and probe `stamp_probe` (step 3 by thread count). |

## Rules of the crate

- **Never `File::sync_all` or `sync_data`** (`F_FULLFSYNC` on macOS). Use `sys::fsync_file` and
  `sys::fdatasync_file`, a plain fsync on macOS.
- **Grow the file with `sys::allocate_range`** (real blocks), so a full disk is an `io::Error`,
  not a `SIGBUS`, on a file that shares no blocks and has no holes (copy-on-write file systems,
  reflinked copies and sparse copies, whose holes can lie inside the pages in use, can still
  `SIGBUS` on a full disk: README, Platform notes). On macOS the first writes into preallocated
  slack before an `fsync` are slow (README, Platform notes); that cost is accepted: do not grow
  sparse to avoid it. A tail the file had at open is not this process's growth
  (`PageFile::distrust_tail`): never let the writer allocate into it without the cut and regrowth
  at the first growth (which covers the tail only).
- **Every sync wrapper reports to `disk_model` in tests.** `msync` (`msync_range`, `msync_page`)
  calls `flushed_range`; on macOS `msync_ranges` reports through `msync` in its `MS_SYNC` pass,
  and `msync_flags` for `MS_SYNC` only (an `MS_ASYNC` promises nothing); `fsync`/`fdatasync`
  (`fdatasync_all`, the size sync) call `flushed_all`; a truncation (`grow`'s cut, `trim_to`)
  calls `truncated`, which keeps a later msync past the cut off the model's disk until the next
  fsync. A sync path that skips the model makes the crash tests lie.
- **Never panic on bytes from disk.** Page accessors bounds-check and return `Corrupt`. A page
  read from the file passes `page::validate` before its first change, which copy-on-write does.
- **Never rely on zeros from `alloc` or `alloc_run`.** A new page or run keeps its old bytes, so
  a layout writes every byte a reader interprets (`page::init_header` the header and trailer,
  `overflow::encode_run` the whole run). Debug builds fill each with `tree::POISON`, so a
  reliance on zeros fails the tests (README do-not-flag 17).
- **Damage returns `Error::Corrupt` and latches** (`fail_damaged`). I/O, growth and commit errors
  (a refused mapping is `OutOfMemory`, a file out of page numbers `Full`) and panics latch through
  `fail` or `UnwindLatch`; `Error::latches` is the one list, so a new variant is classed there. Validation errors (`KeyTooLarge`,
  `KeyTooShort`, `ValueTooLarge`, `AppendOutOfOrder`, `KeyExists`, table errors, `TableKind`,
  `InvalidDupOptions`, `ReadOnly`, `WrongTxn`) are checked before anything changes and do not latch.
  A broken writer invariant (a split or new root that cannot be laid out, an outer cell past its
  limits after its nested tree is built) is `Error::Failed`, a bug, not damage; it latches.
- **Every change to a table's tree goes through `Work::set_record`,** which sets its write stamp: a
  `TxnCursor` trusts its saved path only while the stamp is the one it saw, so a write that skips
  it leaves detached cursors on stale, perhaps freed, pages. **Every dup write ends in
  `set_record`** (through `Work::set_dup_record`), a nested-only write included: a change to a
  nested root and an entry created or removed with its cell move the stamp; a load, a miss or a
  refusal moves nothing. So does every packed change (create, rewrite in place, row delete, cell
  removal, promotion), even one that leaves the outer root's bytes as they were: a packed key has
  no map entry, so the stamp is a dup cursor's only sign of it
  (`packed_writes_move_the_table_stamp`).
- **A dup table's working map is the truth for its key until commit.** Inside a write transaction
  read an outer cell only through the map (`dup_entry`, `Work::key_rows`, the free walks'
  `write::NestedRoots`, a `DupTxnCursor`'s `WorkRoots`): a cell the transaction wrote may name a
  superseded root (README do-not-flag 18). Never evict an entry before commit. A dup cursor takes a
  key's nested root from the map while the map holds the key, never from the cell.
- **A packed key has no map entry (invariant P).** A packed write rewrites the outer cell at once
  (README do-not-flag 22), so the cell is the truth for a packed key and nothing overlays it. Map
  entries come only from a nested cell, a nested create and a promotion, and leave with their cell;
  a nested key never demotes (do-not-flag 24), so a map hit is always nested. `dup_entry` checks it
  in debug builds (`Corrupt` on a disagreement: only damage), and the free walks' hit count fails a
  drop or clear that meets an entry whose cell is packed.
- **A packed set is checked at its first use, never at copy-on-write.** Reads run
  `PackedView::new` and the rows they touch; every write that decodes a set, the free walks and
  `Env::check` run `packed::validate` before anything changes. Never parse a set in the page
  layer or in `check_cells_naming_this_txn` (README do-not-flag 23, 25).
- **A stale `DupTxnCursor` forgets its paths before it descends.** Its seek on a changed table
  (`seek_call`) and its reseek (`refresh`) unset the walker first: the walker's same-key paths
  trust the held nested root and leaves, which the change may have freed. A new move that can
  run on a stale cursor does the same.
- **Open never writes to an existing file.** Fail closed: never reinitialise a non-empty file,
  never fall back past a short file, never repair a meta. `open_read_only` never writes, syncs
  (not even step 7's fsync) or creates.
- **Never reuse a page past the gate,** `min(valid slot txns, oldest reader)`. Only the writer
  changes pages, and only private ones. Copy anything published.
- **Tests:** default tests stay small, because CI runs nextest in debug. Heavy variants go under
  `#[ignore = "heavy: run with --release"]` next to their small twin, with a `_heavy` suffix. A
  measurement with guards that is too slow for the default suite goes under
  `#[ignore = "measurement: ..."]` (`packed_sets_file_size_by_bound`): the heavy command runs it
  too. Only the benchmark and the stamp probe, which guard nothing, are skipped there.
- **Mutation-check every new test.** Break the code it guards, watch it fail, restore. A broken
  cursor step can loop forever, so mutate with a single-test filter, not the whole suite.
- **Comments and docs use durable anchors only:** function, test and type names, real issue
  numbers. Never review ids, agent or task ids, or "the plan".
- **Every public item has docs and every public type is `Debug`** (`missing_docs` is on). A
  dev-dependency the lib does not use needs `#[cfg(test)] use x as _;` in `lib.rs`.
- **A helper on a read path the adapter compiles needs `#[inline]`.** The adapter instantiates
  the generic and `#[inline]` read paths in its own crate (no LTO), so a non-generic helper
  without `#[inline]` is a cross-crate call on every row. Mark it `#[inline]`, or
  `#[inline(always)]` once it has many callers and LLVM stops inlining it (`leaf_cell`,
  `LeafView::entry` and `entry_with_key_tail` each left the adapter's
  `RoCursor::current_with_key_tail` when they gained callers). A new caller of a hot helper can
  push it out of line: compare the read rows after adding one. Mark a call chain whole: a helper
  inlined into the adapter turns each non-`#[inline]` function it calls into a cross-crate call
  (marking only `Snapshot::child` made each fetch call `MapView::slice`, `crc_is_zero`, `pgno`
  and `txn` out of line), and keep its error formatting in a `#[cold]` helper so the inlined
  code stays small. A small function that calls nothing (`get_u16`, `slot`) inlines across
  crates on its own; the rule matters for one that calls others. To see what crosses, read the
  release asm of a downstream crate (the bench crate instantiates the read paths as the adapter
  does): `cargo build --release -p tn-lmdb-bench`, then
  `objdump -d -C --no-show-raw-insn target/release/tn-lmdb-bench` and, in a hot function's body
  (a read row's loop, a cursor move), a `call` (x86) or `bl` (arm64) to a non-generic
  `tn_lmdb::` function is a helper that crossed.
- **The reth adapter relies on the cursors' documented states**, the dup cursors' too (each equal
  to a plain cursor's on the composite twin). A change to what a cursor does after a delete,
  after `last`, after a refused write or on a reseek, or to a public signature, also runs
  `crates/tn-lmdb-reth`'s checks (its `CLAUDE.md`): its differential test is the oracle for the
  cursor behaviour reth sees.
- **Test-only knobs, reports and hooks are `#[cfg(test)]`** fields and functions; production
  never depends on them, and one on a timed path costs at most a branch on a field.
  - Knobs on the writer: `CrashAt`, `data_by_fdatasync`, `data_by_msync`, `record_pages`,
    `stamp_threads` (1 is the serial loop), `no_path_cache` (empties the path cache before each
    put and delete and drops a delete's slot: a `TxnCursor` finds its place by key),
    `no_write_hint` (every cursor write passes `write::NO_HINT`/`NO_DUP_HINT`); on `MemStore` or
    `Writer`: `PageWrite::no_merge`, `PageWrite::no_fused_copy` (a leaf's copy, then its change);
    `Allocator::fail_allocs_after`, `Allocator::log_pages` (fills `load_log` and `free_log`; off
    at every open, so the release benches never grow them), `Allocator::reference_save` (the save
    through `reusable_reference` plus a sort); `PageFile::fail_next_msync`,
    `PageFile::fail_next_fsync` (the size sync's or `fdatasync_all`'s),
    `PageFile::trust_tail` (the whole file as this process's growth, undoing `distrust_tail`),
    `PageFile::open_with_max_growth`; `Env::crash` (`EnvInner::crash_on_drop`: drop as a crash
    would); `Writer::clock_at` (the time `Writer::sync_due`'s period check reads, so a test
    reaches `EnvOptions::sync_period` without waiting).
  - Reports: `max_save_rounds`, `data_by_file` (on macOS also set by step 4's
    `DataSync::Ranges`), `stamped_on`, the allocator's `load_log`, `free_log` and `run_stats`,
    `PageSet::visited`, `PageFile::dirty_os_pages` (macOS), `file::take_advice_log` (the advice
    each mapping got), `sys::take_dir_sync_log` (the directories `fsync_directory` synced); plus
    test accessors (the `Allocator`'s `freed`, `pool`, `take_freed`, `add_to_pool`).
  - Hooks: `RwTxn::forget_nested`, `RwTxn::plant_nested` (a working entry no nested cell
    names), `PageWrite::merges_applied` (the `MergeReport` of a delete that merged or refused a
    sibling; `MemStore.merge_report` keeps the last, reset by a test before the delete it reads;
    `Writer.merging_deletes`); `TxnCursor::reseeks`; `DupTxnCursor::reseeks`, `placements`,
    `landings`.
  - Counters (`COW_VERIFIES`, `CACHED_PUTS`, `CACHED_DELETES`, `RANGE_CHECKS`, `HINTED_LEVELS`,
    `HINT_REFUSALS`, `WRITE_SEARCHES` in `tree/write.rs`; `CATALOG_RESOLVES`, `OUTER_FINDS` in
    `txn.rs`; `ROOT_DESCENTS` in `tree/cursor.rs`) exist only under
    `#[cfg(all(test, debug_assertions))]`: the release benchmark compiles with the test
    configuration, so a counter on a timed path would show in its numbers. A test that reads one
    is gated the same way or accepts zero in release.

## Checks before handing back

```text
cargo nextest run -p tn-lmdb
cargo +nightly-2026-03-20 clippy -p tn-lmdb --all-targets -- -D warnings
cargo +nightly-2026-03-20 clippy -p tn-lmdb --all-targets --all-features -- -D warnings
cargo +nightly-2026-03-20 fmt --all && cargo +nightly-2026-03-20 fmt --all -- --check
RUSTDOCFLAGS="-D warnings" cargo doc -p tn-lmdb --no-deps
# the heavy variants and the guarded measurement, about 10 min:
cargo test --release -p tn-lmdb -- --ignored --skip tn_lmdb_sync_and_merge --skip stamp_probe
./etc/ci-lanes.sh clippy   # the workspace lane, as CI runs it
```

A change to the public API or to cursor behaviour also runs `crates/tn-lmdb-reth`'s checks.

**A change to a read, write or commit path runs the regression check** before handing back:
`etc/tn-lmdb-bench-compare.sh --tier quick --worktree HEAD` (the working tree against HEAD, four
rounds, about 2 minutes; `--rows` narrows it, exit 1 is a gated regression;
`crates/tn-lmdb-bench/README.md` has the rows). A perf change states its rows' ratios from it, and
from `--tier default` before a commit. A change to `begin_ro`, a read transaction's drop or
`Snapshot`'s layout states `ro.txn` and `ro.txn.mt` from the default tier.

The benchmark runs on demand:
`cargo test --release -p tn-lmdb tn_lmdb_sync_and_merge -- --ignored --nocapture --test-threads 1`.
Set `TN_LMDB_BENCH_N` for a quick run. The stamp probe runs the same way, by its name
(`stamp_probe`). If you set `TMPDIR`, scope it to the one command, and never point it at a
tmpfs. The comparison benchmark of this engine runs from the adapter crate
(`crates/tn-lmdb-reth/src/engine_bench.rs`, results in its `engine-bench.md`), on the public API
only.

## Where tests live

Whole-engine suites are in `src/tests/` (written `tests/` below); a module's own tests are in
its `mod tests` or `<module>/tests.rs`. "Neighbours": the tests next to the one named.

| kind | file | key tests |
|---|---|---|
| page codec, CRC, one page's damage | `page.rs` | proptest models (`views_match_an_independent_model`, `inserts_at_every_index_match_a_model`, `damaged_valid_pages_never_panic`; nested cells in their generators); `the_page_crc_covers_the_page_outside_its_gap`; `nested_cells_are_canonical_roots`; `level_prefetch_stays_inside_the_page_from_empty_to_full`; `splits_match_the_reference_split`; `copy_page_copies_every_byte_outside_the_free_gap` and its neighbour; `split_and_root_invariants_fail_as_writer_bugs` |
| tree shape and copy-on-write against a `BTreeMap`, on `MemStore` | `tree/write.rs`, `tree/read.rs` | `fused_copies_match_the_copy_then_the_change` (`_heavy`), `copy_on_write_is_the_two_step_copy` and neighbours; `a_flip_the_page_crc_covers_fails_the_copy_on_write_verify`; path cache: `an_append_split_caches_the_path_a_descent_finds`, `a_path_cache_of_another_epoch_is_never_used`, `a_cached_delete_leaves_a_leafs_last_entry_to_the_descent`; cells: `a_nested_cell_in_a_private_leaf_is_corrupt_at_every_write`, `outer_trees_of_nested_cells_match_a_model`, `outer_trees_of_packing_cells_match_a_model`, `a_published_outer_cell_naming_this_txn_is_refused_at_copy`; hints: `a_hinted_descent_matches_the_search` (`_heavy`), `a_hinted_write_meets_damage_as_a_descent_does`; `an_entry_count_past_a_trees_bounds_is_corrupt` (a refused put leaves every page as it was), `emptying_a_single_child_root_is_corrupt` |
| merging on delete | `tree/write.rs`; `tests/damage_merge.rs`, `tests/merge.rs`, `tests/multi_model.rs` | `random_batches_with_merging_match_a_btreemap_model` (merge-closed after every delete), `deleting_90_percent_leaves_no_page_merge_open`, `a_merge_refuses_a_damaged_leaf_sibling` and `_branch_sibling`, `an_unlink_or_a_collapse_checks_the_range_its_neighbour_gains`; `deletes_that_merge_commit_and_reopen_to_the_model`, the crash matrix on a merging commit; `a_draining_multi_table_workload_with_crashes_matches_the_model` |
| cursor moves, seeks, prefix steps | `tree/cursor/tests.rs` | `run_moves` (`Op`, `Moves::apply`, `apply_owned`, `RwModel`); `a_reset_cursor_moves_as_a_fresh_one`; `seeks_of_every_key_and_gap_match_a_model` and `seeks_from_random_positions_match_a_fresh_seek` (both `_heavy`); by `ROOT_DESCENTS`: `a_seek_near_the_cursor_starts_from_its_leaf` and neighbours; damage: `the_descent_names_the_first_check_a_page_fails`, `a_seek_from_the_leaf_meets_damage_as_a_descent_does`; the narrowed seek check (fixture `a_partly_misrouted_leaf`): `a_seek_lands_in_a_leaf_partly_outside_its_range_and_reading_on_refuses_it` and neighbours; `tree_cursor_stays_small`, `txn_cursor_stays_small` |
| step 3's parallel stamp | `stamp.rs`, `env/tests.rs` | `crcs_stamp_the_bytes_of_the_serial_loop`, `a_zero_crc_payload_gets_the_serial_loops_sentinel`, `threads_grow_with_the_bytes_to_the_cap`; `a_parallel_stamp_writes_the_serial_stamps_bytes`, `a_commit_past_the_stamp_threshold_writes_the_serial_stamps_bytes` |
| free list, reuse timing, accounting, runs | `freelist/tests.rs` | `the_save_writes_the_records_the_reference_writes` (`Allocator::reference_save`), `a_spill_below_the_freed_pages_is_merged_into_them`, `the_page_logs_stay_empty_until_a_test_turns_them_on`, `a_run_on_a_scattered_free_list_costs_what_it_loads` |
| open, commit, NoSync, latch, growth, close | `env/tests.rs` (helpers `pub(crate)`), `file.rs` | NoSync's automatic syncs: `a_nosync_commit_reaching_sync_bytes_commits_durably`, `a_nosync_commit_past_sync_period_commits_durably` (`Writer::clock_at`), `nosync_churn_with_sync_bytes_keeps_the_file_bounded`; `max_growth_under_the_minimum_is_refused_at_open`, `a_growth_cap_bounds_the_tail_past_the_pages`, `growth_steps_stop_doubling_at_the_cap`; the trim: `a_clean_close_trims_the_file_to_its_pages` and neighbours, `a_failed_final_sync_skips_the_trim`; the inherited tail: `an_inherited_tail_is_cut_and_regrown_with_real_blocks`, and its crash matrix in `tests/crash_runs.rs`; `a_flip_the_page_crc_covers_fails_open_and_check`; `a_version_4_file_is_refused_at_open`; `oldest_reader_age_follows_the_oldest_superseded_reader`; table roots: `a_snapshot_resolves_each_handle_once` and neighbours; `every_mapping_follows_the_readahead_option`, `every_mapping_carries_the_random_read_flag_on_linux`; macOS's msyncs: `scattered_pages_msync_their_merged_ranges`, and on macOS by `dirty_os_pages`: `a_scattered_durable_commit_leaves_no_page_dirty`, `a_writer_open_writes_the_pages_another_mapping_left_dirty`, `fdatasync_all_writes_every_page_dirtied_through_the_mapping` |
| the I/O counters (`Env::io_stats`) | `env/io_stats_tests.rs`, `stats.rs` | commits, copies and checks, NoSync and `Env::sync`, the gate's lag, runs by policy step (`RunStats`), a read-only open; `IoStats::since` |
| whole engine: crashes, damage, the multi-table model, concurrency, path cache, append | `tests/{crash,damage,multi_model,concurrency,path_cache,append}.rs` | `every_crash_point_recovers_the_state_the_protocol_allows` (the template, below); `matrix_of` in a `Mode` (`AutoSync`: a NoSync crash commit past `sync_period`, `every_crash_point_of_an_automatic_sync_recovers_the_state_the_protocol_allows`); `damage.rs`'s `sweep`; `slot_rewrites_over_sorted_keys_delete_in_the_cached_leaf`; `hinted_writes_leave_the_bytes_of_unhinted_ones` (`_heavy`) |
| overflow runs in the whole engine | `tests/crash_runs.rs`, `tests/damage_runs.rs` | `crash.rs`'s `matrix` on another `Base` (`runs_base`; in `Mode::AutoSync` too, a crash commit that does not grow the file, so no size sync covers the deferred pages: `every_crash_point_of_an_automatic_sync_recovers_a_workload_of_overflow_runs`), and on the same commit cutting the inherited tail (`cutting_base`: `every_crash_point_of_a_commit_that_cuts_the_inherited_tail_recovers`, `_heavy`); `damage.rs`'s `sweep` on runs; `a_cursor_delete_reads_the_key_alone` |
| key tails | `tests/key_tail.rs`, `page.rs` | `current_with_key_tail` against `current`; `seek_with_key_tail_is_a_seek_then_current_with_key_tail`, `seek_with_key_tail_clears_a_txn_cursors_end_and_deleted_states`; `key_tail_joins_the_keys_last_bytes_to_an_inline_value` |
| detached write cursors (`TxnCursor`) | `tests/txn_cursor.rs` | `txn_cursors_and_direct_writes_match_a_model` (rounds with the hint on and off), `follower_shares_prefix_answers_for_the_deleted_entrys_follower`, `next_after_last_finds_entries_put_past_it`, `a_failed_delete_through_a_write_cursor_leaves_it_unset`; debug-gated: `a_write_at_the_cursor_routes_by_its_path`, `a_stale_or_unplaced_cursor_passes_no_hint` |
| the read-only open | `tests/read_only.rs` | byte-identical file and mtime, 0444, no sync, the shared lock, `ReadOnly` without a latch, never creating; `sweep_read_only` |
| nested-cell damage | `tests/nested_damage.rs` | `a_forged_nested_cell_is_corrupt_and_latches_at_every_use`, `a_dup_record_over_inline_cells_is_corrupt_at_first_use` |
| dup tables | `tests/dup.rs` (`check_dup`, `damaged`; `Bound`, `PACKED_SMALL`) | Oracle 1 `dup_tables_match_a_composite_model` (`_heavy`) at each `Bound` (`Unpacked`, `Small`, `Default`); `every_bound_resolves_to_its_limit`; kinds, options, key limits, the working map, the stamp, totals (`a_dup_write_its_totals_cannot_count_changes_nothing`), `delete_prefix`, drop, clear, forged damage |
| the packed-set codec | `packed.rs` | `packed_sets_built_by_random_edits_match_a_model` (`_heavy`), `the_lazy_checks_refuse_what_they_touch`, `packed_codec_limits` |
| packed sets through the engine | `tests/dup_packed.rs` | `packed_dup_options_resolve`, `a_set_at_the_bound_stays_packed_and_one_byte_more_promotes`, `packed_writes_move_the_table_stamp`, `a_free_walk_refuses_packed_totals_that_miss_its_sets`, `a_free_walk_refuses_a_working_entry_no_nested_cell_names`, `check_counts_packed_sets_and_refuses_their_damage`; ignored: `packed_sets_file_size_by_bound` |
| dup read cursors | `tests/dup_cursor.rs` (`twins`, `Shape`, `Coverage`, `forms`, `expected`) | Oracle 2a `dup_cursor_moves_match_the_composite_twin` (`_unpacked`, `_packed_small`, `_heavy`); `dup_seeks_from_random_positions_match_a_fresh_seek` (the same variants); `bounded_seeks_never_read_another_key`; `dup_walk_stays_small` in `tree/dup_cursor.rs` |
| dup tail moves | `tests/dup_key_tail.rs` | `dup_tail_moves_match_the_move_then_current_with_key_tail` (`_heavy`), `dup_txn_tail_moves_match_the_move_then_current_with_key_tail` (`_heavy`), `tail_moves_meet_damage_as_their_plain_moves` |
| a write transaction's dup cursors | `tests/dup_txn_cursor.rs` (`twins`, `PACKING`, `pair`, `wipe`, `fresh_seek`) | Oracle 2b `dup_txn_cursors_and_direct_writes_match_the_composite_twin` (`_unpacked`, `_packed_small`, `_heavy`); `damage_under_a_dup_cursor_is_corrupt`, `packed_damage_under_a_dup_cursor_is_corrupt`; `a_txn_cursor_follows_its_packed_key_rewritten_promoted_and_emptied`, `another_writers_packed_rewrite_reseeks_the_cursor`; `a_dup_write_at_the_cursor_routes_both_levels`, `a_held_outer_leaf_never_overrides_the_map`, `a_held_outer_leaf_is_searched_only_once_checked_in_full` |
| dup tables under sweeps, crashes, the model | `tests/dup_sweep.rs`, `tests/crash.rs`, `tests/multi_model.rs` | `damage_sweep_on_a_dup_table`, `read_only_sweep_on_a_dup_table` (`rewrite_through_cursors`); `every_crash_point_recovers_a_dup_table`; `a_multi_table_workload_with_dup_tables_and_crashes_matches_the_model` |
| packed-set damage | `tests/packed_damage.rs` | `damage_sweep_on_a_packed_dup_table`, `read_only_sweep_on_a_packed_dup_table` (`sweep_dup_writing`; `never_packs`: "holds no nested tree"), `packed_damage_refuses_drop_and_clear_before_any_free` |
| module codecs | each module's `mod tests` | meta: `a_version_4_record_is_refused` and neighbours; catalog: `table_record_encoding`, `dup_records_round_trip_and_refuse_damage`, `dup_records_are_usable_within_this_builds_limits`; alloc: `reusable_matches_the_reference`, `sort_pages_matches_a_comparison_sort`, `merge_sorted_matches_a_sort`, `the_fit_knowledge_keeps_every_allocation`; `page_set.rs`: `clear_and_drain_cost_what_was_inserted`; sys: `retry_interrupted_reissues_only_an_interrupted_call`; overflow, file, latch, crc |

## Adding a crash test

1. **Track the file.** Call `disk_model::track(&path)` once the file exists. From then on its bytes
   are "on disk", and only synced writes reach that image.
2. **Arm a crash point.** Set `CrashAt` on the writer before the commit or sync:
   - inside a transaction, `txn.writer_mut().crash_at = Some(at)`;
   - before `Env::sync`, `env.with_writer(|w| w.crash_at = Some(at))`.

   `CrashAt` names a point of the commit protocol: `BeforeSave`, `AfterSave`, `AfterStamp`,
   `AfterDataSync`, `AfterSizeSync`, `AfterMetaCopy`, `AfterMetaMsync`. The commit stops there with
   an error, as a process death would: no further I/O, and the environment latches. A NoSync
   commit stops only at the first three, but one past `sync_period` or `sync_bytes` at every
   point, as a Durable commit does (`tests/crash.rs`'s `Mode::AutoSync`). Step 4 takes
   one `msync` or one `fdatasync` (`data_sync`; on macOS the `msync`s of `DataSync::Ranges` in
   place of the `fdatasync`); to run a crash point on the path a commit would not take, set
   `data_by_msync` on the writer (as `tests/crash.rs`'s `run_case` does; it gets the `fdatasync`
   path, on macOS the `Ranges` one, from a crash commit whose pages form several ranges) or
   `data_by_fdatasync`.
3. **Drop the environment as a crash would.** Call `env.crash()`, which skips the close: the
   NoSync final sync and the trim of the preallocated tail (hold no other handle or transaction).
   A base that needs its slack for the crash commit closes this way too (`crash_runs.rs`'s
   `runs_base`), and the case's writer takes that slack as its own after the reopen
   (`PageFile::trust_tail`, as `run_case` does for a base that does not grow): a reopened writer
   otherwise cuts it at its first growth. A base that keeps its slack and `grows` runs the cut
   instead (`crash_runs.rs`'s `cutting_base`).
4. **Choose the loss:**
   - nothing, for a process crash (the page cache survives);
   - `disk_model::power_loss(&path)` for a strict power loss (only synced bytes and the durable
     length survive);
   - `disk_model::power_loss_random(&path, seed)` for random early writeback: each unsynced OS
     page is kept old or new, torn per 512-byte sector.
5. **Reopen and assert** the exact allowed state: L before the meta's msync, M after it, and M
   exactly when the slot holds M's intact record in between. Then assert that `Env::check`
   passes. Call `disk_model::untrack(&path)` at the end.

`tests/crash.rs` (`every_crash_point_recovers_the_state_the_protocol_allows`) is the template.
`tests/crash_runs.rs` and `tests/merge.rs` run its `matrix` on other workloads (a `Base` names
its `unsynced` and `crash` changes, whether the crash commit `grows` the file, and a `layout`
check its dry run asserts, such as `crash_runs.rs`'s `last_run_ends_the_file`). A commit that
grows the file ends step 5 with an `fsync`, which writes every page: only a commit that does not
grow shows what one `msync` covers. `tests/multi_model.rs` does the same under a random workload.
