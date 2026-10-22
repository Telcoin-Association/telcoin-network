# tndb-b: a guide for working in this crate

`tndb-b` is an embedded LMDB/MDBX-style key-value engine. It has one page file, copy-on-write
B+trees with the values in their leaves (a value too large for its leaf goes to an overflow run of
contiguous pages, up to 16 MiB), snapshot readers, a single writer, two meta pages, and a
persisted free list. There is no log: the meta pages, the gated page reuse and the free list carry
all crash safety. It depends on nothing in the workspace. Its primitives are copied from
`tn-storage`, so never add a `tn-storage` dependency, nor a reth one: reth's database API lives
in the adapter crate `crates/tndb-b-reth`, which builds on this crate's public API (its own
`CLAUDE.md` covers it).

**Read in this order:**
1. this file;
2. `README.md`, the section for your topic: on-disk format (overflow runs included), deletes
   and merging, commit protocol, free list and page reuse, readers, open and recovery, failure latch, concurrency,
   and the "please do not flag" list;
3. the module docs at the top of the file you change.

No file is too big to read whole, though `tree/write.rs` (4.8k lines) and `page.rs` (3.5k) are
about half tests. Grep for the symbol and read with an offset.

## Crate map

| file | role |
|---|---|
| `lib.rs` | crate docs, `compile_error!` off unix, the re-exports (the whole public API), the `use x as _;` lines for dev-dependencies |
| `error.rs` | `Error` (`thiserror`) and `Result` |
| `env.rs` | `Env`: open and recovery (steps 1–9; `choose_state` is steps 4–6 and `checked_snapshot` step 9, shared by `open_read_only`, which locks shared through `open_shared`, never creates, skips step 7's fsync, maps through `ReadOnlyFile`, and keeps the writer slot empty: `EnvInner::writable` gives `ReadOnly` to `begin_rw`/`try_begin_rw`, and `sync` returns `Ok`), the writer hand-off (`Mutex` + `Condvar`), snapshots (`Snapshot::page_again`: an owned cursor's unchecked re-fetch of a leaf it entered with every check, README do-not-flag 14; `Snapshot::tables`, the `TableRoots` its readers resolved, built empty by `Snapshot::new` at both build sites, `Writer::publish` and `checked_snapshot`), the writer's `view` (a clone of the file's one `MapView`, which `remap` updates in place) and its `PageWrite::copy_published` (copy-on-write in one copy: the source through `view.slice` and `checked_page`, the checks `Writer::page` makes, the destination through `file.page_mut`; a private source refused, `private_source`; a page past the published bytes takes the cold `copy_unpublished`, the two-step copy, so its checks and messages stay `page()`'s), the reader queue and `reuse_gate`, `make_durable` (commit steps 4–6; `data_sync` picks one msync when the dirty pages form one range, else one fdatasync), `sync`, `free_pages` (the free list's page count through `freelist::next_record`), the test-only `CrashAt` hook |
| `txn.rs` | `RoTxn`, `RwTxn` (`Work::commit` = steps 1–3 and 7), `ReadTxn`, `Table` resolution (`StaleTable`; a `RoTxn`'s through its snapshot's `TableRoots`, so a `RoTxn` is `{inner, snapshot}`, two `Arc`s, a compile-time assert: `RoTxn::root`, `#[inline]`, checks the handle's home slot `id % 32`, and `resolve_root`, `#[inline(never)]`, probes on and then reads the catalog, remembering a resolved root in the first free slot; with every slot taken a handle resolves uncached; the test-only `CATALOG_RESOLVES` counts the catalog reads; README do-not-flag 15), `RoCursor` (`RoTxn::cursor_owned` on an `Arc<RoTxn>`: owns the transaction, a detached `TreeCursor`, steps from its leaf re-fetched by `Snapshot::page_again`, as does `current_with_key_tail`, `#[inline]` so the adapter's re-read costs no call; `cursor_owned`, `seek`, `seek_exact` and `seek_with_key_tail`, a seek and `current_with_key_tail` from one leaf access, are `#[inline]` too, as the adapter calls them across the crate boundary), `RwCursor` (after `delete_current` it is on the deleted entry's follower with `deleted` set, as MDBX's cursor: `next` and `current` return the follower and clear the flag, a second delete deletes it; it keeps the deleted key, `deleted_key`, for the prefix steps; `prefix_step` and `find_again` are shared with `TxnCursor`); `TxnCursor` (`RwTxn::cursor_detached`: no borrow, each call takes the transaction; staleness from the per-table write stamp `Working::writes`, set by `Work::set_record` from `Work::stamp`, the schema counter `Work::schema` moved by create and drop, and the process-unique `Work::serial`, `WrongTxn` on a mismatch; `up_to_date` is the inline check of every call; `seek_with_key_tail` clears `deleted` and `soft_end` and saves the landed key, as `seek` does; `sync_slow`/`refresh` reseek the saved `key` (the entry it is on: after a delete, the follower's), a miss putting it on the follower in the deleted state with the gone key in `deleted_key`; `soft_end`, MDBX's end after `last` (or `prev` from unset): `next` and `next_in_prefix` return `None` and keep it, every other move and write clears it, a reseek hit keeps it and a miss clears it; its own writes record the stamp they left, so no reseek; test-only `reseeks`); `RwTxn::insert`/`RwCursor::insert` are `PutKind::Insert`; each `Working` table record holds its `write::PathCache`, used by `RwTxn::put` and `RwTxn::append` (`put_entry`), which also places an `RwCursor` from the cached path after a put that did not split or did an append split (which refills the cache by a second, trusted descent of the new path; any other split re-seeks) |
| `meta.rs` | meta record codec, `choose_meta`, `slot_to_write` |
| `page.rs` | slotted page codec, `Geometry` (limits: `check_entry` holds the catalog and the free list to the inline limits; `place` sends a table value inline or to an overflow run, `Place`; `max_table_value` is the table limit, 16 MiB for a key up to `max_key − 10`), `validate` (an overflow cell is canonical: flags 1, a 12-byte value, a length past the inline limit), `Val` (a leaf value as its cell holds it: inline bytes or an overflow reference; `Val::inline` refuses a reference in a tree of inline values), `CellVal` (what the leaf mutators write: generic over `&[u8]`, the hot inline path, and `Val`, so an inline put never matches on the overflow form), splits through the writer's reused `SplitBuf` (plan from a copy of the page, then `write_right` into the zeroed new page and `write_left` over the old one, each half laid out by `place_cells`; one page borrowed at a time; a leaf's append split leaves the left page untouched; a delete's merge reuses the same buffers: `SplitBuf::merge` plans the merged page from both siblings, a branch pair taking the parent's key for the right page as that page's cell-0 key, and `write_merged` lays it out over the survivor, sized by `View::used` (cells plus slots); `init_leaf_zeroed`, `init_root` and `write_right` skip the zero-fill of a page `alloc` returned zeroed, yet write every header, trailer, cell and slot byte themselves, so only the free gap relies on the zeros), the one-pass searches (`cmp_keys`, `leaf_find`, `branch_route`; each probe prefetches the two cells the next probe may read, `prefetch_next_probes`, through `prefetch`'s `asm!` hint: one of the crate's five `unsafe` places, listed in the README's do-not-flag item 2), the parsed views (`View`, `LeafView`, `BranchView`: one header check, many cell reads; `LeafView::entry_with_key_tail`, behind `leaf_entry_with_key_tail`, gives an inline value with the key's last `t` bytes before it as one slice of the cell, `t` clamped to the key's length), `LeafMut` (a leaf put's read and change behind one header parse); the hot accessors (`hdr`, `hdr_of`, the cell reads, and `nkeys`, `upper` and `valid_len`, so `hdr` inlines whole into the adapter's code) are `#[inline]`, and `leaf_cell`, `LeafView::entry` and `LeafView::entry_with_key_tail`, which every leaf read runs and which have many callers, are `#[inline(always)]` (see the inline rule below), with their errors built out of line by the `#[cold]` constructors `hdr_error`, `kind_error`, `cell_error`, `leaf_cell_error`, `branch_cell_error` and `reserve_error` (`hdr_error` re-runs the header checks in their original order, so every message is unchanged) |
| `overflow.rs` | overflow runs: `Ovf` (the 12-byte cell reference: `ovf_pgno`, `ovf_txn`, `len`), `MAX_VALUE` (16 MiB), the run codec (`run_pages`, `encode_run`, `value`, `pad_is_zero`, `run_txn`), `checked_run` (every check a run read makes, shared by every `PageRead` store's `run`) and `RunCrc` (`Skip` for the walks that free a run or check its txn, `MultiPage` for a value read: the full CRC only past one page, `Always` for `Env::check`) |
| `tree/mod.rs` | `TreeRoot` (the 32-byte handle with its page counters; `add_run`/`remove_run` count a run's pages), the `PageRead` store (`page`, `child`, `run`, and the provided `overflow` and `value` that every value read goes through) and the `PageWrite` store (`alloc`, `free`, `page_mut`, `copy_published` (copy-on-write in one copy, the source fetched with `page`'s checks, a private source refused), and for runs `alloc_run`, `run_mut`, `free_run`, `run_privacy`) |
| `tree/read.rs` | `checked_page` (the per-fetch checks), `checked_child` (those plus the identity check `txn as u32 == child_txn`, one header read; behind `PageRead::child`, which every descent calls), the readers' `descend`, `get`, `check_miss` (a miss's keys either side lie inside the leaf's separators; shared with a delete's miss) |
| `tree/write.rs` | copy-on-write `put`/`delete` (plan, reserve, apply; a write that copies or splits takes the writer's reused `Scratch` through `PageWrite::take_scratch`, so it allocates no heap memory once those buffers have grown; `writable` copies a published page with one `PageWrite::copy_published`, then `set_pgno_txn`, `zero_crc` and `free`, in that order, and `copy_page` holds the length check), splits, unlink and collapse, `free_subtree`, `clear`; merging on delete (`plan_delete` decides it read-only: `plan_merges` has the page the change left, while not the root and under a quarter of `U`, absorb same-parent siblings that fit, left ones first, cascading up and letting a root with one child give way; `sibling_view` makes a descent's cheap checks of every neighbour read, before its fit test; `check_sibling` the rest on the one absorbed: `verify_published`, `check_cells_naming_this_txn`, the separator range, and for a branch `check_sibling_chain`, its child-0 chain descended to the leaves with a descent's checks (a single-child branch's keys say nothing, and `sibling_view` does not tell levels apart), plus the branch seam check; `handle_after` takes the absorbed pages and a dropped root off the handle, `Merged`; `apply_merges` moves the sibling's cells into the page on the path, which survives, and frees the sibling as it is, so a merge allocates nothing); `descend_write` (the writer's descent: provenance trust skips the per-fetch and range checks on pages this txn built, lazy bounds via `path_bounds`, the fixed `Path`; `Verify::Now` verifies each published page as it goes, a put's, and `Verify::Later` leaves that to `verify_path`, a delete's, which descends once and runs it only on a hit, so a miss verifies nothing and returns after `read::check_miss`), `check_cells_naming_this_txn` (the copy-time `child_txn` rule, which also covers a copied leaf's overflow cells); overflow puts (`put_from_root` asks `Geometry::place`; `reserve_put` allocates the tree pages, then the run, which `write_run` writes in full before any leaf names it; the old run is freed after the change applies; `leaving_run` checks a run leaving the tree, header, identity and provenance, without its CRC); `tree_pages` (a tree's pages and runs collected before a free, `root_trusted` the handle's provenance: a private page under a page this txn did not write, or under an untrusted handle, is `Corrupt`, and a private child gets the identity check; only the first leaf and the private ones are read when `overflow_pages` is 0, else every leaf, and the run pages must add up to the counter); `PathCache` and `put_cached` (a put into the cached leaf with no descent while the epoch holds; a put that descends records its path straight into the cache's buffer (`PathCache::lend_path`, which empties the cache and lends the rest as a `CacheTail` that the put fills: from its own descent if it did not split, after an append split from a second, trusted descent of the new path); `PutKind::Append` refuses a key not past the last with `AppendOutOfOrder`; `PutKind::Insert` refuses a present key with `KeyExists`, in the cached leaf (the cache kept) or in `plan` (the cache left empty by `lend_path`); `Placed` reports the entry's slot when no split happened; a put into a run, or a replace of one, bypasses the cache and descends) |
| `tree/cursor.rs` | `TreeCursor` (a detached path, page numbers and `u16` slots in two arrays, `pages: [u32; MAX_HEIGHT]` and `slots: [u16; MAX_HEIGHT]`: 6 bytes a level and no padding, kept small because every new cursor zeroes and moves it, `tree_cursor_stays_small`, at most 6·MAX_HEIGHT + 16; `descend` parses each page's header once, `View::new`, after the fetch, identity and kind checks, and runs the range check, the empty-page checks and the search through that view, returning the leaf's view, `Reached` (`Edge::Exact`, `seek_exact`'s, returns the search's `Find::Found` value); `land` reads through that view, and a crossing is `climb` + `land_beyond`, the view never repacked in an `Option` or `Result` (that copy stalled); `seek_with_key_tail`, `start_with` with a `LeafView::entry_with_key_tail` read, for `RoCursor` and `TxnCursor`; `view_entry` and `push` are `#[inline(always)]`; `place` sets it from a write's path; the prefix steps `step_in_prefix`, from the leaf the caller holds, and `step_if`, both through `move_if`, which moves a copy and keeps it only on a match; `step_from` and `leaf_pgno` for `RoCursor`; `current_with_key_tail`, the leaf passed in when the caller holds it) and the public `Cursor` (leaf cache); `KeyTail` (public: what every cursor's `current_with_key_tail` returns, `Joined`, an inline value with the key's tail before it in one slice of the page, the adapter's zero-copy dup read, or `Apart`, an overflow value alone) |
| `alloc.rs` | `Allocator`: `private`, `stamped`, `pool`, `freed`, consumed records, `next_pgno`, `abort`, and the `epoch` that every allocate, free, stamp, `begin_txn` and abort moves (the path cache's validity); overflow runs: the run policy (`Writer::alloc_run`: `first_fit` in the pool, the lowest `n` consecutive pages; else load up to `RUN_LOAD_RECORDS` more gated free records and fit again; else tail reuse, pool pages ending the file plus growth; else `n` pages at the file end in one growth; steps 1, 3 and 4 are `allocate_run`), the test-only `RunStats`, `private_runs` (run pages are in `private` too), `free_run` (`n` single pages), `run_privacy` (a private run, a published one, or an overlap: damage), and `stamp_private`'s `PrivatePages` (`units`: one CRC per tree page and one per run; run pages never enter `stamped`) |
| `freelist.rs` | record codec, `next_record`, `decode_pages`, the fixed-point `save` (the pool's pages go back under the keys of the records taken, reusable at once; the freed pages under the committing txn) |
| `catalog.rs` | `TableRecord` and catalog ops |
| `check.rs` | `Env::check`: structure plus exact page accounting; each root's entry count and page counters (branch, leaf, overflow) against its walk; `claim_run` checks each run a leaf names as a read does, its full CRC included (`RunCrc::Always`), plus its zero pad, and claims its pages |
| `latch.rs` | `Latch` (first cause wins, `fail` vs `fail_damaged`), `UnwindLatch` |
| `file.rs` | `PageFile` (reserved mapping, growth, `msync_range`/`msync_page`, `fdatasync_all`, the size sync, poison), `ReadOnlyFile` (a read-only open's `PROT_READ` mapping of the file's length: no growth, no sync; holds the file, so its shared lock), `MapView` (the readers' view of either) |
| `sys.rs` | `fsync_file`, `fdatasync_file`, `fsync_directory`, `allocate_range`, `flock_exclusive` (a writer's open), `flock_shared` (a read-only open's) |
| `crc.rs` | a copy of `crates/storage/src/archive/crc.rs`; keep it a copy |
| `page_set.rs` | a copy of `crates/storage/src/archive/page_set.rs` plus a touched-word list, so `clear` and `drain` cost the words an insert touched, not the file; keep the rest a copy |
| `disk_model.rs` | test-only: the page cache and power-loss model |
| `tree/test_harness.rs` | test-only: `MemStore` (one flat byte buffer, so runs span pages; its `copy_published` is one `copy_within`, as the buffer has no readers), `walk_tree` (counts run pages against `overflow_pages`), `assert_accounting` (claims runs), the seeded `Rng`, `ReportSeed` |
| `bench.rs` | the `#[ignore]`d benchmark against MDBX (`md` with the OS page size, `md4` with 4 KiB pages: the like-for-like column for `t4d`); the API rows (`append`), with the tndb-b-only "txn cursor" rows (`ApiPut::TxnCursor`, `TxnCursorAppend`); the `big` rows (`TNDB_B_BENCH_ROWS=big`: put, commit and get of 2, 8 and 32 KiB values, each size in a fresh environment, `tndb_big`/`mdbx_big`); the delete rows (`TNDB_B_BENCH_ROWS=delete`: delete 90% of a random table and commit, a scan of what is left, a mixed 50/50 delete and put workload, and the churn file size with merging on and off, the off run through `Writer::no_merge`) |

## Rules of the crate

- **Never `File::sync_all` or `sync_data`.** On macOS these are `F_FULLFSYNC`. Use
  `sys::fsync_file` and `sys::fdatasync_file`, which are a plain fsync on macOS.
- **Grow the file with `sys::allocate_range`** (real blocks), so a full disk is an `io::Error`,
  not a `SIGBUS`. On macOS this makes the first writes into slack preallocated before an `fsync`
  slow (README, Platform notes); that cost is accepted, so do not grow sparse to avoid it.
- **Every sync wrapper reports to `disk_model` in tests.** `msync` (behind `msync_range` and
  `msync_page`) calls `flushed_range`, and `fsync`/`fdatasync` (behind `fdatasync_all` and the
  size sync) call `flushed_all`. A new sync path that skips the model makes the crash tests lie.
- **Never panic on bytes from disk.** Page accessors bounds-check and return `Corrupt`. A page
  read from the file must pass `page::validate` before its first change, which copy-on-write
  does.
- **Damage returns `Error::Corrupt` and latches** (`fail_damaged`). I/O, growth and commit errors
  and panics latch through `fail` or `UnwindLatch`. Validation errors (`KeyTooLarge`,
  `ValueTooLarge`, `AppendOutOfOrder`, `KeyExists`, table errors, `ReadOnly`, `WrongTxn`) are
  checked before anything changes and do not latch.
- **Every change to a table's tree goes through `Work::set_record`,** which sets its write stamp:
  a `TxnCursor` trusts its saved path only while the stamp is the one it saw. A new write path
  that skips it leaves detached cursors on stale, perhaps freed, pages.
- **Open never writes to an existing file.** Fail closed: never reinitialise a non-empty file,
  never fall back past a short file, never repair a meta. `open_read_only` never writes or syncs
  at all (not even step 7's fsync) and never creates.
- **Never reuse a page past the gate.** The gate is `min(valid slot txns, oldest reader)`. The
  writer is the only one who changes pages, and only private ones. Copy anything published.
- **Tests:** default tests stay small, because CI runs nextest in debug. Heavy variants go under
  `#[ignore = "heavy: run with --release"]` next to their small twin, with a `_heavy` suffix.
- **Mutation-check every new test.** Break the code the test guards, watch it fail, restore. A
  broken cursor step can loop forever, so mutate with a single-test filter, not the whole suite.
- **Comments and docs use durable anchors only:** function, test and type names, real issue
  numbers. Never cite review ids, agent or task ids, or "the plan".
- **Every public item has docs and every public type is `Debug`** (`missing_docs` is on). A
  dev-dependency the lib does not use needs `#[cfg(test)] use x as _;` in `lib.rs`.
- **A helper on a read path the adapter compiles needs `#[inline]`.** The adapter instantiates
  the generic and `#[inline]` read paths in its own crate (no LTO), so a non-generic helper they
  call without `#[inline]` becomes a cross-crate call on every row there. Mark it `#[inline]`, or
  `#[inline(always)]` once it has many callers and LLVM stops inlining it: `leaf_cell` left the
  adapter's `RoCursor::current_with_key_tail` that way when its callers grew (a dup scan row
  +3 ns), and `LeafView::entry` and `entry_with_key_tail` went out of line when each gained a
  caller. A new caller of a hot helper can push it out of line, so compare the read rows after
  adding one.
- **The reth adapter relies on the cursors' MDBX-aligned states.** A change to what a cursor
  does after a delete, after `last`, after a refused write or on a reseek, or to a public
  signature, also runs `crates/tndb-b-reth`'s checks (its `CLAUDE.md`): its differential test
  against MDBX is the oracle for cursor behaviour.
- **Test-only knobs** (on the writer: `CrashAt`, `data_by_fdatasync`, `data_by_msync`,
  `no_path_cache`, `no_merge`, `record_pages`; `Allocator::fail_allocs_after` for the
  failed-allocation sweeps; `PageFile::fail_next_msync`), reports (`max_save_rounds`,
  `data_by_file`, the allocator's `load_log`, `free_log` and `run_stats`) and hooks (`PageWrite::merges_applied`, which
  reports the `MergeReport` of a delete that merged or refused a sibling, so the other deletes
  pay one branch: `MemStore.merge_report` keeps the last, which a test resets before the delete
  it reads, and `Writer.merging_deletes` counts the deletes that absorbed a sibling;
  `TxnCursor::reseeks` a detached cursor's reseeks, off the timed paths) are `#[cfg(test)]` fields
  and functions. `no_merge` (`PageWrite::no_merge`, set on `MemStore` or `Writer`) stops deletes
  merging, so a test can build the shapes a tree without merging has. Production
  behaviour never depends on them. The test-only counters (`COW_VERIFIES`, `CACHED_PUTS`,
  `RANGE_CHECKS` in `tree/write.rs`, `CATALOG_RESOLVES` in `txn.rs`) exist only in debug test builds,
  `#[cfg(all(test, debug_assertions))]`: the
  release benchmark is compiled with the test configuration, so a counter on a timed path would
  show in its numbers. A test that reads one is gated the same way or accepts zero in release.
  A knob on a timed path costs at most a branch on a field.

## Checks before handing back

```text
cargo nextest run -p tndb-b
cargo +nightly-2026-03-20 clippy -p tndb-b --all-targets -- -D warnings
cargo +nightly-2026-03-20 clippy -p tndb-b --all-targets --all-features -- -D warnings
cargo +nightly-2026-03-20 fmt --all && cargo +nightly-2026-03-20 fmt --all -- --check
RUSTDOCFLAGS="-D warnings" cargo doc -p tndb-b --no-deps
cargo test --release -p tndb-b -- --ignored --skip tndb_b_vs_mdbx   # heavy variants, about 4–5 min
./etc/ci-lanes.sh clippy                                            # the workspace lane, as CI runs it
```

A change to the public API or to cursor behaviour also runs `crates/tndb-b-reth`'s checks.

The benchmark runs on demand:
`cargo test --release -p tndb-b tndb_b_vs_mdbx -- --ignored --nocapture --test-threads 1`.
Set `TNDB_B_BENCH_N` for a quick run. If you set `TMPDIR`, scope it to the one command, and never
point it at a tmpfs.

## Where tests live

| kind | place |
|---|---|
| codec, limits, damage of one page | `page.rs` `mod tests` (proptest models) |
| tree shape and copy-on-write against a `BTreeMap`, large values in runs included | `tree/write.rs`, `tree/read.rs` (on `MemStore`); copy-on-write's one copy, on `MemStore` and the real `Writer` (`copy_on_write_is_the_two_step_copy`, `copy_on_write_reads_a_page_published_before_a_remap`, `copy_on_write_fails_as_the_page_fetch_fails`) in `tree/write.rs` |
| merging on delete: the model with merging on (merge-closed after every delete, a coverage gate on merge shapes), the fill bound, damaged siblings, a failed allocation | `tree/write.rs` (`random_batches_with_merging_match_a_btreemap_model`, `deleting_90_percent_leaves_no_page_merge_open`, `a_merge_refuses_a_damaged_*_sibling`); at the environment level `tests/damage_merge.rs` (damage at rest met by a merging delete) and `tests/merge.rs` (an `RwTxn` workload with reopens and `check`, `RwCursor::delete_current` sweeps, the path cache after merges, the crash matrix on a merging commit); `tests/multi_model.rs` counts merging deletes in its coverage (`a_draining_multi_table_workload_with_crashes_matches_the_model`) |
| cursor moves and prefix steps against a sorted model | `tree/cursor/tests.rs` (`run_moves` drives the detached and the public cursor through `Op`/`Moves::apply`; `apply_owned` drives `RoCursor` beside `Cursor`; `Op::pick` generates `SeekWithKeyTail` too; `RwModel` adds `delete_current` and the deleted key); every key and gap sought against a model (`seeks_of_every_key_and_gap_match_a_model`, `_heavy`), `seek_with_key_tail` against damage (`a_seek_with_key_tail_meets_damage_as_a_seek_does`), the path's size (`tree_cursor_stays_small`) |
| free list, reuse timing, accounting; overflow runs in a whole environment (clear, drop, `check` and get against damaged runs and counters) | `freelist/tests.rs` |
| open, commit, NoSync, latch, handles; the per-snapshot table roots (`a_snapshot_resolves_each_handle_once`, `a_dropped_tables_handle_is_stale_only_in_later_snapshots`, `a_remembered_root_needs_the_handles_name_too`, `more_tables_than_a_snapshot_remembers_still_resolve`) | `env/tests.rs` (its helpers are `pub(crate)`) |
| whole-engine: crash matrix, damage, multi-table model (inline and large values), concurrency, path cache vs descent, append, insert and cursor put vs put | `tests/{crash,damage,multi_model,concurrency,path_cache,append}.rs` |
| `current_with_key_tail` on `Cursor`, `RoCursor` and `TxnCursor` (inline, empty and overflow values; walks, seeks, prefix steps, a reseek, after a delete) against `current`; `seek_with_key_tail` on `RoCursor` and `TxnCursor` against `seek` then `current_with_key_tail` (`seek_with_key_tail_is_a_seek_then_current_with_key_tail`) | `tests/key_tail.rs`; the page codec's span (`key_tail_joins_the_keys_last_bytes_to_an_inline_value`, and `exercise`'s `assert_key_tail_matches` on damaged pages) in `page.rs` |
| detached write cursors (`TxnCursor`): several cursors and direct writes on two tables against a per-cursor model (`txn_cursors_and_direct_writes_match_a_model`), no reseek for another table's writes (the test-only `TxnCursor::reseeks`), `StaleTable` after a drop, `WrongTxn`, a refused insert, prefix steps after other writers' changes, `last` ending the table (`last_ends_the_table_for_next_until_another_move`) | `tests/txn_cursor.rs` |
| the read-only open: reads, byte-identical file and mtime, 0444, no sync (power-loss model), the shared lock both ways, `ReadOnly` without a latch, never creating, the meta fallback, a read-only damage sweep (`sweep_read_only`, on `damage.rs`'s fixture) | `tests/read_only.rs` |
| whole-engine with overflow runs: the crash matrix on a run workload (`crash.rs`'s `matrix` on another `Base`), damaged runs and cells met at their first use (`damage.rs`'s `sweep`) | `tests/{crash_runs,damage_runs}.rs` |
| meta, catalog, alloc (runs included), overflow (the run codec and its damage), file, sys, latch, crc | each module's `mod tests` |

## Adding a crash test

1. **Track the file.** Call `disk_model::track(&path)` once the file exists. From then on its bytes
   are "on disk", and only synced writes reach that image.
2. **Arm a crash point.** Set `CrashAt` on the writer before the commit or sync:
   - inside a transaction, `txn.writer_mut().crash_at = Some(at)`;
   - before `Env::sync`, `env.with_writer(|w| w.crash_at = Some(at))`.

   `CrashAt` names a point of the commit protocol: `BeforeSave`, `AfterSave`, `AfterStamp`,
   `AfterDataSync`, `AfterSizeSync`, `AfterMetaCopy`, `AfterMetaMsync`. The commit stops there with
   an error, as a process death would: no further I/O, and the environment latches. Step 4 takes
   one `msync` or one `fdatasync` (`data_sync`); to run a crash point on the path a commit would
   not take, set `data_by_msync` or `data_by_fdatasync` on the writer, as `tests/crash.rs` does.
3. **Drop the environment as a crash would.** Call `env.crash()`, which skips the NoSync final
   sync (hold no other handle or transaction).
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
