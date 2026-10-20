# tndb-b: a guide for working in this crate

`tndb-b` is an embedded LMDB/MDBX-style key-value engine. It has one page file, copy-on-write
B+trees with the values in their pages, snapshot readers, a single writer, two meta pages, and a
persisted free list. There is no log: the meta pages, the gated page reuse and the free list carry
all crash safety. It depends on nothing in the workspace. Its primitives are copied from
`tn-storage`, so never add a `tn-storage` dependency.

**Read in this order:**
1. this file;
2. `README.md`, the section for your topic: on-disk format, commit protocol, free list and page
   reuse, readers, open and recovery, failure latch, concurrency, and the "please do not flag"
   list;
3. the module docs at the top of the file you change.

No file is too big to read whole, though `tree/write.rs` (1.8k lines) and `page.rs` (1.7k) are
more than half tests. Grep for the symbol and read with an offset.

## Crate map

| file | role |
|---|---|
| `env.rs` | `Env`: open and recovery (steps 1–9), the writer hand-off (`Mutex` + `Condvar`), snapshots, the reader queue and `reuse_gate`, `make_durable` (commit steps 4–6; `data_sync` picks per-range msync or one fdatasync), `sync`, the test-only `CrashAt` hook |
| `txn.rs` | `RoTxn`, `RwTxn` (`Work::commit` = steps 1–3 and 7), `ReadTxn`, `Table` resolution (`StaleTable`), `RwCursor`; each `Working` table record holds its `write::PathCache`, used by `RwTxn::put` and `RwTxn::append` (`put_entry`), which also places an `RwCursor` from the cached path after a put that did not split or did an append split (which refills the cache by a second, trusted descent of the new path; any other split re-seeks) |
| `meta.rs` | meta record codec, `choose_meta`, `slot_to_write` |
| `page.rs` | slotted page codec, `Geometry` (limits), `validate`, splits through the writer's reused `SplitBuf` (plan from a copy of the page, then `write_right` into the zeroed new page and `write_left` over the old one, each half laid out by `place_cells`; one page borrowed at a time; a leaf's append split leaves the left page untouched; `init_leaf_zeroed`, `init_root` and `write_right` skip the zero-fill of a page `alloc` returned zeroed, yet write every header, trailer, cell and slot byte themselves, so only the free gap relies on the zeros), the one-pass searches (`cmp_keys`, `leaf_find`, `branch_route`; each probe prefetches the two cells the next probe may read, `prefetch_next_probes`, through `prefetch`'s `asm!` hint: one of the crate's five `unsafe` places, listed in the README's do-not-flag item 2), the parsed views (`View`, `LeafView`, `BranchView`: one header check, many cell reads), `LeafMut` (a leaf put's read and change behind one header parse); the hot accessors (`hdr`, `hdr_of`, the cell reads) are `#[inline]`, with their errors built out of line by the `#[cold]` constructors `hdr_error`, `kind_error`, `cell_error`, `leaf_cell_error`, `branch_cell_error` and `reserve_error` (`hdr_error` re-runs the header checks in their original order, so every message is unchanged) |
| `tree/read.rs` | `checked_page` (the per-fetch checks), `checked_child` (those plus the identity check `txn as u32 == child_txn`, one header read; behind `PageRead::child`, which every descent calls), the readers' `descend`, `get` |
| `tree/write.rs` | copy-on-write `put`/`delete` (plan, reserve, apply; a write that copies or splits takes the writer's reused `Scratch` through `PageWrite::take_scratch`, so it allocates no heap memory once those buffers have grown), splits, unlink and collapse, `free_subtree`, `clear`; `descend_write` (the writer's descent: provenance trust skips the per-fetch and range checks on pages this txn built, lazy bounds via `path_bounds`, the fixed `Path`), `check_cells_naming_this_txn` (the copy-time `child_txn` rule); `PathCache` and `put_cached` (a put into the cached leaf with no descent while the epoch holds; a put that descends records its path straight into the cache's buffer (`PathCache::lend_path`, which empties the cache and lends the rest as a `CacheTail` that the put fills: from its own descent if it did not split, after an append split from a second, trusted descent of the new path); `PutKind::Append` refuses a key not past the last with `AppendOutOfOrder`; `Placed` reports the entry's slot when no split happened) |
| `tree/cursor.rs` | `TreeCursor` (detached path stack; `place` sets it from a write's path) and the public `Cursor` (leaf cache) |
| `alloc.rs` | `Allocator`: `private`, `stamped`, `pool`, `freed`, consumed records, `next_pgno`, `abort`, and the `epoch` that every allocate, free, stamp, `begin_txn` and abort moves (the path cache's validity) |
| `freelist.rs` | record codec, `next_record`, `decode_pages`, the fixed-point `save` |
| `catalog.rs` | `TableRecord` and catalog ops |
| `check.rs` | `Env::check`: structure plus exact page accounting |
| `latch.rs` | `Latch` (first cause wins, `fail` vs `fail_damaged`), `UnwindLatch` |
| `file.rs` | `PageFile` (reserved mapping, growth, msync, poison), `MapView` |
| `sys.rs` | `fsync_file`, `fdatasync_file`, `fsync_directory`, `allocate_range`, `flock_exclusive` |
| `crc.rs` | a copy of `crates/storage/src/archive/crc.rs`; keep it a copy |
| `page_set.rs` | a copy of `crates/storage/src/archive/page_set.rs` plus a touched-word list, so `clear` and `drain` cost the words an insert touched, not the file; keep the rest a copy |
| `disk_model.rs` | test-only: the page cache and power-loss model |
| `tree/test_harness.rs` | test-only: `MemStore`, `walk_tree`, the seeded `Rng`, `ReportSeed` |
| `bench.rs` | the `#[ignore]`d benchmark against MDBX (`md` with the OS page size, `md4` with 4 KiB pages: the like-for-like column for `t4d`) |

## Rules of the crate

- **Never `File::sync_all` or `sync_data`.** On macOS these are `F_FULLFSYNC`. Use
  `sys::fsync_file` and `sys::fdatasync_file`, which are a plain fsync on macOS.
- **Grow the file with `sys::allocate_range`** (real blocks), so a full disk is an `io::Error`,
  not a `SIGBUS`.
- **Every sync wrapper reports to `disk_model` in tests.** `msync` calls `flushed_range`, and
  `fsync`/`fdatasync` call `flushed_all`. A new sync path that skips the model makes the crash
  tests lie.
- **Never panic on bytes from disk.** Page accessors bounds-check and return `Corrupt`. A page
  read from the file must pass `page::validate` before its first change, which copy-on-write
  does.
- **Damage returns `Error::Corrupt` and latches** (`fail_damaged`). I/O, growth and commit errors
  and panics latch through `fail` or `UnwindLatch`. Validation errors (`KeyTooLarge`,
  `ValueTooLarge`, `AppendOutOfOrder`, table errors) are checked before anything changes and do
  not latch.
- **Open never writes to an existing file.** Fail closed: never reinitialise a non-empty file,
  never fall back past a short file, never repair a meta.
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
- **Test-only knobs** (`CrashAt`, `data_by_fdatasync`, `max_save_rounds`, `fail_next_msync`,
  `no_path_cache`) are `#[cfg(test)]` fields and functions. Production behaviour never depends
  on them. The test-only counters (`COW_VERIFIES`, `CACHED_PUTS`, `RANGE_CHECKS` in
  `tree/write.rs`) exist only in debug test builds, `#[cfg(all(test, debug_assertions))]`: the
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
cargo test --release -p tndb-b -- --ignored --skip tndb_b_vs_mdbx   # heavy variants, ~3 min
```

The benchmark runs on demand:
`cargo test --release -p tndb-b tndb_b_vs_mdbx -- --ignored --nocapture --test-threads 1`.
Set `TNDB_B_BENCH_N` for a quick run. If you set `TMPDIR`, scope it to the one command, and never
point it at a tmpfs.

## Where tests live

| kind | place |
|---|---|
| codec, limits, damage of one page | `page.rs` `mod tests` (proptest models) |
| tree shape and copy-on-write against a `BTreeMap` | `tree/write.rs`, `tree/read.rs` (on `MemStore`) |
| cursor moves against a sorted model | `tree/cursor/tests.rs` (`run_moves` drives both cursors) |
| free list, reuse timing, accounting | `freelist/tests.rs` |
| open, commit, NoSync, latch, handles | `env/tests.rs` (its helpers are `pub(crate)`) |
| whole-engine: crash matrix, damage, multi-table model, concurrency, path cache vs descent, append and cursor put vs put | `tests/{crash,damage,multi_model,concurrency,path_cache,append}.rs` |
| meta, catalog, alloc, file, sys, latch, crc | each module's `mod tests` |

## Adding a crash test

1. **Track the file.** Call `disk_model::track(&path)` once the file exists. From then on its bytes
   are "on disk", and only synced writes reach that image.
2. **Arm a crash point.** Set `CrashAt` on the writer before the commit or sync:
   - inside a transaction, `txn.writer_mut().crash_at = Some(at)`;
   - before `Env::sync`, `env.with_writer(|w| w.crash_at = Some(at))`.

   `CrashAt` names a point of the commit protocol: `BeforeSave`, `AfterSave`, `AfterStamp`,
   `DataMsync(k)`, `AfterSizeSync`, `AfterMetaCopy`, `AfterMetaMsync`. The commit stops there with
   an error, as a process death would: no further I/O, and the environment latches.
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
`tests/multi_model.rs` does the same under a random workload.
