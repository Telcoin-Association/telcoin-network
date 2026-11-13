# Verification Report: storage-v2 review, round 1 (T1, T3, T9)
Date: 2026-11-13
Scope: `sls_storage_v2_3` at HEAD 4acc752fb against origin/main (merge base a3df64830). Findings from T1 (tn-lmdb engine), T3 (storage layouts, migration, reset, CLI), T9 (upgrade/rollback, feature matrix, docs).
Source: T1.md, T3.md, T9.md ("## Findings" sections). Saved by the coordinator from the verifier's hand-back (the verifier cannot write files).

## Verdict table
| ID | Initial | Verdict | Verified | One line |
|---|---|---|---|---|
| T3-1 | Low | CONFIRMED | Low | The migration opens MDBX non-exclusive; a holder that has not committed (needs a second datadir naming the same `db/`) is not detected, and `finish` unlinks `mdbx.dat`/`mdbx.lck` under it |
| T3-2 | Low | CONFIRMED | Low | `migrate_from_mdbx` checks which files exist before `lock_db_dir` and never re-checks; the race gives a false "resolve by hand / fresh datadir" error, and no file changes |
| T1-3 | Low | CONFIRMED | Low | `BootFile::write`'s `named` wrapper drops the OS error, so the adapter reports EIO instead of the real errno (breaks the PORTING.md "Errors" promise) |
| T1-1 | Low | PARTIALLY_VALID | Informational | `cheapest` can fall back to `u32::MAX` and nothing clamps the Auto width, but no reachable input produces all-NaN costs |
| T1-2 | Low | PARTIALLY_VALID | Informational | The `len` half is false (`page::validate` refuses `len > MAX_VALUE` before the read-ahead). The `pgno` half holds (no in-file bound), but it costs I/O only and needs a forged, CRC-valid leaf |
| T9-2 | Low | PARTIALLY_VALID | Informational | The code behaviour is confirmed (the `mdbx` build leaves version 2, the consensus DB is still migrated, `v0.16.0-adiri` would start with empty consensus DBs). The docs already sit in passages scoped to the tn-lmdb build, and the `mdbx` build ships in no release |
| T3-3 | Info | not verified | Info | README "nothing changed" vs the `database.version` and empty `tn-lmdb.dat` the open creates; unwind-target row missing |
| T3-4 | Info | not verified | Info | `import_chain_scaffold` comment cites a Headers-height check that the code no longer uses |
| T3-5 | Info | not verified | Info | reset-execution resume advice loops; `db stats`/`check`/`migrate-execution` ignore a `.partial` |
| T9-1 | Info | not verified | Info | README lists the two first-start migrations in the wrong order |
| T9-3 | Info | not verified | Info | `upstream-backports.md` and vendor `CLAUDE.md` still say storage v1 on MDBX |
| T9-4 | Info | not verified | Info | Hardware floors cite 2026-11-12 runs with no commit hash or command recorded |

Verification stats: 12 submitted; 6 Lows verified by 2 subagents (batched by subsystem), 6 Informationals listed, not verified. 3 CONFIRMED, 3 PARTIALLY_VALID, 0 FALSE_POSITIVE. Test run by a verifier: `cargo nextest run -p tn-lmdb -E 'test(=env::tests::a_weak_head_check_reads_ahead_what_it_descends_into)'` passed. No repo file edited.

## Findings

### T3-1. The migration does not detect a process holding the MDBX environment open without committing; `finish` unlinks `mdbx.dat`/`mdbx.lck` under it
- **Severity**: initial Low; verified Low. **Status**: CONFIRMED (HIGH confidence)
- **Location**: `crates/tn-reth/src/env/database/migrate.rs:557-571` (`open_mdbx_read_only`, args at `:565`), `:660-663` and `:736-742` (source record and commit-point check), `:966-978` (`finish`)
- **Evidence**: the open never calls `with_exclusive` (`exclusive` defaults to false: `vendor/reth/crates/storage/db/src/implementation/mdbx/mod.rs:143, :447`; `MDBX_EXCLUSIVE` set only when true, `libmdbx-rs/src/flags.rs:164-165`). The source record (last txnid, length, mtime) and `finish`'s stat see only commits; nothing checks `mdbx.lck`, the reader table or other holders. On one datadir the PidLock refuses a second holder (`crates/telcoin-network-cli/src/node.rs:364` before `:393`; `db.rs:172`; `v0.16.0-adiri` takes the same lock), so the gap needs two datadirs naming one `db/` (symlink or bind mount), the case `lock_db_dir` exists for (`migrate.rs:34-37, 312-314`) but old binaries never take TN's flock. libmdbx returns `MDBX_BUSY` under `MDBX_EXCLUSIVE` when another process has the environment open (`mdbx.c:24675-24745, 25877-25878`; `RDONLY|EXCLUSIVE` valid, `mdbx.h:1067-1068`). No test covers an idle holder.
- **Why Low**: two live MDBX nodes sharing one `db/` are already broken before any upgrade, and the holder must commit nothing for the whole copy.
- **Proposed fix**: `migrate.rs:565`: `DatabaseArguments::default().with_max_read_transaction_duration(Some(limit)).with_exclusive(Some(true))`. Map `MdbxError::Busy` in `describe_mdbx_open_error` to "another process holds the MDBX database open: stop it, then start again" and add the README Refusals row. Replace `an_mdbx_commit_during_the_copy_fails_the_migration` (its writer can no longer open read-write during the copy) with an idle-holder refusal test (`assert_untouched`). Optional, human decision: keep the exclusive `src` alive until `write_db_version_file` returns to close the window from `drop(src)` (`:738`) to the version file (`:711`).

### T3-2. `migrate_from_mdbx` checks which files exist before it takes the directory lock and never re-checks them
- **Severity**: initial Low; verified Low. **Status**: CONFIRMED (HIGH confidence)
- **Location**: `crates/tn-reth/src/env/database/migrate.rs:279-284` (three `file_exists`, then `lock_db_dir`), `:285-306` (the match)
- **Evidence**: `mdbx`, `tn_lmdb`, `marker` are read at `:279-281`, the lock taken at `:283-284`; only `stopped_after_commit_point` (`:288, :293`; `:341-344`) is re-read under the lock. When another process (a second datadir on the same `db/`) finishes a migration between the checks and the lock: `(true,true,false)` → `both_engines` "resolve by hand" for a healthy dir; `(true,_,true)` → `written_after_migration`; `(true,false,false)` → `migrate` → read-only open fails ENOENT (libmdbx creates nothing under `MDBX_RDONLY`, `mdbx.c:9388-9390`) with "MDBX database is unchanged", untrue. Every branch is transient (the next start sees `(false,true,true)` and opens, `:305`); no file is touched. Needs two datadirs on one `db/`; no test (the `RUN_AT` hooks act only inside `migrate`).
- **Proposed fix** (`migrate.rs:279-284`): compute `needs_lock = exists(mdbx.dat) || exists(MIGRATING_DIR)`, take the lock, then read `mdbx`, `tn_lmdb`, `marker` under it. If `mdbx.dat` is gone by then the match falls to the `(false, ..)` arms, correct for a finished migration.

### T1-3. `BootFile::write` drops the OS error code that the adapter reports
- **Severity**: initial Low; verified Low (diagnostics; breaks a documented contract). **Status**: CONFIRMED (HIGH confidence)
- **Location**: `crates/tn-lmdb/src/boot_record.rs:303-305` (`named`), used at `:318-331`; `crates/tn-lmdb/src/sys.rs:144-174` (`Context`); `crates/tn-lmdb/src/env.rs:1425-1447` (`mark_open`); `crates/tn-lmdb-reth/src/error.rs:115-134`
- **Evidence**: `named` is `io::Error::new(e.kind(), format!(..))`: a `String` payload has `raw_os_error() == None` and no `source()`, so only the kind and the "(os error N)" text survive for the open, metadata, `write_all_at`, `set_len` and `fsync_file` errors (`fsync_directory` at `:333` keeps its `Context`). `mark_open` catches only `created && (PermissionDenied | ReadOnlyFilesystem)` by kind; everything else leaves `Env::open` as `Error::Io`, which the adapter maps to `os_error_code(io).unwrap_or(libc::EIO)` walking `source()`: an ENOSPC, EDQUOT or EACCES on the boot record reaches `DatabaseError::Open` as EIO. `PORTING.md:350` promises the OS code. No code branches on it (tn-reth matches Busy, NotFound, WannaRecovery only).
- **Proposed fix** (cold path, open step 10): `named = |e| sys::Context::wrap(format!("the boot record {}", path.display()), e)` with `Context`/`wrap` made `pub(crate)`; one PORTING.md "Errors" line; test: a 0444 existing record, `write(.., created=false, ..)`, assert the source chain's `raw_os_error() == Some(EACCES)` (mirrors `sys::tests::a_failed_directory_sync_keeps_the_original_error`).

### T1-1. Auto gap-fill width is not clamped to `MAX_GAP_FILL` on the fill path
- **Severity**: initial Low; verified Informational. **Status**: PARTIALLY_VALID (HIGH confidence): the code facts hold, the fallback is unreachable.
- **Location**: `crates/tn-lmdb/src/env/gap_auto.rs:514-524` (`cheapest`), `:369-374` (`take`), `:141` (`slot`, `debug_assert!` only); `crates/tn-lmdb/src/env.rs:1100-1105, :1210-1220`
- **Evidence**: `cheapest` starts at `(u32::MAX, INFINITY)` and `take` stores any width; `fill_gaps` bounds only `clean > width`; open/`FromStr` bound only `GapFill::Pages`. But both callers pass non-empty iterators (`WARMUP` 3 widths, `:341`; `beside(..).flatten()` ≥ 1, `:151-153, :428`), the first candidate wins unless its cost is NaN (+inf wins via the tie arm), and every input is finite (`solve_on` finite betas `:845`, `nnls` zeros `:786`, u64 samples `stats.rs:280-285`, `correction` divides by `max(1.0)` `:454`; `better_beside` rejects NaN `:429`). If reached: unbounded allocation and I/O, bytes unchanged.
- **Proposed fix** (optional): in `cheapest` map a NaN cost to `f64::INFINITY` (`gap_auto.rs:518`) so a candidate is always returned.

### T1-2. Weak-head check read-ahead does not bound a cell's run to the file
- **Severity**: initial Low; verified Informational. **Status**: PARTIALLY_VALID (HIGH confidence)
- **Location**: `crates/tn-lmdb/src/check.rs:536-556` (`read_ahead_of_leaf`), `:204-223` (`Ahead`); `crates/tn-lmdb/src/sys.rs:393-402`; `crates/tn-lmdb/src/overflow.rs:178-192`
- **Evidence**: the read-ahead runs only on a checked leaf (`check.rs:313-337`: bounds, fetch with tag, CRC, `page::validate`, txn identity, kind/depth, key range). The `len` half is false: `page::validate` refuses `len > MAX_VALUE` (`page.rs:1652-1659`), so the run is at most ~4097 pages (16 MiB), never 4 GiB. The `pgno` half holds: no `>= 2`/`<= page_count` bound before `will_need`, but it is advice whose error is discarded (no-op off Linux), the walk then refuses the cell through `claim_run` → `checked_run`, and it needs a forged CRC-valid leaf. `a_weak_head_check_reads_ahead_what_it_descends_into` passed.
- **Proposed fix** (optional): `check.rs:551`: `in_file = pgno >= 2 && pgno.checked_add(pages).is_some_and(|end| end <= page_count)` before `descends`.

### T9-2. The `mdbx` build's upgrade from `v0.16.0-adiri` leaves no version-file tripwire
- **Severity**: initial Low; verified Informational. **Status**: PARTIALLY_VALID (code HIGH, docs criticism MEDIUM)
- **Location**: `crates/tn-reth/src/env/database.rs:119-129, :227-260, :811-896`; `crates/storage/src/lib.rs:437-439`; `crates/tn-reth/README.md:962-972`; `docs/src/getting-started/validator-operations.md:122`
- **Evidence**: `TN_LMDB_DB_VERSION` and the version-file functions are `#[cfg(not(feature = "mdbx"))]`; under `mdbx`, `open_rw` uses reth's `init_db` (version 2). `tn-storage` runs `migrate_from_mdbx` under its default `reth-libmdbx` feature and deletes the MDBX consensus environments (`lib.rs:318, :340, :346-365`). `v0.16.0-adiri` at the tag passes version 2 and opens new empty MDBX consensus environments (`storage/src/lib.rs:241-244` warns of this). But the README "Rollback" sits under a section scoped to "the default build, tn-lmdb" (`:871-873`; every release is the tn-lmdb build, `:495-497`), `validator-operations.md:122` speaks of the migrated execution database, and the `mdbx` build ships in no release (`etc/backend-gate.sh`; Makefile uses it only as the e2e previous binary).
- **Proposed fix** (docs only): one README sentence near `:968` or `:497`: the tripwire is the tn-lmdb build's; the `mdbx` build leaves `database.version` at 2 but still moves the consensus database to tndb, so `v0.16.0-adiri` on such a datadir starts with empty consensus databases; never run that build on a validator datadir you may roll back.

## Informational (not verified, as submitted)
- **T3-3.** `crates/tn-reth/README.md:755-757` says "nothing changed" for refused datadirs, but `open_rw` (`database.rs:193-198`) creates `db/`, `database.version` (1002) and an empty `tn-lmdb.dat` before `settle` refuses; the table omits the unwind-target refusal (`env/mod.rs:479-493`).
- **T3-4.** `snapshot.rs:626-629, :852-855` comments cite a Headers-height check `open` no longer uses (since R16 it keys on the `Finish` checkpoint, `env/helpers.rs:347-354`); the heal is `check_consistency`'s prune. Add a crash-mid-scaffold test.
- **T3-5.** `reset_execution.rs:172-174` "run the reset again in a second" cannot succeed on resume when both `<stamp>` and `<stamp>.partial` exist; `db.rs:166-178, :210-224` (`migrate-execution`, `stats`, `check`) do not call `refuse_interrupted_reset`.
- **T9-1.** `crates/tn-reth/README.md:871-879` lists the consensus-DB migration first; the code migrates the reth DB first (`cli/src/node.rs:393` before `launch_node` → `node/src/lib.rs:80`).
- **T9-3.** `vendor/reth/tn/notes/upstream-backports.md:28-33, :113-120, :131-133` and `vendor/reth/CLAUDE.md:54, :136` still describe storage v1 on MDBX and "tndb-b owners"; the 1b20c9bfc and 6bd6bf28a verdicts rest on it.
- **T9-4.** `docs/src/getting-started/hardware-requirements.md` `[^exec-disk]` floors (20,000 IOPS, 800 MiB/s) cite 2026-11-12 runs with no commit hash or command; `crates/tn-reth/README.md:1132-1160` names only the 2026-11-04 run.

## Action items, by severity
1. T3-1 (Low): exclusive MDBX open in `open_mdbx_read_only`; Busy hint; README row; idle-holder test. Holding `src` until the version file is written: human decision.
2. T3-2 (Low): lock first, then read the three files under the lock.
3. T1-3 (Low): `sys::Context::wrap` in `BootFile::write`; PORTING note; EACCES source-chain test.
4. Informational: T1-1 NaN guard, T1-2 in-file bound, T9-2 README sentence, T3-3/T3-4/T3-5/T9-1/T9-3/T9-4 doc and comment fixes.
