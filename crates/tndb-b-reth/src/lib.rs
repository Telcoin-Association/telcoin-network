//! reth's database API (`reth-db-api`, reth v1.11.3) on the tndb-b engine.
//!
//! [`TndbEnv`] is a reth [`Database`](reth_db_api::database::Database) whose transactions and
//! cursors behave as reth's MDBX ones do, and report MDBX's error codes (below), so reth code and
//! reth's tests written against MDBX run unchanged:
//!
//! | reth | here | engine |
//! |---|---|---|
//! | `DatabaseEnv` (`DatabaseEnvKind`, `DatabaseArguments`) | [`TndbEnv`] ([`TndbEnvKind`], [`TndbArgs`]) | [`tndb_b::Env`], read-only through [`tndb_b::Env::open_read_only`] |
//! | `Database::TX` | [`TndbTx`] | an `Arc` of [`tndb_b::RoTxn`] |
//! | `Database::TXMut` | [`TndbTxMut`] | [`tndb_b::RwTxn`] behind a shared mutex |
//! | `DbTx::Cursor` (read-only) | [`TndbCursor`] | [`tndb_b::RoCursor`] |
//! | `DbTxMut::CursorMut` | [`TndbCursorMut`] | [`tndb_b::TxnCursor`] |
//! | `DatabaseMetrics` | [`TndbEnv`] ([`TndbEnv::table_stats`], [`RethTableStat`]) | [`tndb_b::TableStat`], [`tndb_b::Env::free_pages`] |
//! | none (MDBX's sync and check are its own tools) | [`TndbEnv::sync`], [`TndbEnv::check`] ([`CheckReport`]) | [`tndb_b::Env::sync`], [`tndb_b::Env::check`] |
//! | none (MDBX's `page_ops` in its environment info are the nearest) | [`TndbEnv::io_stats`] ([`IoStats`]) | [`tndb_b::Env::io_stats`] |
//!
//! Each of reth's tables is an engine table of the same name; the environment maps each name to
//! its engine handle. Keys are stored encoded (`Encode`) and values compressed (`Compress`), as
//! MDBX stores them.
//!
//! **Dup-sort tables** are stored as composite keys: each value is a row of its own. reth's five
//! dup-sort tables each have a fixed-width key and values that start with their encoded subkey
//! (`DupSort::SubKey`):
//!
//! | table | key | subkey (W) |
//! |---|---|---|
//! | `PlainStorageState` | `Address` (20 bytes) | `B256` slot (32) |
//! | `StorageChangeSets` | `BlockNumberAddress` (28) | `B256` slot (32) |
//! | `HashedStorages` | `B256` (32) | `B256` slot (32) |
//! | `AccountChangeSets` | `BlockNumber` (8) | `Address` (20) |
//! | `StoragesTrie` | `B256` (32) | `StoredNibblesSubKey` (65) |
//!
//! A row's stored key is the encoded key followed by the compressed value's first W bytes, and its
//! stored value is the rest. A leaf cell holds its value right after its key, so a read takes the
//! value, the key's last W bytes then the rest, as one slice of the page (the engine's
//! `current_with_key_tail`), with no copy; only a value in an overflow run is joined in a buffer
//! of the cursor's. A cursor's `seek_exact` and `seek_by_key_subkey` read their row in the seek
//! itself, from the leaf the seek fetched (the engine's `seek_with_key_tail`). Since each table's
//! keys have one width, the rows sort by key, then by subkey, as MDBX sorts a key's values. A row
//! this layout cannot hold (a value shorter than W, or a key of another width) is refused with
//! `MDBX_BAD_VALSIZE`. Another dup-sort table (reth has none) is created, counted and cleared, but
//! every other access to its rows is refused with a
//! [`DatabaseError::Other`](reth_db_api::DatabaseError), as is a dup cursor call on a table without
//! duplicates. reth's calls map onto the engine's ordered rows:
//!
//! | reth | here |
//! |---|---|
//! | `get(k)`, `seek_exact(k)` | `seek(k)` (a cursor's `seek_exact` as `seek_by_key_subkey(k, [])`), and the row found must be `k`'s: the key's first value; a `k` of another width than the key's misses with no seek |
//! | `seek(k)` | `seek(k)`: the first row of the first key at or after `k` |
//! | `seek_by_key_subkey(k, s)` | `seek_with_key_tail(k ‖ s, W)`: `seek(k ‖ s)` and the row's read from one leaf access, `k ‖ s` built on the stack; the row found must be `k`'s (MDBX's `get_both_range` never leaves the key); a `k` of another width misses with no seek |
//! | `next_dup`, `prev_dup`, `next_dup_val` | the engine's prefix steps over the key's width |
//! | `next_no_dup` | `seek(k ‖ 0xFF × (W + 1))`, past every row of `k`, when another row of `k` follows; else `next` |
//! | `last_dup` | that seek, then `prev` |
//! | `walk_dup` | MDBX's start (`first`; `seek_exact`; `seek_by_key_subkey`; with a subkey only, `first` then `seek_by_key_subkey` of its key), then `next_dup` |
//! | `put`, `upsert` | the engine's insert of the row: a new subkey is stored, the same value again changes nothing (the cursor then on the row), another value for a subkey the key holds is refused with `MDBX_KEYEXIST` |
//! | `insert` | `MDBX_KEYEXIST` if the key has any row (the cursor then on its first) |
//! | `append` | the engine's append, or an insert as by `put` if the key is the table's last key (MDBX takes a value anywhere among the last key's); a smaller key is `MDBX_EKEYMISMATCH`, the cursor on the last row |
//! | `append_dup` | the value must sort after the key's last (by subkey), else `MDBX_EKEYMISMATCH` with the cursor on the key's last row (on its first when the value is exactly the first's); a key without rows takes any value, wherever the key sorts |
//! | `delete(k, Some(v))` | deletes the row of `v`'s subkey if it holds exactly the rest of `v` |
//! | `delete(k, None)`, `delete_current_duplicates` | delete every row of the key |
//! | `delete_current` | the engine's `delete_current`, then its `follower_shares_prefix` over the key's width, with no step: a follower of the key's leaves the cursor on it, else `prev_in_prefix` finds whether the key keeps an earlier row (the cursor then ends the key's rows, on it) or none (the cursor on the next key's first row) |
//! | `entries` | the rows: MDBX's count of key/value pairs |
//!
//! As on MDBX, a fresh cursor's `next_dup`, `next_no_dup` and `next_dup_val` start at the first
//! row and its `prev_dup` at the last, a missed `seek_exact` or `seek_by_key_subkey` leaves the
//! cursor unset (no relative move finds a row until an absolute move or a write places it again),
//! and right after a delete that leaves a key no rows (or `delete_current_duplicates`) the cursor
//! is on the next key's first row, where `prev_dup` finds nothing.
//!
//! **Errors** are reth's `DatabaseError`, with the variant MDBX's implementation gives for the
//! same operation (`Open` for a table that does not exist, `Read`, `Write` with the operation,
//! table and key, `Delete`, `Commit`, `InitTx`, `InitCursor`, `Stats`, `CreateTable`), and an
//! information whose code and message are MDBX's:
//!
//! | engine | MDBX |
//! |---|---|
//! | `KeyExists` | `MDBX_KEYEXIST` (for a second value for a dup row's subkey, with this crate's message) |
//! | `AppendOutOfOrder` | `MDBX_EKEYMISMATCH` |
//! | `KeyTooLarge`, `KeyTooShort`, `ValueTooLarge` | `MDBX_BAD_VALSIZE` |
//! | `Corrupt` | `MDBX_CORRUPTED` (the detail is in [`TndbEnv::failure`]) |
//! | `Locked` | `MDBX_BUSY` |
//! | `ReadOnly` | `MDBX_EACCESS` |
//! | `WrongTxn`, a write transaction already finished | `MDBX_BAD_TXN` |
//! | `TableNotFound`, `StaleTable`, a table never created | `MDBX_NOTFOUND` |
//! | an unpositioned `delete_current` | `MDBX_ENODATA` |
//! | `TableKind` | `MDBX_INCOMPATIBLE` |
//! | `InvalidTableName`, `InvalidPageSize`, `InvalidDupOptions` | `MDBX_EINVAL` |
//!
//! A refused second value for a dup-sort row's subkey (below) is `MDBX_KEYEXIST` with a message of
//! this crate's saying it stores one value per subkey. Four engine errors keep their own message,
//! which says more than MDBX's would, under the MDBX code reth reads the same way: `Failed` (the
//! environment latched an earlier failure: writes and commits fail until it is reopened) is
//! `MDBX_PANIC`, MDBX's "fatal environment error"; `Io` carries the OS error number when there is
//! one, else `EIO`; `NotTndbB` is `MDBX_INVALID`; and `UnsupportedVersion` is
//! `MDBX_VERSION_MISMATCH`.
//!
//! **Differences from MDBX.** reth's code reaches none of them. Each cursor difference is pinned
//! beside MDBX's result by `known_cursor_deviations_from_mdbx` or
//! `known_dup_cursor_deviations_from_mdbx`, and the differential test (below) skips them.
//!
//! The environment:
//! - no read transaction times out, so `disable_long_read_transaction_safety` does nothing and
//!   `db.timed_out_not_aborted_transactions` is always 0. A long reader is reported instead: while
//!   a read transaction holds a superseded snapshot, the pages freed since are not reused, so the
//!   file grows until it ends and they return; a write commit made once that has lasted over 60
//!   seconds logs a `warn!` with its age, at most once a minute per environment
//!   (`tndb_b::Env::oldest_reader_age`, a lock-free read: readers pay nothing, and a commit one
//!   atomic load while no reader holds a superseded snapshot, plus a clock read while one does);
//! - one file, `<dir>/tndb.dat`, with the engine's `flock` as the lock: a second open of a file
//!   another handle holds is `MDBX_BUSY`;
//! - the table stats ([`TndbEnv::table_stats`]) and the metrics skip a table the file lacks (MDBX's
//!   metrics stop at it).
//!
//! The data:
//! - a dup-sort table holds one value per subkey: a put, `upsert` or `append` of a second value for
//!   an existing (key, subkey) is refused with `MDBX_KEYEXIST` and a message saying so, and changes
//!   nothing (not the table, not the cursor, and the environment does not latch), where MDBX stores
//!   both. Writing the value the row holds again succeeds, as on MDBX. reth deletes a (key, subkey)
//!   entry before it writes a new one (`seek_by_key_subkey`, then `delete_current`, then `upsert`),
//!   so it never meets the refusal; a future reth change that writes real duplicates fails loudly
//!   instead of losing a row;
//! - an `append_dup` with the subkey of the key's last row is refused for its order
//!   (`MDBX_EKEYMISMATCH`, whatever its value), where MDBX takes it when the key has two rows or
//!   more (a second value, or the pair it holds again) and refuses it when the key has one;
//! - a dup-sort table takes values up to 16 MiB (the engine's limit, in overflow runs), where MDBX
//!   refuses a dup value much larger than its key size with `MDBX_BAD_VALSIZE`. reth's dup values
//!   are far smaller. A cursor `insert` of a key that has rows is refused as present
//!   (`MDBX_KEYEXIST`) whatever its value's size;
//! - a range walk may start past an excluded key, and every row of it (MDBX's cursor panics on
//!   one);
//! - a raw subkey `s` wider than the layout's W that starts with a row's subkey, its bytes past W
//!   sorting at or before that row's rest, makes `seek_by_key_subkey` take the row after: the seek
//!   key `k ‖ s` sorts after the row's stored key `k ‖ s[..W]`, of which it is a longer copy, while
//!   MDBX compares the row's whole value (subkey, then the rest) with `s` and takes the row. No
//!   reth subkey type encodes wider than W, so only a raw view (`RawDupSort`) meets this (the
//!   differential test's typed subkeys never do); `known_dup_cursor_deviations_from_mdbx` pins it
//!   beside MDBX's result, and `seek_by_key_subkey_longer_than_the_stack_buffer_matches_mdbx` with
//!   seek keys at and past the cursor's stack buffer.
//!
//! The cursor positions. Right after a delete the cursor is on the follower as MDBX's is
//! (`current` returns it, `delete_current` deletes it, `delete_current_duplicates` deletes its
//! key's rows), on a dup-sort table a delete of a key's last row that leaves the key earlier rows
//! ends the key's rows as MDBX's does, and after `last` (or a fresh cursor's `prev`, a refused
//! `append`, or an `append` that replaces the last key's value) a cursor, read-only or write, ends
//! the table as MDBX's does. These still differ:
//! 1. **past either end:** after `next` past the last row, `prev` before the first, a delete of the
//!    table's last row or a `seek` past the end, MDBX's cursor stays on the end row (`current`
//!    returns it, `delete_current` deletes it, `next` and `prev` step from it), where this one is
//!    past the end (`current` finds nothing, `delete_current` is `MDBX_ENODATA`). This covers
//!    `next_no_dup` before the first row (MDBX's goes to the second key) and
//!    `delete_current_duplicates` there (MDBX's deletes the first key's rows), a table emptied by a
//!    delete and given a row (MDBX's `prev` finds nothing), and two unset cursors that MDBX's
//!    places: after a missed seek on an empty table that another writer then fills (MDBX's is as a
//!    fresh one), and after a missed seek once another writer has deleted the row it landed on and
//!    every row after it (MDBX's is then at the end);
//! 2. **another writer's delete and put:** another writer's delete of the cursor's row, then a put
//!    between the row and its follower, or of the row's key, before the cursor's next call: the
//!    cursor finds its place again at that call and is on the new row, where MDBX's is on the
//!    follower;
//! 3. **another writer changes the cursor's dup key:** the cursor finds its place again by key at
//!    its next call, where MDBX's works from a stale copy of the key's rows (below);
//! 4. **`last_dup` or `delete_current` right after a delete of a key's only row,** or after
//!    `delete_current_duplicates`, which leave the cursor on the follower: `last_dup` returns the
//!    follower's key's last value on both, but MDBX's cursor stays on the follower and this one is
//!    on that last row, so the moves after it differ; `delete_current` deletes the follower on
//!    both, and when the row after it is in the same key, MDBX's dup moves then act as if its
//!    cursor were outside the key's rows (`next_dup` and `next_dup_val` find nothing, `next_no_dup`
//!    returns that row) where this one's act as after any delete;
//! 5. **`append_dup` with the subkey of a key's last row,** with two rows or more: refused here for
//!    its order, taken by MDBX (above);
//! 6. **one value per subkey:** a second value for a (key, subkey) is refused here, stored by MDBX
//!    (above);
//! 7. **`last_dup` before the first row** is `None`, where libmdbx aborts (below);
//! 8. **a dup key of one row,** where MDBX's cursor misbehaves (below) and this one acts as on any
//!    key.
//!
//! **What MDBX gets wrong.** The differential test found these; they are why MDBX is no oracle in
//! classes 3, 7 and 8 above. reth never puts a cursor in these states.
//! - **Stale cursors when another writer changes a cursor's dup key.** In one write transaction,
//!   when the transaction's own `put` or `delete`, or another cursor, changes the rows of the dup
//!   key a cursor is in, MDBX's cursor keeps working from its own copy of the key's nested rows,
//!   which may or may not go stale, by the cursor's history and the page layout. Its next step may
//!   then skip a row put after it (`next` from a key's last row after `next_dup` found nothing, or
//!   after a `prev` from the next key, goes to the next key), or step to another key.
//! - **A wrong `entries` count.** In that state a `delete_current_duplicates` removes the right
//!   rows but leaves the table's `entries` one off: one more after the transaction put a row in the
//!   key, one less after it deleted the cursor's row.
//! - **libmdbx assertion aborts.** In that state a `delete_current` can fail an assertion in
//!   `tree_rebalance`, which aborts the process: `seek_exact` of a key with one row, the
//!   transaction's `put` of a second, then `delete_current` (`mc->tree->items > 0`); or
//!   `seek_by_key_subkey` of a key's first row of two, the transaction's `delete` of the second,
//!   then `delete_current` (`(mc->tree->items == 0) == (nkeys == 0)`). `last_dup` before the first
//!   row aborts too, and MDBX's debug build aborts on a dup value much larger than its key size.
//! - **One-row-key quirks.** MDBX holds a dup key's only value apart from its dup values (as a
//!   plain row, with no nested ones) until a second value arrives. After an `append` gives such a
//!   key (the table's last) a second row, MDBX's cursor is on the new row but `prev` skips the
//!   key's first, `prev_dup` finds nothing, and `next`, `next_dup` and `next_no_dup` find nothing
//!   even after another writer adds rows. Ending such a key (after `last`, a fresh `prev` or
//!   `prev_dup`, a refused `append`, or an `append` of its row unchanged, which MDBX's `append`
//!   takes as `last`: it finds the last key's last value and returns), its `next` does not see a
//!   later key another writer adds, as on a table without duplicates. A missed `seek_by_key_subkey`
//!   past such a key's row leaves its cursor on that row once it has moved, where this one is
//!   unset.
//!
//! `known_dup_cursor_deviations_from_mdbx` pins the cases that do not abort. Here a cursor finds
//! its place again by key at its next call, as after any other writer's change; the engine's
//! model tests and this crate's multi-cursor tests cover that.
//!
//! **The differential test** (`random_scripts_match_mdbx`, and `random_scripts_match_mdbx_heavy`
//! in release) draws random scripts with proptest and runs each on a [`TndbEnv`] and on reth's
//! MDBX `DatabaseEnv`; the two outcome lists must be equal, value for value and error for error.
//! A script works on `CanonicalHeaders`, `PlainStorageState` (dup-sort, with keys and subkeys
//! drawn from small sets so rows collide) and `Bytecodes` (values from 8 bytes to 40 KiB, so
//! overflow runs), through the transaction's calls, two write cursors and a read cursor (every
//! cursor call and the walks), commits, aborts, and a read transaction that must keep its
//! snapshot. Each value names the write that made it. The runner models the tables and each
//! cursor's place from the run's own outcomes, and skips an op that would land in one of the
//! differences above or in the zone where MDBX is no oracle, counting each skip by class: past
//! either end; another writer's delete and put; another writer's change to the cursor's dup key;
//! `last_dup` or `delete_current` after an outer delete; `append_dup` of the key's last value;
//! one value per subkey; `last_dup` before the first row; a dup key of one row. A write to a dup
//! row that exists mostly writes the value it holds, which both databases take; a quarter of them
//! write another value, a second value for the subkey, which runs on tndb-b only and must be
//! refused (`MDBX_KEYEXIST` with this crate's message) and change nothing: it is skipped on MDBX,
//! and the ops after it must still match MDBX's. About 4% of the ops are skipped; the test fails if
//! a tenth or more are. On a difference proptest shrinks the script and prints both outcome lists.
//!
//! **The crash cycles** (`killed_child_reopens_to_its_last_commit`, 5 cycles, and
//! `killed_child_reopens_to_its_last_commit_heavy`, 200 in release; Unix only) run a child process
//! that commits rounds of puts, appends and deletes (`CanonicalHeaders`, with cursor `append`s;
//! dup rows of `PlainStorageState`, with `append_dup`, `delete_current_duplicates` and now and
//! then a `clear`; overflow-sized `Bytecodes`; and a progress row) and kill it with `SIGKILL` at a
//! random point: in its open, its writes or its commit. The reopened environment must hold the
//! last round the child reported committed (or, killed in a commit, the round before), every table
//! exactly as a model of those rounds, and pass the engine's `check`. The heavy variant also
//! requires a kill in a commit. A `SIGKILL` keeps the page cache, so this tests process death; the
//! engine's own crash tests cover power loss.
//!
//! **The benchmark** (`tndb_reth_vs_mdbx`, `#[ignore]`d; the `bench` module docs and the README
//! give the command, the selectors and a table) runs the workloads of reth's database benches
//! (`criterion.rs`, `hash_keys.rs`, `get.rs`, `put.rs`) on a [`TndbEnv`] and on reth's MDBX
//! `DatabaseEnv` with the same seeded data, and prints the time per operation of each with the
//! ratio. A second one, `tndb_b_vs_mdbx` (the `engine_bench` module; its runs are in
//! `engine-bench.md`), runs the engine itself, through its own API, against `reth-libmdbx`.

mod cursor;
mod dup;
mod env;
mod error;
mod tx;

#[cfg(test)]
mod bench;
#[cfg(test)]
mod engine_bench;
#[cfg(test)]
mod tests;

pub use cursor::{TndbCursor, TndbCursorMut};
pub use env::{RethTableStat, TndbArgs, TndbEnv, TndbEnvKind, DATA_FILE};
pub use tndb_b::{CheckReport, IoStats, SyncMode, TableCheck};
pub use tx::{TndbTx, TndbTxMut};

#[cfg(test)]
use proptest as _;
#[cfg(test)]
use reth_db as _;
