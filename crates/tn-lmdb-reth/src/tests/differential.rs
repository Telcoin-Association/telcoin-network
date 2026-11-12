//! A randomized differential test: op scripts drawn by proptest run on
//! [`TnLmdbEnv`](crate::TnLmdbEnv) and on reth's MDBX `DatabaseEnv` (the oracle), and the two
//! outcome lists must be equal, value for value and error for error.
//!
//! **Tables:** `CanonicalHeaders` (u64 keys); `PlainStorageState`, dup-sort, with 4 addresses and
//! 6 slots so rows collide; `Bytecodes`, with 6 keys and values from 8 bytes to 40 KiB (inline,
//! one overflow page, and longer runs).
//!
//! **Ops:** the transaction's `get`, `put`, `append` (mostly in order), `delete` with no value,
//! a matching value or another, `clear` and `entries`; up to two write cursors and one read cursor
//! of the write transaction at a time, each on any table, with every cursor method and short walks
//! (a dup method on a plain table is its plain counterpart: `next_dup` is `next`, `last_dup` is
//! `last` and so on); commit, abort, and a read transaction opened at one point and read later,
//! which must show its snapshot: its `get`, `entries` and a walk of a new cursor, and a cursor of
//! its own (slot [`SNAP`], over the engine's read-only cursors) with every read method and walk,
//! kept across the write transaction's commits and aborts and reopened now and then (the one it
//! replaces goes back to the transaction's cursor pool, and the new one may take it, reset).
//! Every value is made from its row and a version counter, so a value read back names the write
//! that made it.
//!
//! **Exclusions.** Scripts stay clear of the known differences from MDBX (the crate docs' list,
//! pinned by `known_cursor_deviations_from_mdbx` and `known_dup_cursor_deviations_from_mdbx`), and
//! of the one zone where MDBX is no oracle (class 3). The runner keeps a model of the tables and of
//! each cursor's place, built from the run's own outcomes, and skips an op that would fall in one
//! of them (the skips are the same on both databases until their outcomes first differ). A cursor
//! in a state that leads to a difference is *tainted*: its relative calls (`next`, `prev`,
//! `current`, the dup moves and the deletes) are skipped until an absolute one (a seek, `first`,
//! `last`, a write or a walk) places it again. The read transaction's cursor reads a snapshot no
//! writer changes (its model is the snapshot's tables), so only classes 1, 7 and 8 reach it. Each
//! skip is counted against its class:
//! 1. **past either end** (MDBX's cursor stays on an end row, this one goes past it; the crate docs
//!    give each case): `next` or `next_no_dup` finding nothing (except right after `last`, which
//!    MDBX matches), `prev` finding nothing, a `seek`, `first` or `last` finding nothing, a delete
//!    of the table's last key (and, by another writer, of the cursor's row with no row after it), a
//!    walk that runs out, a missed `seek_exact` or `seek_by_key_subkey` on an empty table (or past
//!    the last row, when another writer then deletes any row, or once another writer has deleted
//!    the row it landed on and every row after it), and a `clear` under the cursor taint it;
//! 2. **reseek after another writer's delete and put:** another writer's delete of the cursor's row
//!    marks it; a put by another writer between that row and the cursor's next row, before the
//!    cursor's next call, taints it (on the dup table, class 3 takes these first);
//! 3. **another writer changes the cursor's dup key:** another writer's put or delete of any row of
//!    the dup key the cursor's row belongs to taints it. MDBX's cursor keeps its own copy of the
//!    key's values; another writer's change can leave that copy stale or not, by page layout, and
//!    its next step may skip a row, or find none, and its delete may leave a wrong `entries` count
//!    or fail an assertion in libmdbx (`tree_rebalance`), which aborts the process;
//! 4. **`last_dup` or `delete_current` after an outer delete:** a `last_dup` or `delete_current`
//!    right after the cursor's delete of a key's only row or its `delete_current_duplicates` runs,
//!    then taints it;
//! 5. **`append_dup` of the key's last value:** an `append_dup` whose subkey is the key's last
//!    row's while the key has two rows or more is skipped (tn-lmdb refuses it for its order; MDBX
//!    takes it);
//! 6. **one value per subkey:** a write to a dup row that exists mostly writes the value it holds
//!    (both databases keep the pair as it is); a quarter of them ([`K::other_value`]) write another
//!    value. A `put`, `upsert`, or `append` of the table's last key (a transaction's or a cursor's)
//!    with another value is a second value for the subkey: MDBX would store it beside the first, so
//!    it is skipped there and counted here, while tn-lmdb runs it and must refuse it
//!    (`MDBX_KEYEXIST` with this crate's message, `second_value_refused`) and change nothing: its
//!    outcome reads as the skip on both, and every op after it must still match MDBX's, which never
//!    ran it. (Another value for an earlier key's row by `append` is refused for its order on both
//!    and runs on both; `insert` and `append_dup` write the value the row holds.)
//! 7. **`last_dup` before the first row** (libmdbx aborts): only a cursor tainted by `prev` finding
//!    nothing (or a reverse walk running out) gets there; such a skip counts here;
//! 8. **a dup key of one row** (MDBX keeps a key's only value apart from its dup values): a
//!    cursor's `append` that gives its key (the table's last, with one row) a second taints it, as
//!    does a later key's put while the cursor ends a key of one row (after `last`, a fresh `prev`
//!    or `prev_dup`, a refused `append`, or an `append` of the key's row unchanged, which MDBX
//!    takes as `last`), and a missed `seek_by_key_subkey` (or `walk_dup` start) past a key's one
//!    row from a cursor that has moved.
//!
//! An error other than the expected ones (`MDBX_KEYEXIST`, `MDBX_EKEYMISMATCH`, `MDBX_ENODATA`)
//! also taints the cursor, counted apart. About 4.5% of the ops are skipped (about 3% in the
//! default's shorter scripts); a run fails if a tenth or more are (skipped against run), as its
//! scripts would then test little. On a difference proptest shrinks the script, then prints it
//! with both outcome lists.
//!
//! **Forms and seeds.** Each script runs on MDBX once and on tn-lmdb in each form of the nested
//! dup layouts' tables ([`Form`]), `PlainStorageState` among them: engine dup tables packing up
//! to the page size's most (production), the same packing at most
//! [`PACKED_SMALL_RETH`](crate::env::PACKED_SMALL_RETH) bytes (a key promotes at its second or
//! third row), then composite keys in plain tables
//! ([`TnLmdbArgs::composite_dup_tables`](crate::TnLmdbArgs::composite_dup_tables), the changesets'
//! path). The runner asserts each form it asked for, and each form's outcomes and skips by class
//! must equal MDBX's (the skips depend only on the outcomes), so within a run every form takes
//! the same scripts and skips the same ops. The random tests draw their scripts from a new seed
//! each run (`PROPTEST_RNG_SEED` picks one), printed with the totals and in a failure's message,
//! which `PROPTEST_RNG_SEED` then reruns. `random_scripts_skip_counts_are_pinned_at_seed_1` and its
//! `_heavy` variant run the same strategies from the fixed [`RNG_SEED`] and pin the totals by
//! class, so a class whose share moved fails there. A test's scripts have a shape ([`Scripts`]):
//! the narrow one above, or the multi-leaf one of `random_scripts_match_mdbx_multi_leaf` (and
//! `_heavy`), seeded with [`MULTI_LEAF_ROWS`] rows under one dup key and as many header keys, the
//! drawn keys and slots spread over them and the dup values in the wide encoding ([`WIDE`]), so
//! the cursor calls run inside multi-leaf nested and composite trees. Every run also counts the
//! dup table's cursor calls run and skipped apart, and fails if the skipped reach a fifth.

use super::{
    adapter::create_mdbx_db, dup::second_value_refused, mdbx_port::create_test_db_with, Form,
};
use crate::{TnLmdbEnv, TnLmdbEnvKind};
use alloy::primitives::{Address, Bytes, B256, U256};
use proptest::{
    prelude::*,
    test_runner::{RngSeed, TestCaseError, TestRunner},
};
use reth_db_api::{
    common::PairResult,
    cursor::{DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW},
    database::Database,
    table::{Table, TableRow},
    tables::{Bytecodes, CanonicalHeaders, PlainStorageState},
    transaction::{DbTx, DbTxMut},
    DatabaseError,
};
use reth_primitives_traits::{Bytecode, StorageEntry};
use reth_storage_errors::db::DatabaseWriteOperation;
use std::{
    cell::RefCell,
    collections::BTreeMap,
    hash::{BuildHasher as _, RandomState},
    mem,
    ops::Bound,
};

/// A row of the model: the key's index and, on `PlainStorageState`, the slot's (else 0). Rows
/// sort as their tables' rows do.
type Row = (u64, u8);

/// A row or value read back: its row, and the version whose write made it (`None`: no write did).
type Item = (Row, Option<u32>);

/// The tables the scripts use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Tb {
    /// `CanonicalHeaders`.
    H,
    /// `PlainStorageState` (dup-sort).
    S,
    /// `Bytecodes`.
    B,
}

impl Tb {
    const fn i(self) -> usize {
        self as usize
    }

    const fn dup(self) -> bool {
        matches!(self, Self::S)
    }

    /// The row a drawn key names: from small sets, so rows collide (16 header keys, 4 dup keys of
    /// 6 slots); with `spread` ([`Scripts::MultiLeaf`]) over the seeded leaves (256 header keys,
    /// 4 dup keys of 256 slots).
    fn row(self, k: K, spread: bool) -> Row {
        match (self, spread) {
            (Self::H, false) => (u64::from(k.0 % 16), 0),
            (Self::H, true) => (u64::from(k.0 % 16) * 16 + u64::from(k.1 % 16), 0),
            (Self::S, false) => (u64::from(k.0 % 4), k.1 % 6),
            (Self::S, true) => (u64::from(k.0 % 4), k.1),
            (Self::B, _) => (u64::from(k.0 % 6), 0),
        }
    }

    /// The row a drawn key names, without its slot: what a key-only call takes.
    fn key_row(self, k: K, spread: bool) -> Row {
        (self.row(k, spread).0, 0)
    }
}

/// A drawn key: a key index and a slot index, each table reading them in its own range.
#[derive(Clone, Copy, Debug)]
struct K(u8, u8);

impl K {
    /// Whether a write of a dup row that exists writes another value for its subkey (class 6): a
    /// key index of 12 or more, which names the same dup key as one under 12, so a quarter of
    /// such writes.
    const fn other_value(self) -> bool {
        self.0 >= 12
    }
}

/// Which value a `delete` passes.
#[derive(Clone, Copy, Debug)]
enum Del {
    /// None: the key (every row of it).
    All,
    /// The row's value, when the row exists.
    Match,
    /// A value no write made.
    Other,
}

/// A drawn cursor call. Keys are drawn ([`K`]); the runner resolves them, and the versions of
/// written values, into a [`Call`].
#[derive(Clone, Copy, Debug)]
enum C {
    First,
    Last,
    Next,
    Prev,
    Current,
    Seek(K),
    SeekExact(K),
    Upsert(K),
    Insert(K),
    /// With `true`, the key after the table's last.
    Append(K, bool),
    DeleteCurrent,
    NextDup,
    PrevDup,
    NextNoDup,
    LastDup,
    NextDupVal,
    SeekByKeySubkey(K),
    /// With `true`, a slot after the key's last.
    AppendDup(K, bool),
    DeleteCurrentDuplicates,
    /// Up to `n` rows.
    Walk(Option<K>, u8),
    /// The end bound is included with `true`.
    WalkRange(Option<K>, Option<(K, bool)>, u8),
    WalkBack(Option<K>, u8),
    /// A key, a slot.
    WalkDup(Option<K>, Option<K>, u8),
}

/// One op of a script.
#[derive(Clone, Copy, Debug)]
enum Op {
    Get(Tb, K),
    Put(Tb, K),
    /// With `true`, the key after the table's last.
    Append(Tb, K, bool),
    Delete(Tb, K, Del),
    Clear(Tb),
    Entries(Tb),
    /// (Re)opens cursor slot 0 or 1 (write), 2 (read) or [`SNAP`] (the read transaction's) on a
    /// table.
    Open(usize, Tb),
    /// A call on a cursor slot; a closed slot opens on the table first.
    Cursor(usize, Tb, C),
    Commit,
    Abort,
    /// (Re)opens the read transaction.
    SnapOpen,
    SnapGet(Tb, K),
    SnapWalk(Tb, Option<K>, u8),
    SnapEntries(Tb),
}

/// A resolved cursor call: rows, and the version of a written value.
#[derive(Clone, Copy, Debug)]
enum Call {
    First,
    Last,
    Next,
    Prev,
    Current,
    Seek(Row),
    SeekExact(Row),
    Upsert(Row, u32),
    Insert(Row, u32),
    Append(Row, u32),
    DeleteCurrent,
    NextDup,
    PrevDup,
    NextNoDup,
    LastDup,
    NextDupVal,
    SeekByKeySubkey(Row),
    AppendDup(Row, u32),
    DeleteCurrentDuplicates,
    Walk(Option<Row>, u8),
    WalkRange(Option<Row>, Bound<Row>, u8),
    WalkBack(Option<Row>, u8),
    WalkDup(Option<u64>, Option<u8>, u8),
}

impl Call {
    /// A call that moves from (or acts on) the cursor's place: skipped on a tainted cursor.
    const fn relative(&self) -> bool {
        matches!(
            self,
            Self::Next
                | Self::Prev
                | Self::Current
                | Self::DeleteCurrent
                | Self::NextDup
                | Self::PrevDup
                | Self::NextNoDup
                | Self::LastDup
                | Self::NextDupVal
                | Self::DeleteCurrentDuplicates
        )
    }
}

/// The skip classes, by the module docs' numbers; the last counts unexpected errors.
const CLASSES: [&str; 9] = [
    "1 past either end",
    "2 reseek after another writer's delete and put",
    "3 another writer changes the cursor's dup key",
    "4 last_dup or delete_current after an outer delete",
    "5 append_dup of the key's last value",
    "6 one value per subkey (a second value, refused on tn-lmdb only)",
    "7 last_dup before the first row",
    "8 a dup key of one row",
    "an unexpected error",
];
const PAST_END: usize = 0;
const RESEEK: usize = 1;
const KEY_CHANGED: usize = 2;
const LAST_DUP_AFTER_OUTER: usize = 3;
const APPEND_DUP_EQUAL: usize = 4;
const SECOND_VALUE: usize = 5;
const LAST_DUP_BEFORE_FIRST: usize = 6;
const ONE_ROW_KEY: usize = 7;
const UNEXPECTED: usize = 8;

/// The lengths of `Bytecodes` values: inline, one overflow page, and runs of pages.
const CODE_SIZES: [usize; 10] = [8, 60, 400, 1000, 1800, 3000, 4100, 9000, 17_000, 40_000];

/// A table of the scripts, with the value each (row, version) writes.
trait TestTable: Table {
    fn key(row: Row) -> Self::Key;
    fn value(row: Row, ver: u32) -> Self::Value;
    /// The row a pair is, and the version whose write made its value.
    fn parse(key: &Self::Key, value: &Self::Value) -> Item;
}

/// The model's key indexes as one byte (keys past 255 are never made).
fn byte(k: u64) -> u8 {
    u8::try_from(k).expect("key index past 255")
}

impl TestTable for CanonicalHeaders {
    fn key(row: Row) -> u64 {
        row.0
    }
    fn value(row: Row, ver: u32) -> B256 {
        let mut b = [0x5a; 32];
        b[..8].copy_from_slice(&row.0.to_be_bytes());
        b[8..12].copy_from_slice(&ver.to_be_bytes());
        B256::from(b)
    }
    fn parse(key: &u64, value: &B256) -> Item {
        let row = (*key, 0);
        let ver = u32::from_be_bytes(value.0[8..12].try_into().expect("4 bytes"));
        (row, (*value == Self::value(row, ver)).then_some(ver))
    }
}

/// The bit every version of a multi-leaf run carries ([`Scripts::MultiLeaf`]): a dup value of
/// such a version takes the wide encoding ([`subkey`], [`PlainStorageState::value`]).
const WIDE: u32 = 1 << 30;

/// The subkey of slot `slot`: `B256` with the slot last; `wide`, the slot first, the other bytes
/// a seek's zeros (the first subkey of the slot, which the stored subkeys of the slot sort at or
/// after) or a stored row's noise ([`PlainStorageState::value`]).
fn subkey(slot: u8, wide: bool) -> B256 {
    let mut key = B256::ZERO;
    key.0[if wide { 0 } else { 31 }] = slot;
    key
}

/// A `PlainStorageState` value read without its key: the value holds its row.
fn parse_entry(value: &StorageEntry) -> Item {
    let n = value.value.as_limbs()[0];
    let ver = (n & 0xffff_ffff) as u32;
    let slot = if ver & WIDE != 0 { value.key.0[0] } else { value.key.0[31] };
    let row = ((n >> 40) & 0xff, slot);
    (row, (*value == PlainStorageState::value(row, ver)).then_some(ver))
}

impl TestTable for PlainStorageState {
    fn key(row: Row) -> Address {
        Address::with_last_byte(byte(row.0))
    }
    /// A version with [`WIDE`] takes the wide encoding: the subkey's bytes past the slot are
    /// noise of the row's (so subkeys differ past their first byte, and sort by slot), and the
    /// value's high limbs, set by the version, make its rest from a few bytes to 32.
    fn value(row: Row, ver: u32) -> StorageEntry {
        let n = (row.0 << 40) | (u64::from(row.1) << 32) | u64::from(ver);
        if ver & WIDE == 0 {
            return StorageEntry { key: B256::with_last_byte(row.1), value: U256::from(n) };
        }
        let mut key = subkey(row.1, true);
        for (i, b) in key.0[1..].iter_mut().enumerate() {
            *b = byte(row.0).wrapping_mul(37).wrapping_add(row.1).wrapping_add(i as u8);
        }
        let mut limbs = [n, 0, 0, 0];
        for (i, limb) in limbs[1..].iter_mut().enumerate() {
            if i < (ver % 4) as usize {
                *limb =
                    u64::from(ver).wrapping_mul(0x9e37_79b9_7f4a_7c15).rotate_left(i as u32 * 7)
                        | 1;
            }
        }
        StorageEntry { key, value: U256::from_limbs(limbs) }
    }
    fn parse(key: &Address, value: &StorageEntry) -> Item {
        let (row, ver) = parse_entry(value);
        let row = (u64::from(key.0[19]), row.1);
        (row, ver.filter(|&v| *key == Self::key(row) && *value == Self::value(row, v)))
    }
}

impl TestTable for Bytecodes {
    fn key(row: Row) -> B256 {
        B256::with_last_byte(byte(row.0))
    }
    fn value(row: Row, ver: u32) -> Bytecode {
        let len = CODE_SIZES[(ver as usize + row.0 as usize) % CODE_SIZES.len()];
        let mut code: Vec<u8> =
            (0..len as u32).map(|i| i.wrapping_mul(131).wrapping_add(ver) as u8).collect();
        code[..4].copy_from_slice(&ver.to_be_bytes());
        Bytecode::new_raw(Bytes::from(code))
    }
    fn parse(key: &B256, value: &Bytecode) -> Item {
        let row = (u64::from(key.0[31]), 0);
        let code = value.0.original_byte_slice();
        let ver = code.get(..4).map(|b| u32::from_be_bytes(b.try_into().expect("4 bytes")));
        (row, ver.filter(|&v| *key == Self::key(row) && *value == Self::value(row, v)))
    }
}

/// Runs `$e` with `$t` the table type `$tb` names.
macro_rules! by_table {
    ($tb:expr, $t:ident => $e:expr) => {
        match $tb {
            Tb::H => {
                type $t = CanonicalHeaders;
                $e
            }
            Tb::S => {
                type $t = PlainStorageState;
                $e
            }
            Tb::B => {
                type $t = Bytecodes;
                $e
            }
        }
    };
}

/// What a cursor call returned.
#[derive(Debug)]
enum Res {
    Item(Result<Option<Item>, DatabaseError>),
    Done(Result<(), DatabaseError>),
    Walk(Vec<Result<Item, DatabaseError>>),
}

fn item<T: TestTable>(result: PairResult<T>) -> Result<Option<Item>, DatabaseError> {
    result.map(|row| row.map(|(k, v)| T::parse(&k, &v)))
}

fn walked<T: TestTable>(
    walker: impl Iterator<Item = Result<TableRow<T>, DatabaseError>>,
    n: u8,
) -> Vec<Result<Item, DatabaseError>> {
    walker.take(n.into()).map(|row| row.map(|(k, v)| T::parse(&k, &v))).collect()
}

fn walk_res<W>(
    walker: Result<W, DatabaseError>,
    rows: impl FnOnce(W) -> Vec<Result<Item, DatabaseError>>,
) -> Res {
    match walker {
        Ok(w) => Res::Walk(rows(w)),
        Err(e) => Res::Item(Err(e)),
    }
}

fn exec_ro<T: TestTable, Cur: DbCursorRO<T>>(c: &mut Cur, call: &Call) -> Res {
    let start = |r: Option<Row>| r.map(T::key);
    match *call {
        Call::First => Res::Item(item::<T>(c.first())),
        Call::Last => Res::Item(item::<T>(c.last())),
        Call::Next => Res::Item(item::<T>(c.next())),
        Call::Prev => Res::Item(item::<T>(c.prev())),
        Call::Current => Res::Item(item::<T>(c.current())),
        Call::Seek(r) => Res::Item(item::<T>(c.seek(T::key(r)))),
        Call::SeekExact(r) => Res::Item(item::<T>(c.seek_exact(T::key(r)))),
        Call::Walk(s, n) => walk_res(c.walk(start(s)), |w| walked::<T>(w, n)),
        Call::WalkBack(s, n) => walk_res(c.walk_back(start(s)), |w| walked::<T>(w, n)),
        Call::WalkRange(s, end, n) => {
            let from = s.map_or(Bound::Unbounded, |r| Bound::Included(T::key(r)));
            walk_res(c.walk_range((from, end.map(T::key))), |w| walked::<T>(w, n))
        }
        _ => unreachable!("{call:?} is no read of a cursor without duplicates"),
    }
}

fn exec_rw<T: TestTable, Cur: DbCursorRO<T> + DbCursorRW<T>>(c: &mut Cur, call: &Call) -> Res {
    match *call {
        Call::Upsert(r, v) => Res::Done(c.upsert(T::key(r), &T::value(r, v))),
        Call::Insert(r, v) => Res::Done(c.insert(T::key(r), &T::value(r, v))),
        Call::Append(r, v) => Res::Done(c.append(T::key(r), &T::value(r, v))),
        Call::DeleteCurrent => Res::Done(c.delete_current()),
        _ => exec_ro::<T, Cur>(c, call),
    }
}

type S = PlainStorageState;

fn entry_res(result: Result<Option<StorageEntry>, DatabaseError>) -> Res {
    Res::Item(result.map(|v| v.as_ref().map(parse_entry)))
}

/// `wide`: the subkeys sought take the wide encoding ([`subkey`]).
fn exec_dup<Cur: DbCursorRO<S> + DbDupCursorRO<S>>(c: &mut Cur, call: &Call, wide: bool) -> Res {
    match *call {
        Call::NextDup => Res::Item(item::<S>(c.next_dup())),
        Call::PrevDup => Res::Item(item::<S>(c.prev_dup())),
        Call::NextNoDup => Res::Item(item::<S>(c.next_no_dup())),
        Call::LastDup => entry_res(c.last_dup()),
        Call::NextDupVal => entry_res(c.next_dup_val()),
        Call::SeekByKeySubkey(r) => entry_res(c.seek_by_key_subkey(S::key(r), subkey(r.1, wide))),
        Call::WalkDup(key, slot, n) => {
            walk_res(c.walk_dup(key.map(|k| S::key((k, 0))), slot.map(|s| subkey(s, wide))), |w| {
                walked::<S>(w, n)
            })
        }
        _ => exec_ro::<S, Cur>(c, call),
    }
}

fn exec_dup_rw<Cur>(c: &mut Cur, call: &Call, wide: bool) -> Res
where
    Cur: DbCursorRO<S> + DbCursorRW<S> + DbDupCursorRO<S> + DbDupCursorRW<S>,
{
    match *call {
        Call::AppendDup(r, v) => Res::Done(c.append_dup(S::key(r), S::value(r, v))),
        Call::DeleteCurrentDuplicates => Res::Done(c.delete_current_duplicates()),
        Call::Upsert(..) | Call::Insert(..) | Call::Append(..) | Call::DeleteCurrent => {
            exec_rw::<S, Cur>(c, call)
        }
        _ => exec_dup::<Cur>(c, call, wide),
    }
}

/// A cursor of the write transaction, by table; `Ro` from `cursor_read` (slot 2).
enum AnyCursor<TX: DbTxMut + DbTx> {
    H(TX::CursorMut<CanonicalHeaders>),
    S(TX::DupCursorMut<PlainStorageState>),
    B(TX::CursorMut<Bytecodes>),
    Ro(AnyReadCursor<TX>),
}

/// A read cursor (`cursor_read`, `cursor_dup_read`), by table: the write transaction's (slot 2)
/// or the read transaction's (slot [`SNAP`]).
enum AnyReadCursor<TX: DbTx> {
    H(TX::Cursor<CanonicalHeaders>),
    S(TX::DupCursor<PlainStorageState>),
    B(TX::Cursor<Bytecodes>),
}

impl<TX: DbTx> AnyReadCursor<TX> {
    fn open(tx: &TX, tb: Tb) -> Result<Self, DatabaseError> {
        Ok(match tb {
            Tb::H => Self::H(tx.cursor_read()?),
            Tb::S => Self::S(tx.cursor_dup_read()?),
            Tb::B => Self::B(tx.cursor_read()?),
        })
    }

    fn exec(&mut self, call: &Call, wide: bool) -> Res {
        match self {
            Self::H(c) => exec_ro::<CanonicalHeaders, _>(c, call),
            Self::S(c) => exec_dup(c, call, wide),
            Self::B(c) => exec_ro::<Bytecodes, _>(c, call),
        }
    }
}

/// The cursor slot of the read transaction's cursor; 0 and 1 are the write transaction's write
/// cursors, 2 its read cursor.
const SNAP: usize = 3;

/// Why a cursor's relative calls are skipped: the skip class, and whether the cursor is before
/// the first row (where libmdbx's `last_dup` aborts).
#[derive(Clone, Copy, Debug)]
struct Taint {
    class: usize,
    before_start: bool,
}

/// The model of a cursor.
#[derive(Clone, Debug)]
struct CurModel {
    tb: Tb,
    /// Not moved yet.
    fresh: bool,
    /// Left unset by a missed `seek_exact` or `seek_by_key_subkey`.
    missed: bool,
    /// While `missed`: the row the miss landed on (the first at or after the row sought), where
    /// MDBX's unset cursor sits, or the row after it another writer's delete moved it to; `None`
    /// past the table's last row.
    landing: Option<Row>,
    /// The row the cursor is on: after its own delete, the row after the deleted one (on a dup
    /// table, the key's row before when the key's last row went and earlier ones stay).
    row: Option<Row>,
    /// On the row `last` (or a fresh `prev`, a refused append, or an append of the table's last
    /// key, or on the dup table of its last row) placed it on: `next` finds nothing and the cursor
    /// stays.
    soft_end: bool,
    /// Another writer deleted the cursor's row (this one) since its last call.
    pending: Option<Row>,
    /// Its last call deleted a dup key's only row or a key's rows.
    outer_deleted: bool,
    taint: Option<Taint>,
}

impl CurModel {
    const fn new(tb: Tb) -> Self {
        Self {
            tb,
            fresh: true,
            missed: false,
            landing: None,
            row: None,
            soft_end: false,
            pending: None,
            outer_deleted: false,
            taint: None,
        }
    }

    fn place(&mut self, row: Row, soft_end: bool) {
        self.row = Some(row);
        self.soft_end = soft_end;
        self.missed = false;
        self.taint = None;
    }

    fn taint(&mut self, class: usize, before_start: bool) {
        self.taint = Some(Taint { class, before_start });
        self.row = None;
        self.soft_end = false;
        self.missed = false;
    }
}

/// One op's outcome: the op as the runner resolved it, and what the database returned.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Outcome {
    op: String,
    result: String,
}

/// A script's run on one database.
#[derive(Debug, Default)]
struct Run {
    outcomes: Vec<Outcome>,
    /// Skipped ops, by class ([`CLASSES`]).
    skips: [u64; 9],
    /// Ops run.
    ran: u64,
    /// The dup table's cursor calls run, and skipped (by a class, or a second value): the
    /// cursor calls the dup layouts get, kept apart so a run whose dup cursors mostly sit in a
    /// skip class shows ([`differential_cases`]).
    dup_cursor_ran: u64,
    dup_cursor_skipped: u64,
    /// Where the model disagreed with the database: a test bug when MDBX is the database, a
    /// behaviour difference the outcomes did not show when tn-lmdb is.
    model_misses: Vec<String>,
}

type Tables = [BTreeMap<Row, u32>; 3];

/// Runs scripts on one database.
struct Runner<'d, DB: Database> {
    db: &'d DB,
    /// Run the writes of a second value for a dup row's subkey, which must be refused (tn-lmdb);
    /// else skip them (MDBX, which would store the second value).
    second_values: bool,
    /// The scripts' shape: whether drawn keys spread over the seeded leaves ([`Tb::row`]) and dup
    /// values take the wide encoding ([`WIDE`]).
    scripts: Scripts,
    tx: Option<DB::TXMut>,
    cursors: [Option<AnyCursor<DB::TXMut>>; 3],
    /// The cursors' models, by slot ([`SNAP`] last).
    models: [Option<CurModel>; 4],
    snap: Option<DB::TX>,
    /// The read transaction's cursor (slot [`SNAP`]).
    snap_cursor: Option<AnyReadCursor<DB::TX>>,
    model: Tables,
    /// The read transaction's tables: what was committed when it opened.
    snap_model: Tables,
    committed: Tables,
    ver: u32,
    run: Run,
}

fn show_item(item: &Item) -> String {
    let ((k, s), ver) = *item;
    ver.map_or_else(|| format!("{k}/{s} BAD"), |v| format!("{k}/{s} v{v}"))
}

fn show_res(res: &Res) -> String {
    match res {
        Res::Item(Ok(Some(it))) => show_item(it),
        Res::Item(Ok(None)) => "none".into(),
        Res::Item(Err(e)) | Res::Done(Err(e)) => format!("{e:?}"),
        Res::Done(Ok(())) => "ok".into(),
        Res::Walk(rows) => {
            let rows: Vec<_> = rows
                .iter()
                .map(|r| r.as_ref().map_or_else(|e| format!("{e:?}"), show_item))
                .collect();
            format!("[{}]", rows.join(", "))
        }
    }
}

fn show_done<E: std::fmt::Debug>(result: &Result<impl std::fmt::Debug, E>) -> String {
    result.as_ref().map_or_else(|e| format!("{e:?}"), |v| format!("{v:?}"))
}

/// Puts a few rows in every table and commits, so scripts start on populated tables.
const SEED: [Op; 10] = [
    Op::Put(Tb::H, K(2, 0)),
    Op::Put(Tb::H, K(5, 0)),
    Op::Put(Tb::H, K(9, 0)),
    Op::Put(Tb::S, K(1, 1)),
    Op::Put(Tb::S, K(1, 3)),
    Op::Put(Tb::S, K(2, 2)),
    Op::Put(Tb::S, K(3, 0)),
    Op::Put(Tb::B, K(1, 0)),
    Op::Put(Tb::B, K(3, 0)),
    Op::Commit,
];

/// The rows under one dup key, and the header keys, [`multi_leaf_seed`] puts: enough rows for
/// the key's nested tree (52-byte stored keys, rests up to 32 bytes) and the headers' tree
/// (8-byte keys, 32-byte values) to take several leaves at 4 KiB pages.
const MULTI_LEAF_ROWS: u8 = 250;

/// Puts [`MULTI_LEAF_ROWS`] rows under dup key 1 (slots 0 up), as many header keys (spread,
/// [`Tb::row`]) and two bytecodes, and commits: the scripts of [`Scripts::MultiLeaf`] then run
/// their cursor writes and moves inside multi-leaf nested trees and composite trees.
fn multi_leaf_seed() -> Vec<Op> {
    let slots = (0..MULTI_LEAF_ROWS).map(|s| Op::Put(Tb::S, K(1, s)));
    let headers = (0..MULTI_LEAF_ROWS).map(|n| Op::Put(Tb::H, K(n / 16, n % 16)));
    let codes = [Op::Put(Tb::B, K(1, 0)), Op::Put(Tb::B, K(3, 0)), Op::Commit];
    slots.chain(headers).chain(codes).collect()
}

/// The scripts a test draws: [`SEED`]'s few rows, with keys and slots from small sets so rows
/// collide; or the multi-leaf scripts, seeded by [`multi_leaf_seed`], their drawn keys and slots
/// spread over the seeded leaves ([`Tb::row`]) and their dup values in the wide encoding
/// ([`WIDE`]: subkeys that differ past their first byte, rests of several lengths).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scripts {
    Narrow,
    MultiLeaf,
}

impl Scripts {
    /// Whether drawn keys spread over the seeded leaves, and dup values take the wide encoding.
    const fn spread(self) -> bool {
        matches!(self, Self::MultiLeaf)
    }

    /// The ops that seed the tables before a script.
    fn seed(self) -> Vec<Op> {
        match self {
            Self::Narrow => SEED.to_vec(),
            Self::MultiLeaf => multi_leaf_seed(),
        }
    }
}

/// Runs `ops` (after `scripts`'s seed) on `db`, then commits and reads every table back; with
/// `second_values`, the writes of a second value for a dup row's subkey too ([`Runner`]).
fn run<DB: Database>(db: &DB, scripts: Scripts, ops: &[Op], second_values: bool) -> Run {
    let mut r = Runner {
        db,
        second_values,
        scripts,
        tx: Some(db.tx_mut().expect("begin")),
        cursors: [None, None, None],
        models: [None, None, None, None],
        snap: None,
        snap_cursor: None,
        model: Default::default(),
        snap_model: Default::default(),
        committed: Default::default(),
        ver: if scripts.spread() { WIDE } else { 0 },
        run: Run::default(),
    };
    for op in scripts.seed().iter().chain(ops) {
        r.op(*op);
    }
    r.op(Op::Commit);
    r.snap_cursor = None;
    r.snap = None;
    let tx = db.tx().expect("read");
    for tb in [Tb::H, Tb::S, Tb::B] {
        let rows = by_table!(tb, T => walk_res(tx.cursor_read::<T>().and_then(|mut c| {
            c.walk(None).map(|w| walked::<T>(w, u8::MAX))
        }), |rows| rows));
        r.push(format!("final {tb:?}"), show_res(&rows));
    }
    r.run
}

impl<DB: Database> Runner<'_, DB> {
    fn tx(&self) -> &DB::TXMut {
        self.tx.as_ref().expect("a write transaction")
    }

    fn push(&mut self, op: String, result: String) {
        self.run.outcomes.push(Outcome { op, result });
    }

    fn miss(&mut self, what: String) {
        let at = self.run.outcomes.len();
        self.run.model_misses.push(format!("outcome {at}: {what}"));
    }

    fn rows(&self, tb: Tb) -> &BTreeMap<Row, u32> {
        &self.model[tb.i()]
    }

    /// The first row after `row`.
    fn after(&self, tb: Tb, row: Row) -> Option<Row> {
        self.rows(tb).range((Bound::Excluded(row), Bound::Unbounded)).next().map(|(r, _)| *r)
    }

    /// The first row at or after `row`.
    fn at_or_after(&self, tb: Tb, row: Row) -> Option<Row> {
        self.rows(tb).range(row..).next().map(|(r, _)| *r)
    }

    /// Key `k`'s rows.
    fn key_rows(&self, tb: Tb, k: u64) -> impl DoubleEndedIterator<Item = Row> + '_ {
        self.rows(tb).range((k, 0)..=(k, u8::MAX)).map(|(r, _)| *r)
    }

    /// A new version; on a dup table, when the row exists, the version it holds (MDBX keeps the
    /// pair as it is), unless `other` asks for another: a second value for its subkey (class 6).
    fn write_ver(&mut self, tb: Tb, row: Row, other: bool) -> u32 {
        if let Some(&v) = self.rows(tb).get(&row).filter(|_| tb.dup() && !other) {
            return v;
        }
        self.ver += 1;
        self.ver
    }

    /// Whether a write of version `ver` of `row` (by an append with `append`) gives the row's
    /// subkey a second value (class 6): on the dup table, the row holds another version, and an
    /// append names the table's last key (one of an earlier key is refused for its order on both
    /// databases, and is run on both).
    fn second_value(&self, tb: Tb, row: Row, ver: u32, append: bool) -> bool {
        let last_key = || self.rows(tb).last_key_value().is_some_and(|(&(k, _), _)| k == row.0);
        tb.dup()
            && self.rows(tb).get(&row).is_some_and(|&held| held != ver)
            && (!append || last_key())
    }

    /// A write of a second value for a dup row's subkey, `label`, by `write` with `operation` of
    /// the row of key `key` (class 6). MDBX would store it beside the first, so it is skipped
    /// there; tn-lmdb must refuse it ([`second_value_refused`]) and change nothing, which the ops
    /// after it check against MDBX, which never ran it. Its outcome reads as the skip on both,
    /// unless tn-lmdb's result is not that refusal.
    fn second_value_write(
        &mut self,
        label: String,
        key: Address,
        operation: DatabaseWriteOperation,
        write: impl FnOnce(&mut Self) -> Result<(), DatabaseError>,
    ) {
        self.run.skips[SECOND_VALUE] += 1;
        let mut result = format!("skipped: {}", CLASSES[SECOND_VALUE]);
        if self.second_values {
            let res = write(self);
            if !second_value_refused(&res, operation, key) {
                result = show_done(&res);
            }
        }
        self.push(label, result);
    }

    /// The row after the table's last (on the dup table, mostly a slot after it), for appends in
    /// order.
    fn next_row(&self, tb: Tb, k: K) -> Row {
        let spread = self.scripts.spread();
        let Some((&(lk, ls), _)) = self.rows(tb).last_key_value() else { return tb.row(k, spread) };
        let step = u64::from(k.0 % 2) + 1;
        match tb {
            Tb::S if !k.1.is_multiple_of(3) && ls < u8::MAX => (lk, ls + 1),
            Tb::S => ((lk + 1).min(255), k.1 % 6),
            Tb::H => (lk + step, 0),
            Tb::B => ((lk + step).min(255), 0),
        }
    }

    fn op(&mut self, op: Op) {
        let spread = self.scripts.spread();
        match op {
            Op::Get(tb, k) => {
                let row = tb.key_row(k, spread);
                let got = by_table!(tb, T => self.tx().get::<T>(T::key(row))
                    .map(|v| v.map(|v| T::parse(&T::key(row), &v))));
                self.run.ran += 1;
                self.push(format!("get {tb:?} {row:?}"), show_res(&Res::Item(got)));
            }
            Op::Put(tb, k) => {
                let row = tb.row(k, spread);
                let ver = self.write_ver(tb, row, k.other_value());
                let label = format!("put {tb:?} {row:?} v{ver}");
                if self.second_value(tb, row, ver, false) {
                    self.second_value_write(
                        label,
                        S::key(row),
                        DatabaseWriteOperation::PutUpsert,
                        |r| r.tx().put::<S>(S::key(row), S::value(row, ver)),
                    );
                    return;
                }
                let res = by_table!(tb, T => self.tx().put::<T>(T::key(row), T::value(row, ver)));
                self.run.ran += 1;
                if res.is_ok() {
                    self.written(None, tb, row, ver);
                }
                self.push(label, show_done(&res));
            }
            Op::Append(tb, k, in_order) => {
                let row = if in_order { self.next_row(tb, k) } else { tb.row(k, spread) };
                let ver = self.write_ver(tb, row, k.other_value());
                let label = format!("append {tb:?} {row:?} v{ver}");
                if self.second_value(tb, row, ver, true) {
                    self.second_value_write(
                        label,
                        S::key(row),
                        DatabaseWriteOperation::PutAppend,
                        |r| r.tx().append::<S>(S::key(row), S::value(row, ver)),
                    );
                    return;
                }
                let res =
                    by_table!(tb, T => self.tx().append::<T>(T::key(row), T::value(row, ver)));
                self.run.ran += 1;
                if res.is_ok() {
                    self.written(None, tb, row, ver);
                }
                self.push(label, show_done(&res));
            }
            Op::Delete(tb, k, del) => self.delete(tb, k, del),
            Op::Clear(tb) => {
                let res = by_table!(tb, T => self.tx().clear::<T>());
                self.run.ran += 1;
                if res.is_ok() {
                    self.model[tb.i()].clear();
                    for m in self.models.iter_mut().flatten().filter(|m| m.tb == tb) {
                        m.taint(PAST_END, false);
                    }
                }
                self.push(format!("clear {tb:?}"), show_done(&res));
            }
            Op::Entries(tb) => {
                let res = by_table!(tb, T => self.tx().entries::<T>());
                self.run.ran += 1;
                if res.as_ref().is_ok_and(|&n| n != self.rows(tb).len()) {
                    self.miss(format!("entries {tb:?}: {res:?}"));
                }
                self.push(format!("entries {tb:?}"), show_done(&res));
            }
            Op::Open(slot, tb) => self.open(slot, tb),
            Op::Cursor(slot, tb, c) => self.cursor(slot, tb, c),
            Op::Commit => {
                self.close_cursors();
                let res = self.tx.take().expect("a write transaction").commit();
                self.run.ran += 1;
                if res.is_ok() {
                    self.committed = self.model.clone();
                } else {
                    self.model = self.committed.clone();
                }
                self.push("commit".into(), show_done(&res));
                self.check_model();
                self.tx = Some(self.db.tx_mut().expect("begin"));
            }
            Op::Abort => {
                self.close_cursors();
                self.tx.take().expect("a write transaction").abort();
                self.run.ran += 1;
                self.model = self.committed.clone();
                self.push("abort".into(), "ok".into());
                self.tx = Some(self.db.tx_mut().expect("begin"));
            }
            Op::SnapOpen => self.snap_open(),
            Op::SnapGet(tb, k) => {
                let row = tb.key_row(k, spread);
                let snap = self.snap();
                let got = by_table!(tb, T => snap.get::<T>(T::key(row))
                    .map(|v| v.map(|v| T::parse(&T::key(row), &v))));
                self.run.ran += 1;
                self.push(format!("snap get {tb:?} {row:?}"), show_res(&Res::Item(got)));
            }
            Op::SnapWalk(tb, start, n) => {
                let start = start.map(|k| tb.key_row(k, spread));
                let snap = self.snap();
                let rows = by_table!(tb, T => walk_res(snap.cursor_read::<T>().and_then(|mut c| {
                    c.walk(start.map(T::key)).map(|w| walked::<T>(w, n))
                }), |rows| rows));
                self.run.ran += 1;
                self.push(format!("snap walk {tb:?} {start:?} {n}"), show_res(&rows));
            }
            Op::SnapEntries(tb) => {
                let snap = self.snap();
                let res = by_table!(tb, T => snap.entries::<T>());
                self.run.ran += 1;
                self.push(format!("snap entries {tb:?}"), show_done(&res));
            }
        }
    }

    fn snap_open(&mut self) {
        self.snap_cursor = None;
        self.models[SNAP] = None;
        self.snap = None;
        let snap = self.db.tx();
        self.run.ran += 1;
        self.push("snap open".into(), show_done(&snap.as_ref().map(drop)));
        self.snap = snap.ok();
        self.snap_model = self.committed.clone();
    }

    /// The read transaction, opened here if none is.
    fn snap(&mut self) -> &DB::TX {
        if self.snap.is_none() {
            self.snap_open();
        }
        self.snap.as_ref().expect("a read transaction")
    }

    /// Closes the write transaction's cursors; the read transaction's stays.
    fn close_cursors(&mut self) {
        self.cursors = [None, None, None];
        self.models[..SNAP].fill(None);
    }

    /// Reads every table in a new read transaction and checks the model holds the same rows.
    fn check_model(&mut self) {
        let tx = self.db.tx().expect("read");
        for tb in [Tb::H, Tb::S, Tb::B] {
            let rows = by_table!(tb, T => tx.cursor_read::<T>().and_then(|mut c| {
                c.walk(None)?.map(|r| r.map(|(k, v)| T::parse(&k, &v))).collect::<Result<Vec<_>, _>>()
            }));
            let model: Vec<Item> = self.rows(tb).iter().map(|(&r, &v)| (r, Some(v))).collect();
            if rows.as_ref().ok() != Some(&model) {
                self.miss(format!("{tb:?} holds {rows:?}, the model {model:?}"));
            }
        }
    }

    /// The transaction (`by` `None`) or cursor `by` wrote `row` with version `ver`.
    fn written(&mut self, by: Option<usize>, tb: Tb, row: Row, ver: u32) {
        self.model[tb.i()].insert(row, ver);
        self.others_on_put(by, tb, row);
    }

    /// On the dup table, the row of `row`'s key when the key has one row and it is not `row`: a
    /// write of `row` gives the key a second.
    fn only_other_row(&self, tb: Tb, row: Row) -> Option<Row> {
        let rows: Vec<Row> = self.key_rows(tb, row.0).take(2).collect();
        match rows.as_slice() {
            [only] if tb.dup() && *only != row => Some(*only),
            _ => None,
        }
    }

    fn delete(&mut self, tb: Tb, k: K, del: Del) {
        let spread = self.scripts.spread();
        let row = if matches!(del, Del::All) { tb.key_row(k, spread) } else { tb.row(k, spread) };
        let other = 0x8000_0000 | self.ver;
        let ver = match del {
            Del::All => None,
            Del::Match => Some(self.rows(tb).get(&row).copied().unwrap_or(other)),
            Del::Other => Some(other),
        };
        let res =
            by_table!(tb, T => self.tx().delete::<T>(T::key(row), ver.map(|v| T::value(row, v))));
        self.run.ran += 1;
        let removed: Vec<Row> = match ver {
            None => self.key_rows(tb, row.0).collect(),
            Some(v) => self
                .rows(tb)
                .get(&row)
                .filter(|&&have| have == v)
                .map(|_| row)
                .into_iter()
                .collect(),
        };
        if res.as_ref().is_ok_and(|&gone| gone == removed.is_empty()) {
            self.miss(format!("delete {tb:?} {row:?} {ver:?}: {res:?}, the model {removed:?}"));
        }
        if res.is_ok() {
            for r in &removed {
                self.model[tb.i()].remove(r);
            }
            self.others_on_delete(None, tb, &removed);
        }
        let label = match ver {
            None => format!("delete {tb:?} {row:?}"),
            Some(v) => format!("delete {tb:?} {row:?} v{v}"),
        };
        self.push(label, show_done(&res));
    }

    /// Another writer than cursor `by` (`None`: the transaction) deleted `removed` from `tb`. The
    /// read transaction's cursor ([`SNAP`]) reads a snapshot no writer changes.
    fn others_on_delete(&mut self, by: Option<usize>, tb: Tb, removed: &[Row]) {
        for j in 0..SNAP {
            let Some(m) = self.models[j].as_ref().filter(|m| m.tb == tb && Some(j) != by) else {
                continue;
            };
            if m.missed {
                // MDBX's unset cursor moves to the follower when another writer deletes the row
                // it landed on; with none (or past the last row already) it is then at the end
                // (class 1).
                let moved = m.landing.map(|l| (removed.contains(&l), self.after(tb, l)));
                let m = self.models[j].as_mut().expect("open");
                match moved {
                    Some((false, _)) => {}
                    Some((true, Some(f))) => m.landing = Some(f),
                    _ => m.taint(PAST_END, false),
                }
                continue;
            }
            let Some(r) = m.row else { continue };
            let follower = self.after(tb, r);
            let m = self.models[j].as_mut().expect("open");
            if tb.dup() && removed.iter().any(|x| x.0 == r.0) {
                m.taint(KEY_CHANGED, false);
                continue;
            }
            if !removed.contains(&r) {
                continue;
            }
            match follower {
                None => m.taint(PAST_END, false),
                Some(f) => {
                    m.pending = m.pending.or(Some(r));
                    m.row = Some(f);
                    m.soft_end = false;
                }
            }
        }
    }

    /// Another writer than cursor `by` put `row` in `tb` (not seen by [`SNAP`]'s snapshot).
    fn others_on_put(&mut self, by: Option<usize>, tb: Tb, row: Row) {
        for j in 0..SNAP {
            let Some(m) = self.models[j].as_ref().filter(|m| m.tb == tb && Some(j) != by) else {
                continue;
            };
            // Ended on a dup key of one row, as after `last`, with a later key put (class 8).
            let ended_alone = tb.dup()
                && m.soft_end
                && m.row.is_some_and(|r| r.0 < row.0 && self.key_rows(tb, r.0).count() == 1);
            let m = self.models[j].as_mut().expect("open");
            if tb.dup() && m.row.is_some_and(|r| r.0 == row.0) {
                m.taint(KEY_CHANGED, false);
                continue;
            }
            if ended_alone {
                m.taint(ONE_ROW_KEY, false);
                continue;
            }
            if let (Some(p), Some(f)) = (m.pending, m.row) {
                if p <= row && row < f {
                    m.taint(RESEEK, false);
                }
            }
        }
    }

    fn open(&mut self, slot: usize, tb: Tb) {
        if slot == SNAP {
            // The old cursor goes back to the transaction's pool, and the new one may take it.
            self.snap_cursor = None;
            let cursor = AnyReadCursor::open(self.snap(), tb);
            self.run.ran += 1;
            self.push(format!("open c{slot} {tb:?}"), show_done(&cursor.as_ref().map(drop)));
            self.snap_cursor = cursor.ok();
            self.models[slot] = Some(CurModel::new(tb));
            return;
        }
        self.cursors[slot] = None;
        let tx = self.tx();
        let cursor = match (slot, tb) {
            (2, _) => AnyReadCursor::open(tx, tb).map(AnyCursor::Ro),
            (_, Tb::H) => tx.cursor_write::<CanonicalHeaders>().map(AnyCursor::H),
            (_, Tb::S) => tx.cursor_dup_write::<PlainStorageState>().map(AnyCursor::S),
            (_, Tb::B) => tx.cursor_write::<Bytecodes>().map(AnyCursor::B),
        };
        self.run.ran += 1;
        self.push(format!("open c{slot} {tb:?}"), show_done(&cursor.as_ref().map(drop)));
        self.cursors[slot] = cursor.ok();
        self.models[slot] = Some(CurModel::new(tb));
    }

    /// Resolves a drawn call on a cursor of `tb`.
    fn resolve(&mut self, tb: Tb, c: C) -> Call {
        let spread = self.scripts.spread();
        let key = |k: K| tb.key_row(k, spread);
        if !tb.dup() {
            // A dup method on a plain table is its plain counterpart.
            let plain = match c {
                C::NextDup | C::NextNoDup | C::NextDupVal => Some(C::Next),
                C::PrevDup => Some(C::Prev),
                C::LastDup => Some(C::Last),
                C::SeekByKeySubkey(k) => Some(C::SeekExact(k)),
                C::AppendDup(k, in_order) => Some(C::Append(k, in_order)),
                C::DeleteCurrentDuplicates => Some(C::DeleteCurrent),
                C::WalkDup(k, _, n) => Some(C::Walk(k, n)),
                _ => None,
            };
            if let Some(c) = plain {
                return self.resolve(tb, c);
            }
        }
        match c {
            C::First => Call::First,
            C::Last => Call::Last,
            C::Next => Call::Next,
            C::Prev => Call::Prev,
            C::Current => Call::Current,
            C::Seek(k) => Call::Seek(key(k)),
            C::SeekExact(k) => Call::SeekExact(key(k)),
            C::Upsert(k) => {
                let row = tb.row(k, spread);
                Call::Upsert(row, self.write_ver(tb, row, k.other_value()))
            }
            C::Insert(k) => {
                let row = tb.row(k, spread);
                Call::Insert(row, self.write_ver(tb, row, false))
            }
            C::Append(k, in_order) => {
                let row = if in_order { self.next_row(tb, k) } else { tb.row(k, spread) };
                Call::Append(row, self.write_ver(tb, row, k.other_value()))
            }
            C::DeleteCurrent => Call::DeleteCurrent,
            C::NextDup => Call::NextDup,
            C::PrevDup => Call::PrevDup,
            C::NextNoDup => Call::NextNoDup,
            C::LastDup => Call::LastDup,
            C::NextDupVal => Call::NextDupVal,
            C::SeekByKeySubkey(k) => Call::SeekByKeySubkey(tb.row(k, spread)),
            C::AppendDup(k, in_order) => {
                let mut row = tb.row(k, spread);
                if in_order {
                    if let Some(last) = self.key_rows(tb, row.0).next_back() {
                        row.1 = last.1.saturating_add(1);
                    }
                }
                Call::AppendDup(row, self.write_ver(tb, row, false))
            }
            C::DeleteCurrentDuplicates => Call::DeleteCurrentDuplicates,
            C::Walk(s, n) => Call::Walk(s.map(key), n),
            C::WalkRange(s, end, n) => {
                let end = match end {
                    None => Bound::Unbounded,
                    Some((k, true)) => Bound::Included(key(k)),
                    Some((k, false)) => Bound::Excluded(key(k)),
                };
                Call::WalkRange(s.map(key), end, n)
            }
            C::WalkBack(s, n) => Call::WalkBack(s.map(key), n),
            C::WalkDup(k, s, n) => {
                Call::WalkDup(k.map(|k| tb.row(k, spread).0), s.map(|s| tb.row(s, spread).1), n)
            }
        }
    }

    /// The class of the exclusion `call` on cursor `slot` falls in, if any.
    fn excluded(&self, slot: usize, call: &Call) -> Option<usize> {
        let m = self.models[slot].as_ref().expect("open");
        if let Some(t) = m.taint.filter(|_| call.relative()) {
            let last_dup = matches!(call, Call::LastDup);
            return Some(if t.before_start && last_dup { LAST_DUP_BEFORE_FIRST } else { t.class });
        }
        if let Call::AppendDup(row, _) = *call {
            let rows: Vec<Row> = self.key_rows(m.tb, row.0).collect();
            if rows.len() >= 2 && rows.last().is_some_and(|last| last.1 == row.1) {
                return Some(APPEND_DUP_EQUAL);
            }
        }
        None
    }

    fn cursor(&mut self, slot: usize, hint: Tb, c: C) {
        if slot == SNAP {
            if self.snap_cursor.is_none() {
                self.open(slot, hint);
            }
            // The cursor reads the read transaction's snapshot: for its call the model's tables
            // are the snapshot's, which no writer changes (its calls are reads only).
            mem::swap(&mut self.model, &mut self.snap_model);
            self.cursor_call(slot, c);
            mem::swap(&mut self.model, &mut self.snap_model);
            return;
        }
        if self.cursors[slot].is_none() {
            self.open(slot, hint);
        }
        self.cursor_call(slot, c);
    }

    /// Runs drawn call `c` on open cursor `slot`.
    fn cursor_call(&mut self, slot: usize, c: C) {
        let wide = self.scripts.spread();
        let tb = self.models[slot].as_ref().expect("open").tb;
        let call = self.resolve(tb, c);
        let label = format!("c{slot} {tb:?} {call:?}");
        if let Some(class) = self.excluded(slot, &call) {
            self.run.skips[class] += 1;
            self.run.dup_cursor_skipped += u64::from(tb.dup());
            self.push(label, format!("skipped: {}", CLASSES[class]));
            return;
        }
        if let Call::Upsert(row, ver) | Call::Append(row, ver) = call {
            let append = matches!(call, Call::Append(..));
            if self.second_value(tb, row, ver, append) {
                self.run.dup_cursor_skipped += 1;
                let operation = if append {
                    DatabaseWriteOperation::CursorAppend
                } else {
                    DatabaseWriteOperation::CursorUpsert
                };
                self.second_value_write(label, S::key(row), operation, |r| {
                    match r.cursors[slot].as_mut().map(|c| match c {
                        AnyCursor::S(c) => exec_dup_rw(c, &call, wide),
                        _ => unreachable!("a dup row write on a write cursor of the dup table"),
                    }) {
                        Some(Res::Done(res)) => res,
                        other => unreachable!("{other:?}"),
                    }
                });
                return;
            }
        }
        self.run.ran += 1;
        self.run.dup_cursor_ran += u64::from(tb.dup());
        let res = if slot == SNAP {
            self.snap_cursor.as_mut().expect("open").exec(&call, wide)
        } else {
            match self.cursors[slot].as_mut().expect("open") {
                AnyCursor::H(c) => exec_rw::<CanonicalHeaders, _>(c, &call),
                AnyCursor::S(c) => exec_dup_rw(c, &call, wide),
                AnyCursor::B(c) => exec_rw::<Bytecodes, _>(c, &call),
                AnyCursor::Ro(c) => c.exec(&call, wide),
            }
        };
        let shown = show_res(&res);
        self.moved(slot, tb, &call, res);
        self.push(label, shown);
    }

    /// Key `k` has one row, with a slot before `slot`.
    fn alone_before(&self, tb: Tb, k: u64, slot: u8) -> bool {
        let rows: Vec<Row> = self.key_rows(tb, k).take(2).collect();
        matches!(rows.as_slice(), [only] if only.1 < slot)
    }

    /// Updates the models after cursor `slot`'s `call` returned `res`.
    fn moved(&mut self, slot: usize, tb: Tb, call: &Call, res: Res) {
        // A missed `seek_by_key_subkey` past a key's one row (class 8).
        let miss_alone =
            matches!(*call, Call::SeekByKeySubkey(t) if self.alone_before(tb, t.0, t.1));
        let empty = self.rows(tb).is_empty();
        let landing = match *call {
            Call::SeekExact(t) | Call::SeekByKeySubkey(t) => self.at_or_after(tb, t),
            _ => None,
        };
        let m = self.models[slot].as_mut().expect("open");
        let keeps_fresh =
            matches!(call, Call::Current | Call::DeleteCurrent | Call::DeleteCurrentDuplicates)
                && matches!(res, Res::Item(Ok(None)) | Res::Done(Err(_)));
        let was_fresh = m.fresh;
        m.fresh &= keeps_fresh;
        let was_soft = mem::take(&mut m.soft_end);
        let outer = mem::take(&mut m.outer_deleted);
        m.pending = None;
        match (*call, res) {
            (Call::First | Call::Last | Call::Seek(_), Res::Item(Ok(found))) => match found {
                Some((row, _)) => m.place(row, matches!(call, Call::Last)),
                None => m.taint(PAST_END, false),
            },
            (Call::SeekExact(_) | Call::SeekByKeySubkey(_), Res::Item(Ok(found))) => match found {
                Some((row, _)) => m.place(row, false),
                // On an empty table MDBX's cursor is then as a fresh one (class 1).
                None if empty => m.taint(PAST_END, false),
                None if miss_alone && !was_fresh => m.taint(ONE_ROW_KEY, false),
                None => {
                    m.row = None;
                    m.missed = true;
                    m.landing = landing;
                    m.taint = None;
                }
            },
            (Call::Current, Res::Item(Ok(Some((row, _))))) => {
                if m.row.is_some_and(|r| r != row) {
                    let had = m.row;
                    self.miss(format!("c{slot} current is {row:?}, the model {had:?}"));
                }
                self.models[slot].as_mut().expect("open").place(row, was_soft);
            }
            (Call::Current, Res::Item(Ok(None))) => m.soft_end = was_soft,
            (_, Res::Item(Ok(Some((row, _))))) => {
                // Next, Prev and the dup moves; `last_dup` keeps a soft end (the cursor is then on
                // its key's last row already).
                let soft = matches!(call, Call::Prev | Call::PrevDup) && was_fresh
                    || matches!(call, Call::LastDup) && was_soft;
                m.place(row, soft);
                if matches!(call, Call::LastDup) && outer {
                    m.taint(LAST_DUP_AFTER_OUTER, false);
                }
            }
            (_, Res::Item(Ok(None))) if m.missed => {}
            (Call::Next | Call::NextNoDup, Res::Item(Ok(None))) => {
                // On a dup table MDBX's `prev` then steps past the key's rows.
                if was_soft && !tb.dup() {
                    m.soft_end = true;
                } else {
                    m.taint(PAST_END, false);
                }
            }
            (Call::Prev, Res::Item(Ok(None))) => m.taint(PAST_END, true),
            // The cursor stays: still at a soft end, or right after its outer delete.
            (Call::NextDup | Call::NextDupVal | Call::PrevDup, Res::Item(Ok(None))) => {
                m.soft_end = was_soft;
                m.outer_deleted = outer;
            }
            (Call::LastDup, Res::Item(Ok(None))) => {
                if outer {
                    m.taint(LAST_DUP_AFTER_OUTER, false);
                }
            }
            (Call::DeleteCurrent, Res::Done(Ok(()))) => {
                self.deleted_current(slot, tb);
                if outer {
                    self.models[slot].as_mut().expect("open").taint(LAST_DUP_AFTER_OUTER, false);
                }
            }
            (Call::DeleteCurrentDuplicates, Res::Done(Ok(()))) => self.deleted_dups(slot, tb),
            // MDBX_ENODATA (unplaced, or at the end of a key's rows after its own delete): the
            // cursor stays.
            (Call::DeleteCurrent | Call::DeleteCurrentDuplicates, Res::Done(Err(_))) => {}
            (
                Call::Upsert(row, v)
                | Call::Insert(row, v)
                | Call::Append(row, v)
                | Call::AppendDup(row, v),
                Res::Done(Ok(())),
            ) => {
                // An `append` that gives a dup key with one row a second (class 8).
                let append = matches!(call, Call::Append(..));
                let second = append && self.only_other_row(tb, row).is_some();
                // One that replaces the last key's value ends the table, as `last` does; on the
                // dup table one of the table's last row (which holds that value, one value per
                // subkey) does too: MDBX's `append` takes `last` first and, finding the same value,
                // returns. A later key's put then taints the cursor if its key has one row (class
                // 8, in `others_on_put`).
                let last = self.rows(tb).last_key_value().is_some_and(|(&r, _)| r == row);
                self.written(Some(slot), tb, row, v);
                let m = self.models[slot].as_mut().expect("open");
                m.place(row, append && last);
                if second {
                    m.taint(ONE_ROW_KEY, false);
                }
            }
            (Call::Insert(row, _), Res::Done(Err(_))) => {
                // MDBX_KEYEXIST: on the key's first row.
                let first = self.key_rows(tb, row.0).next();
                self.place_or_miss(slot, first, false, "a refused insert");
            }
            (Call::Append(..), Res::Done(Err(_))) => {
                // MDBX_EKEYMISMATCH: on the table's last row, as after `last`.
                let last = self.rows(tb).last_key_value().map(|(r, _)| *r);
                self.place_or_miss(slot, last, true, "a refused append");
            }
            (Call::AppendDup(row, _), Res::Done(Err(_))) => {
                // MDBX_EKEYMISMATCH: on the key's last row, or on its first when the value is
                // the first's (one value per subkey: when the row is the first).
                let first = self.key_rows(tb, row.0).next().filter(|&f| f == row);
                let to = first.or_else(|| self.key_rows(tb, row.0).next_back());
                self.place_or_miss(slot, to, false, "a refused append_dup");
            }
            (
                Call::Walk(..) | Call::WalkBack(..) | Call::WalkRange(..) | Call::WalkDup(..),
                Res::Walk(rows),
            ) => {
                self.walked(slot, tb, call, &rows, was_fresh);
            }
            (_, Res::Walk(_)) => unreachable!("only walks walk"),
            (_, Res::Item(Err(_)) | Res::Done(Err(_))) => m.taint(UNEXPECTED, false),
            (_, Res::Done(Ok(()))) => unreachable!("{call:?} is no write"),
            (_, Res::Item(Ok(None))) => unreachable!("{call:?} returns no row"),
        }
        let m = self.models[slot].as_mut().expect("open");
        if m.taint.is_none() && !m.fresh && !m.missed && m.row.is_none() {
            m.taint(PAST_END, false);
        }
    }

    fn place_or_miss(&mut self, slot: usize, row: Option<Row>, soft: bool, what: &str) {
        match row {
            Some(row) => self.models[slot].as_mut().expect("open").place(row, soft),
            None => {
                self.miss(format!("c{slot}: {what} with no row to land on"));
                self.models[slot].as_mut().expect("open").taint(UNEXPECTED, false);
            }
        }
    }

    /// Cursor `slot` deleted its row.
    fn deleted_current(&mut self, slot: usize, tb: Tb) {
        let Some(r) = self.models[slot].as_ref().expect("open").row else {
            self.miss(format!("c{slot} deleted with no row"));
            self.models[slot].as_mut().expect("open").taint(UNEXPECTED, false);
            return;
        };
        if self.model[tb.i()].remove(&r).is_none() {
            self.miss(format!("c{slot} deleted {r:?}, not in the model"));
        }
        self.others_on_delete(Some(slot), tb, &[r]);
        let later = self.key_rows(tb, r.0).find(|&x| x > r);
        let earlier = self.key_rows(tb, r.0).rev().find(|&x| x < r);
        let follower = self.after(tb, r);
        let m = self.models[slot].as_mut().expect("open");
        match (tb.dup(), later, earlier, follower) {
            // Plain, or a dup key's row with a later one: on the follower.
            (false, ..) | (true, Some(_), ..) => match follower {
                Some(f) => m.place(f, false),
                None => m.taint(PAST_END, false),
            },
            // A dup key's last row, with earlier ones: at the end of the key's rows, on the row
            // before.
            (true, None, Some(p), _) => m.place(p, false),
            // A dup key's only row: on the next key's first.
            (true, None, None, Some(f)) => {
                m.place(f, false);
                m.outer_deleted = true;
            }
            (true, None, None, None) => m.taint(PAST_END, false),
        }
    }

    /// Cursor `slot` deleted its key's rows.
    fn deleted_dups(&mut self, slot: usize, tb: Tb) {
        let Some(r) = self.models[slot].as_ref().expect("open").row else {
            self.miss(format!("c{slot} deleted duplicates with no row"));
            self.models[slot].as_mut().expect("open").taint(UNEXPECTED, false);
            return;
        };
        let removed: Vec<Row> = self.key_rows(tb, r.0).collect();
        for x in &removed {
            self.model[tb.i()].remove(x);
        }
        self.others_on_delete(Some(slot), tb, &removed);
        let follower = self.at_or_after(tb, (r.0 + 1, 0));
        let m = self.models[slot].as_mut().expect("open");
        match follower {
            Some(f) => {
                m.place(f, false);
                m.outer_deleted = true;
            }
            None => m.taint(PAST_END, false),
        }
    }

    /// Cursor `slot`'s walk returned `rows` (`was_fresh`: from a fresh cursor).
    fn walked(
        &mut self,
        slot: usize,
        tb: Tb,
        call: &Call,
        rows: &[Result<Item, DatabaseError>],
        was_fresh: bool,
    ) {
        let empty = self.rows(tb).is_empty();
        // A start by `seek_by_key_subkey` that missed past a key's one row (class 8); with a
        // subkey only, `first` placed the cursor on the first key.
        let miss_alone = match *call {
            Call::WalkDup(Some(k), Some(s), _) => !was_fresh && self.alone_before(tb, k, s),
            Call::WalkDup(None, Some(s), _) => {
                self.rows(tb).keys().next().is_some_and(|f| self.alone_before(tb, f.0, s))
            }
            _ => false,
        };
        // Where a missed start landed.
        let landing = match *call {
            Call::WalkDup(Some(k), s, _) => self.at_or_after(tb, (k, s.unwrap_or(0))),
            Call::WalkDup(None, Some(s), _) => {
                self.rows(tb).keys().next().and_then(|f| self.at_or_after(tb, (f.0, s)))
            }
            _ => None,
        };
        let m = self.models[slot].as_mut().expect("open");
        if rows.iter().any(Result::is_err) {
            // `walk_dup` from a slot alone on an empty table: MDBX_NOTFOUND.
            m.taint(if empty { PAST_END } else { UNEXPECTED }, false);
            return;
        }
        let last = rows.last().map(|r| r.as_ref().expect("no error").0);
        let n = match *call {
            Call::Walk(_, n) | Call::WalkBack(_, n) | Call::WalkRange(_, _, n) => n,
            Call::WalkDup(_, _, n) => n,
            _ => unreachable!(),
        };
        if rows.len() == usize::from(n) {
            m.place(last.expect("n > 0"), matches!(call, Call::WalkBack(None, 1)));
            return;
        }
        match *call {
            Call::Walk(..) => m.taint(PAST_END, false),
            Call::WalkBack(..) => m.taint(PAST_END, last.is_some()),
            Call::WalkRange(start, end, _) => {
                // Stopped at the first row past the end bound, or at the table's end.
                let next = match last {
                    Some(row) => self.after(tb, row),
                    None => self.at_or_after(tb, start.unwrap_or((0, 0))),
                };
                let past = |f: Row| match end {
                    Bound::Included(e) => f.0 > e.0,
                    Bound::Excluded(e) => f.0 >= e.0,
                    Bound::Unbounded => false,
                };
                match next {
                    Some(f) if past(f) => self.models[slot].as_mut().expect("open").place(f, false),
                    Some(f) => {
                        self.miss(format!("c{slot}: the range walk stopped before {f:?}"));
                        self.models[slot].as_mut().expect("open").taint(UNEXPECTED, false);
                    }
                    None => self.models[slot].as_mut().expect("open").taint(PAST_END, false),
                }
            }
            Call::WalkDup(key, slot_only, _) => match last {
                // `next_dup` found nothing: at the key's last row.
                Some(row) => m.place(row, false),
                // The start missed, or the table is empty.
                None if empty => m.taint(PAST_END, false),
                None if miss_alone => m.taint(ONE_ROW_KEY, false),
                None if key.is_none() && slot_only.is_none() => m.taint(PAST_END, false),
                None => {
                    m.row = None;
                    m.missed = true;
                    m.landing = landing;
                    m.taint = None;
                }
            },
            _ => unreachable!(),
        }
    }
}

/// A drawn key: a key index under 16 and a slot index under 6, or under 255 with `spread`
/// ([`Tb::row`]).
fn key(spread: bool) -> impl Strategy<Value = K> {
    (0u8..16, 0u8..if spread { u8::MAX } else { 6 }).prop_map(|(k, s)| K(k, s))
}

fn table() -> impl Strategy<Value = Tb> {
    prop_oneof![3 => Just(Tb::H), 5 => Just(Tb::S), 2 => Just(Tb::B)]
}

fn walk_len() -> impl Strategy<Value = u8> {
    1u8..=4
}

/// Calls a read cursor takes.
fn read_call(spread: bool) -> impl Strategy<Value = C> {
    let moves = prop_oneof![
        2 => Just(C::First),
        2 => Just(C::Last),
        6 => Just(C::Next),
        4 => Just(C::Prev),
        3 => Just(C::Current),
        3 => key(spread).prop_map(C::Seek),
        3 => key(spread).prop_map(C::SeekExact),
    ];
    let dup_moves = prop_oneof![
        3 => Just(C::NextDup),
        2 => Just(C::PrevDup),
        3 => Just(C::NextNoDup),
        2 => Just(C::LastDup),
        2 => Just(C::NextDupVal),
        3 => key(spread).prop_map(C::SeekByKeySubkey),
    ];
    let walks = prop_oneof![
        (proptest::option::of(key(spread)), walk_len()).prop_map(|(s, n)| C::Walk(s, n)),
        (
            proptest::option::of(key(spread)),
            proptest::option::of((key(spread), any::<bool>())),
            walk_len()
        )
            .prop_map(|(s, e, n)| C::WalkRange(s, e, n)),
        (proptest::option::of(key(spread)), walk_len()).prop_map(|(s, n)| C::WalkBack(s, n)),
        (proptest::option::of(key(spread)), proptest::option::of(key(spread)), walk_len())
            .prop_map(|(k, s, n)| C::WalkDup(k, s, n)),
    ];
    prop_oneof![23 => moves, 15 => dup_moves, 5 => walks]
}

/// Calls only a write cursor takes.
fn write_call(spread: bool) -> impl Strategy<Value = C> {
    prop_oneof![
        4 => key(spread).prop_map(C::Upsert),
        2 => key(spread).prop_map(C::Insert),
        3 => (key(spread), prop::bool::weighted(0.75)).prop_map(|(k, o)| C::Append(k, o)),
        3 => Just(C::DeleteCurrent),
        2 => (key(spread), prop::bool::weighted(0.6)).prop_map(|(k, o)| C::AppendDup(k, o)),
        2 => Just(C::DeleteCurrentDuplicates),
    ]
}

fn op(spread: bool) -> impl Strategy<Value = Op> {
    let tx_ops = prop_oneof![
        3 => (table(), key(spread)).prop_map(|(t, k)| Op::Get(t, k)),
        6 => (table(), key(spread)).prop_map(|(t, k)| Op::Put(t, k)),
        2 => (table(), key(spread), prop::bool::weighted(0.75)).prop_map(|(t, k, o)| Op::Append(t, k, o)),
        4 => (table(), key(spread), prop_oneof![Just(Del::All), Just(Del::Match), Just(Del::Other)])
            .prop_map(|(t, k, d)| Op::Delete(t, k, d)),
        1 => table().prop_map(Op::Entries),
    ];
    let cursor_ops = prop_oneof![
        5 => (0usize..2, table(), prop_oneof![3 => read_call(spread), 2 => write_call(spread)])
            .prop_map(|(s, t, c)| Op::Cursor(s, t, c)),
        2 => (table(), read_call(spread)).prop_map(|(t, c)| Op::Cursor(2, t, c)),
        2 => (table(), read_call(spread)).prop_map(|(t, c)| Op::Cursor(SNAP, t, c)),
        1 => (0..=SNAP, table()).prop_map(|(s, t)| Op::Open(s, t)),
    ];
    let txns = prop_oneof![
        4 => Just(Op::Commit),
        2 => Just(Op::Abort),
        1 => table().prop_map(Op::Clear),
        2 => Just(Op::SnapOpen),
        3 => (table(), key(spread)).prop_map(|(t, k)| Op::SnapGet(t, k)),
        2 => (table(), proptest::option::of(key(spread)), walk_len())
            .prop_map(|(t, s, n)| Op::SnapWalk(t, s, n)),
        1 => table().prop_map(Op::SnapEntries),
    ];
    prop_oneof![16 => tx_ops, 40 => cursor_ops, 5 => txns]
}

/// Both outcome lists, a line per op, the differing ones marked `XX` with both sides.
fn render(want: &[Outcome], have: &[Outcome]) -> String {
    let mut out = String::new();
    for i in 0..want.len().max(have.len()) {
        let (w, h) = (want.get(i), have.get(i));
        let line = match (w, h) {
            (Some(w), Some(h)) if w == h => format!("   {i:3} {} => {}", w.op, w.result),
            (Some(w), Some(h)) if w.op == h.op => {
                format!("XX {i:3} {} => MDBX {} | tn-lmdb {}", w.op, w.result, h.result)
            }
            _ => format!("XX {i:3} MDBX {w:?} | tn-lmdb {h:?}"),
        };
        out.push_str(&line);
        out.push('\n');
    }
    out
}

/// Asserts that `db` holds `PlainStorageState` in `form`: an engine dup table of 20-byte keys
/// with the form's `packed_max` as the engine records it, or a plain table (composite keys).
fn assert_form(db: &TnLmdbEnv, form: Form) {
    use tn_lmdb::ReadTxn;
    let txn = db.env().begin_ro().unwrap();
    let table = txn.open_table(PlainStorageState::NAME).unwrap().unwrap();
    let dup = txn.table_stat(&table).unwrap().dup;
    let expected = form
        .plain_storage_packed_max()
        .map(|packed_max| tn_lmdb::DupOptions { key_len: 20, packed_max });
    assert_eq!(dup, expected, "PlainStorageState's form, asked {form:?}");
}

/// Runs `ops` on MDBX and on tn-lmdb in each form of its nested layouts' tables ([`Form::ALL`]),
/// each in a new environment, and compares each form's outcomes and skips with MDBX's. Returns
/// MDBX's run, which every form's equals.
fn differential(scripts: Scripts, ops: &[Op]) -> Result<Run, TestCaseError> {
    let (_mdbx_dir, mdbx) = create_mdbx_db();
    let want = run(&mdbx, scripts, ops, false);
    for form in Form::ALL {
        let (_dir, db) = create_test_db_with(TnLmdbEnvKind::RW, form);
        assert_form(&db, form);
        let form = form.name();
        let have = run(&db, scripts, ops, true);
        if !want.model_misses.is_empty() {
            let misses = want.model_misses.join("\n");
            return Err(TestCaseError::fail(format!(
                "the test's model disagrees with MDBX:\n{misses}\nscript: {ops:?}\n{}",
                render(&want.outcomes, &have.outcomes)
            )));
        }
        if want.outcomes != have.outcomes {
            return Err(TestCaseError::fail(format!(
                "tn-lmdb's outcomes ({form}) differ from MDBX's\nscript: {ops:?}\n{}",
                render(&want.outcomes, &have.outcomes)
            )));
        }
        // The outcomes matched MDBX's, so a miss here is one the outcomes did not show: a
        // committed table (`check_model`) or a cursor place that differs from the model's.
        if !have.model_misses.is_empty() {
            let misses = have.model_misses.join("\n");
            return Err(TestCaseError::fail(format!(
                "the test's model disagrees with tn-lmdb ({form}):\n{misses}\nscript: {ops:?}\n{}",
                render(&want.outcomes, &have.outcomes)
            )));
        }
        let counts = |r: &Run| (r.ran, r.skips, r.dup_cursor_ran, r.dup_cursor_skipped);
        if counts(&have) != counts(&want) {
            return Err(TestCaseError::fail(format!(
                "tn-lmdb's run ({form}) skipped other ops than MDBX's: ran {} against {}, skips \
                 {:?} against {:?}, dup cursor calls run and skipped {:?} against {:?}\nscript: \
                 {ops:?}",
                have.ran,
                want.ran,
                have.skips,
                want.skips,
                (have.dup_cursor_ran, have.dup_cursor_skipped),
                (want.dup_cursor_ran, want.dup_cursor_skipped),
            )));
        }
    }
    Ok(want)
}

/// The fixed seed of the pinned tests' scripts.
const RNG_SEED: u64 = 1;

/// The seed of a random test's scripts: `PROPTEST_RNG_SEED` when it is set, else a new random
/// one for each run.
fn random_seed() -> u64 {
    match ProptestConfig::default().rng_seed {
        RngSeed::Fixed(seed) => seed,
        RngSeed::Random => RandomState::new().hash_one(0u8),
    }
}

/// Ops run and skipped by class ([`CLASSES`]) over a test's scripts.
type Totals = (u64, [u64; 9]);

/// Runs `cases` scripts of up to `len` ops from `seed`, of the shape `scripts`, each on MDBX and
/// on every form of tn-lmdb ([`differential`]), prints the ops run and skipped (by class, and
/// the dup table's cursor calls apart) with the seed, and returns the ops run and skipped by
/// class. A failure's message names the seed, which `PROPTEST_RNG_SEED` reruns. The skips must
/// stay under a tenth of the ops run, and the dup table's skipped cursor calls under a fifth of
/// its cursor calls: the dup layouts' cursor code is what the skip classes mostly cover, so a
/// rate low over every op could still leave it mostly skipped.
fn differential_cases(cases: u32, len: usize, seed: u64, scripts: Scripts) -> Totals {
    let config = ProptestConfig {
        cases,
        rng_seed: RngSeed::Fixed(seed),
        failure_persistence: None,
        ..ProptestConfig::default()
    };
    let totals = RefCell::new(Run::default());
    let ops = prop::collection::vec(op(scripts.spread()), 1..=len);
    let result = TestRunner::new(config).run(&ops, |ops| {
        let run = differential(scripts, &ops)?;
        let mut totals = totals.borrow_mut();
        totals.ran += run.ran;
        totals.dup_cursor_ran += run.dup_cursor_ran;
        totals.dup_cursor_skipped += run.dup_cursor_skipped;
        for (sum, n) in totals.skips.iter_mut().zip(run.skips) {
            *sum += n;
        }
        Ok(())
    });
    if let Err(e) = result {
        panic!("{e}\nseed {seed}: PROPTEST_RNG_SEED={seed} reruns these scripts");
    }
    let totals = totals.into_inner();
    let skipped: u64 = totals.skips.iter().sum();
    println!(
        "differential (seed {seed}, {scripts:?}, every form): {cases} scripts, {} ops run, \
         {skipped} skipped; dup cursor calls: {} run, {} skipped",
        totals.ran, totals.dup_cursor_ran, totals.dup_cursor_skipped
    );
    for (class, n) in CLASSES.iter().zip(totals.skips) {
        println!("  {n:7}  {class}");
    }
    assert!(
        skipped * 10 < totals.ran,
        "skipped ops reached a tenth of the ops run at seed {seed}: the scripts test little"
    );
    assert!(
        totals.dup_cursor_skipped * 5 < totals.dup_cursor_ran,
        "skipped dup cursor calls reached a fifth of those run at seed {seed} ({} of {}): the \
         scripts test the dup cursors little",
        totals.dup_cursor_skipped,
        totals.dup_cursor_ran
    );
    (totals.ran, totals.skips)
}

/// What a pinned test's message says when its totals moved.
const PINNED_MOVED: &str = "ops run and skipped by class at the fixed seed, against the pinned \
                            figures: a change of the scripts' strategy changes them (pin the new \
                            ones), anything else is a behaviour change to find first";

/// [`random_scripts_skip_counts_are_pinned_at_seed_1`]'s totals.
const DEFAULT_TOTALS: Totals = (1604, [20, 0, 11, 1, 0, 7, 0, 3, 0]);

/// [`random_scripts_skip_counts_are_pinned_at_seed_1_heavy`]'s totals.
const HEAVY_TOTALS: Totals = (294_529, [8988, 4, 2304, 23, 62, 1022, 52, 1048, 0]);

/// Random scripts of transaction and cursor calls on three tables (one dup-sort, one with
/// overflow values) give the same outcomes on tn-lmdb, in every form, as on MDBX.
#[test]
fn random_scripts_match_mdbx() {
    differential_cases(32, 64, random_seed(), Scripts::Narrow);
}

#[test]
#[ignore = "heavy: run with --release"]
fn random_scripts_match_mdbx_heavy() {
    differential_cases(2000, 256, random_seed(), Scripts::Narrow);
}

/// [`random_scripts_match_mdbx`] on tables seeded with one dup key of [`MULTI_LEAF_ROWS`] rows
/// and as many header keys ([`Scripts::MultiLeaf`]), the drawn keys and slots spread over them,
/// so the cursor writes and moves run inside multi-leaf nested trees (packed and packed-small
/// forms) and multi-leaf composite trees, with dup subkeys that differ past their first byte and
/// rests of several lengths ([`WIDE`]).
#[test]
fn random_scripts_match_mdbx_multi_leaf() {
    differential_cases(32, 64, random_seed(), Scripts::MultiLeaf);
}

#[test]
#[ignore = "heavy: run with --release"]
fn random_scripts_match_mdbx_multi_leaf_heavy() {
    differential_cases(2000, 256, random_seed(), Scripts::MultiLeaf);
}

/// [`random_scripts_match_mdbx`]'s scripts from [`RNG_SEED`] run and skip the pinned ops by
/// class, on every form.
#[test]
fn random_scripts_skip_counts_are_pinned_at_seed_1() {
    let totals = differential_cases(32, 64, RNG_SEED, Scripts::Narrow);
    assert_eq!(totals, DEFAULT_TOTALS, "{PINNED_MOVED}");
}

/// [`random_scripts_match_mdbx_heavy`]'s scripts from [`RNG_SEED`] run and skip the pinned ops
/// by class, on every form.
#[test]
#[ignore = "heavy: run with --release"]
fn random_scripts_skip_counts_are_pinned_at_seed_1_heavy() {
    let totals = differential_cases(2000, 256, RNG_SEED, Scripts::Narrow);
    assert_eq!(totals, HEAVY_TOTALS, "{PINNED_MOVED}");
}
