//! Dup-sort tables stored as composite keys: each reth call on a dup-sort table, run on MDBX too
//! (the oracle), the layouts against reth's encodings, rows the layout cannot hold, and the known
//! differences from MDBX's cursors.

use super::{adapter::create_mdbx_db, mdbx_port::create_test_db};
use crate::{
    dup::{dup_layout, DupLayout},
    tx::compressed,
    TndbEnvKind,
};
use alloy::primitives::{Address, B256, U256};
use reth_db_api::{
    common::{PairResult, ValueOnlyResult},
    cursor::{DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW},
    database::Database,
    models::{AccountBeforeTx, BlockNumberAddress},
    table::{DupSort, Encode, Table, TableImporter},
    tables::{
        AccountChangeSets, CanonicalHeaders, HashedStorages, PlainStorageState, RawDupSort, RawKey,
        RawValue, StorageChangeSets, StoragesTrie, Tables,
    },
    transaction::{DbTx, DbTxMut},
    DatabaseError,
};
use reth_libmdbx::Error;
use reth_primitives_traits::{Account, StorageEntry, ValueWithSubKey};
use reth_storage_errors::db::{DatabaseWriteError, DatabaseWriteOperation};
use reth_trie_common::{BranchNodeCompact, Nibbles, StorageTrieEntry, StoredNibblesSubKey};
use std::fmt::Debug;

/// The address of key `n`.
fn a(n: u8) -> Address {
    Address::with_last_byte(n)
}

/// The entry of subkey `s` holding `v`.
fn e(s: u8, v: u64) -> StorageEntry {
    StorageEntry { key: B256::with_last_byte(s), value: U256::from(v) }
}

/// The subkey `s`.
fn s(n: u8) -> B256 {
    B256::with_last_byte(n)
}

/// Every dup cursor call from each cursor state, on `PlainStorageState`: a key with three dups
/// (1), one with one (2), one with two (4). Every result, errors included, is logged.
fn dup_moves<D: Database>(db: &D) -> Vec<String> {
    let mut log = Vec::new();
    macro_rules! log {
        ($e:expr) => {{
            let result = $e;
            log.push(format!("{}: {:?}", stringify!($e), result));
        }};
    }
    let tx = db.tx_mut().unwrap();
    // A walk from a subkey on an empty table.
    log!(tx
        .cursor_dup_write::<PlainStorageState>()
        .unwrap()
        .walk_dup(None, Some(s(1)))
        .unwrap()
        .collect::<Vec<_>>());
    for (k, sk) in [(1, 1), (1, 3), (1, 5), (2, 2), (4, 1), (4, 2)] {
        tx.put::<PlainStorageState>(a(k), e(sk, u64::from(k) * 10 + u64::from(sk))).unwrap();
    }
    type C<D> = <<D as Database>::TXMut as DbTxMut>::DupCursorMut<PlainStorageState>;
    let fresh = |tx: &D::TXMut| -> C<D> { tx.cursor_dup_write::<PlainStorageState>().unwrap() };
    // A fresh cursor.
    log!(fresh(&tx).next_dup());
    log!(fresh(&tx).prev_dup());
    log!(fresh(&tx).next_no_dup());
    log!(fresh(&tx).last_dup());
    log!(fresh(&tx).next_dup_val());
    log!(fresh(&tx).current());
    log!(fresh(&tx).delete_current_duplicates());
    let mut c = fresh(&tx);
    // Stepping through one key's dups.
    log!(c.seek_exact(a(1)));
    log!(c.next_dup());
    log!(c.next_dup());
    log!(c.next_dup());
    log!(c.current());
    log!(c.next());
    log!(c.prev_dup());
    log!(c.prev_dup());
    log!(c.current());
    // A missed seek_exact.
    log!(c.seek_exact(a(3)));
    log!(c.next_dup());
    log!(c.current());
    log!(c.seek_exact(a(3)));
    log!(c.next_no_dup());
    log!(c.seek_exact(a(3)));
    log!(c.prev_dup());
    log!(c.seek_exact(a(3)));
    log!(c.next());
    log!(c.seek_exact(a(3)));
    log!(c.last_dup());
    log!(c.seek_exact(a(3)));
    log!(c.next_dup_val());
    // seek lands on the next key's first dup.
    log!(c.seek(a(3)));
    log!(c.prev_dup());
    log!(c.current());
    log!(c.prev());
    log!(c.seek(a(5)));
    log!(c.next_dup());
    log!(c.prev_dup());
    // seek_by_key_subkey.
    log!(c.seek_by_key_subkey(a(1), s(2)));
    log!(c.current());
    log!(c.next_dup());
    log!(c.seek_by_key_subkey(a(1), s(3)));
    log!(c.seek_by_key_subkey(a(1), s(0)));
    log!(c.seek_by_key_subkey(a(1), s(6)));
    log!(c.current());
    log!(c.next_dup());
    log!(c.seek_by_key_subkey(a(1), s(6)));
    log!(c.next());
    log!(c.seek_by_key_subkey(a(1), s(6)));
    log!(c.prev());
    log!(c.seek_by_key_subkey(a(3), s(0)));
    log!(c.current());
    log!(c.next_dup());
    log!(c.seek_by_key_subkey(a(3), s(0)));
    log!(c.next());
    log!(c.seek_by_key_subkey(a(9), s(0)));
    log!(c.next());
    log!(c.seek_by_key_subkey(a(4), s(2)));
    log!(c.next_dup());
    // last_dup, next_dup_val and next_no_dup.
    log!(c.seek_exact(a(1)));
    log!(c.next_no_dup());
    log!(c.seek_exact(a(1)));
    log!(c.last_dup());
    log!(c.current());
    log!(c.next_dup_val());
    log!(c.prev_dup());
    log!(c.next_no_dup());
    log!(c.next_dup_val());
    log!(c.last_dup());
    log!(c.next_no_dup());
    log!(c.next_dup_val());
    log!(c.next_no_dup());
    log!(c.current());
    log!(c.next_dup());
    log!(c.prev_dup());
    log!(c.last());
    log!(c.next_dup());
    log!(c.next_no_dup());
    log!(c.current());
    log!(c.first());
    log!(c.prev_dup());
    log!(c.prev());
    log!(c.next_dup());
    log!(c.current());
    // walk_dup from each start.
    log!(c.walk_dup(None, None).unwrap().collect::<Vec<_>>());
    log!(c.walk_dup(None, Some(s(2))).unwrap().collect::<Vec<_>>());
    log!(c.walk_dup(None, Some(s(9))).unwrap().collect::<Vec<_>>());
    log!(c.walk_dup(Some(a(4)), None).unwrap().collect::<Vec<_>>());
    log!(c.walk_dup(Some(a(4)), Some(s(2))).unwrap().collect::<Vec<_>>());
    log!(c.walk_dup(Some(a(4)), Some(s(3))).unwrap().collect::<Vec<_>>());
    log!(c.walk_dup(Some(a(3)), None).unwrap().collect::<Vec<_>>());
    log!(c.walk_dup(Some(a(3)), Some(s(0))).unwrap().collect::<Vec<_>>());
    log!(c.walk_dup(Some(a(9)), None).unwrap().collect::<Vec<_>>());
    // Each move right after a delete of a middle row (put back after each).
    macro_rules! after_delete {
        ($($move:tt)*) => {
            log!(c.seek_by_key_subkey(a(1), s(3)));
            log!(c.delete_current());
            log!(c.$($move)*);
            log!(c.next());
            log!(tx.put::<PlainStorageState>(a(1), e(3, 13)));
        };
    }
    after_delete!(next_dup());
    after_delete!(prev_dup());
    after_delete!(next_no_dup());
    after_delete!(last_dup());
    after_delete!(next_dup_val());
    after_delete!(next());
    after_delete!(prev());
    after_delete!(seek_exact(a(1)));
    // Deletes of a key's last row, and of its only row.
    log!(c.seek_by_key_subkey(a(1), s(5)));
    log!(c.delete_current());
    log!(c.next_dup());
    log!(c.next());
    log!(tx.put::<PlainStorageState>(a(1), e(5, 15)));
    log!(c.seek_by_key_subkey(a(1), s(5)));
    log!(c.delete_current());
    log!(c.last_dup());
    log!(c.next());
    log!(c.seek_by_key_subkey(a(1), s(3)));
    log!(c.delete_current());
    log!(c.prev());
    log!(c.next_dup());
    log!(c.seek_exact(a(2)));
    log!(c.delete_current());
    log!(c.next_dup());
    log!(c.prev_dup());
    log!(c.next());
    log!(c.seek_exact(a(2)));
    log!(c.seek_by_key_subkey(a(1), s(5)));
    log!(c.delete_current());
    log!(c.next_no_dup());
    log!(c.seek_by_key_subkey(a(1), s(1)));
    log!(c.delete_current());
    log!(c.next_dup());
    log!(c.next_no_dup());
    log!(c.seek_exact(a(1)));
    log!(c.next_dup());
    log!(c.next_no_dup());
    log!(c.prev());
    log!(c.next_no_dup());
    // delete_current_duplicates from a middle row, then of the next key.
    log!(tx.put::<PlainStorageState>(a(4), e(3, 43)));
    log!(tx.put::<PlainStorageState>(a(5), e(1, 51)));
    log!(c.seek_by_key_subkey(a(4), s(2)));
    log!(c.delete_current_duplicates());
    log!(c.next_dup());
    log!(c.next());
    log!(c.delete_current_duplicates());
    log!(c.prev());
    log!(c.next());
    log!(c.delete_current_duplicates());
    log!(tx.entries::<PlainStorageState>());
    // insert, append and append_dup.
    log!(tx.put::<PlainStorageState>(a(1), e(1, 11)));
    log!(tx.put::<PlainStorageState>(a(1), e(4, 14)));
    log!(tx.put::<PlainStorageState>(a(4), e(2, 42)));
    log!(c.first());
    log!(c.insert(a(1), &e(9, 19)));
    log!(c.current());
    log!(c.insert(a(1), &e(1, 11)));
    log!(c.insert(a(3), &e(5, 35)));
    log!(c.current());
    log!(c.append(a(4), &e(1, 41)));
    log!(c.current());
    log!(c.append(a(4), &e(2, 42)));
    log!(c.current());
    log!(c.append(a(3), &e(9, 39)));
    log!(c.current());
    log!(c.append(a(6), &e(4, 64)));
    log!(c.current());
    log!(c.append(a(6), &e(6, 66)));
    log!(c.append_dup(a(6), e(5, 65)));
    log!(c.current());
    log!(c.append_dup(a(6), e(7, 67)));
    log!(c.current());
    log!(c.append_dup(a(2), e(1, 21)));
    log!(c.current());
    log!(c.append_dup(a(1), e(9, 19)));
    log!(c.current());
    log!(c.append_dup(a(1), e(8, 18)));
    log!(c.current());
    log!(c.append_dup(a(2), e(1, 21)));
    log!(c.current());
    log!(c.append_dup(a(7), e(1, 71)));
    log!(c.current());
    // Appends through the transaction.
    log!(tx.append::<PlainStorageState>(a(7), e(0, 70)));
    log!(tx.append::<PlainStorageState>(a(7), e(2, 72)));
    log!(tx.append::<PlainStorageState>(a(6), e(9, 69)));
    log!(tx.append::<PlainStorageState>(a(8), e(1, 81)));
    log!(tx.delete::<PlainStorageState>(a(8), None));
    // Deletes and reads through the transaction.
    log!(tx.delete::<PlainStorageState>(a(1), Some(e(9, 19))));
    log!(tx.delete::<PlainStorageState>(a(1), Some(e(4, 999))));
    log!(tx.delete::<PlainStorageState>(a(1), Some(e(2, 12))));
    log!(tx.delete::<PlainStorageState>(a(8), Some(e(1, 81))));
    log!(tx.delete::<PlainStorageState>(a(6), None));
    log!(tx.delete::<PlainStorageState>(a(6), None));
    log!(tx.get::<PlainStorageState>(a(1)));
    log!(tx.get::<PlainStorageState>(a(3)));
    log!(tx.get::<PlainStorageState>(a(6)));
    log!(tx.entries::<PlainStorageState>());
    log!(c.walk(None).unwrap().collect::<Vec<_>>());
    drop(c);
    tx.commit().unwrap();
    // A read-only cursor.
    let tx = db.tx().unwrap();
    let mut c = tx.cursor_dup_read::<PlainStorageState>().unwrap();
    log!(c.next_dup_val());
    log!(c.next_no_dup());
    log!(c.seek_by_key_subkey(a(1), s(2)));
    log!(c.next_dup());
    log!(c.last_dup());
    log!(c.prev_dup());
    log!(c.seek_exact(a(3)));
    log!(c.next_dup());
    log!(c.seek_exact(a(5)));
    log!(c.next());
    log!(c.seek_exact(a(5)));
    log!(c.prev());
    log!(c.next_dup());
    log!(c.current());
    log!(c.walk_dup(Some(a(4)), None).unwrap().collect::<Vec<_>>());
    log!(c.walk_back(None).unwrap().collect::<Vec<_>>());
    log!(tx.get::<PlainStorageState>(a(4)));
    log!(tx.entries::<PlainStorageState>());
    log
}

#[test]
fn dup_cursor_calls_match_mdbx() {
    let (_mdbx_dir, mdbx) = create_mdbx_db();
    let (_dir, db) = create_test_db(TndbEnvKind::RW);
    let expected = dup_moves(&mdbx);
    let got = dup_moves(&db);
    for (i, (want, have)) in expected.iter().zip(&got).enumerate() {
        assert_eq!(have, want, "step {i}");
    }
    assert_eq!(got.len(), expected.len());
}

/// The outcome of a call, short: a row as `key/subkey` (their last bytes), a value as its
/// subkey's last byte, `none`, `ok`, or an error by its MDBX name.
fn row(result: PairResult<PlainStorageState>) -> String {
    match result {
        Ok(Some((key, value))) => format!("{}/{}", key.0[19], value.key.0[31]),
        Ok(None) => "none".into(),
        Err(e) => error_name(e),
    }
}

/// [`row`] for a value.
fn val(result: ValueOnlyResult<PlainStorageState>) -> String {
    match result {
        Ok(Some(value)) => format!("{}", value.key.0[31]),
        Ok(None) => "none".into(),
        Err(e) => error_name(e),
    }
}

/// [`row`] for a write or delete.
fn done(result: Result<(), DatabaseError>) -> String {
    result.map_or_else(error_name, |()| "ok".into())
}

/// `nodata` or `keymismatch` for those MDBX errors, else the error.
pub(super) fn error_name(e: DatabaseError) -> String {
    let info = match &e {
        DatabaseError::Delete(info) | DatabaseError::Read(info) => info.clone(),
        DatabaseError::Write(write) => write.info.clone(),
        _ => return format!("{e:?}"),
    };
    if info == Error::NoData.into() {
        "nodata".into()
    } else if info == Error::KeyMismatch.into() {
        "keymismatch".into()
    } else {
        format!("{e:?}")
    }
}

/// Runs `steps` (space-separated) on a write transaction holding `PlainStorageState` rows of
/// key/subkey 1/1, 1/3, 1/5, 2/2, 4/1 and 4/2, and returns each step with its outcome: a row as
/// `key/subkey`, a value as its subkey, `none`, `ok`, or an error by its MDBX name. The steps:
/// `sk:K/S` `seek_by_key_subkey`, `se:K` `seek_exact`, `f` `first`, `l` `last`, `n` `next`, `p`
/// `prev`, `c` `current`, `nd` `next_dup`, `pd` `prev_dup`, `nnd` `next_no_dup`, `ld`
/// `last_dup`, `ndv` `next_dup_val`, `d` `delete_current`, `dd` `delete_current_duplicates`,
/// `ad:K/S` `append_dup`, `adx:K/S` `append_dup` of another value for that subkey, `a:K/S`
/// `append`; `put:K/S`, `tap:K/S` and `del:K/S` the transaction's `put`, `append` and `delete` of
/// that row; `en` the transaction's `entries`. The transaction aborts.
fn dup_steps<D: Database>(db: &D, steps: &str) -> String {
    let entry = |s: u8| e(s, u64::from(s));
    let tx = db.tx_mut().unwrap();
    for (k, sk) in [(1, 1), (1, 3), (1, 5), (2, 2), (4, 1), (4, 2)] {
        tx.put::<PlainStorageState>(a(k), entry(sk)).unwrap();
    }
    let mut c = tx.cursor_dup_write::<PlainStorageState>().unwrap();
    let mut out = Vec::new();
    for step in steps.split_whitespace() {
        let (op, arg) = step.split_once(':').unwrap_or((step, ""));
        let (k, sk) = arg.split_once('/').unwrap_or((arg, "0"));
        let (k, sk): (u8, u8) = (k.parse().unwrap_or(0), sk.parse().unwrap_or(0));
        let outcome = match op {
            "sk" => val(c.seek_by_key_subkey(a(k), s(sk))),
            "se" => row(c.seek_exact(a(k))),
            "f" => row(c.first()),
            "l" => row(c.last()),
            "n" => row(c.next()),
            "p" => row(c.prev()),
            "c" => row(c.current()),
            "nd" => row(c.next_dup()),
            "pd" => row(c.prev_dup()),
            "nnd" => row(c.next_no_dup()),
            "ld" => val(c.last_dup()),
            "ndv" => val(c.next_dup_val()),
            "d" => done(c.delete_current()),
            "dd" => done(c.delete_current_duplicates()),
            "ad" => done(c.append_dup(a(k), entry(sk))),
            "a" => done(c.append(a(k), &entry(sk))),
            "adx" => done(c.append_dup(a(k), e(sk, 99))),
            "tap" => done(tx.append::<PlainStorageState>(a(k), entry(sk))),
            "put" => done(tx.put::<PlainStorageState>(a(k), entry(sk))),
            "del" => done(tx.delete::<PlainStorageState>(a(k), Some(entry(sk))).map(drop)),
            "en" => tx.entries::<PlainStorageState>().map_or_else(error_name, |n| n.to_string()),
            _ => panic!("unknown step {op}"),
        };
        out.push(format!("{step}={outcome}"));
    }
    out.join(" ")
}

/// Dup step sequences ([`dup_steps`]) where the cursor is MDBX's step for step. Right after a
/// delete of a key's middle row, or of its only row, the cursor is on the follower, as on a table
/// without duplicates; `delete_current_duplicates` leaves it on the next key's first row the same
/// way. After a delete of a key's last row while the key keeps earlier rows, it ends the key's
/// rows on the row before (`current` returns it, `next_dup` nothing, `prev` and `prev_dup` the
/// key's last row, `next` the next key's first, a delete is `MDBX_ENODATA` until `next_dup`).
/// After `last` it ends the last key's rows the same way, softly: `next_dup` and `next` skip rows
/// put after it in its key, but `next` and `next_no_dup` take a later key (put or appended, and
/// after a refused `append` too). A refused `append_dup` leaves it on the key's last row, or on
/// its first when the value is exactly the first's. After an `append` gives a key with one row a
/// second, the calls that match MDBX's are those that do not step (`current`, `delete_current`,
/// `last_dup`, `delete_current_duplicates`), `next_dup`, and any call after a third `append`.
/// Right after a delete that leaves a key no rows, `prev_dup` finds nothing, even once another
/// writer has put rows of the key again.
const DUP_MATCHED: &[&str] = &[
    "sk:1/3 d c n",
    "sk:1/3 d nd nd",
    "sk:1/3 d pd",
    "sk:1/3 d d c",
    "sk:1/3 d dd c n",
    "sk:1/3 d ld c",
    "sk:1/3 d ld n",
    "sk:1/3 d ld p",
    "sk:1/3 d nnd",
    "sk:1/3 d d nd pd",
    "sk:1/3 d put:1/4 n",
    "sk:1/3 d put:1/4 c",
    "se:2 d c n",
    "se:2 d nd",
    "se:2 d pd",
    "se:2 d ld",
    "se:2 d ld d c",
    "se:2 d d c",
    "se:2 d dd c",
    "se:2 d nnd",
    "se:2 d p",
    "se:1 dd ld",
    "se:1 dd c n",
    "se:1 dd nd",
    "se:1 dd pd",
    "se:1 dd d c",
    "se:1 dd dd c",
    "sk:1/5 d c n",
    "sk:1/5 d c c n",
    "sk:1/5 d c nd",
    "sk:1/5 d c pd",
    "sk:1/5 d nd pd",
    "sk:1/5 d nd n",
    "sk:1/5 d nd p",
    "sk:1/5 d nd c pd",
    "sk:1/5 d nd d c",
    "sk:1/5 d nd d d c",
    "sk:1/5 d nd ld",
    "sk:1/5 d nd nnd",
    "sk:1/5 d nd nd pd",
    "sk:1/5 d nd nd n",
    "sk:1/5 d nd pd pd",
    "sk:1/5 d nd dd c",
    "sk:1/5 d pd n",
    "sk:1/5 d pd pd",
    "sk:1/5 d p",
    "sk:1/5 d n",
    "sk:1/5 d ld c n",
    "sk:1/5 d d c n",
    "sk:1/5 d dd c n",
    "sk:1/5 d nnd",
    "sk:1/5 d ndv",
    "sk:1/5 d sk:1/1 n",
    "sk:1/5 d put:1/6 c",
    "sk:1/5 d put:1/6 n",
    "sk:1/5 d put:1/6 nd",
    "sk:1/5 d put:1/6 p",
    "sk:1/5 d put:1/6 pd",
    "sk:1/5 d put:1/6 nnd",
    "sk:1/5 d put:1/6 ld",
    "sk:1/5 d put:1/4 c n",
    "sk:1/5 d put:1/4 p",
    "sk:4/2 d p",
    "sk:4/2 d pd",
    "sk:4/2 d n",
    "sk:4/2 d nnd",
    "sk:4/2 d ld",
    "sk:4/2 d d c",
    "sk:1/5 del:1/5 n",
    "sk:1/5 del:1/5 p",
    "sk:1/5 del:1/5 nd",
    "sk:1/5 del:1/5 pd",
    "l n c",
    "l n d",
    "l nd c",
    "l nd n c",
    "l nnd c",
    "l d c",
    "l nd d c",
    "l p n",
    "l put:5/1 c n",
    "l put:5/1 nnd",
    "l put:5/1 nd",
    "l put:5/1 ld",
    "l put:4/3 nd",
    "l put:4/3 n",
    "l put:4/3 nnd",
    "l put:4/3 c",
    "l put:4/3 ld n",
    "l put:4/3 d c",
    "l put:4/3 p",
    "l put:4/3 pd",
    "l tap:5/1 n",
    "l tap:5/1 nnd",
    "a:1/1 c",
    "a:1/1 tap:5/1 n",
    "ad:1/1 c",
    "ad:1/1 n",
    "ad:1/3 c",
    "ad:4/1 p",
    "adx:4/1 c",
    "a:5/1 a:5/2 c",
    "a:5/1 a:5/2 d c",
    "a:5/1 a:5/2 ld",
    "a:5/1 a:5/2 nd",
    "a:5/1 a:5/2 dd c",
    "a:5/1 a:5/2 a:5/3 tap:5/4 n",
    "se:2 d put:2/3 pd n",
    "se:1 dd put:1/2 pd c",
];

#[test]
fn dup_cursor_steps_after_a_delete_or_last_match_mdbx() {
    let (_mdbx_dir, mdbx) = create_mdbx_db();
    let (_dir, db) = create_test_db(TndbEnvKind::RW);
    for seq in DUP_MATCHED {
        assert_eq!(dup_steps(&db, seq), dup_steps(&mdbx, seq));
    }
}

/// The known differences from MDBX's dup cursors, pinned on both sides (as `(steps, MDBX's
/// outcome, tndb-b's)`) so a change in either shows.
///
/// - **Past either end**, as on a table without duplicates (`known_cursor_deviations_from_mdbx` in
///   the adapter tests): MDBX's cursor stays on a row there, which `prev` steps back from (and,
///   before the first row, `next_no_dup` steps past the first key and `delete_current_duplicates`
///   deletes its rows); tndb-b's is after the end or before the start.
/// - **`last_dup` right after a delete of a key's only row** (or after
///   `delete_current_duplicates`): both return the follower's key's last value, but MDBX's cursor
///   stays on the follower, as right after the delete; tndb-b's is on that last row.
/// - **Another writer changes the key the cursor is in** (the crate docs' zone where MDBX is no
///   oracle): MDBX's cursor works from a stale copy of the key's rows. After the transaction's
///   delete of a key's last row while the key keeps earlier rows, it ends the key's rows on the row
///   before, as after its own delete, and after a delete that leaves the cursor's row alone, `prev`
///   skips it; after a `prev` from the next key, or a `next_dup` that found nothing, its `next`
///   skips a row the transaction puts after it; and `delete_current_duplicates` leaves `entries`
///   one more than the rows after the transaction's put in the key, one less after its delete of
///   the cursor's row. tndb-b's cursor finds its place again by key.
/// - **`append_dup` of the value a key's last row holds** is refused here (`MDBX_EKEYMISMATCH`);
///   MDBX takes it when the key has two rows or more (and refuses it when the key has one).
/// - **A key of one row**, which MDBX holds apart from its dup values: after an `append` gives such
///   a key (the table's last) a second, MDBX's cursor is on the new row, but `prev` steps to the
///   key before, `prev_dup` finds nothing, and `next`, `next_dup` and `next_no_dup` find nothing
///   even after another writer adds a row after it; after `last` on such a key, `next` and
///   `next_no_dup` do not see a later key the transaction appends; and a missed
///   `seek_by_key_subkey` past such a key's row, from a cursor that has moved, leaves MDBX's cursor
///   on that row. tndb-b's steps from the new row, takes the later key, and is unset.
/// - **A raw subkey wider than the layout's W** (pinned after the table, on `PlainStorageState`'s
///   raw view, [`put_wide_rows`]): when it starts with a row's subkey and its bytes past W sort at
///   or before that row's rest, MDBX's `seek_by_key_subkey` takes the row, since it compares the
///   row's whole value with the subkey. tndb-b's takes the row after, since its seek key is a
///   longer copy of the row's stored key (`key ‖ subkey` against `key ‖ subkey[..W]`), so sorts
///   after it. No reth subkey type encodes wider than W.
///
/// Not run on MDBX, where libmdbx fails an assertion and aborts: `last_dup` before the first row
/// (here: `None`); and, in `tree_rebalance`, `delete_current` after the transaction gives the
/// cursor's key of one row a second (`se:2 put:2/3 d`), or takes the second of the cursor's key's
/// two (`sk:4/1 del:4/2 d`), both of which delete the row here.
const DUP_DEVIATIONS: &[(&str, &str, &str)] = &[
    ("se:4 dd c", "se:4=4/1 dd=ok c=2/2", "se:4=4/1 dd=ok c=none"),
    ("l n p", "l=4/2 n=none p=2/2", "l=4/2 n=none p=4/2"),
    ("sk:4/2 d c n p", "sk:4/2=2 d=ok c=4/1 n=none p=2/2", "sk:4/2=2 d=ok c=4/1 n=none p=4/1"),
    ("f p nnd", "f=1/1 p=none nnd=2/2", "f=1/1 p=none nnd=1/1"),
    ("f p dd c", "f=1/1 p=none dd=ok c=2/2", "f=1/1 p=none dd=nodata c=none"),
    ("se:2 d ld n p", "se:2=2/2 d=ok ld=2 n=4/1 p=1/5", "se:2=2/2 d=ok ld=2 n=none p=4/2"),
    ("se:1 dd ld n", "se:1=1/1 dd=ok ld=2 n=2/2", "se:1=1/1 dd=ok ld=2 n=4/1"),
    ("sk:1/5 del:1/5 c ld", "sk:1/5=5 del:1/5=ok c=1/3 ld=3", "sk:1/5=5 del:1/5=ok c=2/2 ld=2"),
    ("ad:4/2", "ad:4/2=ok", "ad:4/2=keymismatch"),
    ("a:5/1 a:5/2 p", "a:5/1=ok a:5/2=ok p=4/2", "a:5/1=ok a:5/2=ok p=5/1"),
    ("a:5/1 a:5/2 pd n", "a:5/1=ok a:5/2=ok pd=none n=none", "a:5/1=ok a:5/2=ok pd=5/1 n=5/2"),
    (
        "a:5/1 a:5/2 tap:5/3 n",
        "a:5/1=ok a:5/2=ok tap:5/3=ok n=none",
        "a:5/1=ok a:5/2=ok tap:5/3=ok n=5/3",
    ),
    (
        "a:5/1 a:5/2 tap:6/1 nnd",
        "a:5/1=ok a:5/2=ok tap:6/1=ok nnd=none",
        "a:5/1=ok a:5/2=ok tap:6/1=ok nnd=6/1",
    ),
    (
        "tap:5/1 l tap:6/1 n",
        "tap:5/1=ok l=5/1 tap:6/1=ok n=none",
        "tap:5/1=ok l=5/1 tap:6/1=ok n=6/1",
    ),
    ("f sk:2/3 c", "f=1/1 sk:2/3=none c=2/2", "f=1/1 sk:2/3=none c=none"),
    ("se:2 p put:1/6 n", "se:2=2/2 p=1/5 put:1/6=ok n=2/2", "se:2=2/2 p=1/5 put:1/6=ok n=1/6"),
    (
        "sk:4/1 nd nd put:4/3 n",
        "sk:4/1=1 nd=4/2 nd=none put:4/3=ok n=none",
        "sk:4/1=1 nd=4/2 nd=none put:4/3=ok n=4/3",
    ),
    (
        "sk:4/1 del:4/2 c n p",
        "sk:4/1=1 del:4/2=ok c=4/1 n=none p=2/2",
        "sk:4/1=1 del:4/2=ok c=4/1 n=none p=4/1",
    ),
    ("se:4 put:4/3 dd en", "se:4=4/1 put:4/3=ok dd=ok en=5", "se:4=4/1 put:4/3=ok dd=ok en=4"),
    ("f del:1/1 dd en", "f=1/1 del:1/1=ok dd=ok en=2", "f=1/1 del:1/1=ok dd=ok en=3"),
];

#[test]
fn known_dup_cursor_deviations_from_mdbx() {
    let (_mdbx_dir, mdbx) = create_mdbx_db();
    let (_dir, db) = create_test_db(TndbEnvKind::RW);
    for (seq, on_mdbx, on_tndb) in DUP_DEVIATIONS {
        assert_eq!(dup_steps(&mdbx, seq), *on_mdbx, "MDBX");
        assert_eq!(dup_steps(&db, seq), *on_tndb, "tndb-b");
    }
    let tx = db.tx_mut().unwrap();
    tx.put::<PlainStorageState>(a(1), e(1, 1)).unwrap();
    let mut c = tx.cursor_dup_write::<PlainStorageState>().unwrap();
    assert_eq!(row(c.first()), "1/1");
    assert_eq!(row(c.prev()), "none");
    assert_eq!(val(c.last_dup()), "none", "last_dup before the first row");
    drop(c);
    drop(tx);

    // A raw subkey wider than the layout's: subkey 4 of key 2, then bytes that sort before the
    // row's rest (three 7s) or equal it. MDBX takes the row; tndb-b takes the row after (6).
    put_wide_rows::<PlainStorageState, _>(&mdbx);
    put_wide_rows::<PlainStorageState, _>(&db);
    let layout = dup_layout(PlainStorageState::NAME).unwrap();
    for past in [&[0][..], &[7, 7, 7]] {
        let (key, subkey) = (wide_key(layout, 2), [wide_subkey(layout, 4), past.to_vec()].concat());
        assert_eq!(seek_taken(&mdbx, key.clone(), subkey.clone()), Some(4), "MDBX, {past:?}");
        assert_eq!(seek_taken(&db, key, subkey), Some(6), "tndb-b, {past:?}");
    }
}

/// Checks table `T`'s layout against reth's encodings: `key`'s width, the width of `value`'s
/// subkey, and that the compressed `value` starts with its encoded subkey.
fn layout_matches<T: DupSort>(key: T::Key, value: &T::Value)
where
    T::Value: ValueWithSubKey<SubKey = T::SubKey>,
{
    let layout = dup_layout(T::NAME).unwrap_or_else(|| panic!("{} has no layout", T::NAME));
    assert_eq!(key.encode().as_ref().len(), layout.key, "{}: key width", T::NAME);
    let subkey = value.get_subkey().encode();
    assert_eq!(subkey.as_ref().len(), layout.subkey, "{}: subkey width", T::NAME);
    let mut buf = Vec::new();
    assert!(
        compressed(value, &mut buf).starts_with(subkey.as_ref()),
        "{}: the value does not start with its subkey",
        T::NAME
    );
}

/// A `StorageTrieEntry` at `nibbles`.
fn trie(nibbles: &[u8]) -> StorageTrieEntry {
    StorageTrieEntry {
        nibbles: StoredNibblesSubKey(Nibbles::from_nibbles(nibbles)),
        node: BranchNodeCompact::default(),
    }
}

#[test]
fn dup_layouts_match_reths_encodings() {
    let account = Some(Account { nonce: 1, balance: U256::from(2), bytecode_hash: None });
    layout_matches::<PlainStorageState>(a(1), &e(7, 9));
    layout_matches::<StorageChangeSets>(BlockNumberAddress((5, a(1))), &e(7, 9));
    layout_matches::<HashedStorages>(B256::with_last_byte(1), &e(7, 9));
    layout_matches::<AccountChangeSets>(5, &AccountBeforeTx { address: a(3), info: account });
    layout_matches::<AccountChangeSets>(5, &AccountBeforeTx { address: a(3), info: None });
    layout_matches::<StoragesTrie>(B256::with_last_byte(1), &trie(&[1, 2, 3]));
    layout_matches::<StoragesTrie>(B256::with_last_byte(1), &trie(&[]));
    // Every one of reth's dup-sort tables has a layout, and no other table does.
    for table in Tables::ALL {
        assert_eq!(dup_layout(table.name()).is_some(), table.is_dupsort(), "{}", table.name());
    }
}

/// Rows of `key` (`values`, in subkey order) and one of `other` in table `T`, then reads, a
/// delete with a value, and a read-only walk; every result logged.
fn dup_rows<T: DupSort, D: Database>(
    db: &D,
    key: &T::Key,
    other: &T::Key,
    values: &[T::Value],
) -> Vec<String>
where
    T::Key: Debug,
    T::Value: ValueWithSubKey<SubKey = T::SubKey> + Clone + Debug,
{
    let mut log = Vec::new();
    macro_rules! log {
        ($e:expr) => {{
            let result = $e;
            log.push(format!("{}: {:?}", stringify!($e), result));
        }};
    }
    let tx = db.tx_mut().unwrap();
    for value in values.iter().rev() {
        log!(tx.put::<T>(key.clone(), value.clone()));
    }
    log!(tx.put::<T>(other.clone(), values[0].clone()));
    let mut c = tx.cursor_dup_write::<T>().unwrap();
    log!(c.walk_dup(Some(key.clone()), None).unwrap().collect::<Vec<_>>());
    for value in values {
        log!(c.seek_by_key_subkey(key.clone(), value.get_subkey()));
        log!(c.next_dup());
    }
    log!(c.seek_exact(key.clone()));
    log!(c.last_dup());
    log!(c.next_no_dup());
    log!(c.walk(None).unwrap().collect::<Vec<_>>());
    log!(tx.delete::<T>(key.clone(), Some(values[1].clone())));
    log!(tx.get::<T>(key.clone()));
    log!(tx.entries::<T>());
    drop(c);
    tx.commit().unwrap();
    let tx = db.tx().unwrap();
    log!(tx.cursor_dup_read::<T>().unwrap().walk_back(None).unwrap().collect::<Vec<_>>());
    log
}

#[test]
fn every_dup_table_keeps_its_rows_as_mdbx_does() {
    let (_mdbx_dir, mdbx) = create_mdbx_db();
    let (_dir, db) = create_test_db(TndbEnvKind::RW);
    macro_rules! same {
        ($table:ty, $key:expr, $other:expr, $values:expr) => {
            assert_eq!(
                dup_rows::<$table, _>(&db, &$key, &$other, &$values),
                dup_rows::<$table, _>(&mdbx, &$key, &$other, &$values),
                stringify!($table)
            );
        };
    }
    let block = |n, k| BlockNumberAddress((n, a(k)));
    let before = |k, nonce| AccountBeforeTx {
        address: a(k),
        info: (nonce > 0).then(|| Account { nonce, ..Default::default() }),
    };
    same!(PlainStorageState, a(1), a(0), [e(1, 1), e(2, 2), e(3, 3)]);
    same!(StorageChangeSets, block(7, 1), block(7, 2), [e(1, 1), e(2, 0), e(3, 3)]);
    same!(HashedStorages, B256::with_last_byte(1), B256::ZERO, [e(1, 1), e(2, 2), e(9, 9)]);
    same!(AccountChangeSets, 7, 3, [before(1, 0), before(2, 5), before(3, 0)]);
    same!(
        StoragesTrie,
        B256::with_last_byte(1),
        B256::with_last_byte(2),
        [trie(&[1, 2]), trie(&[1, 2, 3]), trie(&[2])]
    );
}

/// A key or subkey `width` bytes wide whose every byte counts: `base + i` at byte `i`, the last
/// byte `n`.
fn wide(width: usize, base: u8, n: u8) -> Vec<u8> {
    let mut bytes: Vec<u8> = (0..width).map(|i| base.wrapping_add(i as u8)).collect();
    bytes[width - 1] = n;
    bytes
}

/// Key `n` of `layout`'s width ([`wide`]).
fn wide_key(layout: DupLayout, n: u8) -> Vec<u8> {
    wide(layout.key, 0xA0, n)
}

/// Subkey `n` of `layout`'s width ([`wide`]). Subkeys sort before keys, so a seek key with the
/// two swapped sorts before every row.
fn wide_subkey(layout: DupLayout, n: u8) -> Vec<u8> {
    wide(layout.subkey, 0x30, n)
}

/// Commits rows of the raw view of dup-sort table `T` at its layout's widths ([`wide_key`],
/// [`wide_subkey`]): key 1 at subkeys 3 and 5, key 2 at 2, 4 and 6, key 4 at 1 and 5 (no key of
/// one row, where MDBX's cursor misbehaves), each value's rest after the subkey three 7s.
fn put_wide_rows<T: DupSort, D: Database>(db: &D) {
    let layout = dup_layout(T::NAME).unwrap();
    let tx = db.tx_mut().unwrap();
    for (k, sk) in [(1, 3), (1, 5), (2, 2), (2, 4), (2, 6), (4, 1), (4, 5)] {
        let value = [wide_subkey(layout, sk), vec![7; 3]].concat();
        tx.put::<RawDupSort<T>>(RawKey::from_vec(wide_key(layout, k)), RawValue::from_vec(value))
            .unwrap();
    }
    tx.commit().unwrap();
}

/// The rows of [`put_wide_rows`] in dup-sort table `T`, then [`seeks_through`] a read cursor and
/// a write one.
fn subkey_seeks<T: DupSort, D: Database>(db: &D, seeks: &[(Vec<u8>, Vec<u8>)]) -> Vec<String>
where
    T::Key: Debug,
    T::Value: Debug,
{
    put_wide_rows::<T, _>(db);
    let mut log = Vec::new();
    let tx = db.tx().unwrap();
    let mut c = tx.cursor_dup_read::<RawDupSort<T>>().unwrap();
    seeks_through::<T, _>(&mut c, seeks, &mut log);
    let tx = db.tx_mut().unwrap();
    let mut c = tx.cursor_dup_write::<RawDupSort<T>>().unwrap();
    seeks_through::<T, _>(&mut c, seeks, &mut log);
    log
}

/// Each `seek_by_key_subkey` of `seeks` (a key and a subkey) through cursor `c` over the raw view
/// of table `T`, followed by `current` and `next_dup` and by a `walk_dup` from it, every result
/// logged to `log`.
fn seeks_through<T: DupSort, C>(c: &mut C, seeks: &[(Vec<u8>, Vec<u8>)], log: &mut Vec<String>)
where
    C: DbCursorRO<RawDupSort<T>> + DbDupCursorRO<RawDupSort<T>>,
    T::Key: Debug,
    T::Value: Debug,
{
    for (i, (key, subkey)) in seeks.iter().enumerate() {
        let (key, subkey) = (RawKey::from_vec(key.clone()), RawKey::from_vec(subkey.clone()));
        log.push(format!("{i} sk: {:?}", c.seek_by_key_subkey(key.clone(), subkey.clone())));
        log.push(format!("{i} c: {:?}", c.current()));
        log.push(format!("{i} nd: {:?}", c.next_dup()));
        let walk = c.walk_dup(Some(key), Some(subkey)).map(|w| w.collect::<Vec<_>>());
        log.push(format!("{i} walk_dup: {walk:?}"));
    }
}

/// `seek_by_key_subkey` (and `walk_dup` from a key and subkey) on every dup-sort table, with
/// keys and subkeys at its layout's full widths, as on MDBX ([`subkey_seeks`]): hits at a key's
/// first, middle and last rows, misses inside its rows (on the row after), a miss past them
/// (another key's rows follow), and keys absent from the table, before another key and past the
/// last.
#[test]
fn seek_by_key_subkey_matches_mdbx_at_every_layouts_widths() {
    let (_mdbx_dir, mdbx) = create_mdbx_db();
    let (_dir, db) = create_test_db(TndbEnvKind::RW);
    macro_rules! same {
        ($table:ty) => {{
            let layout = dup_layout(<$table>::NAME).unwrap();
            let (key, subkey) = (|n| wide_key(layout, n), |n| wide_subkey(layout, n));
            let seeks = [
                (key(2), subkey(2)),
                (key(2), subkey(4)),
                (key(2), subkey(6)),
                (key(2), subkey(3)),
                (key(2), subkey(1)),
                (key(2), subkey(7)),
                (key(1), subkey(5)),
                (key(1), subkey(6)),
                (key(3), subkey(5)),
                (key(5), subkey(0)),
            ];
            let expected = subkey_seeks::<$table, _>(&mdbx, &seeks);
            let got = subkey_seeks::<$table, _>(&db, &seeks);
            for (want, have) in expected.iter().zip(&got) {
                assert_eq!(have, want, stringify!($table));
            }
            assert_eq!(got.len(), expected.len());
        }};
    }
    same!(PlainStorageState);
    same!(StorageChangeSets);
    same!(HashedStorages);
    same!(AccountChangeSets);
    same!(StoragesTrie);
}

/// A `seek_by_key_subkey` key longer than the cursor's stack buffer for it (128 bytes) is the
/// same seek, on `PlainStorageState`'s raw view (key 20 bytes, subkey 32; [`subkey_seeks`]):
/// subkeys of 108 bytes (a seek key of 128, the longest on the stack), 109 and 200 that go
/// between two rows of the key, skip a row whose subkey they start with, or go past the key's
/// rows, and an oversized key, all as on MDBX.
///
/// A subkey that starts with a row's subkey and then sorts before that row's rest is not as on
/// MDBX, at every length: MDBX compares the row's whole value, `subkey ‖ rest`, with it, so it
/// takes the row, while here the seek key `key ‖ subkey` sorts after the row's stored key
/// `key ‖ subkey[..32]`, of which it is a longer copy, so it takes the row after. No reth
/// subkey is wider than its table's layout, so only a raw one meets this.
/// `known_dup_cursor_deviations_from_mdbx` pins it with short subkeys; this test pins it with
/// subkeys at and past the stack buffer's limit.
#[test]
fn seek_by_key_subkey_longer_than_the_stack_buffer_matches_mdbx() {
    let (_mdbx_dir, mdbx) = create_mdbx_db();
    let (_dir, db) = create_test_db(TndbEnvKind::RW);
    let layout = dup_layout(PlainStorageState::NAME).unwrap();
    let (key, subkey) = (|n| wide_key(layout, n), |n| wide_subkey(layout, n));
    let longer = |n, len, fill| {
        let mut bytes = subkey(n);
        bytes.resize(len, fill);
        bytes
    };
    let mut seeks = Vec::new();
    for len in [108, 109, 200] {
        seeks.extend([
            (key(2), longer(3, len, 0x00)),
            (key(2), longer(4, len, 0xFF)),
            (key(2), longer(6, len, 0xFF)),
        ]);
    }
    seeks.push(([key(2), vec![0; 120]].concat(), subkey(4)));
    let expected = subkey_seeks::<PlainStorageState, _>(&mdbx, &seeks);
    let got = subkey_seeks::<PlainStorageState, _>(&db, &seeks);
    for (want, have) in expected.iter().zip(&got) {
        assert_eq!(have, want);
    }
    assert_eq!(got.len(), expected.len());

    // The difference, pinned beside MDBX's result: subkey 4, then zeros, which sort before the
    // row's rest (7s).
    for len in [108, 200] {
        let (k, sk) = (key(2), longer(4, len, 0x00));
        assert_eq!(seek_taken(&mdbx, k.clone(), sk.clone()), Some(4), "{len}");
        assert_eq!(seek_taken(&db, k, sk), Some(6), "{len}");
    }
}

/// The last byte of the subkey of the value `seek_by_key_subkey(key, subkey)` takes on
/// `PlainStorageState`'s raw view, if any.
fn seek_taken<D: Database>(db: &D, key: Vec<u8>, subkey: Vec<u8>) -> Option<u8> {
    let tx = db.tx().unwrap();
    let mut c = tx.cursor_dup_read::<RawDupSort<PlainStorageState>>().unwrap();
    let value = c.seek_by_key_subkey(RawKey::from_vec(key), RawKey::from_vec(subkey)).unwrap()?;
    Some(value.raw_value()[31])
}

/// Every row of `StorageChangeSets`, by a walk.
fn all_rows(tx: &impl DbTx) -> Vec<(BlockNumberAddress, StorageEntry)> {
    let mut cursor = tx.cursor_dup_read::<StorageChangeSets>().unwrap();
    cursor.walk(None).unwrap().collect::<Result<Vec<_>, _>>().unwrap()
}

/// reth's `import_dupsort` (`next_no_dup`, `walk_dup` and `append_dup`) from MDBX into tndb-b and
/// back, each copy the same as the source.
#[test]
fn import_dupsort_copies_every_row_between_mdbx_and_tndb() {
    let (_mdbx_dir, mdbx) = create_mdbx_db();
    let (_dir, db) = create_test_db(TndbEnvKind::RW);
    let (_back_dir, back) = create_mdbx_db();
    mdbx.update(|tx| {
        for n in [3u64, 5, 9] {
            for k in [1, 2, 4] {
                for sk in 1..=u8::try_from(n).unwrap() {
                    let key = BlockNumberAddress((n, a(k)));
                    tx.put::<StorageChangeSets>(key, e(sk, n + u64::from(sk)))?;
                }
            }
        }
        Ok::<_, DatabaseError>(())
    })
    .unwrap()
    .unwrap();
    let source = mdbx.tx().unwrap();
    db.update(|tx| tx.import_dupsort::<StorageChangeSets, _>(&source)).unwrap().unwrap();
    let copied = db.tx().unwrap();
    back.update(|tx| tx.import_dupsort::<StorageChangeSets, _>(&copied)).unwrap().unwrap();
    let source_rows = all_rows(&source);
    let copied_rows = all_rows(&copied);
    let back_rows = all_rows(&back.tx().unwrap());
    assert_eq!(source_rows.len(), 3 * (3 + 5 + 9));
    assert_eq!(copied_rows, source_rows);
    assert_eq!(back_rows, source_rows);
}

/// A dup-sort table this crate has no layout for.
#[derive(Debug)]
struct UnknownDup;

impl Table for UnknownDup {
    const NAME: &'static str = "UnknownDup";
    const DUPSORT: bool = true;
    type Key = u64;
    type Value = B256;
}

impl DupSort for UnknownDup {
    type SubKey = B256;
}

/// A table without duplicates seen through a type with dup cursor calls.
#[derive(Debug)]
struct PlainAsDup;

impl Table for PlainAsDup {
    const NAME: &'static str = CanonicalHeaders::NAME;
    const DUPSORT: bool = false;
    type Key = u64;
    type Value = B256;
}

impl DupSort for PlainAsDup {
    type SubKey = u64;
}

/// An error's every field, for comparing errors (`DatabaseError` has no `PartialEq`).
fn dbg(error: DatabaseError) -> String {
    format!("{error:?}")
}

/// Rows a layout cannot hold are refused with `MDBX_BAD_VALSIZE`; a dup-sort table without a
/// layout is created, counted and cleared, but every access to its rows is refused; and dup
/// cursor calls on a table without duplicates are refused. Errors, never a panic.
#[test]
fn rows_and_tables_without_a_layout_are_refused() {
    use DatabaseWriteOperation as Op;
    type Raw = RawDupSort<PlainStorageState>;
    let (_dir, db) = create_test_db(TndbEnvKind::RW);
    let bad_size = |operation, key: Vec<u8>| {
        let table_name = PlainStorageState::NAME;
        dbg(DatabaseWriteError { info: Error::BadValSize.into(), operation, table_name, key }
            .into())
    };
    let (key, raw) = (|n| RawKey::from_vec(vec![1; n]), |n| RawValue::from_vec(vec![7; n]));
    let tx = db.tx_mut().unwrap();
    // A value shorter than the subkey, a key not of the table's width.
    assert_eq!(
        dbg(tx.put::<Raw>(key(20), raw(31)).unwrap_err()),
        bad_size(Op::PutUpsert, vec![1; 20])
    );
    assert_eq!(
        dbg(tx.put::<Raw>(key(19), raw(40)).unwrap_err()),
        bad_size(Op::PutUpsert, vec![1; 19])
    );
    assert_eq!(
        dbg(tx.append::<Raw>(key(21), raw(40)).unwrap_err()),
        bad_size(Op::PutAppend, vec![1; 21])
    );
    let mut c = tx.cursor_dup_write::<Raw>().unwrap();
    for (write, op) in [(0, Op::CursorUpsert), (1, Op::CursorInsert), (2, Op::CursorAppend)] {
        let result = match write {
            0 => c.upsert(key(20), &raw(31)),
            1 => c.insert(key(20), &raw(31)),
            _ => c.append(key(20), &raw(31)),
        };
        assert_eq!(dbg(result.unwrap_err()), bad_size(op, vec![1; 20]));
    }
    assert_eq!(
        dbg(c.append_dup(key(21), raw(32)).unwrap_err()),
        bad_size(Op::CursorAppendDup, vec![1; 21])
    );
    // A value of exactly the subkey's width is a row with an empty rest.
    c.upsert(key(20), &raw(32)).unwrap();
    assert_eq!(c.current().unwrap(), Some((key(20), raw(32))));
    // A delete with a value no row can hold deletes nothing.
    assert!(!tx.delete::<Raw>(key(20), Some(raw(31))).unwrap());
    assert_eq!(tx.entries::<Raw>().unwrap(), 1);
    drop(c);
    tx.commit().unwrap();

    // A dup-sort table without a layout.
    let mut txn = db.env().begin_rw().unwrap();
    txn.create_table(UnknownDup::NAME).unwrap();
    txn.commit().unwrap();
    let refused = |e: DatabaseError| {
        assert!(matches!(&e, DatabaseError::Other(m) if m.contains(UnknownDup::NAME)), "{e:?}");
    };
    let tx = db.tx_mut().unwrap();
    refused(tx.put::<UnknownDup>(1, B256::ZERO).unwrap_err());
    refused(tx.append::<UnknownDup>(1, B256::ZERO).unwrap_err());
    refused(tx.get::<UnknownDup>(1).unwrap_err());
    refused(tx.delete::<UnknownDup>(1, None).unwrap_err());
    refused(tx.cursor_write::<UnknownDup>().unwrap_err());
    refused(tx.cursor_dup_write::<UnknownDup>().unwrap_err());
    refused(tx.cursor_dup_read::<UnknownDup>().unwrap_err());
    assert_eq!(tx.entries::<UnknownDup>().unwrap(), 0);
    tx.clear::<UnknownDup>().unwrap();
    tx.commit().unwrap();
    let tx = db.tx().unwrap();
    refused(tx.get::<UnknownDup>(1).unwrap_err());
    refused(tx.cursor_dup_read::<UnknownDup>().unwrap_err());
    assert_eq!(tx.entries::<UnknownDup>().unwrap(), 0);
    drop(tx);

    // Dup cursor calls on a table without duplicates.
    let refused = |e: DatabaseError| {
        assert!(matches!(&e, DatabaseError::Other(m) if m.contains(PlainAsDup::NAME)), "{e:?}");
    };
    let tx = db.tx_mut().unwrap();
    tx.put::<PlainAsDup>(1, B256::ZERO).unwrap();
    let mut c = tx.cursor_dup_write::<PlainAsDup>().unwrap();
    assert_eq!(c.seek_exact(1).unwrap(), Some((1, B256::ZERO)));
    refused(c.next_dup().unwrap_err());
    refused(c.prev_dup().unwrap_err());
    refused(c.last_dup().unwrap_err());
    refused(c.next_no_dup().unwrap_err());
    refused(c.next_dup_val().unwrap_err());
    refused(c.seek_by_key_subkey(1, 1).unwrap_err());
    refused(c.walk_dup(None, None).unwrap_err());
    refused(c.delete_current_duplicates().unwrap_err());
    refused(c.append_dup(2, B256::ZERO).unwrap_err());
    drop(c);
    tx.commit().unwrap();
    let mut c = db.tx().unwrap().cursor_dup_read::<PlainAsDup>().unwrap();
    refused(c.next_dup().unwrap_err());
    refused(c.walk_dup(Some(1), None).unwrap_err());
}

/// A range walk from an excluded key starts past every row of that key (MDBX's cursor panics on
/// an excluded start).
#[test]
fn a_range_walk_on_a_dup_table_starts_past_every_row_of_an_excluded_key() {
    let (_dir, db) = create_test_db(TndbEnvKind::RW);
    db.update(|tx| {
        for (k, sk) in [(1, 1), (1, 2), (2, 1), (2, 2), (2, 3), (3, 1)] {
            tx.put::<PlainStorageState>(a(k), e(sk, 0))?;
        }
        Ok::<_, DatabaseError>(())
    })
    .unwrap()
    .unwrap();
    let tx = db.tx().unwrap();
    let mut cursor = tx.cursor_dup_read::<PlainStorageState>().unwrap();
    let range = (std::ops::Bound::Excluded(a(1)), std::ops::Bound::Excluded(a(3)));
    let rows: Vec<_> = cursor.walk_range(range).unwrap().map(|r| row(r.map(Some))).collect();
    assert_eq!(rows, ["2/1", "2/2", "2/3"]);
}

/// Dup rows whose values are too large for a leaf, so the engine keeps them in overflow runs,
/// where a row's value is not next to its subkey (no reth dup table's values get that large):
/// every read call returns each full value, `subkey ‖ rest`, as with the inline ones beside them,
/// and a `seek_by_key_subkey` past a key's rows or of a key with none misses although a row in a
/// run follows.
#[test]
fn dup_values_in_overflow_runs_read_back_whole() {
    type Raw = RawDupSort<PlainStorageState>;
    let (_dir, db) = create_test_db(TndbEnvKind::RW);
    let key = |n: u8| RawKey::<Address>::from_vec(vec![n; 20]);
    // Subkey `s`, then a rest of `len` bytes: 5000 and 3000 take overflow runs, 10 is inline.
    let value = |s: u8, len: usize| {
        let mut v = vec![s; 32];
        v.extend((0..len).map(|i| (i % 251) as u8 ^ s));
        RawValue::<StorageEntry>::from_vec(v)
    };
    let rows = [
        (key(1), value(1, 5000)),
        (key(1), value(2, 10)),
        (key(1), value(3, 3000)),
        (key(2), value(1, 4000)),
    ];
    let tx = db.tx_mut().unwrap();
    for (k, v) in rows.iter().rev() {
        tx.put::<Raw>(k.clone(), v.clone()).unwrap();
    }
    assert_eq!(tx.get::<Raw>(key(1)).unwrap(), Some(rows[0].1.clone()));
    let mut c = tx.cursor_dup_write::<Raw>().unwrap();
    assert_eq!(c.seek_exact(key(1)).unwrap(), Some(rows[0].clone()));
    assert_eq!(c.next_dup().unwrap(), Some(rows[1].clone()));
    assert_eq!(c.next_dup().unwrap(), Some(rows[2].clone()));
    assert_eq!(c.next_dup().unwrap(), None);
    assert_eq!(
        c.seek_by_key_subkey(key(1), RawKey::new(B256::repeat_byte(3))).unwrap(),
        Some(rows[2].1.clone())
    );
    assert_eq!(c.prev_dup().unwrap(), Some(rows[1].clone()));
    assert_eq!(c.last_dup().unwrap(), Some(rows[2].1.clone()));
    assert_eq!(c.next_no_dup().unwrap(), Some(rows[3].clone()));
    assert_eq!(c.current().unwrap(), Some(rows[3].clone()));
    assert_eq!(c.prev().unwrap(), Some(rows[2].clone()));
    drop(c);
    tx.commit().unwrap();

    let tx = db.tx().unwrap();
    assert_eq!(tx.get::<Raw>(key(2)).unwrap(), Some(rows[3].1.clone()));
    let mut c = tx.cursor_dup_read::<Raw>().unwrap();
    let walked = c.walk(None).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(walked, rows);
    let back = c.walk_back(None).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(back, rows.iter().rev().cloned().collect::<Vec<_>>());
    let dups = c.walk_dup(Some(key(1)), None).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(dups, rows[..3]);
    let from = c.walk_dup(Some(key(1)), Some(RawKey::new(B256::repeat_byte(2)))).unwrap();
    assert_eq!(from.collect::<Result<Vec<_>, _>>().unwrap(), rows[1..3]);
    assert_eq!(c.first().unwrap(), Some(rows[0].clone()));
    assert_eq!(c.next_dup_val().unwrap(), Some(rows[1].1.clone()));
    assert_eq!(c.last().unwrap(), Some(rows[3].clone()));

    // `seek_by_key_subkey` through the read cursor: a hit on a value in a run, and a miss between
    // two subkeys that lands on one, each leaving the cursor at its row; a subkey past the key's
    // rows, and a key with no rows, each a miss although the next row (key 2's, in a run) follows.
    let subkey = |bytes: [u8; 32]| RawKey::new(B256::from(bytes));
    let mut between = [2; 32];
    between[31] = 3;
    assert_eq!(c.seek_by_key_subkey(key(1), subkey([1; 32])).unwrap(), Some(rows[0].1.clone()));
    assert_eq!(c.current().unwrap(), Some(rows[0].clone()));
    assert_eq!(c.next_dup().unwrap(), Some(rows[1].clone()));
    assert_eq!(c.seek_by_key_subkey(key(1), subkey(between)).unwrap(), Some(rows[2].1.clone()));
    assert_eq!(c.prev_dup().unwrap(), Some(rows[1].clone()));
    assert_eq!(c.seek_by_key_subkey(key(1), subkey([4; 32])).unwrap(), None);
    assert_eq!(c.current().unwrap(), None);
    let mut absent = vec![1; 20];
    absent[19] = 9;
    assert_eq!(c.seek_by_key_subkey(RawKey::from_vec(absent), subkey([0; 32])).unwrap(), None);
    assert_eq!(c.next_dup().unwrap(), None);
    let from = c.walk_dup(Some(key(1)), Some(subkey(between))).unwrap();
    assert_eq!(from.collect::<Result<Vec<_>, _>>().unwrap(), rows[2..3]);
}

/// [`dup_steps`] through a cursor over the raw view of `PlainStorageState` (whose values can be
/// any size), on the same rows, with the steps `sk`, `se`, `l`, `n`, `p`, `c`, `nd`, `pd`, `ld`,
/// `d`, `put` and `en`, and writes of a 17 MiB value with subkey `S` (over both engines' limits, so
/// refused with `MDBX_BAD_VALSIZE`, shown as `badvalsize`): `bu:K/S` `upsert`, `bi:K/S`
/// `insert`, `ba:K/S` `append` and `bad:K/S` `append_dup`.
fn big_write_steps<D: Database>(db: &D, steps: &str) -> String {
    type Raw = RawDupSort<PlainStorageState>;
    let raw_key = |k: u8| RawKey::new(a(k));
    let big = |sk: u8| {
        let mut v = vec![0; 17 << 20];
        v[..32].copy_from_slice(s(sk).as_slice());
        RawValue::<StorageEntry>::from_vec(v)
    };
    let row = |result: PairResult<Raw>| match result {
        Ok(Some((key, value))) => format!("{}/{}", key.raw_key()[19], value.raw_value()[31]),
        Ok(None) => "none".into(),
        Err(e) => error_name(e),
    };
    let done = |result: Result<(), DatabaseError>| match result {
        Err(DatabaseError::Write(w)) if w.info == Error::BadValSize.into() => "badvalsize".into(),
        result => done(result),
    };
    let tx = db.tx_mut().unwrap();
    for (k, sk) in [(1, 1), (1, 3), (1, 5), (2, 2), (4, 1), (4, 2)] {
        tx.put::<PlainStorageState>(a(k), e(sk, u64::from(sk))).unwrap();
    }
    let mut c = tx.cursor_dup_write::<Raw>().unwrap();
    let mut out = Vec::new();
    for step in steps.split_whitespace() {
        let (op, arg) = step.split_once(':').unwrap_or((step, ""));
        let (k, sk) = arg.split_once('/').unwrap_or((arg, "0"));
        let (k, sk): (u8, u8) = (k.parse().unwrap_or(0), sk.parse().unwrap_or(0));
        let outcome = match op {
            "sk" => match c.seek_by_key_subkey(raw_key(k), RawKey::new(s(sk))) {
                Ok(Some(value)) => value.raw_value()[31].to_string(),
                Ok(None) => "none".into(),
                Err(e) => error_name(e),
            },
            "se" => row(c.seek_exact(raw_key(k))),
            "l" => row(c.last()),
            "n" => row(c.next()),
            "p" => row(c.prev()),
            "c" => row(c.current()),
            "nd" => row(c.next_dup()),
            "pd" => row(c.prev_dup()),
            "ld" => match c.last_dup() {
                Ok(Some(value)) => value.raw_value()[31].to_string(),
                Ok(None) => "none".into(),
                Err(e) => error_name(e),
            },
            "d" => done(c.delete_current()),
            "bu" => done(c.upsert(raw_key(k), &big(sk))),
            "bi" => done(c.insert(raw_key(k), &big(sk))),
            "ba" => done(c.append(raw_key(k), &big(sk))),
            "bad" => done(c.append_dup(raw_key(k), big(sk))),
            "put" => done(tx.put::<PlainStorageState>(a(k), e(sk, u64::from(sk)))),
            "en" => tx.entries::<PlainStorageState>().map_or_else(error_name, |n| n.to_string()),
            _ => panic!("unknown step {op}"),
        };
        out.push(format!("{step}={outcome}"));
    }
    out.join(" ")
}

/// A write refused for its size leaves the cursor where it was, in every state of
/// `TndbCursorMut`: unset after a missed seek (a delete then has nothing to delete), at a key's
/// end after a delete or `last`, right after a delete that left a key no rows, at a row (an
/// insert's look for the key's rows does not move it), and fresh. MDBX refuses a dup value that
/// large too, but its debug build (reth's tests') fails an assertion there and aborts, so the
/// oracle is MDBX run without the refused writes: every other step's outcome is the same.
#[test]
fn a_dup_write_refused_for_its_size_leaves_the_cursor_where_it_was() {
    const SEQS: &[&str] = &[
        "sk:1/9 d bu:1/9 c d en",
        "se:3 bu:3/1 c d en",
        "sk:1/9 bi:3/1 c d en",
        "sk:1/9 ba:5/1 c d en",
        "sk:1/9 bad:1/9 c d en",
        "sk:1/5 d bu:1/9 c d nd en",
        "sk:1/5 d bad:1/9 c d en",
        "l put:4/3 bu:4/9 nd c",
        "sk:1/1 ld put:1/6 bu:1/9 nd c",
        "se:2 d put:2/3 bu:2/9 pd c",
        "sk:1/3 bi:3/1 c d en",
        "sk:1/3 ba:5/1 c",
        "bu:1/9 n",
        "bi:3/1 p",
    ];
    let (_mdbx_dir, mdbx) = create_mdbx_db();
    let (_dir, db) = create_test_db(TndbEnvKind::RW);
    assert_eq!(
        big_write_steps(&db, SEQS[0]),
        "sk:1/9=none d=nodata bu:1/9=badvalsize c=none d=nodata en=6"
    );
    for seq in SEQS {
        let big = |step: &str| step.starts_with('b');
        let outcomes = big_write_steps(&db, seq);
        let (refused, rest): (Vec<&str>, Vec<&str>) = outcomes.split(' ').partition(|s| big(s));
        assert!(refused.iter().all(|s| s.ends_with("=badvalsize")), "{seq}: {refused:?}");
        let without: Vec<&str> = seq.split(' ').filter(|step| !big(step)).collect();
        assert_eq!(rest.join(" "), big_write_steps(&mdbx, &without.join(" ")), "{seq}");
    }
}
