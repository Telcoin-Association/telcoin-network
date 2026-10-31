//! reth's tests of its MDBX implementation (`reth-db`'s `implementation/mdbx/mod.rs`, and
//! `test_import_table_with_range_works_on_dupsort` of its `cursor.rs`), with only the constructor
//! swapped for [`TndbEnv`]'s; `Address::random` and `B256::random` (alloy's `rand` feature)
//! became fixed values. Three dup-sort tests assert this crate's documented deviation instead of
//! MDBX's behaviour: a second value with the same subkey is refused with `MDBX_KEYEXIST`, the first
//! kept (`db_cursor_upsert`'s dup-sort half, `db_dup_cursor_delete_first` and
//! `dup_value_with_same_subkey`). Not ported:
//! `db_drop_orphan_table` (MDBX's table handles).

use super::dup::second_value_refused;
use crate::{TndbArgs, TndbEnv, TndbEnvKind};
use alloy::{
    consensus::Header,
    primitives::{address, Address, B256, U256},
};
use reth_db_api::{
    cursor::{DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW, ReverseWalker, Walker},
    database::Database,
    models::{AccountBeforeTx, BlockNumberAddress, IntegerList, ShardedKey},
    table::{Encode, Table, TableImporter},
    tables::{
        AccountChangeSets, AccountsHistory, CanonicalHeaders, Headers, PlainAccountState,
        PlainStorageState, StorageChangeSets,
    },
    transaction::{DbTx, DbTxMut},
    DatabaseError,
};
use reth_libmdbx::Error;
use reth_primitives_traits::{Account, StorageEntry};
use reth_storage_errors::db::{DatabaseWriteError, DatabaseWriteOperation};
use std::{path::Path, str::FromStr};
use tempfile::TempDir;

/// Create database for testing. Returns the `TempDir` to prevent cleanup until test ends.
pub(super) fn create_test_db(kind: TndbEnvKind) -> (TempDir, TndbEnv) {
    create_test_db_with(kind, true)
}

/// [`create_test_db`] with the tables of the nested dup layouts in one form: engine dup tables
/// (`nested`), or composite keys in plain tables ([`TndbArgs::composite_dup_tables`]).
pub(super) fn create_test_db_with(kind: TndbEnvKind, nested: bool) -> (TempDir, TndbEnv) {
    let tempdir = tempfile::TempDir::new().expect(ERROR_TEMPDIR);
    let args =
        if nested { TndbArgs::default() } else { TndbArgs::default().composite_dup_tables() };
    let env = create_test_db_with_path(kind, tempdir.path(), args);
    (tempdir, env)
}

/// Create database for testing with specified path
fn create_test_db_with_path(kind: TndbEnvKind, path: &Path, args: TndbArgs) -> TndbEnv {
    let mut env = super::open_env(path, kind, args).expect(ERROR_DB_CREATION);
    env.create_tables().expect(ERROR_TABLE_CREATION);
    env
}

const ERROR_TEMPDIR: &str = "Not able to create a temporary directory.";
const ERROR_TABLE_CREATION: &str = "Not able to create tables in the database.";
const ERROR_DB_CREATION: &str = "Not able to create the tndb-b file.";
const ERROR_PUT: &str = "Not able to insert value into table.";
const ERROR_UPSERT: &str = "Not able to upsert the value to the table.";
const ERROR_GET: &str = "Not able to get value from table.";
const ERROR_COMMIT: &str = "Not able to commit transaction.";
const ERROR_RETURN_VALUE: &str = "Mismatching result.";
const ERROR_INIT_TX: &str = "Failed to create a tndb-b transaction.";
const ERROR_ETH_ADDRESS: &str = "Invalid address.";
const ERROR_DEL: &str = "Not able to delete from table.";
const ERROR_APPEND: &str = "Not able to append the value to the table.";

#[test]
fn db_creation() {
    let _tempdir = create_test_db(TndbEnvKind::RW);
}

#[test]
fn db_manual_put_get() {
    let (_tempdir, env) = create_test_db(TndbEnvKind::RW);

    let value = Header::default();
    let key = 1u64;

    // PUT
    let tx = env.tx_mut().expect(ERROR_INIT_TX);
    tx.put::<Headers>(key, value.clone()).expect(ERROR_PUT);
    tx.commit().expect(ERROR_COMMIT);

    // GET
    let tx = env.tx().expect(ERROR_INIT_TX);
    let result = tx.get::<Headers>(key).expect(ERROR_GET);
    assert_eq!(result.expect(ERROR_RETURN_VALUE), value);
    tx.commit().expect(ERROR_COMMIT);
}

#[test]
fn db_cursor_walk() {
    let (_tempdir, env) = create_test_db(TndbEnvKind::RW);

    let value = Header::default();
    let key = 1u64;

    // PUT
    let tx = env.tx_mut().expect(ERROR_INIT_TX);
    tx.put::<Headers>(key, value.clone()).expect(ERROR_PUT);
    tx.commit().expect(ERROR_COMMIT);

    // Cursor
    let tx = env.tx().expect(ERROR_INIT_TX);
    let mut cursor = tx.cursor_read::<Headers>().unwrap();

    let first = cursor.first().unwrap();
    assert!(first.is_some(), "First should be our put");

    // Walk
    let walk = cursor.walk(Some(key)).unwrap();
    let first = walk.into_iter().next().unwrap().unwrap();
    assert_eq!(first.1, value, "First next should be put value");
}

#[test]
fn db_cursor_walk_range() {
    let (_tempdir, db) = create_test_db(TndbEnvKind::RW);

    // PUT (0, 0), (1, 0), (2, 0), (3, 0)
    let tx = db.tx_mut().expect(ERROR_INIT_TX);
    vec![0, 1, 2, 3]
        .into_iter()
        .try_for_each(|key| tx.put::<CanonicalHeaders>(key, B256::ZERO))
        .expect(ERROR_PUT);
    tx.commit().expect(ERROR_COMMIT);

    let tx = db.tx().expect(ERROR_INIT_TX);
    let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();

    // [1, 3)
    let mut walker = cursor.walk_range(1..3).unwrap();
    assert_eq!(walker.next().unwrap().unwrap(), (1, B256::ZERO));
    assert_eq!(walker.next().unwrap().unwrap(), (2, B256::ZERO));
    assert!(walker.next().is_none());
    // next() returns None after walker is done
    assert!(walker.next().is_none());

    // [1, 2]
    let mut walker = cursor.walk_range(1..=2).unwrap();
    assert_eq!(walker.next().unwrap().unwrap(), (1, B256::ZERO));
    assert_eq!(walker.next().unwrap().unwrap(), (2, B256::ZERO));
    // next() returns None after walker is done
    assert!(walker.next().is_none());

    // [1, ∞)
    let mut walker = cursor.walk_range(1..).unwrap();
    assert_eq!(walker.next().unwrap().unwrap(), (1, B256::ZERO));
    assert_eq!(walker.next().unwrap().unwrap(), (2, B256::ZERO));
    assert_eq!(walker.next().unwrap().unwrap(), (3, B256::ZERO));
    // next() returns None after walker is done
    assert!(walker.next().is_none());

    // [2, 4)
    let mut walker = cursor.walk_range(2..4).unwrap();
    assert_eq!(walker.next().unwrap().unwrap(), (2, B256::ZERO));
    assert_eq!(walker.next().unwrap().unwrap(), (3, B256::ZERO));
    assert!(walker.next().is_none());
    // next() returns None after walker is done
    assert!(walker.next().is_none());

    // (∞, 3)
    let mut walker = cursor.walk_range(..3).unwrap();
    assert_eq!(walker.next().unwrap().unwrap(), (0, B256::ZERO));
    assert_eq!(walker.next().unwrap().unwrap(), (1, B256::ZERO));
    assert_eq!(walker.next().unwrap().unwrap(), (2, B256::ZERO));
    // next() returns None after walker is done
    assert!(walker.next().is_none());

    // (∞, ∞)
    let mut walker = cursor.walk_range(..).unwrap();
    assert_eq!(walker.next().unwrap().unwrap(), (0, B256::ZERO));
    assert_eq!(walker.next().unwrap().unwrap(), (1, B256::ZERO));
    assert_eq!(walker.next().unwrap().unwrap(), (2, B256::ZERO));
    assert_eq!(walker.next().unwrap().unwrap(), (3, B256::ZERO));
    // next() returns None after walker is done
    assert!(walker.next().is_none());
}

#[test]
#[expect(clippy::reversed_empty_ranges, reason = "the test walks empty ranges on purpose")]
fn db_cursor_walk_range_invalid() {
    let (_tempdir, db) = create_test_db(TndbEnvKind::RW);

    // PUT (0, 0), (1, 0), (2, 0), (3, 0)
    let tx = db.tx_mut().expect(ERROR_INIT_TX);
    vec![0, 1, 2, 3]
        .into_iter()
        .try_for_each(|key| tx.put::<CanonicalHeaders>(key, B256::ZERO))
        .expect(ERROR_PUT);
    tx.commit().expect(ERROR_COMMIT);

    let tx = db.tx().expect(ERROR_INIT_TX);
    let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();

    // start bound greater than end bound
    let mut res = cursor.walk_range(3..1).unwrap();
    assert!(res.next().is_none());

    // start bound greater than end bound
    let mut res = cursor.walk_range(15..=2).unwrap();
    assert!(res.next().is_none());

    // returning nothing
    let mut walker = cursor.walk_range(1..1).unwrap();
    assert!(walker.next().is_none());
}

#[test]
fn db_walker() {
    let (_tempdir, db) = create_test_db(TndbEnvKind::RW);

    // PUT (0, 0), (1, 0), (3, 0)
    let tx = db.tx_mut().expect(ERROR_INIT_TX);
    vec![0, 1, 3]
        .into_iter()
        .try_for_each(|key| tx.put::<CanonicalHeaders>(key, B256::ZERO))
        .expect(ERROR_PUT);
    tx.commit().expect(ERROR_COMMIT);

    let tx = db.tx().expect(ERROR_INIT_TX);
    let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();

    let mut walker = Walker::new(&mut cursor, None);

    assert_eq!(walker.next().unwrap().unwrap(), (0, B256::ZERO));
    assert_eq!(walker.next().unwrap().unwrap(), (1, B256::ZERO));
    assert_eq!(walker.next().unwrap().unwrap(), (3, B256::ZERO));
    assert!(walker.next().is_none());

    // transform to ReverseWalker
    let mut reverse_walker = walker.rev();
    assert_eq!(reverse_walker.next().unwrap().unwrap(), (3, B256::ZERO));
    assert_eq!(reverse_walker.next().unwrap().unwrap(), (1, B256::ZERO));
    assert_eq!(reverse_walker.next().unwrap().unwrap(), (0, B256::ZERO));
    assert!(reverse_walker.next().is_none());
}

#[test]
fn db_reverse_walker() {
    let (_tempdir, db) = create_test_db(TndbEnvKind::RW);

    // PUT (0, 0), (1, 0), (3, 0)
    let tx = db.tx_mut().expect(ERROR_INIT_TX);
    vec![0, 1, 3]
        .into_iter()
        .try_for_each(|key| tx.put::<CanonicalHeaders>(key, B256::ZERO))
        .expect(ERROR_PUT);
    tx.commit().expect(ERROR_COMMIT);

    let tx = db.tx().expect(ERROR_INIT_TX);
    let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();

    let mut reverse_walker = ReverseWalker::new(&mut cursor, None);

    assert_eq!(reverse_walker.next().unwrap().unwrap(), (3, B256::ZERO));
    assert_eq!(reverse_walker.next().unwrap().unwrap(), (1, B256::ZERO));
    assert_eq!(reverse_walker.next().unwrap().unwrap(), (0, B256::ZERO));
    assert!(reverse_walker.next().is_none());

    // transform to Walker
    let mut walker = reverse_walker.forward();
    assert_eq!(walker.next().unwrap().unwrap(), (0, B256::ZERO));
    assert_eq!(walker.next().unwrap().unwrap(), (1, B256::ZERO));
    assert_eq!(walker.next().unwrap().unwrap(), (3, B256::ZERO));
    assert!(walker.next().is_none());
}

#[test]
fn db_walk_back() {
    let (_tempdir, db) = create_test_db(TndbEnvKind::RW);

    // PUT (0, 0), (1, 0), (3, 0)
    let tx = db.tx_mut().expect(ERROR_INIT_TX);
    vec![0, 1, 3]
        .into_iter()
        .try_for_each(|key| tx.put::<CanonicalHeaders>(key, B256::ZERO))
        .expect(ERROR_PUT);
    tx.commit().expect(ERROR_COMMIT);

    let tx = db.tx().expect(ERROR_INIT_TX);
    let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();

    let mut reverse_walker = cursor.walk_back(Some(1)).unwrap();
    assert_eq!(reverse_walker.next().unwrap().unwrap(), (1, B256::ZERO));
    assert_eq!(reverse_walker.next().unwrap().unwrap(), (0, B256::ZERO));
    assert!(reverse_walker.next().is_none());

    let mut reverse_walker = cursor.walk_back(Some(2)).unwrap();
    assert_eq!(reverse_walker.next().unwrap().unwrap(), (3, B256::ZERO));
    assert_eq!(reverse_walker.next().unwrap().unwrap(), (1, B256::ZERO));
    assert_eq!(reverse_walker.next().unwrap().unwrap(), (0, B256::ZERO));
    assert!(reverse_walker.next().is_none());

    let mut reverse_walker = cursor.walk_back(Some(4)).unwrap();
    assert_eq!(reverse_walker.next().unwrap().unwrap(), (3, B256::ZERO));
    assert_eq!(reverse_walker.next().unwrap().unwrap(), (1, B256::ZERO));
    assert_eq!(reverse_walker.next().unwrap().unwrap(), (0, B256::ZERO));
    assert!(reverse_walker.next().is_none());

    let mut reverse_walker = cursor.walk_back(None).unwrap();
    assert_eq!(reverse_walker.next().unwrap().unwrap(), (3, B256::ZERO));
    assert_eq!(reverse_walker.next().unwrap().unwrap(), (1, B256::ZERO));
    assert_eq!(reverse_walker.next().unwrap().unwrap(), (0, B256::ZERO));
    assert!(reverse_walker.next().is_none());
}

#[test]
fn db_cursor_seek_exact_or_previous_key() {
    let (_tempdir, db) = create_test_db(TndbEnvKind::RW);

    // PUT
    let tx = db.tx_mut().expect(ERROR_INIT_TX);
    vec![0, 1, 3]
        .into_iter()
        .try_for_each(|key| tx.put::<CanonicalHeaders>(key, B256::ZERO))
        .expect(ERROR_PUT);
    tx.commit().expect(ERROR_COMMIT);

    // Cursor
    let missing_key = 2;
    let tx = db.tx().expect(ERROR_INIT_TX);
    let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();
    assert!(cursor.current().unwrap().is_none());

    // Seek exact
    let exact = cursor.seek_exact(missing_key).unwrap();
    assert_eq!(exact, None);
    assert!(cursor.current().unwrap().is_none());
}

#[test]
fn db_cursor_insert() {
    let (_tempdir, db) = create_test_db(TndbEnvKind::RW);

    // PUT
    let tx = db.tx_mut().expect(ERROR_INIT_TX);
    vec![0, 1, 3, 4, 5]
        .into_iter()
        .try_for_each(|key| tx.put::<CanonicalHeaders>(key, B256::ZERO))
        .expect(ERROR_PUT);
    tx.commit().expect(ERROR_COMMIT);

    let key_to_insert = 2;
    let tx = db.tx_mut().expect(ERROR_INIT_TX);
    let mut cursor = tx.cursor_write::<CanonicalHeaders>().unwrap();

    // INSERT
    assert!(cursor.insert(key_to_insert, &B256::ZERO).is_ok());
    assert_eq!(cursor.current().unwrap(), Some((key_to_insert, B256::ZERO)));
    // INSERT (failure)
    assert!(matches!(
    cursor.insert(key_to_insert, &B256::ZERO).unwrap_err(),
    DatabaseError::Write(err) if *err == DatabaseWriteError {
        info: Error::KeyExist.into(),
        operation: DatabaseWriteOperation::CursorInsert,
        table_name: CanonicalHeaders::NAME,
        key: key_to_insert.encode().into(),
    }));
    assert_eq!(cursor.current().unwrap(), Some((key_to_insert, B256::ZERO)));

    tx.commit().expect(ERROR_COMMIT);

    // Confirm the result
    let tx = db.tx().expect(ERROR_INIT_TX);
    let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();
    let res = cursor.walk(None).unwrap().map(|res| res.unwrap().0).collect::<Vec<_>>();
    assert_eq!(res, vec![0, 1, 2, 3, 4, 5]);
    tx.commit().expect(ERROR_COMMIT);
}

#[test]
fn db_cursor_delete_current_non_existent() {
    let (_tempdir, db) = create_test_db(TndbEnvKind::RW);
    let tx = db.tx_mut().expect(ERROR_INIT_TX);

    let key1 = Address::with_last_byte(1);
    let key2 = Address::with_last_byte(2);
    let key3 = Address::with_last_byte(3);
    let mut cursor = tx.cursor_write::<PlainAccountState>().unwrap();

    assert!(cursor.insert(key1, &Account::default()).is_ok());
    assert!(cursor.insert(key2, &Account::default()).is_ok());
    assert!(cursor.insert(key3, &Account::default()).is_ok());

    // Seek & delete key2
    cursor.seek_exact(key2).unwrap();
    assert!(cursor.delete_current().is_ok());
    assert!(cursor.seek_exact(key2).unwrap().is_none());

    // Seek & delete key2 again
    assert!(cursor.seek_exact(key2).unwrap().is_none());
    assert!(matches!(
        cursor.delete_current().unwrap_err(),
        DatabaseError::Delete(err) if err == reth_libmdbx::Error::NoData.into()));
    // Assert that key1 is still there
    assert_eq!(cursor.seek_exact(key1).unwrap(), Some((key1, Account::default())));
    // Assert that key3 is still there
    assert_eq!(cursor.seek_exact(key3).unwrap(), Some((key3, Account::default())));
}

#[test]
fn db_cursor_insert_wherever_cursor_is() {
    let (_tempdir, db) = create_test_db(TndbEnvKind::RW);
    let tx = db.tx_mut().expect(ERROR_INIT_TX);

    // PUT
    vec![0, 1, 3, 5, 7, 9]
        .into_iter()
        .try_for_each(|key| tx.put::<CanonicalHeaders>(key, B256::ZERO))
        .expect(ERROR_PUT);
    tx.commit().expect(ERROR_COMMIT);

    let tx = db.tx_mut().expect(ERROR_INIT_TX);
    let mut cursor = tx.cursor_write::<CanonicalHeaders>().unwrap();

    // INSERT (cursor starts at last)
    cursor.last().unwrap();
    assert_eq!(cursor.current().unwrap(), Some((9, B256::ZERO)));

    for pos in (2..=8).step_by(2) {
        assert!(cursor.insert(pos, &B256::ZERO).is_ok());
        assert_eq!(cursor.current().unwrap(), Some((pos, B256::ZERO)));
    }
    tx.commit().expect(ERROR_COMMIT);

    // Confirm the result
    let tx = db.tx().expect(ERROR_INIT_TX);
    let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();
    let res = cursor.walk(None).unwrap().map(|res| res.unwrap().0).collect::<Vec<_>>();
    assert_eq!(res, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    tx.commit().expect(ERROR_COMMIT);
}

#[test]
fn db_cursor_append() {
    let (_tempdir, db) = create_test_db(TndbEnvKind::RW);

    // PUT
    let tx = db.tx_mut().expect(ERROR_INIT_TX);
    vec![0, 1, 2, 3, 4]
        .into_iter()
        .try_for_each(|key| tx.put::<CanonicalHeaders>(key, B256::ZERO))
        .expect(ERROR_PUT);
    tx.commit().expect(ERROR_COMMIT);

    // APPEND
    let key_to_append = 5;
    let tx = db.tx_mut().expect(ERROR_INIT_TX);
    let mut cursor = tx.cursor_write::<CanonicalHeaders>().unwrap();
    assert!(cursor.append(key_to_append, &B256::ZERO).is_ok());
    tx.commit().expect(ERROR_COMMIT);

    // Confirm the result
    let tx = db.tx().expect(ERROR_INIT_TX);
    let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();
    let res = cursor.walk(None).unwrap().map(|res| res.unwrap().0).collect::<Vec<_>>();
    assert_eq!(res, vec![0, 1, 2, 3, 4, 5]);
    tx.commit().expect(ERROR_COMMIT);
}

#[test]
fn db_cursor_append_failure() {
    let (_tempdir, db) = create_test_db(TndbEnvKind::RW);

    // PUT
    let tx = db.tx_mut().expect(ERROR_INIT_TX);
    vec![0, 1, 3, 4, 5]
        .into_iter()
        .try_for_each(|key| tx.put::<CanonicalHeaders>(key, B256::ZERO))
        .expect(ERROR_PUT);
    tx.commit().expect(ERROR_COMMIT);

    // APPEND
    let key_to_append = 2;
    let tx = db.tx_mut().expect(ERROR_INIT_TX);
    let mut cursor = tx.cursor_write::<CanonicalHeaders>().unwrap();
    assert!(matches!(
    cursor.append(key_to_append, &B256::ZERO).unwrap_err(),
    DatabaseError::Write(err) if *err == DatabaseWriteError {
        info: Error::KeyMismatch.into(),
        operation: DatabaseWriteOperation::CursorAppend,
        table_name: CanonicalHeaders::NAME,
        key: key_to_append.encode().into(),
    }));
    assert_eq!(cursor.current().unwrap(), Some((5, B256::ZERO))); // the end of table
    tx.commit().expect(ERROR_COMMIT);

    // Confirm the result
    let tx = db.tx().expect(ERROR_INIT_TX);
    let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();
    let res = cursor.walk(None).unwrap().map(|res| res.unwrap().0).collect::<Vec<_>>();
    assert_eq!(res, vec![0, 1, 3, 4, 5]);
    tx.commit().expect(ERROR_COMMIT);
}

#[test]
fn db_cursor_upsert() {
    let (_tempdir, db) = create_test_db(TndbEnvKind::RW);
    let tx = db.tx_mut().expect(ERROR_INIT_TX);

    let mut cursor = tx.cursor_write::<PlainAccountState>().unwrap();
    let key = Address::with_last_byte(0x5a);

    let account = Account::default();
    cursor.upsert(key, &account).expect(ERROR_UPSERT);
    assert_eq!(cursor.seek_exact(key).unwrap(), Some((key, account)));

    let account = Account { nonce: 1, ..Default::default() };
    cursor.upsert(key, &account).expect(ERROR_UPSERT);
    assert_eq!(cursor.seek_exact(key).unwrap(), Some((key, account)));

    let account = Account { nonce: 2, ..Default::default() };
    cursor.upsert(key, &account).expect(ERROR_UPSERT);
    assert_eq!(cursor.seek_exact(key).unwrap(), Some((key, account)));

    let mut dup_cursor = tx.cursor_dup_write::<PlainStorageState>().unwrap();
    let subkey = B256::with_last_byte(0xa5);

    let value = U256::from(1);
    let entry1 = StorageEntry { key: subkey, value };
    dup_cursor.upsert(key, &entry1).expect(ERROR_UPSERT);
    assert_eq!(dup_cursor.seek_by_key_subkey(key, subkey).unwrap(), Some(entry1));

    // Deviation: MDBX keeps both values of the subkey (`entry1`, then `entry2` from
    // `next_dup_val`). Here a subkey holds one value, so the second upsert is refused with
    // `MDBX_KEYEXIST` and the first stays. reth deletes a (key, subkey) entry before it writes a
    // new one (`seek_by_key_subkey`, then `delete_current`, then `upsert`), so it never asks for
    // two.
    let value = U256::from(2);
    let entry2 = StorageEntry { key: subkey, value };
    let upsert = dup_cursor.upsert(key, &entry2);
    assert!(second_value_refused(&upsert, DatabaseWriteOperation::CursorUpsert, key), "{upsert:?}");
    assert_eq!(dup_cursor.seek_by_key_subkey(key, subkey).unwrap(), Some(entry1));
    assert_eq!(dup_cursor.next_dup_val().unwrap(), None);
}

#[test]
fn db_closure_put_get() {
    let tempdir = TempDir::new().expect(ERROR_TEMPDIR);
    let path = tempdir.path();

    let value = Account {
        nonce: 18446744073709551615,
        bytecode_hash: Some(B256::with_last_byte(0xa5)),
        balance: U256::MAX,
    };
    let key =
        Address::from_str("0xa2c122be93b0074270ebee7f6b7292c7deb45047").expect(ERROR_ETH_ADDRESS);

    {
        let env = create_test_db_with_path(TndbEnvKind::RW, path, TndbArgs::default());

        // PUT
        let result = env.update(|tx| {
            tx.put::<PlainAccountState>(key, value).expect(ERROR_PUT);
            200
        });
        assert_eq!(result.expect(ERROR_RETURN_VALUE), 200);
    }

    let env = super::open_env(path, TndbEnvKind::RO, TndbArgs::default()).expect(ERROR_DB_CREATION);

    // GET
    let result =
        env.view(|tx| tx.get::<PlainAccountState>(key).expect(ERROR_GET)).expect(ERROR_GET);

    assert_eq!(result, Some(value))
}

#[test]
fn db_sharded_key() {
    let (_tempdir, db) = create_test_db(TndbEnvKind::RW);
    let real_key = address!("0xa2c122be93b0074270ebee7f6b7292c7deb45047");

    let shards = 5;
    for i in 1..=shards {
        let key = ShardedKey::new(real_key, if i == shards { u64::MAX } else { i * 100 });
        let list = IntegerList::new_pre_sorted([i * 100u64]);

        db.update(|tx| tx.put::<AccountsHistory>(key.clone(), list.clone()).expect("")).unwrap();
    }

    // Seek value with non existing key.
    {
        let tx = db.tx().expect(ERROR_INIT_TX);
        let mut cursor = tx.cursor_read::<AccountsHistory>().unwrap();

        // It will seek the one greater or equal to the query. Since we have `Address | 100`,
        // `Address | 200` in the database and we're querying `Address | 150` it will return us
        // `Address | 200`.
        let mut walker = cursor.walk(Some(ShardedKey::new(real_key, 150))).unwrap();
        let (key, list) =
            walker.next().expect("element should exist.").expect("should be able to retrieve it.");

        assert_eq!(ShardedKey::new(real_key, 200), key);
        let list200 = IntegerList::new_pre_sorted([200u64]);
        assert_eq!(list200, list);
    }
    // Seek greatest index
    {
        let tx = db.tx().expect(ERROR_INIT_TX);
        let mut cursor = tx.cursor_read::<AccountsHistory>().unwrap();

        // It will seek the MAX value of transition index and try to use prev to get first
        // biggers.
        let _unknown = cursor.seek_exact(ShardedKey::new(real_key, u64::MAX)).unwrap();
        let (key, list) =
            cursor.prev().expect("element should exist.").expect("should be able to retrieve it.");

        assert_eq!(ShardedKey::new(real_key, 400), key);
        let list400 = IntegerList::new_pre_sorted([400u64]);
        assert_eq!(list400, list);
    }
}

#[test]
fn db_dup_cursor_delete_first() {
    let (_tempdir, db) = create_test_db(TndbEnvKind::RW);
    let tx = db.tx_mut().expect(ERROR_INIT_TX);

    let mut dup_cursor = tx.cursor_dup_write::<PlainStorageState>().unwrap();

    let entry_0 = StorageEntry { key: B256::with_last_byte(1), value: U256::from(0) };
    let entry_1 = StorageEntry { key: B256::with_last_byte(1), value: U256::from(1) };

    dup_cursor.upsert(Address::with_last_byte(1), &entry_0).expect(ERROR_UPSERT);
    let upsert = dup_cursor.upsert(Address::with_last_byte(1), &entry_1);

    // Deviation: `entry_0` and `entry_1` share a subkey, so the second upsert is refused (MDBX
    // keeps both: see `db_cursor_upsert`). The rest of the test, a walker deleting its first row,
    // runs on `entry_0` and a second key's row in place of MDBX's two values.
    let key = Address::with_last_byte(1);
    assert!(second_value_refused(&upsert, DatabaseWriteOperation::CursorUpsert, key), "{upsert:?}");
    assert_eq!(
        dup_cursor.walk(None).unwrap().collect::<Result<Vec<_>, _>>().unwrap(),
        vec![(Address::with_last_byte(1), entry_0)]
    );
    dup_cursor.upsert(Address::with_last_byte(2), &entry_0).expect(ERROR_UPSERT);

    let mut walker = dup_cursor.walk(None).unwrap();
    walker.delete_current().expect(ERROR_DEL);

    assert_eq!(walker.next().unwrap().unwrap(), (Address::with_last_byte(2), entry_0));

    // Check the tx view - it correctly holds entry_0 of the second key
    assert_eq!(
        tx.cursor_dup_read::<PlainStorageState>()
            .unwrap()
            .walk(None)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![
            (Address::with_last_byte(2), entry_0), // This is ok - we removed the first row
        ]
    );

    // Check the remainder of walker
    assert!(walker.next().is_none());
}

#[test]
fn db_cursor_walk_range_on_dup_table() {
    let (_tempdir, db) = create_test_db(TndbEnvKind::RW);

    let address0 = Address::ZERO;
    let address1 = Address::with_last_byte(1);
    let address2 = Address::with_last_byte(2);

    let tx = db.tx_mut().expect(ERROR_INIT_TX);
    tx.put::<AccountChangeSets>(0, AccountBeforeTx { address: address0, info: None })
        .expect(ERROR_PUT);
    tx.put::<AccountChangeSets>(0, AccountBeforeTx { address: address1, info: None })
        .expect(ERROR_PUT);
    tx.put::<AccountChangeSets>(0, AccountBeforeTx { address: address2, info: None })
        .expect(ERROR_PUT);
    tx.put::<AccountChangeSets>(1, AccountBeforeTx { address: address0, info: None })
        .expect(ERROR_PUT);
    tx.put::<AccountChangeSets>(1, AccountBeforeTx { address: address1, info: None })
        .expect(ERROR_PUT);
    tx.put::<AccountChangeSets>(1, AccountBeforeTx { address: address2, info: None })
        .expect(ERROR_PUT);
    tx.put::<AccountChangeSets>(2, AccountBeforeTx { address: address0, info: None }) // <- should not be returned by the walker
        .expect(ERROR_PUT);
    tx.commit().expect(ERROR_COMMIT);

    let tx = db.tx().expect(ERROR_INIT_TX);
    let mut cursor = tx.cursor_read::<AccountChangeSets>().unwrap();

    let entries = cursor.walk_range(..).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 7);

    let mut walker = cursor.walk_range(0..=1).unwrap();
    assert_eq!(
        walker.next().unwrap().unwrap(),
        (0, AccountBeforeTx { address: address0, info: None })
    );
    assert_eq!(
        walker.next().unwrap().unwrap(),
        (0, AccountBeforeTx { address: address1, info: None })
    );
    assert_eq!(
        walker.next().unwrap().unwrap(),
        (0, AccountBeforeTx { address: address2, info: None })
    );
    assert_eq!(
        walker.next().unwrap().unwrap(),
        (1, AccountBeforeTx { address: address0, info: None })
    );
    assert_eq!(
        walker.next().unwrap().unwrap(),
        (1, AccountBeforeTx { address: address1, info: None })
    );
    assert_eq!(
        walker.next().unwrap().unwrap(),
        (1, AccountBeforeTx { address: address2, info: None })
    );
    assert!(walker.next().is_none());
}

#[test]
fn db_cursor_insert_dup() {
    let (_tempdir, db) = create_test_db(TndbEnvKind::RW);
    let tx = db.tx_mut().expect(ERROR_INIT_TX);

    let mut dup_cursor = tx.cursor_dup_write::<PlainStorageState>().unwrap();
    let key = Address::with_last_byte(0x5a);
    let subkey1 = B256::with_last_byte(0x11);
    let subkey2 = B256::with_last_byte(0x22);

    let entry1 = StorageEntry { key: subkey1, value: U256::ZERO };
    assert!(dup_cursor.insert(key, &entry1).is_ok());

    // Can't insert
    let entry2 = StorageEntry { key: subkey2, value: U256::ZERO };
    assert!(dup_cursor.insert(key, &entry2).is_err());
}

#[test]
fn db_cursor_dupsort_append() {
    let (_tempdir, db) = create_test_db(TndbEnvKind::RW);

    let transition_id = 2;

    let tx = db.tx_mut().expect(ERROR_INIT_TX);
    let mut cursor = tx.cursor_write::<AccountChangeSets>().unwrap();
    vec![0, 1, 3, 4, 5]
        .into_iter()
        .try_for_each(|val| {
            cursor.append(
                transition_id,
                &AccountBeforeTx { address: Address::with_last_byte(val), info: None },
            )
        })
        .expect(ERROR_APPEND);
    tx.commit().expect(ERROR_COMMIT);

    // APPEND DUP & APPEND
    let subkey_to_append = 2;
    let tx = db.tx_mut().expect(ERROR_INIT_TX);
    let mut cursor = tx.cursor_write::<AccountChangeSets>().unwrap();
    assert!(matches!(
    cursor
        .append_dup(
            transition_id,
            AccountBeforeTx {
                address: Address::with_last_byte(subkey_to_append),
                info: None
            }
        )
        .unwrap_err(),
    DatabaseError::Write(err) if *err == DatabaseWriteError {
        info: Error::KeyMismatch.into(),
        operation: DatabaseWriteOperation::CursorAppendDup,
        table_name: AccountChangeSets::NAME,
        key: transition_id.encode().into(),
    }));
    assert!(matches!(
        cursor
            .append(
                transition_id - 1,
                &AccountBeforeTx {
                    address: Address::with_last_byte(subkey_to_append),
                    info: None
                }
            )
            .unwrap_err(),
        DatabaseError::Write(err) if *err == DatabaseWriteError {
            info: Error::KeyMismatch.into(),
            operation: DatabaseWriteOperation::CursorAppend,
            table_name: AccountChangeSets::NAME,
            key: (transition_id - 1).encode().into(),
        }
    ));
    assert!(cursor
        .append(
            transition_id,
            &AccountBeforeTx { address: Address::with_last_byte(subkey_to_append), info: None }
        )
        .is_ok());
}

#[test]
fn db_dup_sort() {
    let (_tempdir, env) = create_test_db(TndbEnvKind::RW);
    let key =
        Address::from_str("0xa2c122be93b0074270ebee7f6b7292c7deb45047").expect(ERROR_ETH_ADDRESS);

    // PUT (0,0)
    let value00 = StorageEntry::default();
    env.update(|tx| tx.put::<PlainStorageState>(key, value00).expect(ERROR_PUT)).unwrap();

    // PUT (2,2)
    let value22 = StorageEntry { key: B256::with_last_byte(2), value: U256::from(2) };
    env.update(|tx| tx.put::<PlainStorageState>(key, value22).expect(ERROR_PUT)).unwrap();

    // PUT (1,1)
    let value11 = StorageEntry { key: B256::with_last_byte(1), value: U256::from(1) };
    env.update(|tx| tx.put::<PlainStorageState>(key, value11).expect(ERROR_PUT)).unwrap();

    // Iterate with cursor
    {
        let tx = env.tx().expect(ERROR_INIT_TX);
        let mut cursor = tx.cursor_dup_read::<PlainStorageState>().unwrap();

        // Notice that value11 and value22 have been ordered in the DB.
        assert_eq!(Some(value00), cursor.next_dup_val().unwrap());
        assert_eq!(Some(value11), cursor.next_dup_val().unwrap());
        assert_eq!(Some(value22), cursor.next_dup_val().unwrap());
    }

    // Seek value with exact subkey
    {
        let tx = env.tx().expect(ERROR_INIT_TX);
        let mut cursor = tx.cursor_dup_read::<PlainStorageState>().unwrap();
        let mut walker = cursor.walk_dup(Some(key), Some(B256::with_last_byte(1))).unwrap();
        assert_eq!(
            (key, value11),
            walker.next().expect("element should exist.").expect("should be able to retrieve it.")
        );
    }
}

#[test]
fn db_walk_dup_with_not_existing_key() {
    let (_tempdir, env) = create_test_db(TndbEnvKind::RW);
    let key =
        Address::from_str("0xa2c122be93b0074270ebee7f6b7292c7deb45047").expect(ERROR_ETH_ADDRESS);

    // PUT (0,0)
    let value00 = StorageEntry::default();
    env.update(|tx| tx.put::<PlainStorageState>(key, value00).expect(ERROR_PUT)).unwrap();

    // PUT (2,2)
    let value22 = StorageEntry { key: B256::with_last_byte(2), value: U256::from(2) };
    env.update(|tx| tx.put::<PlainStorageState>(key, value22).expect(ERROR_PUT)).unwrap();

    // PUT (1,1)
    let value11 = StorageEntry { key: B256::with_last_byte(1), value: U256::from(1) };
    env.update(|tx| tx.put::<PlainStorageState>(key, value11).expect(ERROR_PUT)).unwrap();

    // Try to walk_dup with not existing key should immediately return None
    {
        let tx = env.tx().expect(ERROR_INIT_TX);
        let mut cursor = tx.cursor_dup_read::<PlainStorageState>().unwrap();
        let not_existing_key = Address::ZERO;
        let mut walker = cursor.walk_dup(Some(not_existing_key), None).unwrap();
        assert!(walker.next().is_none());
    }
}

#[test]
fn db_iterate_over_all_dup_values() {
    let (_tempdir, env) = create_test_db(TndbEnvKind::RW);
    let key1 =
        Address::from_str("0x1111111111111111111111111111111111111111").expect(ERROR_ETH_ADDRESS);
    let key2 =
        Address::from_str("0x2222222222222222222222222222222222222222").expect(ERROR_ETH_ADDRESS);

    // PUT key1 (0,0)
    let value00 = StorageEntry::default();
    env.update(|tx| tx.put::<PlainStorageState>(key1, value00).expect(ERROR_PUT)).unwrap();

    // PUT key1 (1,1)
    let value11 = StorageEntry { key: B256::with_last_byte(1), value: U256::from(1) };
    env.update(|tx| tx.put::<PlainStorageState>(key1, value11).expect(ERROR_PUT)).unwrap();

    // PUT key2 (2,2)
    let value22 = StorageEntry { key: B256::with_last_byte(2), value: U256::from(2) };
    env.update(|tx| tx.put::<PlainStorageState>(key2, value22).expect(ERROR_PUT)).unwrap();

    // Iterate with walk_dup
    {
        let tx = env.tx().expect(ERROR_INIT_TX);
        let mut cursor = tx.cursor_dup_read::<PlainStorageState>().unwrap();
        let mut walker = cursor.walk_dup(None, None).unwrap();

        // Notice that value11 and value22 have been ordered in the DB.
        assert_eq!((key1, value00), walker.next().unwrap().unwrap());
        assert_eq!((key1, value11), walker.next().unwrap().unwrap());
        // NOTE: Dup cursor does NOT iterates on all values but only on duplicated values of the
        // same key. assert_eq!(Ok(Some(value22.clone())), walker.next());
        assert!(walker.next().is_none());
    }

    // Iterate by using `walk`
    {
        let tx = env.tx().expect(ERROR_INIT_TX);
        let mut cursor = tx.cursor_dup_read::<PlainStorageState>().unwrap();
        let first = cursor.first().unwrap().unwrap();
        let mut walker = cursor.walk(Some(first.0)).unwrap();
        assert_eq!((key1, value00), walker.next().unwrap().unwrap());
        assert_eq!((key1, value11), walker.next().unwrap().unwrap());
        assert_eq!((key2, value22), walker.next().unwrap().unwrap());
    }
}

#[test]
fn dup_value_with_same_subkey() {
    let (_tempdir, env) = create_test_db(TndbEnvKind::RW);
    let key1 = Address::new([0x11; 20]);
    let key2 = Address::new([0x22; 20]);

    // PUT key1 (0,1)
    let value01 = StorageEntry { key: B256::with_last_byte(0), value: U256::from(1) };
    env.update(|tx| tx.put::<PlainStorageState>(key1, value01).expect(ERROR_PUT)).unwrap();

    // PUT key1 (0,0). Deviation: subkey 0 holds `value01`, so this put of a second value for it is
    // refused (MDBX keeps both: see `db_cursor_upsert`).
    let value00 = StorageEntry::default();
    let put = env.update(|tx| tx.put::<PlainStorageState>(key1, value00)).unwrap();
    assert!(second_value_refused(&put, DatabaseWriteOperation::PutUpsert, key1), "{put:?}");

    // PUT key2 (2,2)
    let value22 = StorageEntry { key: B256::with_last_byte(2), value: U256::from(2) };
    env.update(|tx| tx.put::<PlainStorageState>(key2, value22).expect(ERROR_PUT)).unwrap();

    // Iterate with walk
    {
        let tx = env.tx().expect(ERROR_INIT_TX);
        let mut cursor = tx.cursor_dup_read::<PlainStorageState>().unwrap();
        let first = cursor.first().unwrap().unwrap();
        let mut walker = cursor.walk(Some(first.0)).unwrap();

        // Deviation: MDBX keeps both values of subkey 0, `value00` then `value01`. Here the
        // second put was refused (see `db_cursor_upsert`).
        assert_eq!((key1, value01), walker.next().unwrap().unwrap());
        assert_eq!((key2, value22), walker.next().unwrap().unwrap());
        assert!(walker.next().is_none());
    }

    // seek_by_key_subkey
    {
        let tx = env.tx().expect(ERROR_INIT_TX);
        let mut cursor = tx.cursor_dup_read::<PlainStorageState>().unwrap();

        // MDBX shows the first of its two values with this subkey, `value00`; here the only one is
        // `value01`.
        assert_eq!(value01, cursor.seek_by_key_subkey(key1, value00.key).unwrap().unwrap());
        // key1 but value is greater than the one in the DB
        assert_eq!(None, cursor.seek_by_key_subkey(key1, value22.key).unwrap());
    }
}

/// `reth-db`'s `implementation/mdbx/cursor.rs` test of the same name.
#[test]
fn test_import_table_with_range_works_on_dupsort() {
    let addr1 = address!("0000000000000000000000000000000000000001");
    let addr2 = address!("0000000000000000000000000000000000000002");
    let addr3 = address!("0000000000000000000000000000000000000003");
    let (_source_dir, source_db) = create_test_db(TndbEnvKind::RW);
    let (_target_dir, target_db) = create_test_db(TndbEnvKind::RW);
    let test_data = vec![
        (
            BlockNumberAddress((100, addr1)),
            StorageEntry { key: B256::with_last_byte(1), value: U256::from(100) },
        ),
        (
            BlockNumberAddress((100, addr1)),
            StorageEntry { key: B256::with_last_byte(2), value: U256::from(200) },
        ),
        (
            BlockNumberAddress((100, addr1)),
            StorageEntry { key: B256::with_last_byte(3), value: U256::from(300) },
        ),
        (
            BlockNumberAddress((101, addr1)),
            StorageEntry { key: B256::with_last_byte(1), value: U256::from(400) },
        ),
        (
            BlockNumberAddress((101, addr2)),
            StorageEntry { key: B256::with_last_byte(1), value: U256::from(500) },
        ),
        (
            BlockNumberAddress((101, addr2)),
            StorageEntry { key: B256::with_last_byte(2), value: U256::from(600) },
        ),
        (
            BlockNumberAddress((102, addr3)),
            StorageEntry { key: B256::with_last_byte(1), value: U256::from(700) },
        ),
    ];

    // setup data
    let tx = source_db.tx_mut().unwrap();
    {
        let mut cursor = tx.cursor_dup_write::<StorageChangeSets>().unwrap();
        for (key, value) in &test_data {
            cursor.append_dup(*key, *value).unwrap();
        }
    }
    tx.commit().unwrap();

    // import data from source db to target
    let source_tx = source_db.tx().unwrap();
    let target_tx = target_db.tx_mut().unwrap();

    target_tx
        .import_table_with_range::<StorageChangeSets, _>(
            &source_tx,
            Some(BlockNumberAddress((100, Address::ZERO))),
            BlockNumberAddress((102, Address::repeat_byte(0xff))),
        )
        .unwrap();
    target_tx.commit().unwrap();

    // fetch all data from target db
    let verify_tx = target_db.tx().unwrap();
    let mut cursor = verify_tx.cursor_dup_read::<StorageChangeSets>().unwrap();
    let copied: Vec<_> = cursor.walk(None).unwrap().collect::<Result<Vec<_>, _>>().unwrap();

    // verify each entry matches the test data
    assert_eq!(copied.len(), test_data.len(), "Should copy all entries including duplicates");
    for ((copied_key, copied_value), (expected_key, expected_value)) in
        copied.iter().zip(test_data.iter())
    {
        assert_eq!(copied_key, expected_key);
        assert_eq!(copied_value, expected_value);
    }
}
