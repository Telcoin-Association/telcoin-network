//! The adapter's own tests: MDBX's error codes, the metrics, a blocked second writer, cursors
//! sharing a write transaction (run on MDBX too, which is the oracle), the long-read tests of
//! `reth-db`'s `implementation/mdbx/tx.rs` (tndb-b has no read timeout), and the long-reader
//! warning that replaces it.

use super::{dup::error_name, mdbx_port::create_test_db, Form};
use crate::{
    cursor::{ReadCursor, ReadPool, WriteCursor},
    env::{warning_due, NEVER_WARNED},
    error, TndbArgs, TndbCursor, TndbCursorMut, TndbEnv, TndbEnvKind,
};
use alloy::primitives::{Address, Bytes, B256, U256};
use reth_db::{mdbx::DatabaseArguments, ClientVersion, DatabaseEnv, DatabaseEnvKind};
use reth_db_api::{
    common::PairResult,
    cursor::{DbCursorRO, DbCursorRW},
    database::Database,
    database_metrics::DatabaseMetrics,
    table::Table,
    tables::{
        Bytecodes, CanonicalHeaders, PlainAccountState, PlainStorageState, RawKey, RawTable,
        RawValue, StorageChangeSets, Tables, Transactions,
    },
    transaction::{DbTx, DbTxMut},
    DatabaseError,
};
use reth_libmdbx::Error;
use reth_primitives_traits::{Account, Bytecode, StorageEntry};
use reth_storage_errors::db::{DatabaseWriteError, DatabaseWriteOperation};
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
    sync::{mpsc, Arc},
    thread,
    time::Duration,
};

/// An error's every field, for comparing errors (`DatabaseError` has no `PartialEq`).
fn dbg(error: DatabaseError) -> String {
    format!("{error:?}")
}

/// How long the long-read tests hold their transaction before reading.
const LONG_READ: Duration = Duration::from_millis(200);

/// An MDBX environment with reth's tables, the oracle.
pub(super) fn create_mdbx_db() -> (tempfile::TempDir, DatabaseEnv) {
    let dir = tempfile::TempDir::new().unwrap();
    let mut db = DatabaseEnv::open(
        dir.path(),
        DatabaseEnvKind::RW,
        DatabaseArguments::new(ClientVersion::default()),
    )
    .unwrap();
    db.create_tables().unwrap();
    (dir, db)
}

#[test]
fn long_read_transaction_safety_disabled() {
    let dir = tempfile::tempdir().unwrap();
    let db = super::open_env(dir.path(), TndbEnvKind::RW, TndbArgs::default()).unwrap();
    let mut tx = db.tx().unwrap();
    tx.disable_long_read_transaction_safety();
    thread::sleep(LONG_READ);
    // Transaction has not timed out.
    assert!(matches!(
        tx.get::<Transactions>(0).unwrap_err(),
        DatabaseError::Open(err) if err == Error::NotFound.into()));
}

/// MDBX times this transaction out (`ReadTransactionTimeout`); tndb-b times no transaction out,
/// so it still reads.
#[test]
fn long_read_transaction_safety_enabled() {
    let (_dir, db) = create_test_db(TndbEnvKind::RW);
    db.update(|tx| tx.put::<CanonicalHeaders>(7, B256::with_last_byte(7))).unwrap().unwrap();
    let tx = db.tx().unwrap();
    thread::sleep(LONG_READ);
    assert_eq!(tx.get::<CanonicalHeaders>(7).unwrap(), Some(B256::with_last_byte(7)));
    assert!(matches!(tx.get::<Transactions>(0), Ok(None)), "a read after the wait is no timeout");
}

/// The first long-reader warning is due at once, and each later one a full interval after the
/// last.
#[test]
fn a_long_reader_warning_is_due_first_then_once_per_interval() {
    let minute = Duration::from_secs(60);
    let secs = |s: u64| s * 1_000_000_000;
    assert!(warning_due(NEVER_WARNED, 0, minute), "the first");
    assert!(warning_due(NEVER_WARNED, secs(5), minute), "the first, later");
    assert!(!warning_due(secs(5), secs(5), minute), "just warned");
    assert!(!warning_due(secs(5), secs(65) - 1, minute), "under the interval");
    assert!(warning_due(secs(5), secs(65), minute), "a full interval later");
    assert!(!warning_due(secs(65), secs(5), minute), "a time before the last warning");
}

/// Keeps the text of each `WARN` event emitted on the thread it is the default subscriber of.
struct WarnCapture(Arc<parking_lot::Mutex<Vec<String>>>);

impl tracing::Subscriber for WarnCapture {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        if *event.metadata().level() == tracing::Level::WARN {
            let mut text = String::new();
            event.record(&mut |field: &tracing::field::Field, value: &dyn std::fmt::Debug| {
                text += &format!("{}={value:?} ", field.name());
            });
            self.0.lock().push(text);
        }
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

/// A commit warns once a reader has held a superseded snapshot past the limit, at most once per
/// interval, and not once the reader has ended. The limits are shortened for the test.
#[test]
fn a_long_reader_warns_after_a_commit_at_most_once_per_interval() {
    const AGE: Duration = Duration::from_millis(100);
    let (_dir, mut db) = create_test_db(TndbEnvKind::RW);
    let put = |db: &TndbEnv, n: u64| {
        db.update(|tx| tx.put::<CanonicalHeaders>(n, B256::with_last_byte(1))).unwrap().unwrap();
    };
    let warnings = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let count = || warnings.lock().len();
    tracing::subscriber::with_default(WarnCapture(Arc::clone(&warnings)), || {
        db.set_long_reader_limits(AGE, Duration::from_secs(3600));
        let reader = db.tx().unwrap();
        put(&db, 1);
        assert_eq!(count(), 0, "the reader's snapshot was superseded just now");
        thread::sleep(2 * AGE);
        put(&db, 2);
        assert_eq!(count(), 1, "the reader is past the limit");
        assert!(warnings.lock()[0].contains("age="), "{:?}", warnings.lock());
        put(&db, 3);
        assert_eq!(count(), 1, "one warning per interval");

        db.set_long_reader_limits(AGE, Duration::ZERO);
        put(&db, 4);
        put(&db, 5);
        assert_eq!(count(), 3, "a warning per commit with no interval");
        drop(reader);
        put(&db, 6);
        assert_eq!(count(), 3, "no reader left");
    });
}

#[test]
fn engine_errors_carry_mdbx_codes_and_messages() {
    use tndb_b::Error as E;
    let same_as_mdbx = [
        (E::KeyExists, Error::KeyExist),
        (E::AppendOutOfOrder, Error::KeyMismatch),
        (E::KeyTooLarge { len: 2000, max: 1005 }, Error::BadValSize),
        (E::ValueTooLarge { len: 1 << 25, max: 1 << 24 }, Error::BadValSize),
        (E::Corrupt("page 9 fails its CRC".into()), Error::Corrupted),
        (E::Locked, Error::Busy),
        (E::ReadOnly, Error::Access),
        (E::WrongTxn, Error::BadTxn),
        (E::TableNotFound("t".into()), Error::NotFound),
        (E::StaleTable("t".into()), Error::NotFound),
        (E::InvalidTableName, Error::DecodeError),
        (E::InvalidPageSize(3), Error::DecodeError),
        (E::KeyTooShort { len: 4, min: 5 }, Error::BadValSize),
        (E::TableKind("t".into()), Error::Incompatible),
        (E::InvalidDupOptions("packed_max must be 0".into()), Error::DecodeError),
    ];
    for (engine, mdbx) in same_as_mdbx {
        assert_eq!(error::info(&engine), mdbx.into(), "{engine}");
    }
    assert_eq!(dbg(crate::tx::no_data()), dbg(DatabaseError::Delete(Error::NoData.into())));

    // Their own message under MDBX's code.
    let own = [
        (E::Failed("commit failed".into()), Error::Panic.to_err_code()),
        (E::NotTndbB, Error::Invalid.to_err_code()),
        (E::UnsupportedVersion(9), Error::VersionMismatch.to_err_code()),
        (E::Io(std::io::Error::from_raw_os_error(libc::ENOSPC)), libc::ENOSPC),
        (E::Io(std::io::Error::other("no errno")), libc::EIO),
    ];
    for (engine, code) in own {
        let info = error::info(&engine);
        assert_eq!((&*info.message, info.code), (&*engine.to_string(), code));
    }
}

/// A cursor `insert` of a present key moves the cursor to the key, as MDBX's does, by a seek. When
/// that seek meets damage (here, in the key's value, an overflow run whose middle page is changed
/// on disk, which the insert's own search does not read), the call reports the damage as
/// `MDBX_CORRUPTED`, not as the present key's `MDBX_KEYEXIST`.
#[test]
fn a_refused_insert_reports_damage_its_seek_meets() {
    type Raw = RawTable<Bytecodes>;
    const MARK: &[u8] = b"overflow middle page";
    let key = RawKey::new(B256::with_last_byte(1));
    let mut value = vec![7u8; 20_000];
    value[9000..9000 + MARK.len()].copy_from_slice(MARK);
    let (dir, db) = create_test_db(TndbEnvKind::RW);
    db.update(|tx| tx.put::<Raw>(key.clone(), RawValue::from_vec(value))).unwrap().unwrap();
    drop(db);

    let path = dir.path().join(crate::DATA_FILE);
    let mut file = std::fs::read(&path).unwrap();
    let at = file.windows(MARK.len()).position(|w| w == MARK).expect("the value is on disk");
    file[at] ^= 0xFF;
    std::fs::write(&path, file).unwrap();

    let db = super::open_env(dir.path(), TndbEnvKind::RW, TndbArgs::default()).unwrap();
    let tx = db.tx_mut().unwrap();
    let mut cursor = tx.cursor_write::<Raw>().unwrap();
    let refused = cursor.insert(key.clone(), &RawValue::from_vec(vec![1; 8])).unwrap_err();
    assert_eq!(
        dbg(refused),
        dbg(DatabaseWriteError {
            info: Error::Corrupted.into(),
            operation: DatabaseWriteOperation::CursorInsert,
            table_name: Bytecodes::NAME,
            key: key.raw_key().clone(),
        }
        .into())
    );
}

#[test]
fn operations_report_mdbx_errors_in_reths_variants() {
    // A second open of a held file: MDBX_BUSY.
    let (dir, db) = create_test_db(TndbEnvKind::RW);
    assert!(matches!(
        super::open_env(dir.path(), TndbEnvKind::RW, TndbArgs::default()).unwrap_err(),
        DatabaseError::Open(err) if err == Error::Busy.into()));

    // A key too large for a page: MDBX_BAD_VALSIZE, as a write error naming the key.
    let tx = db.tx_mut().unwrap();
    let key = vec![1u8; 2000];
    let raw = RawKey::from_vec(key.clone());
    assert_eq!(
        dbg(tx
            .put::<RawTable<CanonicalHeaders>>(raw, RawValue::from_vec(vec![0; 32]))
            .unwrap_err()),
        dbg(DatabaseWriteError {
            info: Error::BadValSize.into(),
            operation: DatabaseWriteOperation::PutUpsert,
            table_name: CanonicalHeaders::NAME,
            key,
        }
        .into())
    );

    // An append out of order: MDBX_EKEYMISMATCH.
    tx.put::<CanonicalHeaders>(5, B256::ZERO).unwrap();
    assert_eq!(
        dbg(tx.append::<CanonicalHeaders>(4, B256::ZERO).unwrap_err()),
        dbg(DatabaseWriteError {
            info: Error::KeyMismatch.into(),
            operation: DatabaseWriteOperation::PutAppend,
            table_name: CanonicalHeaders::NAME,
            key: 4u64.to_be_bytes().to_vec(),
        }
        .into())
    );

    // A cursor of a committed transaction: MDBX_BAD_TXN.
    let mut cursor = tx.cursor_write::<CanonicalHeaders>().unwrap();
    tx.commit().unwrap();
    assert_eq!(dbg(cursor.first().unwrap_err()), dbg(DatabaseError::Read(Error::BadTxn.into())));
    assert!(matches!(
        cursor.upsert(1, &B256::ZERO).unwrap_err(),
        DatabaseError::Write(err) if err.info == Error::BadTxn.into()));
    assert_eq!(
        dbg(cursor.delete_current().unwrap_err()),
        dbg(DatabaseError::Delete(Error::BadTxn.into()))
    );
    // So does one of a transaction dropped without commit (an abort), and the writer is free.
    let tx = db.tx_mut().unwrap();
    let mut cursor = tx.cursor_write::<CanonicalHeaders>().unwrap();
    drop(tx);
    assert_eq!(dbg(cursor.last().unwrap_err()), dbg(DatabaseError::Read(Error::BadTxn.into())));
    db.tx_mut().unwrap().commit().unwrap();
    drop(db);

    // A read-only environment refuses a write transaction: MDBX_EACCESS.
    let mut ro = super::open_env(dir.path(), TndbEnvKind::RO, TndbArgs::default()).unwrap();
    assert_eq!(dbg(ro.tx_mut().unwrap_err()), dbg(DatabaseError::InitTx(Error::Access.into())));
    assert_eq!(
        dbg(ro.create_tables().unwrap_err()),
        dbg(DatabaseError::InitTx(Error::Access.into()))
    );
    assert_eq!(ro.tx().unwrap().get::<CanonicalHeaders>(5).unwrap(), Some(B256::ZERO));

    // A read-only open never creates.
    let missing = tempfile::tempdir().unwrap();
    assert!(matches!(
        super::open_env(&missing.path().join("absent"), TndbEnvKind::RO, TndbArgs::default())
            .unwrap_err(),
        DatabaseError::Open(err) if err.code == libc::ENOENT));
    assert!(!missing.path().join("absent").exists());
}

/// A table the environment did not create or find at open is looked up in the transaction, as
/// MDBX opens a table it has no handle for.
#[test]
fn a_table_created_after_open_is_found_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let db = super::open_env(dir.path(), TndbEnvKind::RW, TndbArgs::default()).unwrap();
    assert!(matches!(
        db.tx().unwrap().get::<CanonicalHeaders>(1).unwrap_err(),
        DatabaseError::Open(err) if err == Error::NotFound.into()));
    let mut txn = db.env().begin_rw().unwrap();
    txn.create_table(CanonicalHeaders::NAME).unwrap();
    txn.commit().unwrap();
    db.update(|tx| tx.put::<CanonicalHeaders>(1, B256::with_last_byte(1))).unwrap().unwrap();
    let tx = db.tx().unwrap();
    assert_eq!(tx.get::<CanonicalHeaders>(1).unwrap(), Some(B256::with_last_byte(1)));
    assert_eq!(tx.entries::<CanonicalHeaders>().unwrap(), 1);
    assert_eq!(tx.cursor_read::<CanonicalHeaders>().unwrap().last().unwrap().unwrap().0, 1);
}

/// A table the file holds in another form than this build's layout stores it in is refused with
/// `MDBX_INCOMPATIBLE` and a message naming both forms: by a transaction that finds it after the
/// open, by `create_tables`, and at open (read-write and read-only). Here `StorageChangeSets`,
/// whose rows are composite keys in a plain engine table, is made an engine dup table by hand. A
/// file whose every table has its layout's form opens.
#[test]
fn a_table_in_another_form_than_its_layout_is_refused() {
    let refused = |result: Result<(), DatabaseError>| match result.err() {
        Some(DatabaseError::Open(info)) => {
            assert_eq!(info.code, Error::Incompatible.to_err_code());
            info.message.to_string()
        }
        other => panic!("not refused at open: {:?}", other.map(dbg)),
    };
    let message = "table StorageChangeSets is stored as a dup table with key_len 28 and \
                   packed_max 0, this build stores it as a plain table; the file was written by \
                   another tndb-b-reth layout";
    let dir = tempfile::tempdir().unwrap();
    let mut db = super::open_env(dir.path(), TndbEnvKind::RW, TndbArgs::default()).unwrap();
    let mut txn = db.env().begin_rw().unwrap();
    let options = tndb_b::DupOptions { key_len: 28, packed_max: 0 };
    let table = txn.create_dup_table(StorageChangeSets::NAME, options).unwrap();
    txn.put(&table, &[7; 60], &[1]).unwrap();
    txn.commit().unwrap();
    assert_eq!(refused(db.tx().unwrap().entries::<StorageChangeSets>().map(drop)), message);
    assert_eq!(
        refused(db.tx_mut().unwrap().cursor_write::<StorageChangeSets>().map(drop)),
        message
    );
    assert!(matches!(
        db.create_tables().unwrap_err(),
        DatabaseError::CreateTable(info) if info == Error::Incompatible.into()));
    drop(db);
    for kind in [TndbEnvKind::RW, TndbEnvKind::RO] {
        assert_eq!(
            refused(super::open_env(dir.path(), kind, TndbArgs::default()).map(drop)),
            message
        );
    }
    // Every table in its layout's form: reopened, read and written.
    let dir = tempfile::tempdir().unwrap();
    let mut db = super::open_env(dir.path(), TndbEnvKind::RW, TndbArgs::default()).unwrap();
    db.create_tables().unwrap();
    let row = StorageEntry { key: B256::with_last_byte(1), value: U256::from(2) };
    db.update(|tx| tx.put::<PlainStorageState>(Address::with_last_byte(1), row)).unwrap().unwrap();
    drop(db);
    let db = super::open_env(dir.path(), TndbEnvKind::RW, TndbArgs::default()).unwrap();
    assert_eq!(
        db.tx().unwrap().get::<PlainStorageState>(Address::with_last_byte(1)).unwrap(),
        Some(row)
    );
    assert_eq!(db.tx().unwrap().entries::<StorageChangeSets>().unwrap(), 0);
}

/// After `create_tables`, the three state tables are engine dup tables keyed by their key widths
/// (20, 32 and 32 bytes) whose packed sets take the longest inline value of that width at 4 KiB
/// pages (987 and 975 bytes; [`crate::env::PACKED_SMALL_RETH`] under
/// `TndbArgs::packed_dup_max`), and every other table (the changesets among them) a plain one;
/// with `TndbArgs::composite_dup_tables` all are plain.
#[test]
fn nested_tables_are_engine_dup_tables() {
    use tndb_b::ReadTxn;
    const NESTED: [(&str, u16, u16); 3] =
        [("PlainStorageState", 20, 987), ("HashedStorages", 32, 975), ("StoragesTrie", 32, 975)];
    for form in Form::ALL {
        let dir = tempfile::tempdir().unwrap();
        let mut db = super::open_env(dir.path(), TndbEnvKind::RW, form.args()).unwrap();
        db.create_tables().unwrap();
        let txn = db.env().begin_ro().unwrap();
        for name in Tables::ALL.iter().map(Tables::name) {
            let table = txn.open_table(name).unwrap().unwrap();
            let expected = NESTED.iter().find(|&&(table, ..)| table == name).and_then(
                |&(_, key_len, most)| {
                    let packed_max = match form {
                        Form::Packed => most,
                        Form::PackedSmall => crate::env::PACKED_SMALL_RETH,
                        Form::Composite => return None,
                    };
                    Some(tndb_b::DupOptions { key_len, packed_max })
                },
            );
            assert_eq!(txn.table_stat(&table).unwrap().dup, expected, "{name}, {form:?}");
        }
    }
}

/// A file whose engine dup tables were made with another `packed_max` (0: every key a nested
/// tree, or [`crate::env::PACKED_SMALL_RETH`]) is refused at a default open, read-write and
/// read-only, with `MDBX_INCOMPATIBLE` and a message naming both bounds; the refusal writes
/// nothing (the file's bytes are unchanged), so the file then opens with its own bound and rows.
/// A file of 16 KiB pages, its bound resolved at that page size, opens with the default.
#[test]
fn a_dup_table_with_other_packing_is_refused_at_open() {
    let a = Address::with_last_byte(1);
    let row = StorageEntry { key: B256::with_last_byte(1), value: U256::from(2) };
    for packed_max in [0, crate::env::PACKED_SMALL_RETH] {
        let args = TndbArgs::default().packed_dup_max(packed_max);
        let dir = tempfile::tempdir().unwrap();
        let mut db = super::open_env(dir.path(), TndbEnvKind::RW, args).unwrap();
        db.create_tables().unwrap();
        db.update(|tx| tx.put::<PlainStorageState>(a, row)).unwrap().unwrap();
        drop(db);
        let file = dir.path().join(crate::DATA_FILE);
        let bytes = std::fs::read(&file).unwrap();
        let message = format!(
            "table PlainStorageState is stored as a dup table with key_len 20 and packed_max \
             {packed_max}, this build stores it as a dup table with key_len 20 and packed_max \
             987; the file was written by another tndb-b-reth layout"
        );
        for kind in [TndbEnvKind::RW, TndbEnvKind::RO] {
            match super::open_env(dir.path(), kind, TndbArgs::default()).map(drop) {
                Err(DatabaseError::Open(info)) => {
                    assert_eq!(info.code, Error::Incompatible.to_err_code());
                    assert_eq!(info.message.to_string(), message, "{kind:?}");
                }
                other => panic!("not refused at open: {:?}", other.map_err(dbg)),
            }
            assert!(std::fs::read(&file).unwrap() == bytes, "the refused open wrote ({kind:?})");
        }
        let db = super::open_env(dir.path(), TndbEnvKind::RO, args).unwrap();
        assert_eq!(db.tx().unwrap().get::<PlainStorageState>(a).unwrap(), Some(row));
    }
    // The bound is resolved at the file's own page size: a file of 16 KiB pages opens with the
    // default arguments, whose page size (4 KiB) is only a new file's.
    let dir = tempfile::tempdir().unwrap();
    let args = TndbArgs::default().with_page_size(16_384);
    let mut db = super::open_env(dir.path(), TndbEnvKind::RW, args).unwrap();
    db.create_tables().unwrap();
    db.update(|tx| tx.put::<PlainStorageState>(a, row)).unwrap().unwrap();
    drop(db);
    let mut db = super::open_env(dir.path(), TndbEnvKind::RW, TndbArgs::default()).unwrap();
    db.create_tables().unwrap();
    assert_eq!(db.page_size(), 16_384);
    let recorded = {
        use tndb_b::ReadTxn;
        let txn = db.env().begin_ro().unwrap();
        let table = txn.open_table(PlainStorageState::NAME).unwrap().unwrap();
        txn.table_stat(&table).unwrap().dup.unwrap()
    };
    assert_eq!(recorded, tndb_b::DupOptions::packed(20).resolved(16_384).unwrap());
    assert!(recorded.packed_max > 987, "{recorded:?}");
    assert_eq!(db.tx().unwrap().get::<PlainStorageState>(a).unwrap(), Some(row));
}

/// A file whose state tables are in one form (engine dup tables, or composite keys with
/// `TndbArgs::composite_dup_tables`) is refused at an open for the other, read-write and
/// read-only, with `MDBX_INCOMPATIBLE` and a message naming the first such table; the refusal
/// writes nothing, so the file then opens in its own form with its rows.
#[test]
fn a_file_whose_table_form_disagrees_is_refused_at_open() {
    let a = Address::with_last_byte(1);
    let row = StorageEntry { key: B256::with_last_byte(1), value: U256::from(2) };
    let nested_form = "a dup table with key_len 20 and packed_max 987";
    for nested in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let form = |nested| Form::nested(nested).args();
        let mut db = super::open_env(dir.path(), TndbEnvKind::RW, form(nested)).unwrap();
        db.create_tables().unwrap();
        db.update(|tx| tx.put::<PlainStorageState>(a, row)).unwrap().unwrap();
        drop(db);
        let (found, expected) =
            if nested { (nested_form, "a plain table") } else { ("a plain table", nested_form) };
        let message = format!(
            "table PlainStorageState is stored as {found}, this build stores it as {expected}; \
             the file was written by another tndb-b-reth layout"
        );
        for kind in [TndbEnvKind::RW, TndbEnvKind::RO] {
            match super::open_env(dir.path(), kind, form(!nested)).map(drop) {
                Err(DatabaseError::Open(info)) => {
                    assert_eq!(info.code, Error::Incompatible.to_err_code());
                    assert_eq!(info.message.to_string(), message, "nested {nested}");
                }
                other => panic!("not refused at open: {:?}", other.map_err(dbg)),
            }
        }
        let db = super::open_env(dir.path(), TndbEnvKind::RO, form(nested)).unwrap();
        assert_eq!(db.tx().unwrap().get::<PlainStorageState>(a).unwrap(), Some(row));
    }
}

#[test]
fn a_second_writer_waits_for_the_first_to_commit() {
    let (_dir, db) = create_test_db(TndbEnvKind::RW);
    let db = Arc::new(db);
    let tx = db.tx_mut().unwrap();
    tx.put::<CanonicalHeaders>(1, B256::with_last_byte(1)).unwrap();

    let (began, waiting) = mpsc::channel();
    let second = thread::spawn({
        let db = Arc::clone(&db);
        move || {
            let tx = db.tx_mut().unwrap();
            began.send(()).unwrap();
            tx.get::<CanonicalHeaders>(1).unwrap()
        }
    });
    assert!(
        waiting.recv_timeout(Duration::from_millis(300)).is_err(),
        "the second write transaction began while the first was open"
    );
    // Reads go on meanwhile.
    assert_eq!(db.tx().unwrap().entries::<CanonicalHeaders>().unwrap(), 0);
    tx.commit().unwrap();
    waiting.recv_timeout(Duration::from_secs(30)).expect("the second writer proceeds");
    assert_eq!(second.join().unwrap(), Some(B256::with_last_byte(1)));
}

/// Two write cursors and the transaction's own writes on one table, each seeing the others'
/// changes (an entry deleted under a cursor included), plus inserts, appends and deletes with a
/// value. Every result, errors included, is logged; MDBX's log is the expected one.
fn shared_write_cursors<D: Database>(db: &D) -> Vec<String> {
    let mut log = Vec::new();
    macro_rules! log {
        ($e:expr) => {{
            let result = $e;
            log.push(format!("{}: {:?}", stringify!($e), result));
        }};
    }
    let v = B256::with_last_byte;
    let tx = db.tx_mut().unwrap();
    for k in [0u64, 2, 4, 6, 8] {
        tx.put::<CanonicalHeaders>(k, v(k as u8)).unwrap();
    }
    let mut c1 = tx.cursor_write::<CanonicalHeaders>().unwrap();
    let mut c2 = tx.cursor_write::<CanonicalHeaders>().unwrap();
    log!(c1.seek_exact(4));
    log!(c2.seek_exact(4));
    // c1 deletes the entry both are on.
    log!(c1.delete_current());
    log!(c2.next());
    log!(c2.prev());
    // The transaction writes past c1's follower; c1 steps to the follower and back.
    log!(tx.put::<CanonicalHeaders>(7, v(7)));
    log!(c1.next());
    log!(c1.prev());
    log!(tx.put::<CanonicalHeaders>(5, v(5)));
    // c2 writes; c1 sees it.
    log!(c2.upsert(3, &v(33)));
    log!(c2.current());
    log!(c1.seek_exact(3));
    log!(c1.next());
    // The transaction deletes the entry c2 is on.
    log!(c2.seek_exact(6));
    log!(tx.delete::<CanonicalHeaders>(6, None));
    log!(c2.next());
    log!(c2.prev());
    log!(tx.get::<CanonicalHeaders>(6));
    // Deletes and inserts through both cursors.
    log!(c1.seek_exact(0));
    log!(c1.delete_current());
    log!(c2.first());
    log!(c2.insert(2, &v(22)));
    log!(c2.current());
    log!(c2.insert(4, &v(4)));
    log!(c1.next());
    log!(c1.next());
    log!(c2.insert(1, &v(1)));
    log!(c2.seek_exact(3));
    log!(c1.seek(7));
    log!(c1.prev());
    // Deletes with a value: only an exact match goes.
    log!(tx.delete::<CanonicalHeaders>(5, Some(v(6))));
    log!(tx.delete::<CanonicalHeaders>(5, Some(v(5))));
    log!(tx.delete::<CanonicalHeaders>(5, None));
    // A refused append leaves the cursor on the last entry; a good one on its entry.
    log!(c1.first());
    log!(c1.append(4, &v(4)));
    log!(c1.current());
    log!(c2.append(9, &v(9)));
    log!(c2.current());
    log!(c1.seek_exact(8));
    log!(c1.next());
    log!(tx.entries::<CanonicalHeaders>());
    log!(c1.walk(None).unwrap().collect::<Result<Vec<_>, _>>());
    log!(c2.walk_back(None).unwrap().collect::<Result<Vec<_>, _>>());
    log!(c1.walk_range(2..8).unwrap().collect::<Result<Vec<_>, _>>());
    // A missed seek_exact leaves the cursor unset: no relative move finds a row.
    log!(c1.seek_exact(5));
    log!(c1.next());
    log!(c1.seek_exact(5));
    log!(c1.prev());
    log!(c1.current());
    log!(c1.delete_current());
    log!(c1.next());
    drop((c1, c2));
    tx.commit().unwrap();
    log!(db
        .view(|tx| tx.cursor_read::<CanonicalHeaders>()?.walk(None)?.collect::<Result<Vec<_>, _>>())
        .unwrap());
    log
}

#[test]
fn shared_write_cursors_see_each_others_writes_as_mdbx_cursors_do() {
    let (_mdbx_dir, mdbx) = create_mdbx_db();
    let (_dir, db) = create_test_db(TndbEnvKind::RW);
    let expected = shared_write_cursors(&mdbx);
    let got = shared_write_cursors(&db);
    for (i, (want, have)) in expected.iter().zip(&got).enumerate() {
        assert_eq!(have, want, "step {i}");
    }
    assert_eq!(got.len(), expected.len());
}

/// Runs `steps` (space-separated) on a write transaction holding `keys` of `CanonicalHeaders`, and
/// returns each step with its outcome: a key, `none`, `ok`, or an error by its MDBX name
/// ([`error_name`]). The steps: `se:K` `seek_exact`, `s:K` `seek`, `f` `first`, `l` `last`, `n`
/// `next`, `p` `prev`, `c` `current`, `d` `delete_current`, `a:K` `append`, `u:K` `upsert`;
/// `put:K` and `del:K` the transaction's `put` and `delete`; `2se:K`, `2d`, `2a:K` and `2u:K` a
/// second cursor's `seek_exact`, `delete_current`, `append` and `upsert`. The transaction aborts.
fn steps<D: Database>(db: &D, keys: &[u64], steps: &str) -> String {
    let v = B256::with_last_byte;
    let tx = db.tx_mut().unwrap();
    for &k in keys {
        tx.put::<CanonicalHeaders>(k, v(k as u8)).unwrap();
    }
    let mut c1 = tx.cursor_write::<CanonicalHeaders>().unwrap();
    let mut c2 = tx.cursor_write::<CanonicalHeaders>().unwrap();
    let row = |r: PairResult<CanonicalHeaders>| match r {
        Ok(Some((k, _))) => k.to_string(),
        Ok(None) => "none".into(),
        Err(e) => error_name(e),
    };
    let done = |r: Result<(), DatabaseError>| r.map_or_else(error_name, |()| "ok".into());
    let mut out = Vec::new();
    for step in steps.split_whitespace() {
        let (op, arg) = step.split_once(':').unwrap_or((step, ""));
        let k: u64 = arg.parse().unwrap_or(0);
        let outcome = match op {
            "se" => row(c1.seek_exact(k)),
            "s" => row(c1.seek(k)),
            "f" => row(c1.first()),
            "l" => row(c1.last()),
            "n" => row(c1.next()),
            "p" => row(c1.prev()),
            "c" => row(c1.current()),
            "d" => done(c1.delete_current()),
            "a" => done(c1.append(k, &v(k as u8))),
            "u" => done(c1.upsert(k, &v(k as u8))),
            "put" => done(tx.put::<CanonicalHeaders>(k, v(k as u8))),
            "del" => done(tx.delete::<CanonicalHeaders>(k, None).map(drop)),
            "2se" => row(c2.seek_exact(k)),
            "2d" => done(c2.delete_current()),
            "2a" => done(c2.append(k, &v(k as u8))),
            "2u" => done(c2.upsert(k, &v(k as u8))),
            _ => panic!("unknown step {op}"),
        };
        out.push(format!("{step}={outcome}"));
    }
    out.join(" ")
}

/// Step sequences ([`steps`]) where the cursor is MDBX's step for step. Right after its own delete
/// it is on the follower: `current` returns it (and `next` then steps past it), `next` returns it,
/// `prev` the row before, and a second delete removes it; keys put before the follower (the
/// deleted key included) come before the cursor, and another writer's delete of the follower
/// moves it to the next. On the row `last` placed it on (or `prev` from a fresh cursor, a refused
/// append, or an append that replaced the last key's value), `next` returns nothing, even after
/// another writer appended or put past it, until another move, a write, or another writer's delete
/// of that row.
const MATCHED: &[(&[u64], &str)] = &[
    (&[2, 4, 6, 8], "se:4 d c n n"),
    (&[2, 4, 6, 8], "se:4 d n n"),
    (&[2, 4, 6, 8], "se:4 d p n"),
    (&[2, 4, 6, 8], "se:4 d d n p"),
    (&[2, 4, 6, 8], "se:2 d p n"),
    (&[2, 4, 6, 8], "se:2 d put:1 p n"),
    (&[2, 4, 6, 8], "se:4 d put:5 n p"),
    (&[2, 4, 6, 8], "se:4 d put:5 p n"),
    (&[2, 4, 6, 8], "se:4 d put:5 c"),
    (&[2, 4, 6, 8], "se:4 d put:4 n p"),
    (&[2, 4, 6, 8], "se:4 d put:4 c"),
    (&[2, 4, 6, 8], "se:4 d put:3 n p"),
    (&[2, 4, 6, 8], "se:4 d put:7 n n"),
    (&[2, 4, 6, 8], "se:4 d 2se:6 2d n p"),
    (&[2, 4, 6, 8], "se:4 d 2se:6 2d c"),
    (&[2, 4, 6, 8], "se:4 d del:6 n"),
    (&[2, 4, 6, 8], "se:4 d 2se:2 2d n p"),
    (&[2, 4, 6, 8], "se:4 d u:5 n"),
    (&[2, 4, 6, 8], "se:4 d se:5 n"),
    (&[2, 4, 6, 8], "se:4 del:4 put:4 n"),
    (&[4], "se:4 d c n p d"),
    (&[4], "se:4 d put:5 n"),
    (&[2, 4, 6, 8], "l n c n p"),
    (&[2, 4, 6, 8], "l n d"),
    (&[2, 4, 6, 8], "l 2a:9 n c p"),
    (&[2, 4, 6, 8], "l 2a:9 c n"),
    (&[2, 4, 6, 8], "l 2a:9 d n"),
    (&[2, 4, 6, 8], "l 2a:9 2a:10 n c"),
    (&[2, 4, 6, 8], "l put:9 c n p"),
    (&[2, 4, 6, 8], "l 2se:6 2a:9 n"),
    (&[2, 4, 6, 8], "l 2se:6 2d n c"),
    (&[2, 4, 6, 8], "l del:8 n"),
    (&[2, 4, 6, 8], "l n 2a:9 n p"),
    (&[2, 4, 6, 8], "l p n 2a:9 n"),
    (&[2, 4, 6, 8], "l c 2a:9 n"),
    (&[2, 4, 6, 8], "a:1 2a:9 n"),
    (&[2, 4, 6, 8], "p 2a:9 n"),
    (&[2, 4, 6, 8], "s:8 2a:9 n"),
    (&[2, 4, 6, 8], "se:8 2a:9 n"),
    (&[2, 4, 6, 8], "se:6 n 2a:9 n"),
    (&[2, 4, 6, 8], "s:9 p 2a:9 n"),
    (&[2, 4, 6, 8], "se:8 d p 2a:9 n"),
    (&[2, 5, 9], "a:9 put:10 n n"),
    (&[2, 5, 9], "a:9 2a:10 n c p"),
];

#[test]
fn cursor_steps_after_a_delete_or_last_match_mdbx() {
    let (_mdbx_dir, mdbx) = create_mdbx_db();
    let (_dir, db) = create_test_db(TndbEnvKind::RW);
    for (keys, seq) in MATCHED {
        assert_eq!(steps(&db, keys, seq), steps(&mdbx, keys, seq), "{keys:?}");
    }
}

/// The known differences from MDBX's cursors, pinned on both sides (as `(keys, steps, MDBX's
/// outcome, tndb-b's)`) so a change in either shows.
///
/// - **Past either end.** MDBX's `next` past the last row leaves its cursor on that row, and `prev`
///   before the first on the first, where `current` returns it and a delete deletes it; deleting
///   the last row, or a `seek` past it, leaves it on the (new) last row, which `current` returns.
///   tndb-b's cursor is then after the end (or before the start), where `current` finds nothing, a
///   delete is `MDBX_ENODATA`, and `prev` (or `next`) takes the last (or first) row. A row `last`
///   placed the cursor on is the exception (see [`MATCHED`]), until another writer deletes it: the
///   cursor is then after the deleted row as after any delete. Two cursors a missed `seek_exact`
///   left unset are placed by another writer on MDBX: on an empty table, a put makes it a fresh
///   cursor (`next` takes the first row); and once deletes have taken the row the miss landed on
///   and every row after it, it is at the end (`prev` takes the new last row). tndb-b's stays
///   unset.
/// - **Another writer's delete and put before the cursor's next call.** MDBX moves its cursor to
///   the follower when the row goes; tndb-b's cursor finds its row again by key at its next call,
///   so a key put meanwhile between the row and its follower is taken as the follower, and the
///   row's key put back is the cursor's row again (after `last`, still ending the table).
const DEVIATIONS: &[(&[u64], &str, &str, &str)] = &[
    (&[2, 4, 6, 8], "se:8 d c n p", "se:8=8 d=ok c=6 n=none p=4", "se:8=8 d=ok c=none n=none p=6"),
    (&[2, 4, 6, 8], "s:9 c p", "s:9=none c=8 p=8", "s:9=none c=none p=8"),
    (&[2, 4, 6, 8], "f p c n", "f=2 p=none c=2 n=4", "f=2 p=none c=none n=2"),
    (&[2, 4, 6, 8], "f p d", "f=2 p=none d=ok", "f=2 p=none d=nodata"),
    (
        &[4],
        "se:4 d put:3 n p",
        "se:4=4 d=ok put:3=ok n=none p=none",
        "se:4=4 d=ok put:3=ok n=none p=3",
    ),
    (
        &[2, 4, 6, 8],
        "se:4 del:4 put:5 c p",
        "se:4=4 del:4=ok put:5=ok c=6 p=5",
        "se:4=4 del:4=ok put:5=ok c=5 p=2",
    ),
    (
        &[2, 4, 6, 8],
        "se:4 del:4 put:4 c p",
        "se:4=4 del:4=ok put:4=ok c=6 p=4",
        "se:4=4 del:4=ok put:4=ok c=4 p=2",
    ),
    (&[], "se:0 put:0 n", "se:0=none put:0=ok n=0", "se:0=none put:0=ok n=none"),
    (&[2, 5, 9], "se:6 del:9 p", "se:6=none del:9=ok p=5", "se:6=none del:9=ok p=none"),
    (
        &[2, 4, 6, 8],
        "l del:8 put:9 put:10 c n",
        "l=8 del:8=ok put:9=ok put:10=ok c=6 n=none",
        "l=8 del:8=ok put:9=ok put:10=ok c=9 n=10",
    ),
    (
        &[2, 4, 6, 8],
        "l 2se:8 2d 2a:9 n",
        "l=8 2se:8=8 2d=ok 2a:9=ok n=none",
        "l=8 2se:8=8 2d=ok 2a:9=ok n=9",
    ),
    (
        &[2, 4, 6, 8],
        "l del:8 put:8 2a:9 n c",
        "l=8 del:8=ok put:8=ok 2a:9=ok n=none c=6",
        "l=8 del:8=ok put:8=ok 2a:9=ok n=none c=8",
    ),
];

#[test]
fn known_cursor_deviations_from_mdbx() {
    let (_mdbx_dir, mdbx) = create_mdbx_db();
    let (_dir, db) = create_test_db(TndbEnvKind::RW);
    for (keys, seq, on_mdbx, on_tndb) in DEVIATIONS {
        assert_eq!(steps(&mdbx, keys, seq), *on_mdbx, "MDBX");
        assert_eq!(steps(&db, keys, seq), *on_tndb, "tndb-b");
    }
}

/// Runs `steps` (`f`, `l`, `n`, `p` and `c`, as in [`steps`]) on a fresh cursor of table `T` in a
/// read-only transaction, and returns each step with its outcome, a row shown by `show`.
fn ro_steps<D: Database, T: Table>(
    db: &D,
    steps: &str,
    show: impl Fn(T::Key, T::Value) -> String,
) -> String {
    let tx = db.tx().unwrap();
    let mut c = tx.cursor_read::<T>().unwrap();
    let mut out = Vec::new();
    for step in steps.split_whitespace() {
        let row = match step {
            "f" => c.first(),
            "l" => c.last(),
            "n" => c.next(),
            "p" => c.prev(),
            "c" => c.current(),
            _ => panic!("unknown step {step}"),
        };
        let outcome = match row {
            Ok(Some((k, v))) => show(k, v),
            Ok(None) => "none".into(),
            Err(e) => error_name(e),
        };
        out.push(format!("{step}={outcome}"));
    }
    out.join(" ")
}

/// A read-only cursor ends the table after `last` (or a fresh `prev`) as MDBX's does, as a write
/// cursor does ([`MATCHED`]): `next` finds nothing and the cursor stays on the row, so `current`
/// returns it and `prev` the row before. On a dup-sort table it ends the last key's rows as a
/// write cursor does: `next` there finds nothing and goes past the end, where MDBX's `current`
/// finds nothing too, but its `prev` skips the key's other rows, a difference the write cursor's
/// `known_dup_cursor_deviations_from_mdbx` pins (`l n p`), pinned here on both sides too.
#[test]
fn read_only_cursor_steps_after_last_match_mdbx() {
    fn fill<D: Database>(db: &D) {
        db.update(|tx| {
            for k in [2, 4, 6, 8] {
                tx.put::<CanonicalHeaders>(k, B256::with_last_byte(k as u8))?;
                for s in [1, 2] {
                    let entry = StorageEntry { key: B256::with_last_byte(s), value: U256::from(s) };
                    tx.put::<PlainStorageState>(Address::with_last_byte(k as u8), entry)?;
                }
            }
            Ok::<_, DatabaseError>(())
        })
        .unwrap()
        .unwrap();
    }
    let (_mdbx_dir, mdbx) = create_mdbx_db();
    let (_dir, db) = create_test_db(TndbEnvKind::RW);
    fill(&mdbx);
    fill(&db);
    let plain = |k: u64, _| k.to_string();
    let dup = |k: Address, v: StorageEntry| format!("{}.{}", k.0[19], v.key.0[31]);
    for seq in ["l n c p", "l n n c p n", "l c n p", "p n c p", "l n p n c"] {
        assert_eq!(
            ro_steps::<_, CanonicalHeaders>(&db, seq, plain),
            ro_steps::<_, CanonicalHeaders>(&mdbx, seq, plain),
            "{seq}"
        );
    }
    assert_eq!(ro_steps::<_, CanonicalHeaders>(&db, "l n c p", plain), "l=8 n=none c=8 p=6");
    for seq in ["l n c", "l n n c", "l c p n", "p n c", "l p n c"] {
        assert_eq!(
            ro_steps::<_, PlainStorageState>(&db, seq, dup),
            ro_steps::<_, PlainStorageState>(&mdbx, seq, dup),
            "dup {seq}"
        );
    }
    assert_eq!(ro_steps::<_, PlainStorageState>(&mdbx, "l n p", dup), "l=8.2 n=none p=6.2");
    assert_eq!(ro_steps::<_, PlainStorageState>(&db, "l n p", dup), "l=8.2 n=none p=8.2");
}

/// A range walk may start past an excluded key, where MDBX's cursor panics.
#[test]
fn a_range_walk_starts_past_an_excluded_key() {
    let (_dir, db) = create_test_db(TndbEnvKind::RW);
    db.update(|tx| (0..7).try_for_each(|k| tx.put::<CanonicalHeaders>(k, B256::ZERO)))
        .unwrap()
        .unwrap();
    let tx = db.tx().unwrap();
    let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();
    let keys = |cursor: &mut crate::TndbCursor<CanonicalHeaders>, range| {
        cursor.walk_range(range).unwrap().map(|row| row.unwrap().0).collect::<Vec<_>>()
    };
    assert_eq!(keys(&mut cursor, (Bound::Excluded(2), Bound::Included(5))), [3, 4, 5]);
    assert_eq!(keys(&mut cursor, (Bound::Excluded(6), Bound::Unbounded)), [] as [u64; 0]);
    assert_eq!(keys(&mut cursor, (Bound::Excluded(9), Bound::Unbounded)), [] as [u64; 0]);
    db.update(|tx| tx.delete::<CanonicalHeaders>(3, None)).unwrap().unwrap();
    let tx = db.tx().unwrap();
    let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();
    assert_eq!(keys(&mut cursor, (Bound::Excluded(3), Bound::Excluded(6))), [4, 5]);
}

/// Writes the metrics test makes, on any database.
fn metrics_writes<D: Database>(db: &D) {
    db.update(|tx| {
        for k in 0..1000u64 {
            tx.put::<CanonicalHeaders>(k, B256::with_last_byte(k as u8))?;
        }
        for a in 0..5u8 {
            tx.put::<PlainAccountState>(Address::with_last_byte(a), Account::default())?;
        }
        // A value in an overflow run.
        let code = Bytecode::new_raw(Bytes::from(vec![0x60; 10_000]));
        tx.put::<Bytecodes>(B256::with_last_byte(1), code)
    })
    .unwrap()
    .unwrap();
    // Frees pages: the free list holds them.
    db.update(|tx| {
        (500..1000u64).try_for_each(|k| tx.delete::<CanonicalHeaders>(k, None).map(drop))
    })
    .unwrap()
    .unwrap();
}

/// The gauges by name and labels.
fn gauges(db: &impl DatabaseMetrics) -> BTreeMap<(String, Vec<(String, String)>), f64> {
    db.gauge_metrics()
        .into_iter()
        .map(|(name, value, labels)| {
            let labels =
                labels.iter().map(|l| (l.key().to_string(), l.value().to_string())).collect();
            ((name.to_string(), labels), value)
        })
        .collect()
}

#[test]
fn metrics_report_every_table_as_mdbx_does() {
    let (_mdbx_dir, mdbx) = create_mdbx_db();
    let (_dir, db) = create_test_db(TndbEnvKind::RW);
    metrics_writes(&mdbx);
    metrics_writes(&db);
    let (want, have) = (gauges(&mdbx), gauges(&db));

    // The same gauges, names and labels.
    let names = |g: &BTreeMap<_, _>| g.keys().cloned().collect::<BTreeSet<_>>();
    assert_eq!(names(&have), names(&want));
    let label = |table: &str| vec![("table".to_string(), table.to_string())];
    for table in Tables::ALL.iter().map(Tables::name) {
        // The same entries in every table.
        let entries = ("db.table_entries".to_string(), label(table));
        assert_eq!(have[&entries], want[&entries], "{table}");
        // The size is the page size times the table's pages.
        let pages = ["leaf", "branch", "overflow"]
            .map(|kind| {
                let mut labels = label(table);
                labels.push(("type".to_string(), kind.to_string()));
                have[&("db.table_pages".to_string(), labels)]
            })
            .iter()
            .sum::<f64>();
        assert_eq!(have[&("db.table_size".to_string(), label(table))], 4096.0 * pages, "{table}");
    }
    let stat = |name: &str| {
        use tndb_b::ReadTxn;
        let txn = db.env().begin_ro().unwrap();
        let table = txn.open_table(name).unwrap().unwrap();
        txn.table_stat(&table).unwrap()
    };
    for (table, entries) in
        [(CanonicalHeaders::NAME, 500.0), (PlainAccountState::NAME, 5.0), (Bytecodes::NAME, 1.0)]
    {
        assert_eq!(have[&("db.table_entries".to_string(), label(table))], entries);
        let s = stat(table);
        let size = 4096.0 * f64::from(s.leaf_pages + s.branch_pages + s.overflow_pages);
        assert!(size > 0.0);
        assert_eq!(have[&("db.table_size".to_string(), label(table))], size, "{table}");
    }
    assert!(stat(Bytecodes::NAME).overflow_pages > 0);
    assert!(stat(CanonicalHeaders::NAME).branch_pages > 0);
    assert_eq!(have[&("db.page_size".to_string(), vec![])], 4096.0);
    assert_eq!(have[&("db.timed_out_not_aborted_transactions".to_string(), vec![])], 0.0);
    let report = db.env().check().unwrap();
    assert!(report.free_pages > report.free_records, "{report:?}");
    assert_eq!(have[&("db.freelist".to_string(), vec![])], report.free_pages as f64);
}

/// `gauge_metrics` reports exactly the tables and figures of `table_stats`.
fn assert_gauges_match_table_stats(db: &TndbEnv) {
    let have = gauges(db);
    let stats = db.table_stats().unwrap();
    let label = |table: &str| vec![("table".to_string(), table.to_string())];
    let gauge = |name: &str, labels: Vec<(String, String)>| have[&(name.to_string(), labels)];
    let page_size = f64::from(db.page_size());
    for stat in &stats {
        let pages = |kind: &str| {
            let mut labels = label(stat.name);
            labels.push(("type".to_string(), kind.to_string()));
            gauge("db.table_pages", labels)
        };
        assert_eq!(gauge("db.table_entries", label(stat.name)), stat.entries as f64);
        assert_eq!(pages("leaf"), stat.leaf_pages as f64, "{}", stat.name);
        assert_eq!(pages("branch"), stat.branch_pages as f64, "{}", stat.name);
        assert_eq!(pages("overflow"), stat.overflow_pages as f64, "{}", stat.name);
        let size = gauge("db.table_size", label(stat.name));
        assert_eq!(size, page_size * stat.pages() as f64, "{}", stat.name);
    }
    let gauged: BTreeSet<String> = have
        .keys()
        .filter(|(name, _)| name == "db.table_entries")
        .map(|(_, labels)| labels[0].1.clone())
        .collect();
    assert_eq!(gauged, stats.iter().map(|s| s.name.to_string()).collect::<BTreeSet<_>>());
}

#[test]
fn table_stats_report_each_tables_rows_and_pages() {
    use tndb_b::ReadTxn;
    let (_dir, db) = create_test_db(TndbEnvKind::RW);
    metrics_writes(&db);
    // A dup-sort table: two keys, five values.
    db.update(|tx| {
        for (a, slots) in [(1u8, 3u8), (2, 2)] {
            for slot in 0..slots {
                let entry =
                    StorageEntry { key: B256::with_last_byte(slot), value: U256::from(slot + 1) };
                tx.put::<PlainStorageState>(Address::with_last_byte(a), entry)?;
            }
        }
        Ok::<_, DatabaseError>(())
    })
    .unwrap()
    .unwrap();
    let stats = db.table_stats().unwrap();

    // Every one of reth's tables, in `Tables::ALL`'s order.
    let names: Vec<_> = stats.iter().map(|s| s.name).collect();
    assert_eq!(names, Tables::ALL.iter().map(Tables::name).collect::<Vec<_>>());
    let txn = db.env().begin_ro().unwrap();
    for stat in &stats {
        let table = txn.open_table(stat.name).unwrap().unwrap();
        let s = txn.table_stat(&table).unwrap();
        assert_eq!(stat.entries, s.entries, "{}", stat.name);
        assert_eq!(stat.branch_pages, u64::from(s.branch_pages), "{}", stat.name);
        assert_eq!(stat.leaf_pages, u64::from(s.leaf_pages), "{}", stat.name);
        assert_eq!(stat.overflow_pages, u64::from(s.overflow_pages), "{}", stat.name);
    }
    let stat = |name: &str| *stats.iter().find(|s| s.name == name).unwrap();
    for (table, entries) in [
        (CanonicalHeaders::NAME, 500),
        (PlainAccountState::NAME, 5),
        (Bytecodes::NAME, 1),
        (PlainStorageState::NAME, 5),
        (Tables::Transactions.name(), 0),
    ] {
        assert_eq!(stat(table).entries, entries, "{table}");
    }
    assert!(stat(CanonicalHeaders::NAME).branch_pages > 0);
    assert!(stat(Bytecodes::NAME).overflow_pages > 0);
    assert!(stat(PlainStorageState::NAME).leaf_pages > 0);
    // A table without rows holds no pages.
    assert_eq!(stat(Tables::Transactions.name()).pages(), 0);
    assert_eq!(db.page_size(), 4096);
    assert_gauges_match_table_stats(&db);
}

#[test]
fn table_stats_skip_a_table_the_file_lacks() {
    use tndb_b::ReadTxn;
    let dir = tempfile::TempDir::new().unwrap();
    // No `create_tables`: the file has none of reth's tables.
    let db = super::open_env(dir.path(), TndbEnvKind::RW, TndbArgs::default()).unwrap();
    assert_eq!(db.table_stats().unwrap(), []);
    assert_gauges_match_table_stats(&db);
    // A table created after the open, which the table map lacks, is opened for its figures.
    let mut txn = db.env().begin_rw().unwrap();
    let table = txn.create_table(CanonicalHeaders::NAME).unwrap();
    txn.put(&table, &[1], &[2]).unwrap();
    txn.commit().unwrap();
    let stats = db.table_stats().unwrap();
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0].name, CanonicalHeaders::NAME);
    assert_eq!(stats[0].entries, 1);
    assert_eq!((stats[0].leaf_pages, stats[0].branch_pages, stats[0].overflow_pages), (1, 0, 0));
    let txn = db.env().begin_ro().unwrap();
    assert!(txn.open_table(PlainAccountState::NAME).unwrap().is_none());
    assert_gauges_match_table_stats(&db);
}

/// `sync` makes `NoSync` commits durable. A copy of the file is what a process crash leaves (the
/// committed pages are in the page cache, but no meta names them until a sync), so each copy here
/// holds the state of the last sync: before the first, not even the tables; after it, every commit
/// up to it; a commit after it only once the next sync runs.
#[test]
fn sync_makes_no_sync_commits_durable() {
    use std::path::Path;
    use tndb_b::SyncMode;

    /// The `CanonicalHeaders` rows of a copy of the file in `dir`, or `None` if it lacks the table.
    fn durable_rows(dir: &Path) -> Option<usize> {
        let copy = tempfile::TempDir::new().unwrap();
        std::fs::copy(dir.join(crate::DATA_FILE), copy.path().join(crate::DATA_FILE)).unwrap();
        let db = super::open_env(copy.path(), TndbEnvKind::RO, TndbArgs::default()).unwrap();
        let tx = db.tx().unwrap();
        tx.entries::<CanonicalHeaders>().ok()
    }

    let dir = tempfile::TempDir::new().unwrap();
    let args = TndbArgs::default().with_sync_mode(SyncMode::NoSync);
    let mut db = super::open_env(dir.path(), TndbEnvKind::RW, args).unwrap();
    db.create_tables().unwrap();
    let put = |n: u64| {
        db.update(|tx| tx.put::<CanonicalHeaders>(n, B256::with_last_byte(n as u8)))
            .unwrap()
            .unwrap()
    };
    put(1);
    assert_eq!(durable_rows(dir.path()), None, "no commit is durable before a sync");
    db.sync().unwrap();
    assert_eq!(durable_rows(dir.path()), Some(1), "every commit before the sync is durable");
    put(2);
    assert_eq!(durable_rows(dir.path()), Some(1), "a commit after the sync is not, yet");
    db.sync().unwrap();
    assert_eq!(durable_rows(dir.path()), Some(2), "until the next sync");
    db.sync().unwrap();
    assert_eq!(db.failure(), None, "a sync with nothing pending changes nothing");
}

/// `io_stats` is the engine's counters: a reth write through the adapter counts one durable
/// commit and the pages it synced, a reth read nothing, and a read-only open stays at zero.
#[test]
fn io_stats_count_a_reth_commit() {
    let (dir, db) = create_test_db(TndbEnvKind::RW);
    let before = db.io_stats();
    db.update(|tx| tx.put::<CanonicalHeaders>(1, B256::with_last_byte(1))).unwrap().unwrap();
    let d = db.io_stats().since(&before);
    assert_eq!(d.commits, 1, "{d:?}");
    assert!(d.pages_synced > 0, "{d:?}");
    assert_eq!(d.msync_syncs + d.fdatasync_syncs, 1, "{d:?}");
    let written = db.io_stats();
    assert!(db.tx().unwrap().get::<CanonicalHeaders>(1).unwrap().is_some());
    assert_eq!(db.io_stats(), written, "a read counts nothing");
    drop(db);
    let ro = super::open_env(dir.path(), TndbEnvKind::RO, TndbArgs::default()).unwrap();
    assert_eq!(ro.io_stats(), crate::IoStats::default());
}

/// `check` reports the newest committed state's tables. Damage it finds (here an overflow run's
/// middle page changed on disk, which the open does not read) is `MDBX_CORRUPTED` as a read
/// error, and latches the environment, with the detail in `failure`.
#[test]
fn check_reports_the_tables_and_damage_latches_the_environment() {
    type Raw = RawTable<Bytecodes>;
    const MARK: &[u8] = b"overflow middle page";
    let mut value = vec![7u8; 20_000];
    value[9000..9000 + MARK.len()].copy_from_slice(MARK);
    let (dir, db) = create_test_db(TndbEnvKind::RW);
    db.update(|tx| {
        for n in 0..300u64 {
            tx.put::<CanonicalHeaders>(n, B256::with_last_byte(n as u8))?;
        }
        tx.put::<Raw>(RawKey::new(B256::with_last_byte(1)), RawValue::from_vec(value))
    })
    .unwrap()
    .unwrap();

    let report = db.check().unwrap();
    assert_eq!(report.tables[CanonicalHeaders::NAME].entries, 300);
    let bytecodes = report.tables[Bytecodes::NAME];
    assert_eq!(bytecodes.entries, 1);
    assert!(bytecodes.pages >= 5, "the value's run counts as the table's: {bytecodes:?}");
    assert_eq!(report.tables[Tables::Transactions.name()].entries, 0);
    assert_eq!(db.failure(), None);
    drop(db);

    let path = dir.path().join(crate::DATA_FILE);
    let mut file = std::fs::read(&path).unwrap();
    let at = file.windows(MARK.len()).position(|w| w == MARK).expect("the value is on disk");
    file[at] ^= 0xFF;
    std::fs::write(&path, file).unwrap();

    let db = super::open_env(dir.path(), TndbEnvKind::RW, TndbArgs::default()).unwrap();
    let damage = db.check().unwrap_err();
    assert_eq!(dbg(damage), dbg(DatabaseError::Read(Error::Corrupted.into())));
    assert!(db.failure().is_some(), "the detail is in `failure`");
    assert!(db.tx_mut().is_err(), "the environment latched");
}

/// The table map's hasher gives each of reth's tables its own hash, so a lookup is one probe.
#[test]
fn name_hasher_separates_reths_tables() {
    use crate::env::NameHasher;
    use std::hash::BuildHasher as _;
    let hasher = std::hash::BuildHasherDefault::<NameHasher>::default();
    let hashes: BTreeSet<u64> = Tables::ALL.iter().map(|t| hasher.hash_one(t.name())).collect();
    assert_eq!(hashes.len(), Tables::ALL.len());
}

/// The engine cursor of a reth cursor ([`ReadCursor`], [`WriteCursor`]) is as large as its larger
/// arm plus at most a tag word (neither arm boxed, never both held). A write cursor holds it
/// inline, beside 160 bytes of its own (`Rows`, the shared transaction, the table handle and two
/// buffers); a read cursor holds it boxed, so its pool takes it back without a move of the
/// engine cursor, beside 96 bytes of its own (its `Rows`, the pool's share and the table's id).
#[cfg(target_pointer_width = "64")]
#[test]
fn tndb_cursors_stay_small() {
    use std::mem::size_of;
    use tndb_b::{DupRoCursor, DupTxnCursor, RoCursor, TxnCursor};
    let read = size_of::<RoCursor>().max(size_of::<DupRoCursor>());
    let write = size_of::<TxnCursor>().max(size_of::<DupTxnCursor>());
    assert!(size_of::<ReadCursor>() <= read + 8, "{}", size_of::<ReadCursor>());
    assert!(size_of::<WriteCursor>() <= write + 8, "{}", size_of::<WriteCursor>());
    assert_eq!(size_of::<Option<Box<ReadCursor>>>(), 8);
    let cursor = size_of::<TndbCursor<PlainStorageState>>();
    assert!(cursor <= 8 + 96, "{cursor}");
    let cursor = size_of::<TndbCursorMut<PlainStorageState>>();
    assert!(cursor <= write + 8 + 160, "{cursor} over {write}");
}

/// Rows of a plain table (`CanonicalHeaders`), a nested dup table (`PlainStorageState`) and a
/// composite dup table (`StorageChangeSets`), with the keys a script seeks (present and absent).
struct PoolRows {
    headers: Vec<u64>,
    addresses: Vec<Address>,
    changes: Vec<reth_db_api::models::BlockNumberAddress>,
    slots: Vec<B256>,
}

fn pool_rows(db: &TndbEnv) -> PoolRows {
    use reth_db_api::models::BlockNumberAddress;
    db.update(|tx| {
        for k in (0..120_u64).step_by(3) {
            tx.put::<CanonicalHeaders>(k, B256::with_last_byte(k as u8))?;
        }
        for a in (0..40_u8).step_by(2) {
            for s in 0..1 + a % 5 {
                let entry =
                    StorageEntry { key: B256::with_last_byte(2 * s + 1), value: U256::from(a + s) };
                tx.put::<PlainStorageState>(Address::with_last_byte(a), entry)?;
                tx.put::<StorageChangeSets>(
                    BlockNumberAddress((u64::from(a % 7), Address::with_last_byte(a))),
                    entry,
                )?;
            }
        }
        Ok::<_, DatabaseError>(())
    })
    .unwrap()
    .unwrap();
    PoolRows {
        headers: (0..125).collect(),
        addresses: (0..42).map(Address::with_last_byte).collect(),
        changes: (0..42)
            .map(|a| BlockNumberAddress((u64::from(a % 7), Address::with_last_byte(a))))
            .collect(),
        slots: (0..12).map(B256::with_last_byte).collect(),
    }
}

/// Call `op` (kinds 0 to 7) of a read cursor's script on `c`: first, last, next (twice the
/// odds), prev, current, and a seek or a `seek_exact` of one of `keys`; the outcome as text.
fn plain_step<T: Table>(c: &mut TndbCursor<T>, (op, i): (u8, usize), keys: &[T::Key]) -> String {
    let key = keys[i % keys.len()].clone();
    match op {
        0 => format!("{:?}", c.first()),
        1 => format!("{:?}", c.last()),
        2 | 3 => format!("{:?}", c.next()),
        4 => format!("{:?}", c.prev()),
        5 => format!("{:?}", c.current()),
        6 => format!("{:?}", c.seek(key)),
        _ => format!("{:?}", c.seek_exact(key)),
    }
}

/// [`plain_step`] with the dup calls too (kinds 8 to 13): `next_dup`, `prev_dup`, `next_no_dup`,
/// `next_dup_val`, `last_dup`, and a `seek_by_key_subkey` of one of `keys` and `subkeys`.
fn dup_step<T: reth_db_api::table::DupSort>(
    c: &mut TndbCursor<T>,
    (op, i): (u8, usize),
    keys: &[T::Key],
    subkeys: &[T::SubKey],
) -> String {
    use reth_db_api::cursor::DbDupCursorRO;
    let key = keys[i % keys.len()].clone();
    let subkey = subkeys[(i / keys.len()) % subkeys.len()].clone();
    match op {
        8 => format!("{:?}", c.next_dup()),
        9 => format!("{:?}", c.prev_dup()),
        10 => format!("{:?}", c.next_no_dup()),
        11 => format!("{:?}", c.next_dup_val()),
        12 => format!("{:?}", c.last_dup()),
        13 => format!("{:?}", c.seek_by_key_subkey(key, subkey)),
        _ => plain_step(c, (op, i), keys),
    }
}

/// Rounds over table `T` of one read-only transaction: a cursor moved by a random script of
/// `kinds` kinds of call (`step`) and dropped, then a cursor of table `O` moved and dropped, so
/// the pool holds one engine cursor of each; then a new cursor of `T`, which takes the pooled
/// one, and a new cursor of another transaction on the same state run one more script, and each
/// call's outcome is the same on both.
fn pooled_reads_as_fresh<T: Table, O: Table>(
    db: &TndbEnv,
    kinds: u8,
    step: impl Fn(&mut TndbCursor<T>, (u8, usize)) -> String,
) {
    let mut seed = 0x9e37_79b9_7f4a_7c15_u64 ^ u64::from(kinds);
    let mut rng = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    let tx = db.tx().unwrap();
    let mut differed = 0;
    for round in 0..300 {
        let mut used = tx.cursor_read::<T>().unwrap();
        for _ in 0..rng() % 8 {
            let r = rng();
            step(&mut used, ((r % u64::from(kinds)) as u8, (r >> 8) as usize));
        }
        drop(used);
        let mut other = tx.cursor_read::<O>().unwrap();
        other.last().unwrap();
        drop(other);
        assert_eq!(tx.pooled_cursors(), 2, "round {round}");
        let mut pooled = tx.cursor_read::<T>().unwrap();
        assert_eq!(tx.pooled_cursors(), 1, "round {round}");
        let fresh_tx = db.tx().unwrap();
        let mut fresh = fresh_tx.cursor_read::<T>().unwrap();
        for s in 0..1 + rng() % 6 {
            let r = rng();
            let op = ((r % u64::from(kinds)) as u8, (r >> 8) as usize);
            let want = step(&mut fresh, op);
            differed += usize::from(want.contains("Some"));
            assert_eq!(step(&mut pooled, op), want, "round {round} step {s} {op:?}");
        }
    }
    assert!(differed > 300, "only {differed} calls found a row");
}

/// A read-only transaction's cursor that takes a pooled engine cursor (one a dropped cursor of
/// the same table gave back, after any moves) reads as a new cursor does, call for call: plain
/// moves and seeks on a plain table, and the dup calls too on a nested and on a composite dup
/// table. Another table's cursor in the pool is never taken.
#[test]
fn a_pooled_cursor_reads_as_a_fresh_one() {
    let (_dir, db) = create_test_db(TndbEnvKind::RW);
    let rows = pool_rows(&db);
    pooled_reads_as_fresh::<CanonicalHeaders, PlainStorageState>(&db, 8, |c, op| {
        plain_step(c, op, &rows.headers)
    });
    pooled_reads_as_fresh::<PlainStorageState, CanonicalHeaders>(&db, 14, |c, op| {
        dup_step(c, op, &rows.addresses, &rows.slots)
    });
    pooled_reads_as_fresh::<StorageChangeSets, PlainStorageState>(&db, 14, |c, op| {
        dup_step(c, op, &rows.changes, &rows.slots)
    });
}

/// A read-only cursor outlives its transaction: it reads the transaction's state after the
/// handle drops, on another thread too, and its engine cursor goes back to the pool, which is
/// freed with the last cursor.
#[test]
fn a_cursor_may_outlive_its_tx() {
    let (_dir, db) = create_test_db(TndbEnvKind::RW);
    pool_rows(&db);
    let tx = db.tx().unwrap();
    let pool = tx.pool_weak();
    let mut kept = tx.cursor_read::<PlainStorageState>().unwrap();
    drop(tx.cursor_read::<CanonicalHeaders>().unwrap());
    drop(tx);
    db.update(|tx| tx.clear::<PlainStorageState>()).unwrap().unwrap();
    assert!(pool.upgrade().is_some(), "the cursor holds the pool");
    let first = kept.first().unwrap().map(|(k, v)| (k, v.key));
    assert_eq!(first, Some((Address::with_last_byte(0), B256::with_last_byte(1))));
    let handle = thread::spawn(move || {
        let rows = kept.walk(None).unwrap().count();
        drop(kept);
        rows
    });
    let rows: usize = (0..40_u8).step_by(2).map(|a| 1 + usize::from(a % 5)).sum();
    assert_eq!(handle.join().unwrap(), rows);
    assert!(pool.upgrade().is_none(), "the last cursor freed the pool");
}

/// The pool keeps at most [`ReadPool::PER_TABLE`] engine cursors a table: six cursors of one
/// table dropped together leave four, six of another four more, and a cursor taken from the
/// pool leaves it.
#[test]
fn the_cursor_pool_is_capped() {
    let (_dir, db) = create_test_db(TndbEnvKind::RW);
    pool_rows(&db);
    let tx = db.tx().unwrap();
    let headers: Vec<_> = (0..6).map(|_| tx.cursor_read::<CanonicalHeaders>().unwrap()).collect();
    assert_eq!(tx.pooled_cursors(), 0);
    drop(headers);
    assert_eq!(tx.pooled_cursors(), ReadPool::PER_TABLE);
    let slots: Vec<_> = (0..6).map(|_| tx.cursor_read::<PlainStorageState>().unwrap()).collect();
    drop(slots);
    assert_eq!(tx.pooled_cursors(), 2 * ReadPool::PER_TABLE);
    let held: Vec<_> = (0..3).map(|_| tx.cursor_read::<CanonicalHeaders>().unwrap()).collect();
    assert_eq!(tx.pooled_cursors(), 2 * ReadPool::PER_TABLE - 3);
    drop(held);
    assert_eq!(tx.pooled_cursors(), 2 * ReadPool::PER_TABLE);
}
