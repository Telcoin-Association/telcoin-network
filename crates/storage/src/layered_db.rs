use std::{
    fmt::Debug,
    marker::PhantomData,
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use crate::mem_db::MemDatabase;
use tn_types::{DBIter, Database, DbTx, DbTxMut, Table};

#[derive(Clone, Debug)]
pub struct LayeredDbTx {
    mem_db: MemDatabase,
}

impl DbTx for LayeredDbTx {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        self.mem_db.get::<T>(key)
    }
}

#[derive(Clone)]
pub struct LayeredDbTxMut<DB: Database> {
    mem_db: MemDatabase,
    tx: Sender<DBMessage<DB>>,
}

impl<DB: Database> Debug for LayeredDbTxMut<DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LayeredDbTxMut")
    }
}

impl<DB: Database> DbTx for LayeredDbTxMut<DB> {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        self.mem_db.get::<T>(key)
    }
}

impl<DB: Database> DbTxMut for LayeredDbTxMut<DB> {
    fn insert<T: Table>(&mut self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        self.mem_db.insert::<T>(key, value)?;
        let ins = Box::new(KeyValueInsert::<T> { key: key.clone(), value: value.clone() });
        self.tx.send(DBMessage::Insert(ins)).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(())
    }

    fn remove<T: Table>(&mut self, key: &T::Key) -> eyre::Result<()> {
        self.mem_db.remove::<T>(key)?;
        let rm = Box::new(KeyRemove::<T> { key: key.clone() });
        self.tx.send(DBMessage::Remove(rm)).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(())
    }

    fn clear_table<T: Table>(&mut self) -> eyre::Result<()> {
        self.mem_db.clear_table::<T>()?;
        let clr = Box::new(ClearTable::<T> { _casper: PhantomData });
        self.tx.send(DBMessage::Clear(clr)).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(())
    }

    fn commit(self) -> eyre::Result<()> {
        self.tx.send(DBMessage::CommitTxn).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(())
    }
}

/// Run the thread to manage the persistant DB in the background.
/// If DB needs compaction this thread will compact on startup and once a day after that.
fn db_run<DB: Database>(db: DB, rx: Receiver<DBMessage<DB>>) {
    let mut txn = None;
    let mut last_compact = Instant::now();
    if let Err(e) = db.compact() {
        tracing::error!("DB ERROR compacting DB on startup (background): {e}");
    }
    while let Ok(msg) = rx.recv() {
        match msg {
            DBMessage::StartTxn => {
                if let Some((_txn, count)) = &mut txn {
                    *count += 1;
                } else {
                    match db.write_txn() {
                        Ok(ntxn) => txn = Some((ntxn, 1)),
                        Err(e) => tracing::error!("DB ERROR getting write txn (background): {e}"),
                    }
                }
            }
            DBMessage::CommitTxn => {
                if let Some((current_txn, count)) = txn.take() {
                    if count <= 1 {
                        if let Err(e) = current_txn.commit() {
                            tracing::error!("DB TXN Commit: {e}")
                        }
                    } else {
                        txn = Some((current_txn, count - 1));
                    }
                }
            }
            DBMessage::Insert(ins) => {
                if let Some((txn, _)) = &mut txn {
                    if let Err(e) = ins.insert_txn(txn) {
                        tracing::error!("DB TXN Insert: {e}")
                    }
                } else if let Err(e) = ins.insert(&db) {
                    tracing::error!("DB Insert: {e}")
                }
            }
            DBMessage::Remove(rm) => {
                if let Some((txn, _)) = &mut txn {
                    if let Err(e) = rm.remove_txn(txn) {
                        tracing::error!("DB TXN Remove: {e}")
                    }
                } else if let Err(e) = rm.remove(&db) {
                    tracing::error!("DB Remove: {e}")
                }
            }
            DBMessage::Clear(clr) => {
                if let Some((txn, _)) = &mut txn {
                    if let Err(e) = clr.clear_table_txn(txn) {
                        tracing::error!("DB TXN Clear table: {e}")
                    }
                } else if let Err(e) = clr.clear_table(&db) {
                    tracing::error!("DB Clear: {e}")
                }
            }
            DBMessage::Shutdown => break,
        }
        // if it has been 24 hours since last compaction then do it again.
        if last_compact.elapsed() > Duration::from_secs(86_400) {
            last_compact = Instant::now();
            if let Err(e) = db.compact() {
                tracing::error!("DB ERROR compacting DB (background): {e}");
            }
        }
    }
    tracing::info!("Layerd DB thread Shutdown complete");
}

/// Implement the Database trait with an in-memory store.
/// This means no persistance.
/// This DB also plays loose with transactions, but since it is in-memory and we do not do
/// roll-backs this should be fine.
#[derive(Clone, Debug)]
pub struct LayeredDatabase<DB: Database> {
    mem_db: MemDatabase,
    db: DB,
    tx: Sender<DBMessage<DB>>,
    thread: Option<Arc<JoinHandle<()>>>, /* Use as a ref count for shuting down the background
                                          * thread and it's handle. */
}

impl<DB: Database> Drop for LayeredDatabase<DB> {
    fn drop(&mut self) {
        if Arc::strong_count(self.thread.as_ref().expect("no db thread!")) == 1 {
            tracing::info!("LayeredDatabase Dropping, shutting down DB thread");
            if let Err(e) = self.tx.send(DBMessage::Shutdown) {
                tracing::error!("Error while trying to send shutdown to layered DB thread {e}");
                return; // The thread may not shutdown so don't try to join...
            }
            // We can not be here without a thread handle
            if let Err(e) =
                Arc::into_inner(self.thread.take().expect("thread handle required to be here"))
                    .expect("only one strong `Arc` reference")
                    .join()
            {
                tracing::error!("Error while waiting for shutdown of layered DB thread {e:?}");
            } else {
                tracing::info!("LayeredDatabase Dropped, DB thread is shutdown");
            }
        }
    }
}

impl<DB: Database> LayeredDatabase<DB> {
    pub fn open(db: DB) -> Self {
        let (tx, rx) = mpsc::channel();
        let db_cloned = db.clone();
        let thread = Some(Arc::new(std::thread::spawn(move || db_run(db_cloned, rx))));
        Self { mem_db: MemDatabase::new(), db, tx, thread }
    }

    pub fn open_table<T: Table>(&self) {
        self.mem_db.open_table::<T>();
        for (key, value) in self.db.iter::<T>() {
            // mem db insert should not fail.
            let _ = self.mem_db.insert::<T>(&key, &value);
        }
    }
}

impl<DB: Database> Database for LayeredDatabase<DB> {
    type TX<'txn>
        = LayeredDbTx
    where
        Self: 'txn;

    type TXMut<'txn>
        = LayeredDbTxMut<DB>
    where
        Self: 'txn;

    fn read_txn(&self) -> eyre::Result<Self::TX<'_>> {
        Ok(LayeredDbTx { mem_db: self.mem_db.clone() })
    }

    /// Note that write transactions for the layerd DB will be "overlapped" and committed when the
    /// last commit happens. Also, all write operations are saved in memory then passed to
    /// thread for persistance in the background so operations will return quickly.
    fn write_txn(&self) -> eyre::Result<Self::TXMut<'_>> {
        self.tx.send(DBMessage::StartTxn).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(LayeredDbTxMut { mem_db: self.mem_db.clone(), tx: self.tx.clone() })
    }

    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        self.mem_db.contains_key::<T>(key)
    }

    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        self.mem_db.get::<T>(key)
    }

    fn insert<T: Table>(&self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        self.mem_db.insert::<T>(key, value)?;
        let ins = Box::new(KeyValueInsert::<T> { key: key.clone(), value: value.clone() });
        self.tx.send(DBMessage::Insert(ins)).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(())
    }

    fn remove<T: Table>(&self, key: &T::Key) -> eyre::Result<()> {
        self.mem_db.remove::<T>(key)?;
        let rm = Box::new(KeyRemove::<T> { key: key.clone() });
        self.tx.send(DBMessage::Remove(rm)).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(())
    }

    fn clear_table<T: Table>(&self) -> eyre::Result<()> {
        self.mem_db.clear_table::<T>()?;
        let clr = Box::new(ClearTable::<T> { _casper: PhantomData });
        self.tx.send(DBMessage::Clear(clr)).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(())
    }

    fn is_empty<T: Table>(&self) -> bool {
        self.mem_db.is_empty::<T>()
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        self.mem_db.iter::<T>()
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        self.mem_db.skip_to::<T>(key)
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        self.mem_db.reverse_iter::<T>()
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        self.mem_db.record_prior_to::<T>(key)
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        self.mem_db.last_record::<T>()
    }
}

trait InsertTrait<DB: Database>: Send + 'static {
    fn insert(&self, db: &DB) -> eyre::Result<()>;
    fn insert_txn(&self, txn: &mut DB::TXMut<'_>) -> eyre::Result<()>;
}

trait RemoveTrait<DB: Database>: Send + 'static {
    fn remove(&self, db: &DB) -> eyre::Result<()>;
    fn remove_txn(&self, txn: &mut DB::TXMut<'_>) -> eyre::Result<()>;
}

trait ClearTrait<DB: Database>: Send + 'static {
    fn clear_table(&self, db: &DB) -> eyre::Result<()>;
    fn clear_table_txn(&self, txn: &mut DB::TXMut<'_>) -> eyre::Result<()>;
}

struct KeyValueInsert<T: Table> {
    key: T::Key,
    value: T::Value,
}

struct KeyRemove<T: Table> {
    key: T::Key,
}

struct ClearTable<T: Table> {
    _casper: PhantomData<T>,
}

impl<T: Table, DB: Database> InsertTrait<DB> for KeyValueInsert<T> {
    fn insert(&self, db: &DB) -> eyre::Result<()> {
        db.insert::<T>(&self.key, &self.value)
    }
    fn insert_txn(&self, txn: &mut DB::TXMut<'_>) -> eyre::Result<()> {
        txn.insert::<T>(&self.key, &self.value)
    }
}

impl<T: Table, DB: Database> RemoveTrait<DB> for KeyRemove<T> {
    fn remove(&self, db: &DB) -> eyre::Result<()> {
        db.remove::<T>(&self.key)
    }

    fn remove_txn(&self, txn: &mut <DB as Database>::TXMut<'_>) -> eyre::Result<()> {
        txn.remove::<T>(&self.key)
    }
}

impl<T: Table, DB: Database> ClearTrait<DB> for ClearTable<T> {
    fn clear_table(&self, db: &DB) -> eyre::Result<()> {
        db.clear_table::<T>()
    }

    fn clear_table_txn(&self, txn: &mut <DB as Database>::TXMut<'_>) -> eyre::Result<()> {
        txn.clear_table::<T>()
    }
}

enum DBMessage<DB: Database> {
    StartTxn,
    CommitTxn,
    Insert(Box<dyn InsertTrait<DB>>),
    Remove(Box<dyn RemoveTrait<DB>>),
    Clear(Box<dyn ClearTrait<DB>>),
    Shutdown,
}

impl<DB: Database> Debug for DBMessage<DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DBMessage::StartTxn => write!(f, "StartTxn"),
            DBMessage::CommitTxn => write!(f, "CommitTxn"),
            DBMessage::Insert(_) => write!(f, "Insert"),
            DBMessage::Remove(_) => write!(f, "Remove"),
            DBMessage::Clear(_) => write!(f, "Clear"),
            DBMessage::Shutdown => write!(f, "Shutdown"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::LayeredDatabase;
    #[cfg(feature = "redb")]
    use crate::redb::ReDB;
    use crate::{mdbx::MdbxDatabase, test::*};
    use std::path::Path;
    use tempfile::tempdir;

    #[cfg(feature = "redb")]
    fn open_redb(path: &Path) -> LayeredDatabase<ReDB> {
        let db = ReDB::open(path).expect("Cannot open database");
        db.open_table::<TestTable>().expect("failed to open table!");
        let db = LayeredDatabase::open(db);
        db.open_table::<TestTable>();
        db
    }

    fn open_mdbx(path: &Path) -> LayeredDatabase<MdbxDatabase> {
        let db = MdbxDatabase::open(path).expect("Cannot open database");
        db.open_table::<TestTable>().expect("failed to open table!");
        let db = LayeredDatabase::open(db);
        db.open_table::<TestTable>();
        db
    }

    #[test]
    fn test_layereddb_contains_key() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_contains_key(db);
        }
        let db = open_mdbx(temp_dir.path());
        test_contains_key(db);
    }

    #[test]
    fn test_layereddb_get() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_get(db);
        }
        let db = open_mdbx(temp_dir.path());
        test_get(db);
    }

    #[test]
    fn test_layereddb_multi_get() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_multi_get(db);
        }
        let db = open_mdbx(temp_dir.path());
        test_multi_get(db);
    }

    #[test]
    fn test_layereddb_skip() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_skip(db);
        }
        let db = open_mdbx(temp_dir.path());
        test_skip(db);
    }

    #[test]
    fn test_layereddb_skip_to_previous_simple() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_skip_to_previous_simple(db);
        }
        let db = open_mdbx(temp_dir.path());
        test_skip_to_previous_simple(db);
    }

    #[test]
    fn test_layereddb_iter_skip_to_previous_gap() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_iter_skip_to_previous_gap(db);
        }
        let db = open_mdbx(temp_dir.path());
        test_iter_skip_to_previous_gap(db);
    }

    #[test]
    fn test_layereddb_remove() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_remove(db);
        }
        let db = open_mdbx(temp_dir.path());
        test_remove(db);
    }

    #[test]
    fn test_layereddb_iter() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_iter(db);
        }
        let db = open_mdbx(temp_dir.path());
        test_iter(db);
    }

    #[test]
    fn test_layereddb_iter_reverse() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_iter_reverse(db);
        }
        let db = open_mdbx(temp_dir.path());
        test_iter_reverse(db);
    }

    #[test]
    fn test_layereddb_clear() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_clear(db);
        }
        let db = open_mdbx(temp_dir.path());
        test_clear(db);
    }

    #[test]
    fn test_layereddb_is_empty() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_is_empty(db);
        }
        let db = open_mdbx(temp_dir.path());
        test_is_empty(db);
    }

    #[test]
    fn test_layereddb_multi_insert() {
        // Init a DB
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_multi_insert(db);
        }
        let db = open_mdbx(temp_dir.path());
        test_multi_insert(db);
    }

    #[test]
    fn test_layereddb_multi_remove() {
        // Init a DB
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            test_multi_remove(db);
        }
        let db = open_mdbx(temp_dir.path());
        test_multi_remove(db);
    }

    #[test]
    fn test_layereddb_dbsimpbench() {
        // Init a DB
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path());
            db_simp_bench(db, "LayeredDB<ReDB>");
        }
        let db = open_mdbx(temp_dir.path());
        db_simp_bench(db, "LayeredDB<MdbxDatabase>");
    }
}
