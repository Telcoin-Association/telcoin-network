use std::{
    fmt::Debug,
    future::Future,
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
use tokio::sync::oneshot::{self, error::TryRecvError};

#[derive(Clone)]
pub struct LayeredDbTx<DB: Database> {
    mem_db: MemDatabase,
    db: DB,
}

impl<DB: Database> Debug for LayeredDbTx<DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LayeredDbTx")
    }
}

impl<DB: Database> DbTx for LayeredDbTx<DB> {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        if let Some(val) = self.mem_db.get::<T>(key)? {
            Ok(Some(val))
        } else {
            self.db.get::<T>(key)
        }
    }
}

#[derive(Clone)]
pub struct LayeredDbTxMut<DB: Database> {
    mem_db: MemDatabase,
    db: DB,
    tx: Sender<DBMessage<DB>>,
}

impl<DB: Database> Debug for LayeredDbTxMut<DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LayeredDbTxMut")
    }
}

impl<DB: Database> DbTx for LayeredDbTxMut<DB> {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        if let Some(val) = self.mem_db.get::<T>(key)? {
            Ok(Some(val))
        } else {
            self.db.get::<T>(key)
        }
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
fn db_run<DB: Database>(db: DB, mem_db: Option<MemDatabase>, rx: Receiver<DBMessage<DB>>) {
    let mut txn = None;
    let mut last_compact = Instant::now();
    let mut committed_inserts: Vec<Box<dyn InsertTrait<DB>>> = Vec::with_capacity(1000);
    if let Err(e) = db.compact() {
        tracing::error!(target: "layered_db_runner", "DB ERROR compacting DB on startup (background): {e}");
    }
    while let Ok(msg) = rx.recv() {
        match msg {
            DBMessage::StartTxn => {
                if let Some((_txn, count)) = &mut txn {
                    *count += 1;
                } else {
                    match db.write_txn() {
                        Ok(ntxn) => txn = Some((ntxn, 1)),
                        Err(e) => {
                            tracing::error!(target: "layered_db_runner", "DB ERROR getting write txn (background): {e}")
                        }
                    }
                }
            }
            DBMessage::CommitTxn => {
                if let Some((current_txn, count)) = txn.take() {
                    if count <= 1 {
                        if let Err(e) = current_txn.commit() {
                            tracing::error!(target: "layered_db_runner", "DB TXN Commit: {e}")
                        }
                        if let Some(mem_db) = mem_db.as_ref() {
                            for insert in committed_inserts.drain(..) {
                                insert.clear_insert_mem(mem_db);
                            }
                        }
                    } else {
                        txn = Some((current_txn, count - 1));
                    }
                }
            }
            DBMessage::Insert(ins) => {
                if let Some((txn, _)) = &mut txn {
                    if let Err(e) = ins.insert_txn(txn) {
                        tracing::error!(target: "layered_db_runner", "DB TXN Insert: {e}")
                    }
                    committed_inserts.push(ins);
                } else {
                    if let Err(e) = ins.insert(&db) {
                        tracing::error!(target: "layered_db_runner", "DB Insert: {e}");
                    }
                    if let Some(mem_db) = mem_db.as_ref() {
                        ins.clear_insert_mem(mem_db);
                    }
                }
            }
            DBMessage::Remove(rm) => {
                if let Some((txn, _)) = &mut txn {
                    if let Err(e) = rm.remove_txn(txn) {
                        tracing::error!(target: "layered_db_runner", "DB TXN Remove: {e}")
                    }
                } else if let Err(e) = rm.remove(&db) {
                    tracing::error!(target: "layered_db_runner", "DB Remove: {e}")
                }
            }
            DBMessage::Clear(clr) => {
                if let Some((txn, _)) = &mut txn {
                    if let Err(e) = clr.clear_table_txn(txn) {
                        tracing::error!(target: "layered_db_runner", "DB TXN Clear table: {e}")
                    }
                } else if let Err(e) = clr.clear_table(&db) {
                    tracing::error!(target: "layered_db_runner", "DB Clear: {e}")
                }
            }
            DBMessage::CaughtUp(tx) => {
                let _ = tx.send(());
            }
            DBMessage::Shutdown => break,
        }
        // if it has been 24 hours since last compaction then do it again.
        if last_compact.elapsed() > Duration::from_secs(86_400) {
            last_compact = Instant::now();
            if let Err(e) = db.compact() {
                tracing::error!(target: "layered_db_runner", "DB ERROR compacting DB (background): {e}");
            }
        }
    }
    tracing::info!(target: "layered_db_runner", "Layerd DB thread Shutdown complete");
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
    thread: Option<Arc<JoinHandle<()>>>, /* Use as a ref count for shutting down the background
                                          * thread and it's handle. */
    full_memory: bool, /* If true then keep all the data in memory otherwise only keep it until
                        * written to disk. */
}

impl<DB: Database> Drop for LayeredDatabase<DB> {
    fn drop(&mut self) {
        if Arc::strong_count(self.thread.as_ref().expect("no db thread!")) == 1 {
            tracing::info!(target: "layered_db", "LayeredDatabase Dropping, shutting down DB thread");
            if let Err(e) = self.tx.send(DBMessage::Shutdown) {
                tracing::error!(target: "layered_db", "Error while trying to send shutdown to layered DB thread {e}");
                return; // The thread may not shutdown so don't try to join...
            }
            // We can not be here without a thread handle
            if let Err(e) =
                Arc::into_inner(self.thread.take().expect("thread handle required to be here"))
                    .expect("only one strong `Arc` reference")
                    .join()
            {
                tracing::error!(target: "layered_db", "Error while waiting for shutdown of layered DB thread {e:?}");
            } else {
                tracing::info!(target: "layered_db", "LayeredDatabase Dropped, DB thread is shutdown");
            }
        }
    }
}

impl<DB: Database> LayeredDatabase<DB> {
    pub fn open(db: DB, full_memory: bool) -> Self {
        let (tx, rx) = mpsc::channel();
        let db_cloned = db.clone();
        let mem_db = MemDatabase::new();
        let mem_db_clone = if full_memory { None } else { Some(mem_db.clone()) };
        let thread =
            Some(Arc::new(std::thread::spawn(move || db_run(db_cloned, mem_db_clone, rx))));
        Self { mem_db, db, tx, thread, full_memory }
    }
}

impl<DB: Database> Database for LayeredDatabase<DB> {
    type TX<'txn>
        = LayeredDbTx<DB>
    where
        Self: 'txn;

    type TXMut<'txn>
        = LayeredDbTxMut<DB>
    where
        Self: 'txn;

    fn open_table<T: Table>(&self) -> eyre::Result<()> {
        self.db.open_table::<T>()?;
        self.mem_db.open_table::<T>()?;
        if self.full_memory {
            for (k, v) in self.db.iter::<T>() {
                let _ = self.mem_db.insert::<T>(&k, &v);
            }
        }
        Ok(())
    }

    fn read_txn(&self) -> eyre::Result<Self::TX<'_>> {
        Ok(LayeredDbTx { mem_db: self.mem_db.clone(), db: self.db.clone() })
    }

    /// Note that write transactions for the layerd DB will be "overlapped" and committed when the
    /// last commit happens. Also, all write operations are saved in memory then passed to
    /// thread for persistance in the background so operations will return quickly.
    fn write_txn(&self) -> eyre::Result<Self::TXMut<'_>> {
        self.tx.send(DBMessage::StartTxn).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(LayeredDbTxMut { mem_db: self.mem_db.clone(), db: self.db.clone(), tx: self.tx.clone() })
    }

    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        Ok(self.mem_db.contains_key::<T>(key)? || self.db.contains_key::<T>(key)?)
    }

    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        if let Some(val) = self.mem_db.get::<T>(key)? {
            Ok(Some(val))
        } else {
            self.db.get::<T>(key)
        }
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
        self.mem_db.is_empty::<T>() && self.db.is_empty::<T>()
    }

    /// This iterator will be acurate for a full memory DB however
    /// a layered db will return all inserted elements even if some
    /// are duplicates while writes occur if this is not full memory.
    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        if self.full_memory {
            Box::new(self.mem_db.iter::<T>())
        } else {
            Box::new(self.db.iter::<T>().chain(self.mem_db.iter::<T>()))
        }
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        if self.full_memory {
            self.mem_db.skip_to::<T>(key)
        } else {
            self.db.skip_to::<T>(key)
        }
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        if self.full_memory {
            self.mem_db.reverse_iter::<T>()
        } else {
            self.db.reverse_iter::<T>()
        }
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        if self.full_memory {
            self.mem_db.record_prior_to::<T>(key)
        } else {
            self.db.record_prior_to::<T>(key)
        }
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        if self.full_memory {
            self.mem_db.last_record::<T>()
        } else {
            self.db.last_record::<T>()
        }
    }

    fn persist<T: Table>(&self) -> impl Future<Output = ()> + Send {
        let (tx, rx) = oneshot::channel();
        let r = self
            .tx
            .send(DBMessage::CaughtUp(tx))
            .map_err(|_| eyre::eyre!("DB thread gone, FATAL!"));
        async move {
            if r.is_ok() {
                let _ = rx.await;
            }
        }
    }

    fn sync_persist(&self) {
        let (tx, mut rx) = oneshot::channel();
        let r = self
            .tx
            .send(DBMessage::CaughtUp(tx))
            .map_err(|_| eyre::eyre!("DB thread gone, FATAL!"));

        // Can not use rx.blocking_recv() because it will be called from some tokio tests and that
        // will panic.
        if r.is_ok() {
            // Wait for rx to not be empty.
            loop {
                match rx.try_recv() {
                    Err(TryRecvError::Empty) => std::thread::sleep(Duration::from_millis(100)),
                    Err(TryRecvError::Closed) => break,
                    _ => break,
                }
            }
        }
    }
}

trait InsertTrait<DB: Database>: Send + 'static {
    fn insert(&self, db: &DB) -> eyre::Result<()>;
    fn insert_txn(&self, txn: &mut DB::TXMut<'_>) -> eyre::Result<()>;
    /// Clear the inserted data from the memdb if present.
    fn clear_insert_mem(&self, mem_db: &MemDatabase);
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
    fn clear_insert_mem(&self, mem_db: &MemDatabase) {
        // Best effort to avoid memory growing forever.
        let _ = mem_db.remove::<T>(&self.key);
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
    CaughtUp(tokio::sync::oneshot::Sender<()>),
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
            DBMessage::CaughtUp(_) => write!(f, "CaughtUp"),
            DBMessage::Shutdown => write!(f, "Shutdown"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::LayeredDatabase;
    #[cfg(feature = "redb")]
    use crate::redb::ReDB;
    use crate::{
        mdbx::{database::MEGABYTE, MdbxDatabase},
        test::*,
    };
    use std::path::Path;
    use tempfile::tempdir;
    use tn_types::Database as _;

    #[cfg(feature = "redb")]
    fn open_redb(path: &Path, full_memory: bool) -> LayeredDatabase<ReDB> {
        let db = ReDB::open(path.join("redb")).expect("Cannot open database");
        db.open_table::<TestTable>().expect("failed to open table!");
        let db = LayeredDatabase::open(db, full_memory);
        db.open_table::<TestTable>().expect("failed to open table!");
        db
    }

    fn open_mdbx(path: &Path, full_memory: bool) -> LayeredDatabase<MdbxDatabase> {
        let db =
            MdbxDatabase::open(path, 4, 16 * MEGABYTE, 8 * MEGABYTE).expect("Cannot open database");
        db.open_table::<TestTable>().expect("failed to open table!");
        let db = LayeredDatabase::open(db, full_memory);
        db.open_table::<TestTable>().expect("failed to open table!");
        db
    }

    #[test]
    fn test_layereddb_contains_key() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path(), true);
            test_contains_key(db);
            let db = open_redb(temp_dir.path(), false);
            test_contains_key(db);
        }
        let db = open_mdbx(temp_dir.path(), true);
        test_contains_key(db);
        let db = open_mdbx(temp_dir.path(), false);
        test_contains_key(db);
    }

    #[test]
    fn test_layereddb_get() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path(), true);
            test_get(db);
            let db = open_redb(temp_dir.path(), false);
            test_get(db);
        }
        let db = open_mdbx(temp_dir.path(), true);
        test_get(db);
        let db = open_mdbx(temp_dir.path(), false);
        test_get(db);
    }

    #[test]
    fn test_layereddb_multi_get() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path(), true);
            test_multi_get(db);
            let db = open_redb(temp_dir.path(), false);
            test_multi_get(db);
        }
        let db = open_mdbx(temp_dir.path(), true);
        test_multi_get(db);
        let db = open_mdbx(temp_dir.path(), false);
        test_multi_get(db);
    }

    #[test]
    fn test_layereddb_skip() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path(), true);
            test_skip(db);
            let db = open_redb(temp_dir.path(), false);
            test_skip(db);
        }
        let db = open_mdbx(temp_dir.path(), true);
        test_skip(db);
        let db = open_mdbx(temp_dir.path(), false);
        test_skip(db);
    }

    #[test]
    fn test_layereddb_skip_to_previous_simple() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path(), true);
            test_skip_to_previous_simple(db);
            let db = open_redb(temp_dir.path(), false);
            test_skip_to_previous_simple(db);
        }
        let db = open_mdbx(temp_dir.path(), true);
        test_skip_to_previous_simple(db);
        let db = open_mdbx(temp_dir.path(), false);
        test_skip_to_previous_simple(db);
    }

    #[test]
    fn test_layereddb_iter_skip_to_previous_gap() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path(), true);
            test_iter_skip_to_previous_gap(db);
            let db = open_redb(temp_dir.path(), false);
            test_iter_skip_to_previous_gap(db);
        }
        let db = open_mdbx(temp_dir.path(), true);
        test_iter_skip_to_previous_gap(db);
        let db = open_mdbx(temp_dir.path(), false);
        test_iter_skip_to_previous_gap(db);
    }

    #[test]
    fn test_layereddb_remove() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path(), true);
            test_remove(db);
            let db = open_redb(temp_dir.path(), false);
            test_remove(db);
        }
        let db = open_mdbx(temp_dir.path(), true);
        test_remove(db);
        let db = open_mdbx(temp_dir.path(), false);
        test_remove(db);
    }

    #[test]
    fn test_layereddb_iter() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path(), true);
            test_iter(db);
            let db = open_redb(temp_dir.path(), false);
            test_iter(db);
        }
        let db = open_mdbx(temp_dir.path(), true);
        test_iter(db);
        let db = open_mdbx(temp_dir.path(), false);
        test_iter(db);
    }

    #[test]
    fn test_layereddb_iter_reverse() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path(), true);
            test_iter_reverse(db);
            let db = open_redb(temp_dir.path(), false);
            test_iter_reverse(db);
        }
        let db = open_mdbx(temp_dir.path(), true);
        test_iter_reverse(db);
        let db = open_mdbx(temp_dir.path(), false);
        test_iter_reverse(db);
    }

    #[test]
    fn test_layereddb_clear() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path(), true);
            test_clear(db);
            let db = open_redb(temp_dir.path(), false);
            test_clear(db);
        }
        let db = open_mdbx(temp_dir.path(), true);
        test_clear(db);
        let db = open_mdbx(temp_dir.path(), false);
        test_clear(db);
    }

    #[test]
    fn test_layereddb_is_empty() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path(), true);
            test_is_empty(db);
            let db = open_redb(temp_dir.path(), false);
            test_is_empty(db);
        }
        let db = open_mdbx(temp_dir.path(), true);
        test_is_empty(db);
        let db = open_mdbx(temp_dir.path(), false);
        test_is_empty(db);
    }

    #[test]
    fn test_layereddb_multi_insert() {
        // Init a DB
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path(), true);
            test_multi_insert(db);
            let db = open_redb(temp_dir.path(), false);
            test_multi_insert(db);
        }
        let db = open_mdbx(temp_dir.path(), true);
        test_multi_insert(db);
        let db = open_mdbx(temp_dir.path(), false);
        test_multi_insert(db);
    }

    #[test]
    fn test_layereddb_multi_remove() {
        // Init a DB
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path(), true);
            test_multi_remove(db);
            let db = open_redb(temp_dir.path(), false);
            test_multi_remove(db);
        }
        let db = open_mdbx(temp_dir.path(), true);
        test_multi_remove(db);
        let db = open_mdbx(temp_dir.path(), false);
        test_multi_remove(db);
    }

    #[test]
    fn test_layereddb_dbsimpbench() {
        // Only test with full memory.  Otherwise iterators, while correct, may not work the test.
        // Init a DB
        let temp_dir = tempdir().expect("failed to create temp dir");
        #[cfg(feature = "redb")]
        {
            let db = open_redb(temp_dir.path(), true);
            db_simp_bench(db, "LayeredDB<ReDB>");
        }
        let db = open_mdbx(temp_dir.path(), true);
        db_simp_bench(db, "LayeredDB<MdbxDatabase>");
    }
}
