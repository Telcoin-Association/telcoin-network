// SPDX-License-Identifier: MIT or Apache-2.0
//! Persistent storage types

// The pack files are mmap-backed and read with positional I/O (`std::os::unix::fs::FileExt`), and
// the heal/import paths identify files by inode: this crate builds for unix targets only.
#[cfg(not(unix))]
compile_error!("tn-storage requires a unix target (mmap-backed pack files, positional file I/O)");

mod stores;
#[cfg(feature = "reth-libmdbx")]
use mdbx::MdbxDatabase;
pub use stores::*;
use tables::{
    CertificateDigestByOrigin, CertificateDigestByRound, Certificates, ConsensusCache,
    KadProviderRecords, KadRecords, KadWorkerProviderRecords, KadWorkerRecords, LastProposed,
    NodeBatchesCache, OurNodeBatchesCache, Payload, ProposedCertificates, Votes,
};
pub mod archive;
pub mod certificate_pack;
/// The `CompositeDatabase` backend that splits the workload into epoch/kad/cache sub-databases.
pub mod composite_db;
pub mod consensus;
pub mod consensus_pack;
/// On-demand comparative benchmark harness across the `Database` backends (see `db_bench.rs`).
#[cfg(test)]
mod db_bench;
pub mod epoch_records;
pub(crate) mod error_latch;
pub mod exec_state_pack;
/// `LayeredDatabase`: a write-behind memory layer whose writer thread commits to a backend.
pub mod layered_db;
/// The reth MDBX key/value backend: the consensus DB of the release before tndb (the migration's
/// source), and a backend for tests and benches.
#[cfg(feature = "reth-libmdbx")]
pub mod mdbx;
pub mod mem_db;
/// On-demand observation benchmark for consensus pack files (see `pack_bench.rs`).
#[cfg(test)]
mod pack_bench;
/// On-demand raw-KV benchmark: digest vs btree pack files vs MDBX, plus sorted scans (see
/// `pack_kv_bench.rs`).
#[cfg(test)]
mod pack_kv_bench;
pub mod pack_validate;
/// On-demand benchmarks replaying production's DB traffic (see `workload_bench.rs`).
#[cfg(test)]
mod workload_bench;

pub mod tndb;

pub use tn_types::error::StoreError;

#[cfg(all(test, feature = "reth-libmdbx"))]
use crate::composite_db::CompositeDatabase;

/// Key type for the proposer's last-proposed-header table.
pub type ProposerKey = u32;
/// A "payload" token sent by workers to their primary as a batch acknowledgement.
pub type PayloadToken = u8;

/// Convenience type to propagate store errors.
/// Use eyre- just YOLO these errors for now...
pub type StoreResult<T> = eyre::Result<T>;

/// The number of rounds of certificates and sub dags to save before garbage collecting them out of
/// the DB.
pub const ROUNDS_TO_KEEP: u32 = 64;

/// The datastore column family names.
const LAST_PROPOSED_CF: &str = "last_proposed";
const VOTES_CF: &str = "votes";
const CERTIFICATES_CF: &str = "certificates";
const CERTIFICATE_DIGEST_BY_ROUND_CF: &str = "certificate_digest_by_round";
const CERTIFICATE_DIGEST_BY_ORIGIN_CF: &str = "certificate_digest_by_origin";
const PROPOSED_CERTIFICATES_CF: &str = "proposed_certificates";
const PAYLOAD_CF: &str = "payload";
const NODE_BATCHES_CACHE_CF: &str = "node_batches_cache";
const OUR_NODE_BATCHES_CACHE_CF: &str = "our_node_batches_cache";
const CONSENSUS_OUTPUT_CACHE_CF: &str = "consensus_output_cache";

/// Discovery records with role and worker id in their row hashes.
const KAD_RECORD_CF: &str = "kad_record_v2";
/// Provider rows with a separately decodable ownership key.
const KAD_PROVIDER_RECORD_CF: &str = "kad_provider_record_v2";
/// Worker discovery records isolated by worker id.
const KAD_WORKER_RECORD_CF: &str = "kad_worker_record_v2";
/// Worker provider rows isolated by worker id and ownership key.
const KAD_WORKER_PROVIDER_RECORD_CF: &str = "kad_worker_provider_record_v2";

macro_rules! tables {
    ( $($table:ident;$name:expr;$hint:expr;<$K:ty, $V:ty>),*) => {
            $(
                #[doc = concat!("The `", stringify!($table), "` database table (see `tn_types::Table`).")]
                #[derive(Debug)]
                pub struct $table {}
                impl tn_types::Table for $table {
                    type Key = $K;
                    type Value = $V;

                    const NAME: &'static str = $name;
                    const HINT: tn_types::TableHint = $hint;
                }
            )*
    };
}

/// Database table type definitions used by the consensus and network stores.
pub mod tables {
    use super::{PayloadToken, ProposerKey};
    use tn_types::{
        AuthorityIdentifier, Batch, BlockHash, Certificate, ConsensusOutput, Header, HeaderDigest,
        Round, TableHint, VoteInfo, WorkerId,
    };

    tables!(
        LastProposed;crate::LAST_PROPOSED_CF;TableHint::Epoch;<ProposerKey, Header>,  // Cleared every epoch
        Votes;crate::VOTES_CF;TableHint::Epoch;<AuthorityIdentifier, VoteInfo>,  // Cleared every epoch
        Certificates;crate::CERTIFICATES_CF;TableHint::Epoch;<HeaderDigest, Certificate>,  // Cleared every epoch
        CertificateDigestByRound;crate::CERTIFICATE_DIGEST_BY_ROUND_CF;TableHint::Epoch;<(Round, AuthorityIdentifier), HeaderDigest>,  // Cleared every epoch
        CertificateDigestByOrigin;crate::CERTIFICATE_DIGEST_BY_ORIGIN_CF;TableHint::Epoch;<(AuthorityIdentifier, Round), HeaderDigest>,  // Cleared every epoch
        ProposedCertificates;crate::PROPOSED_CERTIFICATES_CF;TableHint::Epoch;<HeaderDigest, Certificate>,  // Cleared every epoch
        Payload;crate::PAYLOAD_CF;TableHint::Epoch;<(BlockHash, WorkerId), PayloadToken>,  // Cleared every epoch
        // Cache of current-epoch batches until consensus commits them. A batch is evicted once the output
        // committing it is saved to the consensus pack (`process_output`); the rest are cleared at epoch close.
        NodeBatchesCache;crate::NODE_BATCHES_CACHE_CF;TableHint::Cache;<BlockHash, Batch>,
        // Cache batches we produce until they are accepted (they will move to NodeBatchesCache once accepted).
        OurNodeBatchesCache;crate::OUR_NODE_BATCHES_CACHE_CF;TableHint::Cache;<BlockHash, Batch>,
        // Cache of verified ConsensusOutputs (header + batches) pulled during sync, keyed by number;
        // entries are removed once written to confirmed consensus output.
        ConsensusCache;crate::CONSENSUS_OUTPUT_CACHE_CF;TableHint::Cache;<u64, ConsensusOutput>,
        // These are used for network storage and separate from consensus
        KadRecords;crate::KAD_RECORD_CF;TableHint::Kad;<BlockHash, Vec<u8>>,
        KadProviderRecords;crate::KAD_PROVIDER_RECORD_CF;TableHint::Kad;<BlockHash, Vec<u8>>,
        KadWorkerRecords;crate::KAD_WORKER_RECORD_CF;TableHint::Kad;<BlockHash, Vec<u8>>,
        KadWorkerProviderRecords;crate::KAD_WORKER_PROVIDER_RECORD_CF;TableHint::Kad;<BlockHash, Vec<u8>>
    );
}

/// The node's consensus database: tndb, one shared log per environment ([`tndb::SplitDatabase`]).
pub type DatabaseType = tndb::SplitDatabase;

/// The node's consensus database as the release before tndb opened it, on MDBX: the source the
/// migration tests write.
#[cfg(all(test, feature = "reth-libmdbx"))]
fn _open_mdbx<P: AsRef<std::path::Path> + Send>(store_path: P) -> CompositeDatabase<MdbxDatabase> {
    use tn_types::Database as _;

    let store_path = store_path.as_ref();
    let epoch_db = open_mdbx_env(store_path, MdbxEnv::Epoch).expect("Cannot open database (epoch)");
    let kad_db = open_mdbx_env(store_path, MdbxEnv::Kad).expect("Cannot open database (kad)");
    let cache_db = open_mdbx_env(store_path, MdbxEnv::Cache).expect("Cannot open database (cache)");

    let db = CompositeDatabase::open(epoch_db, kad_db, cache_db);
    // Epoch tables
    db.open_table::<LastProposed>().expect("failed to open table!");
    db.open_table::<Votes>().expect("failed to open table!");
    db.open_table::<Certificates>().expect("failed to open table!");
    db.open_table::<CertificateDigestByRound>().expect("failed to open table!");
    db.open_table::<CertificateDigestByOrigin>().expect("failed to open table!");
    db.open_table::<ProposedCertificates>().expect("failed to open table!");
    db.open_table::<Payload>().expect("failed to open table!");
    // Kad tables
    db.open_table::<KadRecords>().expect("failed to open table!");
    db.open_table::<KadProviderRecords>().expect("failed to open table!");
    db.open_table::<KadWorkerRecords>().expect("failed to open table!");
    db.open_table::<KadWorkerProviderRecords>().expect("failed to open table!");
    // Cache tables
    db.open_table::<NodeBatchesCache>().expect("failed to open table!");
    db.open_table::<OurNodeBatchesCache>().expect("failed to open table!");
    db.open_table::<ConsensusCache>().expect("failed to open table!");
    db
}

/// One of the node's MDBX environments (each its own directory under the store path).
#[cfg(feature = "reth-libmdbx")]
#[derive(Clone, Copy, Debug)]
enum MdbxEnv {
    Epoch,
    Kad,
    Cache,
}

/// Open the MDBX environment `env` under `store_path` with the node's geometry (smaller in test
/// builds, so tests can run in parallel; still large enough for the long e2e restart scenarios).
#[cfg(feature = "reth-libmdbx")]
fn open_mdbx_env(store_path: &std::path::Path, env: MdbxEnv) -> eyre::Result<MdbxDatabase> {
    cfg_if::cfg_if! {
        if #[cfg(any(test, feature = "test-utils"))] {
            use crate::mdbx::database::MEGABYTE;
            const EPOCH_MAX: usize = 16 * MEGABYTE;
            const KAD_MAX: usize = 4 * MEGABYTE;
            const CACHE_MAX: usize = 32 * MEGABYTE;
            const GROWTH: usize = 4 * MEGABYTE;
            const CACHE_GROWTH: usize = 8 * MEGABYTE;
        } else {
            use crate::mdbx::database::{
                PROD_CACHE_GROWTH as CACHE_GROWTH, PROD_CACHE_MAX as CACHE_MAX,
                PROD_EPOCH_MAX as EPOCH_MAX, PROD_GROWTH as GROWTH, PROD_KAD_MAX as KAD_MAX,
            };
        }
    }
    let (dir, tables, max_size, growth) = match env {
        MdbxEnv::Epoch => ("epoch", 8, EPOCH_MAX, GROWTH),
        MdbxEnv::Kad => ("kad", 8, KAD_MAX, GROWTH),
        MdbxEnv::Cache => ("cache", 4, CACHE_MAX, CACHE_GROWTH),
    };
    MdbxDatabase::open(store_path.join(dir), tables, max_size, growth)
}

/// Copy the node's epoch and kad tables and its own uncommitted batches from MDBX (the backend
/// before tndb, under `store_path`) into `db`, the tndb one, the first time the node opens tndb.
/// Returns the rows copied. Kept for the release that switches backends only: the next one drops
/// it with the MDBX consensus backend.
///
/// The epoch tables hold the node's anti-equivocation records for the current epoch (its last
/// proposal, its votes, its proposed certificates): a node switching backends mid-epoch must not
/// forget them, or it could sign a conflicting header in a round it already voted in. That is
/// safe to skip only while the network keeps making rounds as nodes restart; after a stop-all
/// halt every node would return to the same round with no memory of it. The kad tables hold its
/// peer records. Of the caches, only `OurNodeBatchesCache` is copied: its sealed but uncommitted
/// batches have no other copy (their transactions would be lost); the others refill.
///
/// Once the copy is durable, the MDBX environments it read are deleted (MDBX's own files, and each
/// directory if nothing else is in it), so a later reset of tndb cannot bring back their state from
/// the time of the switch; then a marker file in `tndb_root` records that the migration is done.
/// Until then every open runs it again: an environment still present is copied again (MDBX is not
/// written meanwhile, so that only rewrites the same rows) and deleted, and one already deleted was
/// copied before it. A downgrade to the MDBX build after the switch starts with empty epoch tables.
#[cfg(feature = "reth-libmdbx")]
pub(crate) fn migrate_from_mdbx(
    store_path: &std::path::Path,
    tndb_root: &std::path::Path,
    db: &tndb::SplitDatabase,
) -> eyre::Result<usize> {
    use std::io::Write as _;

    let marker = tndb_root.join(tndb::MDBX_MIGRATED);
    if marker.exists() {
        // Done before. An MDBX environment here now was made by an older binary run since (a
        // downgrade): it is never read, so what that binary recorded (its votes, its last
        // proposal) is not in tndb. Say so; it is the operator's to resolve.
        for env in ["epoch", "kad", "cache"] {
            let data = store_path.join(env).join("mdbx.dat");
            if data.exists() {
                tracing::error!(target: "tn::storage", "{} was written after this node moved to tndb (by an older binary?) and is not read; anything it holds is not in the consensus DB", data.display());
            }
        }
        return Ok(0);
    }
    // An environment exists only if its data file does: opening an absent one would create it.
    let present = |dir: &str| store_path.join(dir).join("mdbx.dat").exists();
    let mut copied = 0;
    let read: Vec<&str> =
        ["epoch", "kad", "cache"].into_iter().filter(|dir| present(dir)).collect();
    if read.contains(&"epoch") {
        let src = open_mdbx_env(store_path, MdbxEnv::Epoch)?;
        copied += copy_table::<LastProposed, _>(&src, db, ROWS_PER_TXN)?;
        copied += copy_table::<Votes, _>(&src, db, ROWS_PER_TXN)?;
        copied += copy_table::<Certificates, _>(&src, db, ROWS_PER_TXN)?;
        copied += copy_table::<CertificateDigestByRound, _>(&src, db, ROWS_PER_TXN)?;
        copied += copy_table::<CertificateDigestByOrigin, _>(&src, db, ROWS_PER_TXN)?;
        copied += copy_table::<ProposedCertificates, _>(&src, db, ROWS_PER_TXN)?;
        copied += copy_table::<Payload, _>(&src, db, ROWS_PER_TXN)?;
    }
    if read.contains(&"kad") {
        let src = open_mdbx_env(store_path, MdbxEnv::Kad)?;
        copied += copy_table::<KadRecords, _>(&src, db, ROWS_PER_TXN)?;
        copied += copy_table::<KadProviderRecords, _>(&src, db, ROWS_PER_TXN)?;
        copied += copy_table::<KadWorkerRecords, _>(&src, db, ROWS_PER_TXN)?;
        copied += copy_table::<KadWorkerProviderRecords, _>(&src, db, ROWS_PER_TXN)?;
    }
    if read.contains(&"cache") {
        let src = open_mdbx_env(store_path, MdbxEnv::Cache)?;
        // A batch is up to 1 MB, and a transaction holds its rows until it commits.
        copied += copy_table::<OurNodeBatchesCache, _>(&src, db, 64)?;
    }
    db.wait_durable_blocking()?;
    // The new tndb directories' own entries too (`<store>/tndb`, and its environments in it),
    // before anything of MDBX's is deleted: a power loss must not keep the deletions but lose them.
    archive::data_file::fsync_directory(tndb_root)?;
    archive::data_file::fsync_directory(store_path)?;
    // Only now, with every row copied durable in tndb (and each environment closed).
    for dir in &read {
        remove_mdbx_env(store_path, dir)?;
    }
    if !read.is_empty() {
        archive::data_file::fsync_directory(store_path)?;
    }
    let tmp = tndb_root.join(format!("{}.tmp", tndb::MDBX_MIGRATED));
    {
        let mut file = std::fs::File::create(&tmp)?;
        writeln!(file, "copied {copied} rows from MDBX")?;
        archive::data_file::fsync_file(&file)?;
    }
    std::fs::rename(&tmp, &marker)?;
    archive::data_file::fsync_directory(tndb_root)?;
    if copied > 0 {
        tracing::info!(target: "tn::storage", "copied {copied} rows of the consensus DB from MDBX to tndb");
    }
    Ok(copied)
}

/// The files MDBX keeps in an environment's directory, in the order they are deleted: the data
/// file last, since its presence is what marks an environment still to move (and to delete).
#[cfg(feature = "reth-libmdbx")]
const MDBX_FILES: [&str; 2] = ["mdbx.lck", "mdbx.dat"];

/// Delete the MDBX environment `dir` under `store_path`, whose rows are durably in tndb: MDBX's own
/// files, then the directory if nothing else is in it. A file MDBX did not make is kept, with its
/// directory, and logged; nothing is removed recursively.
#[cfg(feature = "reth-libmdbx")]
fn remove_mdbx_env(store_path: &std::path::Path, dir: &str) -> eyre::Result<()> {
    use eyre::WrapErr as _;

    let env = store_path.join(dir);
    for file in MDBX_FILES {
        let path = env.join(file);
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e).wrap_err_with(|| format!("delete {}", path.display())),
        }
    }
    if let Err(e) = std::fs::remove_dir(&env) {
        if e.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(target: "tn::storage", "kept {} after moving to tndb: {e}", env.display());
        }
    }
    Ok(())
}

/// Rows per transaction when copying a table of small values (see [`copy_table`]).
#[cfg(feature = "reth-libmdbx")]
const ROWS_PER_TXN: usize = 10_000;

/// Copy every row of table `T` from `src` into `dst`, in transactions of at most `rows_per_txn`
/// rows (the whole copy is made durable, and marked done, by the caller). Returns the rows copied.
/// A read error, or fewer rows than MDBX counts, is an error ([`MdbxDatabase::try_for_each`]):
/// MDBX is deleted after the copy, so a copy cut short must never look complete.
#[cfg(feature = "reth-libmdbx")]
fn copy_table<T: tn_types::Table, D: tn_types::Database>(
    src: &MdbxDatabase,
    dst: &D,
    rows_per_txn: usize,
) -> eyre::Result<usize> {
    use tn_types::{Database as _, DbTxMut as _};

    src.open_table::<T>()?;
    let mut txn = dst.write_txn()?;
    let mut in_txn = 0;
    let rows = src.try_for_each::<T>(|key, value| {
        txn.insert::<T>(&key, &value)?;
        in_txn += 1;
        if in_txn == rows_per_txn {
            std::mem::replace(&mut txn, dst.write_txn()?).commit()?;
            in_txn = 0;
        }
        Ok(())
    })?;
    txn.commit()?;
    Ok(rows)
}

/// Open or reopen all the storage of the node: one tndb shared log per environment
/// ([`DatabaseType`]) under `<store_path>/tndb` (beside the consensus packs in `epochs/`), in group
/// commit (a write returns at once; `persist` waits for the sync covering it), compacting in the
/// background, with every table open. A node that ran on MDBX (the release before tndb) has its
/// epoch and kad tables and its own uncommitted batches copied over on the first open, and its
/// MDBX files deleted (`migrate_from_mdbx`).
pub fn open_db<P: AsRef<std::path::Path> + Send>(store_path: P) -> DatabaseType {
    use tn_types::Database as _;

    let store_path = store_path.as_ref();
    let root = store_path.join(tndb::TNDB_DIR);
    let options = tndb::TnDbOptions { commit: tndb::CommitMode::Group, ..Default::default() };
    let db = tndb::SplitDatabase::open_with(&root, options).expect("Cannot open database (tndb)");
    // Epoch tables
    db.open_table::<LastProposed>().expect("failed to open table!");
    db.open_table::<Votes>().expect("failed to open table!");
    db.open_table::<Certificates>().expect("failed to open table!");
    db.open_table::<CertificateDigestByRound>().expect("failed to open table!");
    db.open_table::<CertificateDigestByOrigin>().expect("failed to open table!");
    db.open_table::<ProposedCertificates>().expect("failed to open table!");
    db.open_table::<Payload>().expect("failed to open table!");
    // Kad tables
    db.open_table::<KadRecords>().expect("failed to open table!");
    db.open_table::<KadProviderRecords>().expect("failed to open table!");
    db.open_table::<KadWorkerRecords>().expect("failed to open table!");
    db.open_table::<KadWorkerProviderRecords>().expect("failed to open table!");
    // Cache tables
    db.open_table::<NodeBatchesCache>().expect("failed to open table!");
    db.open_table::<OurNodeBatchesCache>().expect("failed to open table!");
    db.open_table::<ConsensusCache>().expect("failed to open table!");
    // Bring over the anti-equivocation records, peer records and own uncommitted batches of a
    // node that ran on MDBX.
    #[cfg(feature = "reth-libmdbx")]
    migrate_from_mdbx(store_path, &root, &db).expect("Cannot migrate the consensus DB from MDBX");
    db
}

#[cfg(all(test, feature = "reth-libmdbx"))]
mod migrate_test {
    use tempfile::TempDir;
    use tn_types::{AuthorityIdentifier, Database as _, VoteDigest, VoteInfo, B256};

    use super::{
        migrate_from_mdbx,
        tables::*,
        tndb::{CommitMode, SplitDatabase, TnDbOptions, MDBX_MIGRATED, TNDB_DIR},
    };

    /// tndb opened as `open_db` opens it (its tables open), without the migration.
    fn open_split(root: &std::path::Path) -> SplitDatabase {
        let options = TnDbOptions { commit: CommitMode::Group, ..Default::default() };
        let db = SplitDatabase::open_with(root, options).expect("open tndb");
        db.open_table::<Votes>().expect("open table");
        db.open_table::<Payload>().expect("open table");
        db.open_table::<KadRecords>().expect("open table");
        db.open_table::<NodeBatchesCache>().expect("open table");
        db.open_table::<OurNodeBatchesCache>().expect("open table");
        db
    }

    /// A node switching from MDBX to tndb keeps its epoch tables (its votes: anti-equivocation
    /// records), its kad tables and its own uncommitted batches (no other copy), but not the
    /// other caches (they refill); the copy runs once.
    #[test]
    fn test_migrate_from_mdbx_keeps_epoch_and_kad_tables() {
        let tmp = TempDir::with_prefix("migrate_mdbx").expect("temp dir");
        let store = tmp.path();
        let authority = AuthorityIdentifier::dummy_for_test(7);
        let vote = VoteInfo { epoch: 3, round: 41, vote_digest: VoteDigest::default() };
        let peer = B256::repeat_byte(9);
        {
            let mdbx = crate::_open_mdbx(store);
            mdbx.insert::<Votes>(&authority, &vote).expect("vote");
            mdbx.insert::<Payload>(&(B256::repeat_byte(1), 0), &1).expect("payload");
            mdbx.insert::<KadRecords>(&peer, &vec![1, 2, 3]).expect("kad");
            mdbx.insert::<NodeBatchesCache>(&B256::repeat_byte(2), &Default::default())
                .expect("batch");
            mdbx.insert::<OurNodeBatchesCache>(&B256::repeat_byte(3), &Default::default())
                .expect("our batch");
            mdbx.sync_persist();
        }
        // The consensus packs live beside the MDBX environments (`epochs/` next to `epoch/`).
        let pack = store.join("epochs").join("epoch-1").join("data");
        std::fs::create_dir_all(pack.parent().expect("a parent")).expect("mkdir");
        std::fs::write(&pack, b"pack").expect("write");
        let root = store.join(TNDB_DIR);
        let db = open_split(&root);
        assert_eq!(migrate_from_mdbx(store, &root, &db).expect("migrate"), 4, "rows copied");
        for env in ["epoch", "kad", "cache"] {
            assert!(!store.join(env).exists(), "the MDBX {env} environment is deleted");
        }
        assert!(pack.exists(), "the packs beside it are not");
        assert!(
            db.get::<OurNodeBatchesCache>(&B256::repeat_byte(3)).expect("get").is_some(),
            "our own uncommitted batch is carried over"
        );
        assert_eq!(db.get::<Votes>(&authority).expect("get"), Some(vote.clone()));
        assert_eq!(db.get::<Payload>(&(B256::repeat_byte(1), 0)).expect("get"), Some(1));
        assert_eq!(db.get::<KadRecords>(&peer).expect("get"), Some(vec![1, 2, 3]));
        assert!(db.is_empty::<NodeBatchesCache>(), "the peers' batch cache starts empty");
        assert!(root.join(MDBX_MIGRATED).exists(), "the marker");
        drop(db);

        // Once done, it never runs again: MDBX recreated after the switch (as by a downgrade) is
        // not read.
        {
            let mdbx = crate::_open_mdbx(store);
            mdbx.insert::<Votes>(&AuthorityIdentifier::dummy_for_test(8), &vote).expect("vote");
            mdbx.sync_persist();
        }
        let db = open_split(&root);
        assert_eq!(migrate_from_mdbx(store, &root, &db).expect("migrate"), 0, "done once");
        assert_eq!(db.get::<Votes>(&AuthorityIdentifier::dummy_for_test(8)).expect("get"), None);
        assert_eq!(db.get::<Votes>(&authority).expect("get"), Some(vote), "kept across a reopen");
    }

    /// A file in an MDBX environment's directory that is not MDBX's keeps the directory (only
    /// MDBX's files are deleted); a rerun with the marker gone finds nothing left to copy.
    #[test]
    fn test_migrate_from_mdbx_deletes_only_mdbx_files() {
        let tmp = TempDir::with_prefix("migrate_mdbx_files").expect("temp dir");
        let store = tmp.path();
        {
            let mdbx = crate::_open_mdbx(store);
            let vote = VoteInfo { epoch: 1, round: 1, vote_digest: VoteDigest::default() };
            mdbx.insert::<Votes>(&AuthorityIdentifier::dummy_for_test(7), &vote).expect("vote");
            mdbx.sync_persist();
        }
        let stray = store.join("kad").join("notes.txt");
        std::fs::write(&stray, b"an operator's file").expect("write");
        let root = store.join(TNDB_DIR);
        let db = open_split(&root);
        assert_eq!(migrate_from_mdbx(store, &root, &db).expect("migrate"), 1, "rows copied");
        assert!(stray.exists(), "a file MDBX did not make is kept");
        assert!(!store.join("kad").join("mdbx.dat").exists(), "MDBX's own files are deleted");
        assert!(!store.join("epoch").exists());
        std::fs::remove_file(root.join(MDBX_MIGRATED)).expect("remove the marker");
        assert_eq!(migrate_from_mdbx(store, &root, &db).expect("rerun"), 0, "nothing left to copy");
        assert!(root.join(MDBX_MIGRATED).exists(), "the marker, written again");
    }

    /// A read error part way through a table fails the move, before anything of MDBX's is deleted
    /// or the move is marked done: the next open copies again, from the intact MDBX.
    #[test]
    fn test_migrate_from_mdbx_keeps_mdbx_when_a_table_read_fails() {
        let tmp = TempDir::with_prefix("migrate_mdbx_read_error").expect("temp dir");
        let store = tmp.path();
        let vote = VoteInfo { epoch: 2, round: 9, vote_digest: VoteDigest::default() };
        {
            let mdbx = crate::_open_mdbx(store);
            for i in 0..10 {
                mdbx.insert::<Votes>(&AuthorityIdentifier::dummy_for_test(i), &vote).expect("vote");
            }
            mdbx.sync_persist();
        }
        let root = store.join(TNDB_DIR);
        let db = open_split(&root);
        // `LastProposed` is copied first (empty), then `Votes`: fail its walk after 4 rows.
        crate::mdbx::database::fail_next_walk_after(4);
        let err = migrate_from_mdbx(store, &root, &db).expect_err("a read error fails the move");
        assert!(err.to_string().contains("injected cursor error"), "{err}");
        assert!(store.join("epoch").join("mdbx.dat").exists(), "MDBX is kept");
        assert!(!root.join(MDBX_MIGRATED).exists(), "and the move is not marked done");
        assert_eq!(migrate_from_mdbx(store, &root, &db).expect("retry"), 10, "a retry copies all");
        assert!(!store.join("epoch").exists(), "then MDBX is deleted");
    }

    /// A node with no MDBX data (a fresh node) copies nothing, and records that it is done.
    #[test]
    fn test_migrate_from_mdbx_on_a_fresh_node() {
        let tmp = TempDir::with_prefix("migrate_fresh").expect("temp dir");
        let root = tmp.path().join(TNDB_DIR);
        let db = open_split(&root);
        assert_eq!(migrate_from_mdbx(tmp.path(), &root, &db).expect("migrate"), 0);
        assert!(root.join(MDBX_MIGRATED).exists(), "the marker");
        assert!(!tmp.path().join("epoch").exists(), "no MDBX environment created");
    }
}

#[cfg(test)]
mod test {
    use tn_types::{Database, DbTxMut};

    #[derive(Debug)]
    pub(crate) struct TestTable {}
    impl tn_types::Table for TestTable {
        type Key = u64;
        type Value = String;

        const NAME: &'static str = "TestTable";
        const HINT: tn_types::TableHint = tn_types::TableHint::Cache;
    }

    /// Runs a simple bench/test for the provided DB.  Can use it for larger dataset tests as well
    /// as comparing backends. For example run ```cargo test dbsimpbench -- --nocapture
    /// --test-threads 1``` to run each backend through the bench one at a time.
    pub(crate) fn db_simp_bench<DB: Database>(db: DB, name: &str) {
        use tn_types::{DbTx, DbTxMut};

        println!("\nDBBENCH [{name}] starting simpdbbench");
        let max = 50_000;

        let total = std::time::Instant::now();
        let start = std::time::Instant::now();
        let mut txn = db.write_txn().unwrap();
        for (key, value) in (0..max).map(|i| (i, i.to_string())) {
            txn.insert::<TestTable>(&key, &value).unwrap();
        }
        println!("DBBENCH [{name}] insert {max}: {}", start.elapsed().as_secs_f64());
        let startc = std::time::Instant::now();
        txn.commit().unwrap();
        println!(
            "DBBENCH [{name}] commit {max}: {}, total insert/commit: {}",
            startc.elapsed().as_secs_f64(),
            start.elapsed().as_secs_f64()
        );

        // `commit()` on a non-full-memory layered DB only queues the commit to the
        // background writer, so wait for that write path to drain before iterating.
        // Without this barrier the chained disk+mem iterator below can briefly observe
        // a key in both layers and fail the ordering assertions.  A single barrier
        // covers all the reads below since no writes occur until the clear_table.
        // Deliberately untimed: the commit number above measures the (async) commit
        // call itself, not durability.
        db.sync_persist();

        let start = std::time::Instant::now();
        let mut i = 0;
        #[allow(clippy::explicit_counter_loop)]
        for (k, v) in db.iter::<TestTable>() {
            assert_eq!(k, i);
            assert_eq!(v, i.to_string());
            i += 1;
        }
        println!("DBBENCH [{name}] iterate {max}: {}", start.elapsed().as_secs_f64());

        let start = std::time::Instant::now();
        let mut i = max;
        for (k, v) in db.reverse_iter::<TestTable>() {
            i -= 1;
            assert_eq!(k, i);
            assert_eq!(v, i.to_string());
        }
        println!("DBBENCH [{name}] iterate reverse {max}: {}", start.elapsed().as_secs_f64());

        let start = std::time::Instant::now();
        for (key, value) in (0..max).rev().map(|i| (i, i.to_string())) {
            let val = db.get::<TestTable>(&key).unwrap().unwrap();
            assert_eq!(value, val);
        }
        println!("DBBENCH [{name}] loop reverse, no txn {max}: {}", start.elapsed().as_secs_f64());

        let start = std::time::Instant::now();
        let txn = db.read_txn().unwrap();
        for (key, value) in (0..max).rev().map(|i| (i, i.to_string())) {
            let val = txn.get::<TestTable>(&key).unwrap().unwrap();
            assert_eq!(value, val);
        }
        drop(txn);
        println!("DBBENCH [{name}] loop reverse, {max}: {}", start.elapsed().as_secs_f64());

        let start = std::time::Instant::now();
        let txn = db.read_txn().unwrap();
        for (key, value) in (0..(max / 2)).map(|i| (i, i.to_string())) {
            let key2 = max - key - 1;
            let value2 = key2.to_string();
            let val = txn.get::<TestTable>(&key).unwrap().unwrap();
            assert_eq!(value, val);
            let val = txn.get::<TestTable>(&key2).unwrap().unwrap();
            assert_eq!(value2, val);
        }
        drop(txn);
        println!("DBBENCH [{name}] loop two way, {max}: {}", start.elapsed().as_secs_f64());

        let start = std::time::Instant::now();
        let mut txn = db.write_txn().unwrap();
        txn.clear_table::<TestTable>().unwrap();
        txn.commit().unwrap();
        println!("DBBENCH [{name}] clear_table {max}: {}", start.elapsed().as_secs_f64());

        let start = std::time::Instant::now();
        let mut txn = db.write_txn().unwrap();
        for (key, value) in (0..max).map(|i| (i, i.to_string())) {
            txn.insert::<TestTable>(&key, &value).unwrap();
        }
        txn.commit().unwrap();
        println!("DBBENCH [{name}] insert post clear {max}: {}", start.elapsed().as_secs_f64());

        println!("DBBENCH [{name}] Total pre drop: {}", total.elapsed().as_secs_f64());
        let start = std::time::Instant::now();
        drop(db);
        println!("DBBENCH [{name}] drop DB: {}", start.elapsed().as_secs_f64());
        println!("DBBENCH [{name}] Total Runtime: {}", total.elapsed().as_secs_f64());
    }

    pub(crate) fn test_contains_key<DB: Database>(db: DB) {
        db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");
        assert!(db.contains_key::<TestTable>(&123456789).expect("Failed to call contains key"));
        assert!(!db.contains_key::<TestTable>(&000000000).expect("Failed to call contains key"));
    }

    pub(crate) fn test_get<DB: Database>(db: DB) {
        db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");
        assert_eq!(
            Some("123456789".to_string()),
            db.get::<TestTable>(&123456789).expect("Failed to get")
        );
        assert_eq!(None, db.get::<TestTable>(&000000000).expect("Failed to get"));
    }

    pub(crate) fn test_multi_get<DB: Database>(db: DB) {
        db.insert::<TestTable>(&123, &"123".to_string()).expect("Failed to insert");
        db.insert::<TestTable>(&456, &"456".to_string()).expect("Failed to insert");

        let result = db.multi_get::<TestTable>([&123, &456, &789]).expect("Failed to multi get");

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], Some("123".to_string()));
        assert_eq!(result[1], Some("456".to_string()));
        assert_eq!(result[2], None);
    }

    pub(crate) fn test_skip<DB: Database>(db: DB) {
        db.insert::<TestTable>(&123, &"123".to_string()).expect("Failed to insert");
        db.insert::<TestTable>(&456, &"456".to_string()).expect("Failed to insert");
        db.insert::<TestTable>(&789, &"789".to_string()).expect("Failed to insert");
        db.sync_persist(); // Either a no-op or a chance for write ops to catch up.

        // Skip all smaller
        let key_vals: Vec<_> = db.skip_to::<TestTable>(&456).expect("Seek failed").collect();
        assert_eq!(key_vals.len(), 2);
        assert_eq!(key_vals[0], (456, "456".to_string()));
        assert_eq!(key_vals[1], (789, "789".to_string()));

        // Skip to the end
        assert_eq!(db.skip_to::<TestTable>(&999).expect("Seek failed").count(), 0);

        // Skip to last
        assert_eq!(db.last_record::<TestTable>(), Some((789, "789".to_string())));

        // Skip to successor of first value
        assert_eq!(db.skip_to::<TestTable>(&000).expect("Skip failed").count(), 3);
    }

    pub(crate) fn test_skip_to_previous_simple<DB: Database>(db: DB) {
        let mut txn = db.write_txn().unwrap();
        txn.insert::<TestTable>(&123, &"123".to_string()).expect("Failed to insert");
        txn.insert::<TestTable>(&456, &"456".to_string()).expect("Failed to insert");
        txn.insert::<TestTable>(&789, &"789".to_string()).expect("Failed to insert");
        txn.commit().unwrap();
        db.sync_persist(); // Either a no-op or a chance for write ops to catch up.

        // Skip to the one before the end
        let key_val = db.record_prior_to::<TestTable>(&999).expect("Seek failed");
        assert_eq!(key_val, (789, "789".to_string()));

        // Skip to prior of first value
        // Note: returns an empty iterator!
        assert!(db.record_prior_to::<TestTable>(&000).is_none());
    }

    pub(crate) fn test_iter_skip_to_previous_gap<DB: Database>(db: DB) {
        let mut txn = db.write_txn().unwrap();
        for i in 1..100 {
            if i != 50 {
                txn.insert::<TestTable>(&i, &i.to_string()).unwrap();
            }
        }
        txn.commit().unwrap();
        db.sync_persist(); // Either a no-op or a chance for write ops to catch up.

        // Skip prior to will return an iterator starting with an "unexpected" key if the sought one
        // is not in the table
        let val = db.record_prior_to::<TestTable>(&50).map(|(k, _)| k).unwrap();
        assert_eq!(49, val);
    }

    pub(crate) fn test_remove<DB: Database>(db: DB) {
        db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");
        assert!(db.get::<TestTable>(&123456789).expect("Failed to get").is_some());

        db.remove::<TestTable>(&123456789).expect("Failed to remove");
        db.sync_persist(); // Either a no-op or a chance for write ops to catch up.
        assert!(db.get::<TestTable>(&123456789).expect("Failed to get").is_none());
    }

    pub(crate) fn test_iter<DB: Database>(db: DB) {
        db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");

        // Note that inserts "show up" immediadly but there could be a race where they are
        // iterated over twice while being persisted.
        db.sync_persist(); // Either a no-op or a chance for write ops to catch up.
        let mut iter = db.iter::<TestTable>();
        assert_eq!(Some((123456789, "123456789".to_string())), iter.next());
        assert_eq!(None, iter.next());
    }

    pub(crate) fn test_iter_reverse<DB: Database>(db: DB) {
        db.insert::<TestTable>(&1, &"1".to_string()).expect("Failed to insert");
        db.insert::<TestTable>(&2, &"2".to_string()).expect("Failed to insert");
        db.insert::<TestTable>(&3, &"3".to_string()).expect("Failed to insert");
        // Note that inserts "show up" immediadly but there could be a race where they are
        // iterated over twice while being persisted.
        db.sync_persist(); // Either a no-op or a chance for write ops to catch up.
        let mut iter = db.iter::<TestTable>();

        assert_eq!(Some((1, "1".to_string())), iter.next());
        assert_eq!(Some((2, "2".to_string())), iter.next());
        assert_eq!(Some((3, "3".to_string())), iter.next());
        assert_eq!(None, iter.next());
    }

    /// Verify clearing populated and empty tables across database backends.
    pub(crate) fn test_clear<DB: Database>(db: DB) {
        // Test clear of empty map
        let _ = db.clear_table::<TestTable>();

        let mut txn = db.write_txn().unwrap();
        for (key, val) in (0..101).map(|i| (i, i.to_string())) {
            txn.insert::<TestTable>(&key, &val).expect("Failed to batch insert");
        }
        txn.commit().unwrap();
        db.sync_persist(); // Either a no-op or a chance for write ops to catch up.

        // Check we have multiple entries
        assert!(db.iter::<TestTable>().count() > 1);
        let _ = db.clear_table::<TestTable>();
        db.sync_persist(); // Either a no-op or a chance for write ops to catch up.
        assert_eq!(db.iter::<TestTable>().count(), 0);
        // Clear again to ensure safety when clearing empty map
        let _ = db.clear_table::<TestTable>();
        assert_eq!(db.iter::<TestTable>().count(), 0);
        // Clear with one item
        let _ = db.insert::<TestTable>(&1, &"e".to_string());
        // Wait for the write-behind insert before counting across memory and disk.
        db.sync_persist();
        assert_eq!(db.iter::<TestTable>().count(), 1);
        let _ = db.clear_table::<TestTable>();
        db.sync_persist(); // Either a no-op or a chance for write ops to catch up.
        assert_eq!(db.iter::<TestTable>().count(), 0);
    }

    pub(crate) fn test_is_empty<DB: Database>(db: DB) {
        // Test empty map is truly empty
        assert!(db.is_empty::<TestTable>());
        let _ = db.clear_table::<TestTable>();
        db.sync_persist(); // Either a no-op or a chance for write ops to catch up.
        assert!(db.is_empty::<TestTable>());

        let mut txn = db.write_txn().unwrap();
        for (key, val) in (0..101).map(|i| (i, i.to_string())) {
            txn.insert::<TestTable>(&key, &val).expect("Failed to batch insert");
        }
        txn.commit().unwrap();
        db.sync_persist(); // Either a no-op or a chance for write ops to catch up.

        // Check we have multiple entries and not empty
        assert!(db.iter::<TestTable>().count() > 1);
        assert!(!db.is_empty::<TestTable>());

        // Clear again to ensure empty works after clearing
        let _ = db.clear_table::<TestTable>();
        db.sync_persist(); // Either a no-op or a chance for write ops to catch up.
        assert_eq!(db.iter::<TestTable>().count(), 0);
        assert!(db.is_empty::<TestTable>());
    }

    pub(crate) fn test_multi_insert<DB: Database>(db: DB) {
        let mut txn = db.write_txn().unwrap();
        for (key, val) in (0..101).map(|i| (i, i.to_string())) {
            txn.insert::<TestTable>(&key, &val).expect("Failed to batch insert");
        }
        txn.commit().unwrap();

        for (k, v) in (0..101).map(|i| (i, i.to_string())) {
            let val = db.get::<TestTable>(&k).expect("Failed to get inserted key");
            assert_eq!(Some(v), val);
        }
    }

    pub(crate) fn test_multi_remove<DB: Database>(db: DB) {
        // Create kv pairs
        let mut txn = db.write_txn().unwrap();
        for (key, val) in (0..101).map(|i| (i, i.to_string())) {
            txn.insert::<TestTable>(&key, &val).expect("Failed to batch insert");
        }
        txn.commit().unwrap();

        // Check insertion
        for (k, v) in (0..101).map(|i| (i, i.to_string())) {
            let val = db.get::<TestTable>(&k).expect("Failed to get inserted key");
            assert_eq!(Some(v), val);
        }

        // Remove 50 items
        let mut txn = db.write_txn().unwrap();
        for (key, _val) in (0..101).map(|i| (i, i.to_string())).take(50) {
            txn.remove::<TestTable>(&key).expect("Failed to batch remove");
        }
        txn.commit().unwrap();
        db.sync_persist(); // Either a no-op or a chance for write ops to catch up.
        assert_eq!(db.iter::<TestTable>().count(), 101 - 50);

        // Check that the remaining are present
        for (k, v) in (0..101).map(|i| (i, i.to_string())).skip(50) {
            let val = db.get::<TestTable>(&k).expect("Failed to get inserted key");
            assert_eq!(Some(v), val);
        }
    }
}
