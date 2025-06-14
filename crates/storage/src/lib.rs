// SPDX-License-Identifier: MIT or Apache-2.0
//! Persistent storage types

#![warn(future_incompatible, nonstandard_style, rust_2018_idioms, rust_2021_compatibility)]

mod stores;
use layered_db::LayeredDatabase;
#[cfg(feature = "reth-libmdbx")]
use mdbx::MdbxDatabase;
pub use stores::*;
// Always build redb, we use it as the default for persistant consensus data.
pub use redb::database::ReDB;
use tables::{
    Batches, CertificateDigestByOrigin, CertificateDigestByRound, Certificates,
    ConsensusBlockNumbersByDigest, ConsensusBlocks, KadProviderRecords, KadRecords, LastProposed,
    Payload, Votes,
};
// Always build redb, we use it as the default for persistant consensus data.
pub mod layered_db;
#[cfg(feature = "reth-libmdbx")]
pub mod mdbx;
pub mod mem_db;
pub mod redb;

pub use tn_types::error::StoreError;

pub type ProposerKey = u32;
// A type alias marking the "payload" tokens sent by workers to their primary as batch
// acknowledgements
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
const PAYLOAD_CF: &str = "payload";
const BATCHES_CF: &str = "batches";
const CONSENSUS_BLOCK_CF: &str = "consensus_block";
const CONSENSUS_BLOCK_NUMBER_BY_DIGEST_CF: &str = "consensus_block_number_by_digest";
const KAD_RECORD_CF: &str = "kad_record";
const KAD_PROVIDER_RECORD_CF: &str = "kad_provider_record";

macro_rules! tables {
    ( $($table:ident;$name:expr;<$K:ty, $V:ty>),*) => {
            $(
                #[derive(Debug)]
                pub struct $table {}
                impl tn_types::Table for $table {
                    type Key = $K;
                    type Value = $V;

                    const NAME: &'static str = $name;
                }
            )*
    };
}

pub mod tables {
    use super::{PayloadToken, ProposerKey};
    use tn_types::{
        AuthorityIdentifier, Batch, BlockHash, Certificate, CertificateDigest, ConsensusHeader,
        Header, Round, VoteInfo, WorkerId,
    };

    tables!(
        LastProposed;crate::LAST_PROPOSED_CF;<ProposerKey, Header>,
        Votes;crate::VOTES_CF;<AuthorityIdentifier, VoteInfo>,
        Certificates;crate::CERTIFICATES_CF;<CertificateDigest, Certificate>,
        CertificateDigestByRound;crate::CERTIFICATE_DIGEST_BY_ROUND_CF;<(Round, AuthorityIdentifier), CertificateDigest>,
        CertificateDigestByOrigin;crate::CERTIFICATE_DIGEST_BY_ORIGIN_CF;<(AuthorityIdentifier, Round), CertificateDigest>,
        Payload;crate::PAYLOAD_CF;<(BlockHash, WorkerId), PayloadToken>,
        // Table is used for "normal" consensus as well as for the consensus chain.
        Batches;crate::BATCHES_CF;<BlockHash, Batch>,
        // These tables are for the consensus chain not the normal consensus.
        ConsensusBlocks;crate::CONSENSUS_BLOCK_CF;<u64, ConsensusHeader>,
        ConsensusBlockNumbersByDigest;crate::CONSENSUS_BLOCK_NUMBER_BY_DIGEST_CF;<BlockHash, u64>,
        // These are used for network storage and separate from consensus
        KadRecords;crate::KAD_RECORD_CF;<BlockHash, Vec<u8>>,
        KadProviderRecords;crate::KAD_PROVIDER_RECORD_CF;<BlockHash, Vec<u8>>
    );
}

// mdbx is  the default, if redb is set then is used (so proirity is mdbx -> redb)
#[cfg(all(feature = "reth-libmdbx", not(feature = "redb")))]
pub type DatabaseType = LayeredDatabase<MdbxDatabase>;
#[cfg(feature = "redb")]
pub type DatabaseType = LayeredDatabase<ReDB>;

/// Open the configured DB with the required tables.
/// This will return a concrete type for the currently configured Database.
#[allow(unreachable_code)] // Need this so it compiles cleanly with redb.
pub fn open_db<Path: AsRef<std::path::Path> + Send>(store_path: Path) -> DatabaseType {
    // Open the right DB based on feature flags.  The default is MDBX unless the redb flag is
    // set.
    #[cfg(all(feature = "reth-libmdbx", not(feature = "redb")))]
    return _open_mdbx(store_path);
    #[cfg(feature = "redb")]
    return _open_redb(store_path);
    panic!("No DB configured!")
}

/// Open the configured network DB with the required tables.
/// This will return a concrete type for the currently configured Database.
#[allow(unreachable_code)] // Need this so it compiles cleanly with redb.
pub fn open_network_db<Path: AsRef<std::path::Path> + Send>(store_path: Path) -> DatabaseType {
    // Open the right DB based on feature flags.  The default is MDBX unless the redb flag is
    // set.
    #[cfg(all(feature = "reth-libmdbx", not(feature = "redb")))]
    return _open_network_mdbx(store_path);
    #[cfg(feature = "redb")]
    return _open_network_redb(store_path);
    panic!("No DB configured!")
}

// The open functions below are the way they are so we can use if cfg!... on open_db.

/// Open or reopen all the storage of the node backed by MDBX.
#[cfg(feature = "reth-libmdbx")]
fn _open_mdbx<P: AsRef<std::path::Path> + Send>(store_path: P) -> LayeredDatabase<MdbxDatabase> {
    let db = MdbxDatabase::open(store_path).expect("Cannot open database");
    db.open_table::<LastProposed>().expect("failed to open table!");
    db.open_table::<Votes>().expect("failed to open table!");
    db.open_table::<Certificates>().expect("failed to open table!");
    db.open_table::<CertificateDigestByRound>().expect("failed to open table!");
    db.open_table::<CertificateDigestByOrigin>().expect("failed to open table!");
    db.open_table::<Payload>().expect("failed to open table!");
    db.open_table::<Batches>().expect("failed to open table!");
    db.open_table::<ConsensusBlocks>().expect("failed to open table!");
    db.open_table::<ConsensusBlockNumbersByDigest>().expect("failed to open table!");

    let db = LayeredDatabase::open(db);
    db.open_table::<LastProposed>();
    db.open_table::<Votes>();
    db.open_table::<Certificates>();
    db.open_table::<CertificateDigestByRound>();
    db.open_table::<CertificateDigestByOrigin>();
    db.open_table::<Payload>();
    db.open_table::<Batches>();
    db.open_table::<ConsensusBlocks>();
    db.open_table::<ConsensusBlockNumbersByDigest>();
    db
}

/// Open or reopen all the storage of the node backed by ReDB.
#[cfg(feature = "redb")]
fn _open_redb<P: AsRef<std::path::Path> + Send>(store_path: P) -> LayeredDatabase<ReDB> {
    let db = ReDB::open(store_path).expect("Cannot open database");
    db.open_table::<LastProposed>().expect("failed to open table!");
    db.open_table::<Votes>().expect("failed to open table!");
    db.open_table::<Certificates>().expect("failed to open table!");
    db.open_table::<CertificateDigestByRound>().expect("failed to open table!");
    db.open_table::<CertificateDigestByOrigin>().expect("failed to open table!");
    db.open_table::<Payload>().expect("failed to open table!");
    db.open_table::<Batches>().expect("failed to open table!");
    db.open_table::<ConsensusBlocks>().expect("failed to open table!");
    db.open_table::<ConsensusBlockNumbersByDigest>().expect("failed to open table!");

    let db = LayeredDatabase::open(db);
    db.open_table::<LastProposed>();
    db.open_table::<Votes>();
    db.open_table::<Certificates>();
    db.open_table::<CertificateDigestByRound>();
    db.open_table::<CertificateDigestByOrigin>();
    db.open_table::<Payload>();
    db.open_table::<Batches>();
    db.open_table::<ConsensusBlocks>();
    db.open_table::<ConsensusBlockNumbersByDigest>();
    db
}

/// Open or reopen all the storage of the node backed by MDBX.
#[cfg(feature = "reth-libmdbx")]
fn _open_network_mdbx<P: AsRef<std::path::Path> + Send>(
    store_path: P,
) -> LayeredDatabase<MdbxDatabase> {
    let db = MdbxDatabase::open(store_path).expect("Cannot open database");
    db.open_table::<KadRecords>().expect("failed to open table!");
    db.open_table::<KadProviderRecords>().expect("failed to open table!");

    let db = LayeredDatabase::open(db);
    db.open_table::<KadRecords>();
    db.open_table::<KadProviderRecords>();
    db
}

/// Open or reopen all the storage of the node backed by ReDB.
#[cfg(feature = "redb")]
fn _open_network_redb<P: AsRef<std::path::Path> + Send>(store_path: P) -> LayeredDatabase<ReDB> {
    let db = ReDB::open(store_path).expect("Cannot open database");
    db.open_table::<KadRecords>().expect("failed to open kad records table!");
    db.open_table::<KadProviderRecords>().expect("failed to open kad provider table!");

    let db = LayeredDatabase::open(db);
    db.open_table::<KadRecords>();
    db.open_table::<KadProviderRecords>();
    db
}

#[cfg(test)]
mod test {
    use tn_types::{Database, DbTxMut};

    #[derive(Debug)]
    pub struct TestTable {}
    impl tn_types::Table for TestTable {
        type Key = u64;
        type Value = String;

        const NAME: &'static str = "TestTable";
    }

    /// Runs a simple bench/test for the provided DB.  Can use it for larger dataset tests as well
    /// as comparing backends. For example run ```cargo test dbsimpbench --features redb --
    /// --nocapture --test-threads 1``` to run each backend through the bench one at a time.
    pub fn db_simp_bench<DB: Database>(db: DB, name: &str) {
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

    pub fn test_contains_key<DB: Database>(db: DB) {
        db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");
        assert!(db.contains_key::<TestTable>(&123456789).expect("Failed to call contains key"));
        assert!(!db.contains_key::<TestTable>(&000000000).expect("Failed to call contains key"));
    }

    pub fn test_get<DB: Database>(db: DB) {
        db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");
        assert_eq!(
            Some("123456789".to_string()),
            db.get::<TestTable>(&123456789).expect("Failed to get")
        );
        assert_eq!(None, db.get::<TestTable>(&000000000).expect("Failed to get"));
    }

    pub fn test_multi_get<DB: Database>(db: DB) {
        db.insert::<TestTable>(&123, &"123".to_string()).expect("Failed to insert");
        db.insert::<TestTable>(&456, &"456".to_string()).expect("Failed to insert");

        let result = db.multi_get::<TestTable>([&123, &456, &789]).expect("Failed to multi get");

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], Some("123".to_string()));
        assert_eq!(result[1], Some("456".to_string()));
        assert_eq!(result[2], None);
    }

    pub fn test_skip<DB: Database>(db: DB) {
        db.insert::<TestTable>(&123, &"123".to_string()).expect("Failed to insert");
        db.insert::<TestTable>(&456, &"456".to_string()).expect("Failed to insert");
        db.insert::<TestTable>(&789, &"789".to_string()).expect("Failed to insert");

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

    pub fn test_skip_to_previous_simple<DB: Database>(db: DB) {
        let mut txn = db.write_txn().unwrap();
        txn.insert::<TestTable>(&123, &"123".to_string()).expect("Failed to insert");
        txn.insert::<TestTable>(&456, &"456".to_string()).expect("Failed to insert");
        txn.insert::<TestTable>(&789, &"789".to_string()).expect("Failed to insert");
        txn.commit().unwrap();

        // Skip to the one before the end
        let key_val = db.record_prior_to::<TestTable>(&999).expect("Seek failed");
        assert_eq!(key_val, (789, "789".to_string()));

        // Skip to prior of first value
        // Note: returns an empty iterator!
        assert!(db.record_prior_to::<TestTable>(&000).is_none());
    }

    pub fn test_iter_skip_to_previous_gap<DB: Database>(db: DB) {
        let mut txn = db.write_txn().unwrap();
        for i in 1..100 {
            if i != 50 {
                txn.insert::<TestTable>(&i, &i.to_string()).unwrap();
            }
        }
        txn.commit().unwrap();

        // Skip prior to will return an iterator starting with an "unexpected" key if the sought one
        // is not in the table
        let val = db.record_prior_to::<TestTable>(&50).map(|(k, _)| k).unwrap();
        assert_eq!(49, val);
    }

    pub fn test_remove<DB: Database>(db: DB) {
        db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");
        assert!(db.get::<TestTable>(&123456789).expect("Failed to get").is_some());

        db.remove::<TestTable>(&123456789).expect("Failed to remove");
        assert!(db.get::<TestTable>(&123456789).expect("Failed to get").is_none());
    }

    pub fn test_iter<DB: Database>(db: DB) {
        db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");

        let mut iter = db.iter::<TestTable>();
        assert_eq!(Some((123456789, "123456789".to_string())), iter.next());
        assert_eq!(None, iter.next());
    }

    pub fn test_iter_reverse<DB: Database>(db: DB) {
        db.insert::<TestTable>(&1, &"1".to_string()).expect("Failed to insert");
        db.insert::<TestTable>(&2, &"2".to_string()).expect("Failed to insert");
        db.insert::<TestTable>(&3, &"3".to_string()).expect("Failed to insert");
        let mut iter = db.iter::<TestTable>();

        assert_eq!(Some((1, "1".to_string())), iter.next());
        assert_eq!(Some((2, "2".to_string())), iter.next());
        assert_eq!(Some((3, "3".to_string())), iter.next());
        assert_eq!(None, iter.next());
    }

    pub fn test_clear<DB: Database>(db: DB) {
        // Test clear of empty map
        let _ = db.clear_table::<TestTable>();

        let mut txn = db.write_txn().unwrap();
        for (key, val) in (0..101).map(|i| (i, i.to_string())) {
            txn.insert::<TestTable>(&key, &val).expect("Failed to batch insert");
        }
        txn.commit().unwrap();

        // Check we have multiple entries
        assert!(db.iter::<TestTable>().count() > 1);
        let _ = db.clear_table::<TestTable>();
        assert_eq!(db.iter::<TestTable>().count(), 0);
        // Clear again to ensure safety when clearing empty map
        let _ = db.clear_table::<TestTable>();
        assert_eq!(db.iter::<TestTable>().count(), 0);
        // Clear with one item
        let _ = db.insert::<TestTable>(&1, &"e".to_string());
        assert_eq!(db.iter::<TestTable>().count(), 1);
        let _ = db.clear_table::<TestTable>();
        assert_eq!(db.iter::<TestTable>().count(), 0);
    }

    pub fn test_is_empty<DB: Database>(db: DB) {
        // Test empty map is truly empty
        assert!(db.is_empty::<TestTable>());
        let _ = db.clear_table::<TestTable>();
        assert!(db.is_empty::<TestTable>());

        let mut txn = db.write_txn().unwrap();
        for (key, val) in (0..101).map(|i| (i, i.to_string())) {
            txn.insert::<TestTable>(&key, &val).expect("Failed to batch insert");
        }
        txn.commit().unwrap();

        // Check we have multiple entries and not empty
        assert!(db.iter::<TestTable>().count() > 1);
        assert!(!db.is_empty::<TestTable>());

        // Clear again to ensure empty works after clearing
        let _ = db.clear_table::<TestTable>();
        assert_eq!(db.iter::<TestTable>().count(), 0);
        assert!(db.is_empty::<TestTable>());
    }

    pub fn test_multi_insert<DB: Database>(db: DB) {
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

    pub fn test_multi_remove<DB: Database>(db: DB) {
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
        assert_eq!(db.iter::<TestTable>().count(), 101 - 50);

        // Check that the remaining are present
        for (k, v) in (0..101).map(|i| (i, i.to_string())).skip(50) {
            let val = db.get::<TestTable>(&k).expect("Failed to get inserted key");
            assert_eq!(Some(v), val);
        }
    }
}
