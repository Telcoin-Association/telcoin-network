// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![warn(future_incompatible, nonstandard_style, rust_2018_idioms, rust_2021_compatibility)]

pub mod traits;

#[cfg(all(feature = "redb", not(feature = "rocksdb")))]
use redb::database::{open_redatabase, ReDB};
#[cfg(feature = "rocksdb")]
use rocks::database::RocksDatabase;
#[cfg(all(feature = "redb", not(feature = "rocksdb")))]
use tables::{
    Batches, CertificateDigestByOrigin, CertificateDigestByRound, Certificates, CommittedSubDag,
    LastCommitted, LastProposed, Payload, Votes,
};
#[cfg(all(feature = "redb", not(feature = "rocksdb")))]
pub mod redb;
#[cfg(feature = "rocksdb")]
pub mod rocks;

pub use tn_types::error::StoreError;

pub type ProposerKey = u32;
// A type alias marking the "payload" tokens sent by workers to their primary as batch
// acknowledgements
pub type PayloadToken = u8;

/// The datastore column family names.
const LAST_PROPOSED_CF: &str = "last_proposed";
const VOTES_CF: &str = "votes";
const CERTIFICATES_CF: &str = "certificates";
const CERTIFICATE_DIGEST_BY_ROUND_CF: &str = "certificate_digest_by_round";
const CERTIFICATE_DIGEST_BY_ORIGIN_CF: &str = "certificate_digest_by_origin";
const PAYLOAD_CF: &str = "payload";
const BATCHES_CF: &str = "batches";
const LAST_COMMITTED_CF: &str = "last_committed";
const COMMITTED_SUB_DAG_INDEX_CF: &str = "committed_sub_dag";

macro_rules! tables {
    ( $($table:ident;$name:expr;<$K:ty, $V:ty>),*) => {
            $(
                #[derive(Debug)]
                pub struct $table {}
                impl $crate::traits::Table for $table {
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
        AuthorityIdentifier, Batch, BatchDigest, Certificate, CertificateDigest, ConsensusCommit,
        Header, Round, SequenceNumber, VoteInfo, WorkerId,
    };

    tables!(
        LastProposed;crate::LAST_PROPOSED_CF;<ProposerKey, Header>,
        Votes;crate::VOTES_CF;<AuthorityIdentifier, VoteInfo>,
        Certificates;crate::CERTIFICATES_CF;<CertificateDigest, Certificate>,
        CertificateDigestByRound;crate::CERTIFICATE_DIGEST_BY_ROUND_CF;<(Round, AuthorityIdentifier), CertificateDigest>,
        CertificateDigestByOrigin;crate::CERTIFICATE_DIGEST_BY_ORIGIN_CF;<(AuthorityIdentifier, Round), CertificateDigest>,
        Payload;crate::PAYLOAD_CF;<(BatchDigest, WorkerId), PayloadToken>,
        Batches;crate::BATCHES_CF;<BatchDigest, Batch>,
        LastCommitted;crate::LAST_COMMITTED_CF;<AuthorityIdentifier, Round>,
        CommittedSubDag;crate::COMMITTED_SUB_DAG_INDEX_CF;<SequenceNumber, ConsensusCommit>
    );
}

#[cfg(feature = "rocksdb")]
pub type DatabaseType = RocksDatabase;
#[cfg(all(feature = "redb", not(feature = "rocksdb")))]
pub type DatabaseType = ReDB;

/// Open the configured DB with the required tables.
pub fn open_db<Path: AsRef<std::path::Path> + Send>(store_path: Path) -> DatabaseType {
    // Open the right DB based on feature flags.  The default is ReDB unless the rocksdb flag is
    // set.
    if cfg!(feature = "rocksdb") {
        open_rocks(store_path)
    } else {
        open_redb(store_path)
    }
}

// The open functions below are the way they are so we can use if cfg!... on open_db.

/// Open or reopen all the storage of the node backed by rocks DB.
#[allow(unreachable_code)] // Need this so it compiles cleanly with or either redb or rocks.
fn open_rocks<P: AsRef<std::path::Path> + Send>(_store_path: P) -> DatabaseType {
    // Note the _ on _store_path is because depending on feature flags it may not be used.
    #[cfg(feature = "rocksdb")]
    return RocksDatabase::open_db(_store_path).expect("Can not open database.");
    // If the rocksdb feature flag is not set then calling this will panic.
    panic!("Can't use rocks with the rocksdb feature!");
}

/// Open or reopen all the storage of the node backed by ReDB.
#[allow(unreachable_code)] // Need this so it compiles cleanly with or either redb or rocks.
fn open_redb<P: AsRef<std::path::Path> + Send>(_store_path: P) -> DatabaseType {
    // Note the _ on _store_path is because depending on feature flags it may not be used.
    #[cfg(all(feature = "redb", not(feature = "rocksdb")))]
    {
        let db = open_redatabase(_store_path).expect("Cannot open database");
        db.open_table::<LastProposed>().expect("failed to open table!");
        db.open_table::<Votes>().expect("failed to open table!");
        db.open_table::<Certificates>().expect("failed to open table!");
        db.open_table::<CertificateDigestByRound>().expect("failed to open table!");
        db.open_table::<CertificateDigestByOrigin>().expect("failed to open table!");
        db.open_table::<Payload>().expect("failed to open table!");
        db.open_table::<Batches>().expect("failed to open table!");
        db.open_table::<LastCommitted>().expect("failed to open table!");
        db.open_table::<CommittedSubDag>().expect("failed to open table!");
        return db;
    }
    // If the rocksdb feature flag is not set then calling this will panic.
    panic!("Can't use redb with the redb feature (OR with the rocksdb feature)!");
}
