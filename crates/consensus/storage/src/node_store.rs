// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    payload_store::PayloadStore, vote_digest_store::VoteDigestStore, CertificateStore,
    CertificateStoreCache, CertificateStoreCacheMetrics, ConsensusStore, ProposerStore,
};
use narwhal_typed_store::{open_db, DatabaseType};
use std::{num::NonZeroUsize, sync::Arc};

// A type alias marking the "payload" tokens sent by workers to their primary as batch
// acknowledgements
pub use narwhal_typed_store::PayloadToken;

/// All the data stores of the node.
#[derive(Clone)]
pub struct NodeStorage {
    pub proposer_store: ProposerStore<DatabaseType>,
    pub vote_digest_store: VoteDigestStore<DatabaseType>,
    pub certificate_store: CertificateStore<CertificateStoreCache>,
    pub payload_store: PayloadStore<DatabaseType>,
    pub batch_store: DatabaseType,
    pub consensus_store: Arc<ConsensusStore<DatabaseType>>,
}

impl NodeStorage {
    /// Cache size for certificate store.
    ///
    /// Reasoning: 100 nodes * 60 rounds (assuming 1 round/sec)
    /// The cache should hold data for the last ~1 minute
    /// which should be more than enough for advancing the protocol
    /// and also help other nodes advance.
    ///
    /// TODO: take into account committee size instead of having fixed 100.
    pub(crate) const CERTIFICATE_STORE_CACHE_SIZE: usize = 100 * 60;

    /// Open or reopen all the storage of the node.
    pub fn reopen<P: AsRef<std::path::Path> + Send + 'static>(
        store_path: P,
        certificate_store_cache_metrics: Option<CertificateStoreCacheMetrics>,
    ) -> Self {
        // In case the DB dir does not yet exist.
        let _ = std::fs::create_dir_all(&store_path);
        let db = open_db(store_path);

        let proposer_store = ProposerStore::new(db.clone());
        let vote_digest_store = VoteDigestStore::new(db.clone());

        let certificate_store_cache = CertificateStoreCache::new(
            NonZeroUsize::new(Self::CERTIFICATE_STORE_CACHE_SIZE).unwrap(),
            certificate_store_cache_metrics,
        );
        let certificate_store =
            CertificateStore::<CertificateStoreCache>::new(db.clone(), certificate_store_cache);
        let payload_store = PayloadStore::new(db.clone());
        let batch_store = db.clone();
        let consensus_store = Arc::new(ConsensusStore::new(db));

        Self {
            proposer_store,
            vote_digest_store,
            certificate_store,
            payload_store,
            batch_store,
            consensus_store,
        }
    }
}
