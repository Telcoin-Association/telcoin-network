// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    payload_store::PayloadStore, vote_digest_store::VoteDigestStore, CertificateStore,
    ConsensusStore, ProposerStore,
};
use narwhal_typed_store::traits::Database;
use std::sync::Arc;

// A type alias marking the "payload" tokens sent by workers to their primary as batch
// acknowledgements
pub use narwhal_typed_store::PayloadToken;

/// All the data stores of the node.
#[derive(Clone)]
pub struct NodeStorage<DB, PDB> {
    pub proposer_store: ProposerStore<DB>,
    pub vote_digest_store: VoteDigestStore<DB>,
    pub certificate_store: CertificateStore<DB>,
    pub payload_store: PayloadStore<DB>,
    pub batch_store: DB,
    pub consensus_store: Arc<ConsensusStore<DB>>,
    pub consensus_persist_store: Option<PDB>,
}

impl<DB: Database, PDB: Database> NodeStorage<DB, PDB> {
    /// Open or reopen all the storage of the node.
    pub fn reopen(db: DB, consensus_persist_store: Option<PDB>) -> NodeStorage<DB, PDB> {
        let proposer_store = ProposerStore::new(db.clone());
        let vote_digest_store = VoteDigestStore::new(db.clone());

        let certificate_store = CertificateStore::<DB>::new(db.clone());
        let payload_store = PayloadStore::new(db.clone());
        let batch_store = db.clone();
        let consensus_store = Arc::new(ConsensusStore::new(db));

        NodeStorage {
            proposer_store,
            vote_digest_store,
            certificate_store,
            payload_store,
            batch_store,
            consensus_store,
            consensus_persist_store,
        }
    }
}
