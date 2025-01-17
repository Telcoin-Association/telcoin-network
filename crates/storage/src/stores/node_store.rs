use crate::{
    traits::Database, CertificateStore, ConsensusStore, PayloadStore, ProposerStore,
    VoteDigestStore,
};
use std::sync::Arc;

/// All the data stores of the node.
#[derive(Clone)]
pub struct NodeStorage<DB> {
    pub proposer_store: ProposerStore<DB>,
    pub vote_digest_store: VoteDigestStore<DB>,
    pub certificate_store: CertificateStore<DB>,
    pub payload_store: PayloadStore<DB>,
    pub batch_store: DB,
    pub consensus_store: Arc<ConsensusStore<DB>>,
}

impl<DB> std::fmt::Debug for NodeStorage<DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NodeStorage")
    }
}

impl<DB: Database> NodeStorage<DB> {
    /// Open or reopen all the storage of the node.
    pub fn reopen(db: DB) -> NodeStorage<DB> {
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
        }
    }
}
