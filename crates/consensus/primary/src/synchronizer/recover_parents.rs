//! Synchronizer to recover parent certificates for the proposer.

use crate::ConsensusBus;
use std::sync::Arc;
use tn_storage::CertificateStore;

#[derive(Clone)]
pub struct SyncParents<DB> {
    inner: Arc<Inner<DB>>,
}

struct Inner<DB> {
    /// Certificate storage.
    certificate_store: CertificateStore<DB>,
    /// The bus for inner-process communication.
    consensus_bus: ConsensusBus,
}
