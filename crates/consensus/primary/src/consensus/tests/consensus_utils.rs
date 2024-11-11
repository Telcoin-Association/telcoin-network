// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use tn_storage::traits::Database;
use tn_storage::{CertificateStore, ConsensusStore};

pub const NUM_SUB_DAGS_PER_SCHEDULE: u64 = 100;

pub fn make_consensus_store<DB: Database>(db: DB) -> Arc<ConsensusStore<DB>> {
    Arc::new(ConsensusStore::new(db))
}

pub fn make_certificate_store<DB: Database>(db: DB) -> CertificateStore<DB> {
    CertificateStore::new(db)
}
