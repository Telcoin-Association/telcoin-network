// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use narwhal_storage::{CertificateStore, CertificateStoreCache, ConsensusStore};
use narwhal_typed_store::{open_db, DatabaseType};
use std::{num::NonZeroUsize, sync::Arc};

pub const NUM_SUB_DAGS_PER_SCHEDULE: u64 = 100;

pub fn make_consensus_store(store_path: &std::path::Path) -> Arc<ConsensusStore<DatabaseType>> {
    let db = open_db(store_path);
    Arc::new(ConsensusStore::new(db))
}

pub fn make_certificate_store(store_path: &std::path::Path) -> CertificateStore {
    CertificateStore::new(
        open_db(store_path),
        CertificateStoreCache::new(NonZeroUsize::new(100).unwrap(), None),
    )
}
