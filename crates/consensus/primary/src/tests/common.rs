// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use narwhal_storage::{CertificateStore, CertificateStoreCache, PayloadStore, VoteDigestStore};
use narwhal_typed_store::traits::Database;
use std::num::NonZeroUsize;

pub fn create_db_stores<DB: Database>(
    db: DB,
) -> (CertificateStore<DB>, PayloadStore<DB>, VoteDigestStore<DB>) {
    (
        CertificateStore::new(
            db.clone(),
            CertificateStoreCache::new(NonZeroUsize::new(100).unwrap(), None),
        ),
        PayloadStore::new(db.clone()),
        VoteDigestStore::new(db),
    )
}
