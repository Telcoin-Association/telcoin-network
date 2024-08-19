// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use narwhal_storage::{CertificateStore, CertificateStoreCache, PayloadStore, VoteDigestStore};
use narwhal_typed_store::{open_db, DatabaseType};
use std::num::NonZeroUsize;

pub fn create_db_stores<P: AsRef<std::path::Path> + Send>(
    path: P,
) -> (CertificateStore, PayloadStore<DatabaseType>, VoteDigestStore<DatabaseType>) {
    let db = open_db(path);
    (
        CertificateStore::new(
            db.clone(),
            CertificateStoreCache::new(NonZeroUsize::new(100).unwrap(), None),
        ),
        PayloadStore::new(db.clone()),
        VoteDigestStore::new(db),
    )
}
