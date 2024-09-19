// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use narwhal_storage::{CertificateStore, PayloadStore, VoteDigestStore};
use narwhal_typed_store::traits::Database;

pub fn create_db_stores<DB: Database>(
    db: DB,
) -> (CertificateStore<DB>, PayloadStore<DB>, VoteDigestStore<DB>) {
    (CertificateStore::new(db.clone()), PayloadStore::new(db.clone()), VoteDigestStore::new(db))
}
