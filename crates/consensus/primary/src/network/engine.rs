//! Engine communication
// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

use anemo::async_trait;
use consensus_network_types::{CanonicalUpdateMessage, EngineToPrimary};
use tn_storage::traits::Database;

#[allow(dead_code)]
struct EngineReceiverHandler<DB> {
    _db: DB,
}

// TODO: anemo still uses async_trait
#[async_trait]
impl<DB: Database> EngineToPrimary for EngineReceiverHandler<DB> {
    async fn canonical_update(
        &self,
        _request: anemo::Request<CanonicalUpdateMessage>,
    ) -> Result<anemo::Response<()>, anemo::rpc::Status> {
        // update watch channel so proposer has the latest tip
        todo!()
    }

    // TODO: add method to update peers for sync
}
