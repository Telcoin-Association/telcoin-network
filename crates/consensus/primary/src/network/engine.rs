//! Engine communication
// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

use anemo::{async_trait, types::response::StatusCode};
use consensus_network_types::{CanonicalUpdateMessage, EngineToPrimary};
use std::time::Duration;
use tn_storage::traits::Database;
use tracing::{debug, instrument, warn};

/// Maximum duration to fetch certificates from local storage.
const FETCH_CERTIFICATES_MAX_HANDLER_TIME: Duration = Duration::from_secs(10);

struct EngineReceiverHandler<DB> {
    db: DB,
}

// TODO: anemo still uses async_trait
#[async_trait]
impl<DB: Database> EngineToPrimary for EngineReceiverHandler<DB> {
    async fn canonical_update(
        &self,
        request: anemo::Request<CanonicalUpdateMessage>,
    ) -> Result<anemo::Response<()>, anemo::rpc::Status> {
        // update watch channel so proposer has the latest tip
        todo!()
    }

    // TODO: add method to update peers for sync
}
