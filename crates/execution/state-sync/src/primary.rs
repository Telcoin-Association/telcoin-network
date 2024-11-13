//! The handler for receiving requests from the Primary.

use anemo::{async_trait, types::response::StatusCode};
use consensus_network_types::{PrimaryToEngine, VerifyExecutionRequest, VerifyExecutionResponse};
use std::time::Duration;

/// Maximum duration to fetch certificates from local storage.
const FETCH_CERTIFICATES_MAX_HANDLER_TIME: Duration = Duration::from_secs(10);

pub struct PrimaryReceiverHandler<DB> {
    db: DB,
}

// TODO: anemo still uses async_trait
#[async_trait]
impl<DB: Send + Sync + 'static> PrimaryToEngine for PrimaryReceiverHandler<DB> {
    async fn verify_execution(
        &self,
        request: anemo::Request<VerifyExecutionRequest>,
    ) -> Result<anemo::Response<VerifyExecutionResponse>, anemo::rpc::Status> {
        // validate execution block for primary
        //
        // - compare hash of block with its seal
        // - verify block is in db
        // - check block is within 3 (?) rounds
        todo!()
    }
}
