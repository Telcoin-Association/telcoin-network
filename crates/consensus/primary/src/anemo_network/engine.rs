//! Engine communication.
//!
//! Implementation logic for processing inner-node messages from this primary's engine.

use anemo::async_trait;
use reth_primitives::SealedHeader;
use tn_network_types::{CanonicalUpdateMessage, EngineToPrimary};
use tokio::sync::watch;

#[allow(dead_code)]
struct EngineReceiverHandler {
    /// Watch channel with latest canonical header for proposer.
    latest: watch::Sender<SealedHeader>,
}

// TODO: anemo still uses async_trait
#[async_trait]
impl EngineToPrimary for EngineReceiverHandler {
    async fn canonical_update(
        &self,
        request: anemo::Request<CanonicalUpdateMessage>,
    ) -> Result<anemo::Response<()>, anemo::rpc::Status> {
        let canon_update = request.into_body();

        // update watch channel
        self.latest
            .send(canon_update.tip)
            .map(|_| anemo::Response::new(()))
            .map_err(|e| anemo::rpc::Status::internal(e.to_string()))
    }

    // TODO: add method to update peers for handshake
}
