// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Primary Receiver Handler is the entrypoint for peer network requests.
//!
//! This module includes implementations for when the primary receives network
//! requests from it's own workers and other primaries.

use anemo::async_trait;
use narwhal_network_types::{WorkerOthersBlockMessage, WorkerOwnBlockMessage, WorkerToPrimary};
use tn_storage::traits::Database;
use tn_types::TnSender;
use tokio::sync::oneshot;

use crate::proposer::OurDigestMessage;

use super::WorkerReceiverHandler;

// TODO: anemo still uses async_trait
#[async_trait]
impl<DB: Database> WorkerToPrimary for WorkerReceiverHandler<DB> {
    async fn report_own_block(
        &self,
        request: anemo::Request<WorkerOwnBlockMessage>,
    ) -> Result<anemo::Response<()>, anemo::rpc::Status> {
        let message = request.into_body();

        let (tx_ack, rx_ack) = oneshot::channel();
        let response = self
            .consensus_bus
            .our_digests()
            .send(OurDigestMessage {
                digest: message.digest,
                worker_id: message.worker_id,
                timestamp: message.worker_block.created_at(),
                ack_channel: tx_ack,
            })
            .await
            .map(|_| anemo::Response::new(()))
            .map_err(|e| anemo::rpc::Status::internal(e.to_string()))?;

        // If we are ok, then wait for the ack
        rx_ack.await.map_err(|e| anemo::rpc::Status::internal(e.to_string()))?;

        Ok(response)
    }

    async fn report_others_block(
        &self,
        request: anemo::Request<WorkerOthersBlockMessage>,
    ) -> Result<anemo::Response<()>, anemo::rpc::Status> {
        let message = request.into_body();
        self.payload_store
            .write(&message.digest, &message.worker_id)
            .map_err(|e| anemo::rpc::Status::internal(e.to_string()))?;
        Ok(anemo::Response::new(()))
    }
}
