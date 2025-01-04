// Copyright (c) Telcoin, LLC
//! Network contains logic for type that sends and receives messages between other primaries.

use tn_network_libp2p::{
    types::{NetworkEvent, NetworkHandle},
    ConsensusNetwork, PrimaryRequest, PrimaryResponse,
};
use tokio::sync::mpsc;

/// Convenience type for Primary network.
type Req = PrimaryRequest;
/// Convenience type for Primary network.
type Res = PrimaryResponse;

/// Handle inter-node communication between primaries.
pub struct PrimaryNetwork {
    /// Receiver for network events.
    network_events: mpsc::Receiver<NetworkEvent<Req, Res>>,
    /// Network handle to send commands.
    network_handle: NetworkHandle<Req, Res>,
}

impl PrimaryNetwork {
    /// Create a new instance of Self.
    pub fn new(
        network_events: mpsc::Receiver<NetworkEvent<Req, Res>>,
        network_handle: NetworkHandle<Req, Res>,
    ) -> Self {
        Self { network_events, network_handle }
    }

    /// Run the network.
    async fn spawn(mut self) {
        tokio::select! {
            event = self.network_events.recv() => {
                match event {
                    Some(e) => match e {
                        NetworkEvent::Request { request, channel } => {
                            // match request and send to actor
                            // - mpsc::Sender<_>::send(request, oneshot);
                            // let res = oneshot.await?;
                            // self.network_handle.send_response(res, channel).await?;
                            match request {
                                PrimaryRequest::NewCertificate { certificate } => todo!(),
                                PrimaryRequest::Vote { header, parents } => todo!(),
                                PrimaryRequest::MissingCertificates { inner } => todo!(),
                            }
                        }
                        NetworkEvent::Gossip(bytes) => {
                            // match gossip and send to actor
                            todo!()
                        }
                    },
                    None => todo!(),
                }
            }
        }
    }
}
