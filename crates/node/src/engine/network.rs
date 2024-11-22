//! Inner-node network.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use futures::StreamExt;
use reth_provider::providers::BlockchainProvider;
use tn_network::inner_node::PrimaryToEngineMessage;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

/// The type to handle requests from the Primary.
pub(super) struct PrimaryToEngineReceiver<DB> {
    /// Receiving channel from inner-node network.
    ///
    /// The primary sends messages to the engine through the inner-node network.
    /// The messages are forwarded to the engine and processed here.
    network_stream: ReceiverStream<PrimaryToEngineMessage>,
    /// Type that fetches data from the database.
    blockchain_db: BlockchainProvider<DB>,
}

impl<DB> PrimaryToEngineReceiver<DB> {
    /// Create a new instance of Self.
    pub fn new(
        network_receiver: mpsc::Receiver<PrimaryToEngineMessage>,
        blockchain_db: BlockchainProvider<DB>,
    ) -> Self {
        let network_stream = ReceiverStream::new(network_receiver);
        Self { network_stream, blockchain_db }
    }
}

impl<DB> Future for PrimaryToEngineReceiver<DB> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            match this.network_stream.poll_next_unpin(cx) {
                // request to verify execution result from peer
                Poll::Ready(Some(PrimaryToEngineMessage::VerifyExecution)) => {
                    // TODO:
                    // - check block numhash
                    // - verify header too?
                    //
                    // if number not found, get height
                    // need to wait until canonical update
                    ()
                }
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => break,
            }
        }

        Poll::Pending
    }
}
