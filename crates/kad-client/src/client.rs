//! The cloneable handle through which callers issue lookups.

use crate::{
    driver::{Command, Driver, LookupResult},
    error::KadClientError,
    KadClientConfig,
};
use futures::StreamExt as _;
use libp2p::PeerId;
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex, PoisonError},
};
use tn_node_record::BlsPublicKey;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

/// A read-only handle to one Telcoin Network DHT.
///
/// Cheap to clone: every clone talks to the same background driver task, and the task exits once
/// every clone is dropped or [`Self::shutdown`] is called. Lookups from different clones run
/// concurrently on the one swarm.
#[derive(Clone, Debug)]
pub struct KadClient {
    /// Requests to the driver task.
    commands: mpsc::Sender<Command>,
    /// The driver task, awaited by whichever clone shuts the client down.
    task: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl KadClient {
    /// Build the swarm, dial the bootstrap peers, and wait for the first one to connect.
    ///
    /// Returns as soon as one bootstrap peer has an established connection. Fails with
    /// [`KadClientError::NoBootstrapPeerReachable`] once every bootstrap dial has failed or the
    /// configured query timeout elapses with none connected, and with
    /// [`KadClientError::InvalidBootstrapAddr`] / [`KadClientError::NoBootstrapPeers`] before
    /// anything is dialed if the config is unusable.
    ///
    /// A connection proves only that the address and port are live; whether the remote speaks
    /// this client's kademlia protocol (same chain, same role) is learned by the first lookup,
    /// which reports [`KadClientError::NoPeerAnswered`] if it does not.
    pub async fn spawn(config: KadClientConfig) -> Result<Self, KadClientError> {
        let (driver, commands) = Driver::build(&config)?;
        let (ready_tx, ready_rx) = oneshot::channel();
        let task = tokio::spawn(driver.run(ready_tx));
        match ready_rx.await {
            Ok(Ok(())) => Ok(Self { commands, task: Arc::new(Mutex::new(Some(task))) }),
            Ok(Err(error)) => {
                // the driver exits on its own after reporting failure
                let _ = task.await;
                Err(error)
            }
            // the driver panicked or was cancelled before reporting
            Err(_) => Err(KadClientError::Shutdown),
        }
    }

    /// Look up the [`NodeRecord`](crate::NodeRecord) published under `key`.
    ///
    /// Returns `Ok(None)` when the DHT answered and no record exists for the key. Every copy the
    /// lookup returns is verified against this client's `(chain_id, network_type)` domain and the
    /// requested key; the newest valid copy wins. See [`KadClientError`] for the failure modes.
    pub async fn get_node_record(&self, key: BlsPublicKey) -> LookupResult {
        let (reply, rx) = oneshot::channel();
        self.commands
            .send(Command::GetRecord { key: Box::new(key), reply })
            .await
            .map_err(|_| KadClientError::Shutdown)?;
        rx.await.map_err(|_| KadClientError::Shutdown)?
    }

    /// Look up many keys with at most `concurrency` lookups in flight at once.
    ///
    /// Results are returned in the order of `keys`. Each key resolves independently, so one
    /// failure does not affect the others.
    pub async fn get_node_records(
        &self,
        keys: &[BlsPublicKey],
        concurrency: NonZeroUsize,
    ) -> Vec<(BlsPublicKey, LookupResult)> {
        futures::stream::iter(keys.iter().copied())
            .map(|key| async move { (key, self.get_node_record(key).await) })
            .buffered(concurrency.get())
            .collect()
            .await
    }

    /// The bootstrap peers that currently have an established connection.
    ///
    /// Useful as a readiness probe or for diagnostics. An empty list after a period of inactivity
    /// is normal: idle connections are closed and re-dialed by the next lookup.
    pub async fn connected_bootstrap_peers(&self) -> Result<Vec<PeerId>, KadClientError> {
        let (reply, rx) = oneshot::channel();
        self.commands
            .send(Command::ConnectedBootstrapPeers { reply })
            .await
            .map_err(|_| KadClientError::Shutdown)?;
        rx.await.map_err(|_| KadClientError::Shutdown)
    }

    /// Stop the driver task and wait for it to exit.
    ///
    /// In-flight lookups on other clones resolve with [`KadClientError::Shutdown`]. Calling this
    /// on more than one clone is harmless: only the first waits on the task.
    pub async fn shutdown(self) {
        // a closed channel means the driver is already gone
        let _ = self.commands.send(Command::Shutdown).await;
        let task = self.task.lock().unwrap_or_else(PoisonError::into_inner).take();
        if let Some(task) = task {
            // a panicked driver has nothing further to report
            let _ = task.await;
        }
    }
}
