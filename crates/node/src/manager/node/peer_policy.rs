//! One process-lifetime SIGHUP reader and latest-only publication for every swarm.

use futures::TryStreamExt as _;
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};
use tn_config::{NetworkConfig, PolicyPeerLimit, PolicyWorkerCount};
use tn_network_libp2p::PeerPolicyUpdate;
use tn_types::{BlsPublicKey, BootstrapServer};
use tokio::{signal::unix::Signal, sync::watch};
use tracing::{info, warn};

/// Inputs whose authority and resource bounds cannot be changed by a reload.
pub(super) struct ReloadInputs {
    /// Existing operator-owned network configuration path.
    path: PathBuf,
    /// Genesis bootstrap fallback captured before startup overrides are applied.
    genesis: BTreeMap<BlsPublicKey, BootstrapServer>,
    /// CLI override retains precedence for the whole process lifetime.
    cli: Option<BTreeMap<BlsPublicKey, BootstrapServer>>,
    /// Every process-lifetime worker must receive the same validated revision.
    workers: PolicyWorkerCount,
    /// Startup population budget bounds endpoints in every publication.
    peer_limit: PolicyPeerLimit,
}

impl ReloadInputs {
    /// Freeze file ownership, fallback, CLI precedence and limits at startup.
    pub(super) fn new(
        path: PathBuf,
        genesis: BTreeMap<BlsPublicKey, BootstrapServer>,
        cli: Option<BTreeMap<BlsPublicKey, BootstrapServer>>,
        workers: PolicyWorkerCount,
        peer_limit: PolicyPeerLimit,
    ) -> Self {
        Self { path, genesis, cli, workers, peer_limit }
    }
}

/// Read and validate one attempt, then publish either a complete revision or retained-state fault.
async fn reload_once(
    inputs: Arc<ReloadInputs>,
    publisher: &watch::Sender<PeerPolicyUpdate>,
) -> eyre::Result<()> {
    let current = publisher.borrow().clone();
    let attempt = current
        .attempt()
        .next()
        .ok_or_else(|| eyre::eyre!("peer policy revision space exhausted"))?;
    let result = tokio::task::spawn_blocking(move || {
        NetworkConfig::read_operator_peer_policy(
            &inputs.path,
            &inputs.genesis,
            inputs.cli.as_ref(),
            inputs.workers,
            inputs.peer_limit,
        )
    })
    .await
    .map_err(|_| "reader_task")
    .and_then(|result| result.map_err(|error| error.kind()));
    let update = result.map_or_else(|reason| {
        metrics::counter!("tn_node.peer_policy_reload_total", "outcome" => "rejected", "reason" => reason).increment(1);
        warn!(target: "peer-policy", attempt = attempt.as_u64(), accepted_revision = current.revision().as_u64(), reason, "peer policy reload rejected; retaining accepted snapshot with admission fallback");
        current.rejected(attempt)
    }, |policy| {
        metrics::counter!("tn_node.peer_policy_reload_total", "outcome" => "accepted", "reason" => "valid").increment(1);
        info!(target: "peer-policy", revision = attempt.as_u64(), "peer policy reload accepted");
        PeerPolicyUpdate::accepted(attempt, policy)
    });
    publisher.send_replace(update);
    Ok(())
}

/// Process signals serially; tokio coalesces pending signals and watch retains one publication.
///
/// Dropping this future on shutdown cancels further reloads. At most one bounded blocking read
/// can remain finishing; no reconnect task is spawned by the reader.
pub(super) async fn reload(
    signals: Signal,
    inputs: ReloadInputs,
    publisher: watch::Sender<PeerPolicyUpdate>,
) -> eyre::Result<()> {
    let inputs = Arc::new(inputs);
    futures::stream::unfold(signals, |mut signals| async move {
        signals.recv().await.map(|()| (Ok::<_, eyre::Report>(()), signals))
    })
    .try_for_each(|()| reload_once(inputs.clone(), &publisher))
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tn_network_libp2p::{PolicyRevision, PolicyValidity};

    /// Every receiver retains the exact accepted snapshot on rejection and recovers together.
    #[tokio::test]
    async fn peer_policy_reader_publishes_atomic_rejection_and_recovery() -> eyre::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("network-config");
        let policy = NetworkConfig::default().operator_peer_policy(
            &BTreeMap::new(),
            None,
            2usize.into(),
            2usize.into(),
        )?;
        let accepted = PeerPolicyUpdate::accepted(PolicyRevision::default(), policy);
        let (publisher, receiver) = watch::channel(accepted.clone());
        let mut receivers = [receiver.clone(), receiver.clone(), receiver];
        let inputs = Arc::new(ReloadInputs::new(
            path.clone(),
            BTreeMap::new(),
            None,
            2usize.into(),
            2usize.into(),
        ));
        std::fs::write(&path, "bootstrap_peers: {}\ntrusted_nodes: [")?;
        reload_once(inputs.clone(), &publisher).await?;
        receivers.iter_mut().try_for_each(|receiver| -> eyre::Result<()> {
            assert!(receiver.has_changed()?);
            let update = receiver.borrow_and_update();
            assert_eq!(update.validity(), PolicyValidity::Rejected);
            assert_eq!(update.revision(), accepted.revision());
            assert_eq!(update.attempt().as_u64(), 1);
            assert!(std::ptr::eq(update.policy(), accepted.policy()));
            Ok(())
        })?;
        std::fs::write(&path, "bootstrap_peers: {}\ntrusted_nodes: {}\n")?;
        reload_once(inputs.clone(), &publisher).await?;
        receivers.iter_mut().try_for_each(|receiver| -> eyre::Result<()> {
            assert!(receiver.has_changed()?);
            let update = receiver.borrow_and_update();
            assert_eq!(update.validity(), PolicyValidity::Accepted);
            assert_eq!(update.revision().as_u64(), 2);
            assert!(update.policy().worker(0).is_some());
            assert!(update.policy().worker(1).is_some());
            assert!(std::ptr::eq(update.policy(), publisher.borrow().policy()));
            Ok(())
        })?;
        std::fs::remove_file(&path)?;
        reload_once(inputs, &publisher).await?;
        assert_eq!(publisher.borrow().validity(), PolicyValidity::Rejected);
        assert_eq!(publisher.borrow().revision().as_u64(), 2);
        assert_eq!(publisher.borrow().attempt().as_u64(), 3);
        Ok(())
    }
}
