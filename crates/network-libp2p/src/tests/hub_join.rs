//! Live transport qualification for a cold validator joining through an open hub.

use super::*;
use crate::common::{TestWorkerRequest, TestWorkerResponse};
use std::{collections::HashSet, net::UdpSocket, time::Instant};
use tn_config::{AdmissionConfig, AdmissionMode};
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::{wait_until, CommitteeFixture};
use tn_types::TaskManager;
use tokio::task::JoinHandle;

/// Deadline for each transport qualification stage.
const STAGE_BUDGET: Duration = Duration::from_secs(30);

/// Production network specialized for the fixture's request protocol.
type JoinNetwork = ConsensusNetwork<
    TestWorkerRequest,
    TestWorkerResponse,
    MemDatabase,
    Sender<NetworkEvent<TestWorkerRequest, TestWorkerResponse>>,
>;

/// A swarm that has not started publishing its record yet.
struct JoinDraft {
    /// Production network loop.
    network: JoinNetwork,
    /// Authenticated transport identity.
    peer: PeerId,
    /// Governance identity.
    bls: BlsPublicKey,
    /// Advertised and bound loopback QUIC endpoint.
    address: Multiaddr,
    /// Consumer of direct requests and network events.
    events: Receiver<NetworkEvent<TestWorkerRequest, TestWorkerResponse>>,
}

/// A live production swarm whose tasks are cancelled on every exit path.
struct JoinNode {
    /// Command handle for observations and requests.
    handle: NetworkHandle<TestWorkerRequest, TestWorkerResponse>,
    /// Authenticated transport identity.
    peer: PeerId,
    /// Governance identity.
    bls: BlsPublicKey,
    /// Bound direct address.
    address: Multiaddr,
    /// Production event loop.
    network_task: JoinHandle<NetworkResult<()>>,
    /// Fixture responder, separate from admission and discovery.
    response_task: JoinHandle<()>,
}

impl Drop for JoinNode {
    fn drop(&mut self) {
        self.network_task.abort();
        self.response_task.abort();
    }
}

impl JoinDraft {
    /// Seed only the operator's hub mapping.
    fn bootstrap(&mut self, hub: &Self) {
        self.network
            .swarm
            .behaviour_mut()
            .peer_manager
            .add_bootstrap_peer(hub.bls, hub.network.node_record.info.clone());
    }

    /// Install the versioned boundary window sent by the production epoch owner.
    fn window(
        &mut self,
        epoch: u64,
        current: &HashSet<BlsPublicKey>,
        next: &HashSet<BlsPublicKey>,
    ) {
        self.network.swarm.behaviour_mut().peer_manager.update_committees_at(
            epoch,
            HashSet::new(),
            current.clone(),
            next.clone(),
        );
    }

    /// Listen and publish through the production network loop.
    async fn start(self) -> eyre::Result<JoinNode> {
        let Self { mut network, peer, bls, address, events } = self;
        network.swarm.listen_on(address.clone())?;
        let handle = network.network_handle();
        let response_handle = handle.clone();
        let network_task = tokio::spawn(network.run());
        let response_task = tokio::spawn(
            futures::stream::unfold(events, |mut events| async move {
                events.recv().await.map(|event| (event, events))
            })
            .for_each(move |event| {
                let handle = response_handle.clone();
                async move {
                    match event {
                        NetworkEvent::Request { request, channel, .. } => {
                            let response = match request {
                                TestWorkerRequest::MissingBatches(_) => {
                                    Some(TestWorkerResponse::MissingBatches { batches: Vec::new() })
                                }
                                TestWorkerRequest::NewBatch(_)
                                | TestWorkerRequest::PeerExchange(_) => None,
                            };
                            futures::future::join_all(
                                response
                                    .map(|response| async move {
                                        handle.send_response(response, channel).await
                                    })
                                    .into_iter(),
                            )
                            .await;
                        }
                        NetworkEvent::Gossip(_)
                        | NetworkEvent::Error(_, _)
                        | NetworkEvent::InboundStream { .. } => {}
                    }
                }
            }),
        );
        wait_until(STAGE_BUDGET, "bound QUIC listener", || async {
            handle.listeners().await.map(|listeners| !listeners.is_empty()).map_err(Into::into)
        })
        .await?;
        Ok(JoinNode { handle, peer, bls, address, network_task, response_task })
    }
}

/// Construct independent key-bound swarms with empty validator caches.
fn join_drafts(network_type: NetworkType, spawner: TaskSpawner) -> eyre::Result<Vec<JoinDraft>> {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .committee_size(NonZeroUsize::new(4).ok_or_else(|| eyre::eyre!("nonzero committee"))?)
        .number_of_workers(NonZeroUsize::new(2).ok_or_else(|| eyre::eyre!("nonzero workers"))?)
        .build();
    fixture
        .authorities()
        .enumerate()
        .map(|(index, authority)| {
            let config = authority.consensus_config();
            let mut network_config = config.network_config().clone();
            network_config.peer_config_mut().heartbeat_interval = 1;
            let keypair = match network_type {
                NetworkType::Primary => config.key_config().primary_network_keypair().clone(),
                NetworkType::Worker(id) => config.key_config().worker_network_keypair(id),
            };
            let port = UdpSocket::bind("127.0.0.1:0")?.local_addr()?.port();
            let address: Multiaddr = format!("/ip4/127.0.0.1/udp/{port}/quic-v1").parse()?;
            let (events_tx, events) = tokio::sync::mpsc::channel(64);
            let mut network = JoinNetwork::new(
                &network_config,
                events_tx,
                config.key_config().clone(),
                keypair,
                MemDatabase::default(),
                spawner.clone(),
                network_type,
                address.clone(),
                None,
            )?;
            let peer = *network.swarm.local_peer_id();
            let bls = config.key_config().primary_public_key();
            let mode = if index == 3 { AdmissionMode::Open } else { AdmissionMode::Closed };
            network.swarm.behaviour_mut().peer_manager.configure_admission(
                AdmissionConfig::new(mode, Duration::from_secs(300))
                    .with_transition_grace(Duration::from_secs(1)),
                bls,
            );
            Ok(JoinDraft { network, peer, bls, address, events })
        })
        .collect()
}

/// Require authenticated resolution and an established direct connection.
async fn direct_connection(from: &JoinNode, to: &JoinNode) -> eyre::Result<()> {
    wait_until(STAGE_BUDGET, "direct validator connection", || async {
        from.handle.connected_peers().await.map(|peers| peers.contains(&to.bls)).map_err(Into::into)
    })
    .await
}

/// Exercise real request/response after removing hub connections.
async fn direct_request(from: &JoinNode, to: &JoinNode) -> eyre::Result<()> {
    let response =
        from.handle.send_request(TestWorkerRequest::MissingBatches(Vec::new()), to.bls).await?;
    let response = tokio::time::timeout(STAGE_BUDGET, response).await???;
    assert_eq!(response.peer, to.bls);
    assert!(
        matches!(response.result, TestWorkerResponse::MissingBatches { batches } if batches.is_empty())
    );
    Ok(())
}

/// Publish a cold identity through one hub, activate it, and remove that hub.
async fn qualify_hub_join(network_type: NetworkType, delayed_hub: bool) -> eyre::Result<()> {
    let tasks = TaskManager::default();
    let mut drafts = join_drafts(network_type, tasks.get_spawner())?.into_iter();
    let mut first = drafts.next().ok_or_else(|| eyre::eyre!("first validator"))?;
    let mut second = drafts.next().ok_or_else(|| eyre::eyre!("second validator"))?;
    let mut joining = drafts.next().ok_or_else(|| eyre::eyre!("joining validator"))?;
    let mut hub = drafts.next().ok_or_else(|| eyre::eyre!("open hub"))?;
    let current = HashSet::from([first.bls, second.bls]);
    let next = HashSet::from([joining.bls]);
    first.window(7, &current, &HashSet::new());
    second.window(7, &current, &HashSet::new());
    // Only the existing committee starts with cached authenticated records.
    first.network.process_kad_put_request(second.peer, second.network.get_peer_record())?;
    second.network.process_kad_put_request(first.peer, first.network.get_peer_record())?;
    first.bootstrap(&hub);
    second.bootstrap(&hub);
    joining.bootstrap(&hub);
    hub.window(8, &current, &next);
    joining.window(8, &current, &next);
    let first = first.start().await?;
    let second = second.start().await?;
    first.handle.dial(second.peer, second.address.clone()).await?;
    direct_connection(&first, &second).await?;
    direct_request(&first, &second).await?;
    first.handle.update_committees_at(8, HashSet::new(), current.clone(), next.clone()).await?;
    second.handle.update_committees_at(8, HashSet::new(), current.clone(), next.clone()).await?;
    let cold = first.handle.admission_status().await?;
    assert_eq!(cold.resolved_window(), 2);
    assert_eq!(cold.required_window(), 3);
    assert_eq!(cold.effective(), AdmissionMode::Grace);
    let started = Instant::now();
    let joining = joining.start().await?;
    if delayed_hub {
        // Dial the actual unavailable endpoint, then observe the entire minimum Grace interval.
        let unavailable = joining.handle.dial(hub.peer, hub.address.clone()).await;
        assert!(unavailable.is_err(), "an unavailable hub must fail its transport dial");
        first.handle.find_authorities(vec![joining.bls]).await?;
        joining.handle.find_authorities(current.iter().copied().collect()).await?;
        wait_until(
            STAGE_BUDGET,
            "Grace interval elapsed while hub remains unavailable",
            || async {
                first
                    .handle
                    .admission_status()
                    .await
                    .map(|state| state.transition_remaining().is_zero())
                    .map_err(Into::into)
            },
        )
        .await?;
        // Observe the public path before any hub starts listening.
        let missing = first.handle.admission_status().await?;
        assert_eq!(missing.resolved_window(), 2);
        assert_eq!(missing.connected_current(), 1);
        assert_eq!(missing.effective(), AdmissionMode::Grace);
        assert!(!first.handle.connected_peers().await?.contains(&joining.bls));
        direct_request(&first, &second).await?;
    }
    let hub = hub.start().await?;
    futures::future::try_join_all(
        [&first, &second, &joining]
            .into_iter()
            .map(|node| node.handle.dial(hub.peer, hub.address.clone())),
    )
    .await?;
    wait_until(STAGE_BUDGET, "signed record published through open hub", || async {
        hub.handle
            .admission_status()
            .await
            .map(|state| state.resolved_window() == 3)
            .map_err(Into::into)
    })
    .await?;
    let published = started.elapsed();
    first.handle.find_authorities(vec![joining.bls]).await?;
    second.handle.find_authorities(vec![joining.bls]).await?;
    joining.handle.find_authorities(current.iter().copied().collect()).await?;
    wait_until(STAGE_BUDGET, "cold next-committee record resolution", || async {
        futures::future::try_join(
            first.handle.admission_status(),
            joining.handle.admission_status(),
        )
        .await
        .map(|(old, new)| old.resolved_window() == 3 && new.resolved_window() == 3)
        .map_err(Into::into)
    })
    .await?;
    let resolved = started.elapsed();
    direct_connection(&first, &joining).await?;
    direct_connection(&joining, &second).await?;
    let connected = started.elapsed();
    let activated = current.iter().copied().chain(next.iter().copied()).collect::<HashSet<_>>();
    futures::future::try_join_all([&first, &second, &joining].into_iter().map(|node| {
        node.handle.update_committees_at(9, current.clone(), activated.clone(), HashSet::new())
    }))
    .await?;
    wait_until(STAGE_BUDGET, "current-committee admission activation", || async {
        futures::future::try_join_all(
            [&first, &second, &joining].into_iter().map(|node| node.handle.admission_status()),
        )
        .await
        .map(|states| {
            states.iter().all(|state| {
                state.epoch() == Some(9)
                    && state.effective() == AdmissionMode::Closed
                    && state.resolved_current() == 3
                    && state.connected_current() == 2
            })
        })
        .map_err(Into::into)
    })
    .await?;
    let activated_at = started.elapsed();
    let hub_peer = hub.peer;
    drop(hub);
    futures::future::try_join_all(
        [&first, &second, &joining].into_iter().map(|node| node.handle.disconnect_peer(hub_peer)),
    )
    .await?;
    direct_request(&first, &joining).await?;
    direct_request(&joining, &second).await?;
    direct_request(&second, &first).await?;
    println!(
        "hub_join_timing role={network_type:?} delayed={delayed_hub} publication_ms={} resolution_ms={} connection_ms={} activation_ms={}",
        published.as_millis(), resolved.saturating_sub(published).as_millis(),
        connected.saturating_sub(resolved).as_millis(),
        activated_at.saturating_sub(connected).as_millis(),
    );
    Ok(())
}

/// Qualify primary publication, discovery, direct links, and activation.
#[tokio::test]
async fn hub_join_primary_cold_record() -> eyre::Result<()> {
    qualify_hub_join(NetworkType::Primary, false).await
}

/// Qualify worker zero independently.
#[tokio::test]
async fn hub_join_worker_zero_cold_record() -> eyre::Result<()> {
    qualify_hub_join(NetworkType::Worker(0), false).await
}

/// Qualify worker one independently.
#[tokio::test]
async fn hub_join_worker_one_cold_record() -> eyre::Result<()> {
    qualify_hub_join(NetworkType::Worker(1), false).await
}

/// An unavailable hub cannot resolve a primary join or disrupt direct peers.
#[tokio::test]
async fn hub_join_primary_unavailable_hub() -> eyre::Result<()> {
    qualify_hub_join(NetworkType::Primary, true).await
}

/// Worker zero retains its direct network while the public path is unavailable.
#[tokio::test]
async fn hub_join_worker_zero_unavailable_hub() -> eyre::Result<()> {
    qualify_hub_join(NetworkType::Worker(0), true).await
}

/// Worker one cannot inherit readiness from a different swarm.
#[tokio::test]
async fn hub_join_worker_one_unavailable_hub() -> eyre::Result<()> {
    qualify_hub_join(NetworkType::Worker(1), true).await
}

/// Sign a replacement record under the exact primary or worker record domain.
fn replacement_record(draft: &JoinDraft, info: NetworkInfo) -> kad::Record {
    let domain = draft.network.record_domain;
    let (role, worker) = match domain.network_type() {
        NetworkType::Primary => (0_u8, 0_u16),
        NetworkType::Worker(worker) => (1_u8, worker),
    };
    let signature = draft.network.key_config.request_signature_direct(&encode(&(
        b"telcoin-network/node-record/v1".as_slice(),
        domain.chain_id(),
        role,
        worker,
        &info,
    )));
    let publisher = Some(PeerId::from(info.pubkey.clone()));
    kad::Record {
        value: encode(&NodeRecord { info, signature }),
        publisher,
        ..draft.network.get_peer_record()
    }
}

/// Reject wrong bindings, retain newer addresses, and distinguish stubs from signed records.
#[tokio::test(start_paused = true)]
async fn hub_join_record_faults_every_swarm() -> eyre::Result<()> {
    [NetworkType::Primary, NetworkType::Worker(0), NetworkType::Worker(1)].into_iter().try_for_each(
        |network_type| -> eyre::Result<()> {
            let tasks = TaskManager::default();
            let mut drafts = join_drafts(network_type, tasks.get_spawner())?.into_iter();
            let mut observer = drafts.next().ok_or_else(|| eyre::eyre!("observer"))?;
            let publisher = drafts.next().ok_or_else(|| eyre::eyre!("publisher"))?;
            let later = drafts.next().ok_or_else(|| eyre::eyre!("later committee"))?;
            let spare = drafts.next().ok_or_else(|| eyre::eyre!("replacement transport key"))?;
            observer.window(8, &HashSet::from([observer.bls]), &HashSet::from([publisher.bls]));
            let initial = observer.network.swarm.behaviour().peer_manager.admission_status();
            assert_eq!(initial.resolved_window(), 1);
            assert_eq!(initial.required_window(), 2);
            let mut wrong_binding = publisher.network.get_peer_record();
            wrong_binding.publisher = Some(observer.peer);
            let _rejection =
                observer.network.process_kad_put_request(publisher.peer, wrong_binding);
            assert_eq!(
                observer
                    .network
                    .swarm
                    .behaviour()
                    .peer_manager
                    .admission_status()
                    .resolved_window(),
                1
            );
            let valid = publisher.network.get_peer_record();
            observer.network.process_kad_put_request(publisher.peer, valid.clone())?;
            assert_eq!(
                observer
                    .network
                    .swarm
                    .behaviour()
                    .peer_manager
                    .admission_status()
                    .resolved_window(),
                2
            );
            let mut replaced = publisher.network.node_record.info.clone();
            replaced.timestamp = replaced.timestamp.saturating_add(1);
            replaced.multiaddrs = vec!["/ip4/127.0.0.1/udp/19099/quic-v1".parse()?];
            let record = replacement_record(&publisher, replaced.clone());
            observer.network.process_kad_put_request(publisher.peer, record)?;
            // An older signed record cannot undo a replacement.
            observer.network.process_kad_put_request(publisher.peer, valid)?;
            let cached =
                observer.network.swarm.behaviour().peer_manager.known_record(&publisher.bls);
            assert_eq!(cached.map(|info| &info.multiaddrs), Some(&replaced.multiaddrs));
            assert_eq!(cached.map(|info| info.timestamp), Some(replaced.timestamp));
            // A newer signed binding replaces the transport key without changing governance
            // identity.
            let mut rekeyed = replaced.clone();
            rekeyed.pubkey = spare.network.node_record.info.pubkey.clone();
            rekeyed.timestamp = rekeyed.timestamp.saturating_add(1);
            let rekey_record = replacement_record(&publisher, rekeyed.clone());
            observer.network.process_kad_put_request(spare.peer, rekey_record)?;
            observer.network.process_kad_put_request(
                publisher.peer,
                replacement_record(&publisher, replaced),
            )?;
            let binding =
                observer.network.swarm.behaviour().peer_manager.known_record(&publisher.bls);
            assert_eq!(binding.map(|info| &info.pubkey), Some(&rekeyed.pubkey));
            // An overlapping newer notice must retain the unresolved later identity.
            observer.window(
                9,
                &HashSet::from([observer.bls]),
                &HashSet::from([publisher.bls, later.bls]),
            );
            let overlap = observer.network.swarm.behaviour().peer_manager.admission_status();
            assert_eq!(overlap.epoch(), Some(9));
            assert_eq!(overlap.resolved_window(), 2);
            assert_eq!(overlap.required_window(), 3);
            assert_eq!(overlap.effective(), AdmissionMode::Grace);
            // A contradictory renewal falls back without replacing the accepted revision.
            observer.window(9, &HashSet::from([observer.bls]), &HashSet::from([later.bls]));
            let conflicting = observer.network.swarm.behaviour().peer_manager.admission_status();
            assert_eq!(conflicting.epoch(), Some(9));
            assert_eq!(conflicting.effective(), AdmissionMode::Open);
            observer.window(
                9,
                &HashSet::from([observer.bls]),
                &HashSet::from([publisher.bls, later.bls]),
            );
            observer
                .network
                .process_kad_put_request(later.peer, later.network.get_peer_record())?;
            let recovered = observer.network.swarm.behaviour().peer_manager.admission_status();
            assert_eq!(recovered.resolved_window(), 3);
            assert_eq!(recovered.connected_current(), 0);
            Ok(())
        },
    )
}
