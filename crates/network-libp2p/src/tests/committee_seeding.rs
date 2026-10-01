//! Hub-independent launch connectivity through the production seeding command.

use super::*;
use crate::common::{TestWorkerRequest, TestWorkerResponse};
use futures::{TryFutureExt as _, TryStreamExt as _};
use std::collections::BTreeMap;
use tn_config::ConsensusConfig;
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::{wait_until, CommitteeFixture};
use tn_types::{BootstrapServer, P2pNode, TaskManager, WorkerId};
use tokio::sync::mpsc;

/// Exercise every validator's primary and worker swarms, then repeat with fresh databases.
///
/// No hub is started. Only local bindings can resolve initial dials. The second pass discards
/// all first-pass peer storage and swarm state while preserving distributed configuration.
#[tokio::test]
async fn committee_seeding_connects_every_swarm_without_hubs_after_fresh_restart(
) -> eyre::Result<()> {
    let workers = NonZeroUsize::new(2).ok_or_else(|| eyre::eyre!("invalid worker count"))?;
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .number_of_workers(workers)
        .randomize_ports(true)
        .build();
    let configs: Vec<_> =
        fixture.authorities().map(|authority| authority.consensus_config()).collect();
    let inventory: BTreeMap<_, _> = configs
        .iter()
        .map(|config| {
            [0, 1]
                .into_iter()
                .map(|id| {
                    config
                        .worker_address(id)
                        .map(|address| {
                            P2pNode::from((
                                address,
                                config.key_config().worker_network_public_key(id),
                            ))
                        })
                        .ok_or_else(|| eyre::eyre!("missing worker {id}"))
                })
                .collect::<eyre::Result<Vec<_>>>()
                .map(|workers| {
                    (
                        config.key_config().primary_public_key(),
                        BootstrapServer {
                            primary: P2pNode::from((
                                config.primary_address(),
                                config.primary_networkkey(),
                            )),
                            workers,
                        },
                    )
                })
        })
        .collect::<eyre::Result<_>>()?;
    let network_config: NetworkConfig = serde_yaml::from_value(serde_yaml::Value::Mapping(
        [(serde_yaml::Value::String("committee_peers".into()), serde_yaml::to_value(&inventory)?)]
            .into_iter()
            .collect(),
    ))?;
    network_config.validate_committee_peers(
        configs.first().ok_or_else(|| eyre::eyre!("no validators"))?.committee(),
        &BTreeMap::new(),
        2,
    )?;
    futures::stream::iter([0, 1].into_iter().map(Ok::<_, eyre::Report>))
        .try_for_each(|round| {
            exercise_fresh_launch(&configs, &network_config)
                .map_err(move |error| eyre::eyre!("fresh launch {round}: {error:?}"))
        })
        .await
}

/// Start fresh databases on every swarm and require all direct committee connections.
async fn exercise_fresh_launch(
    configs: &[ConsensusConfig<MemDatabase>],
    network_config: &NetworkConfig,
) -> eyre::Result<()> {
    let mut tasks = TaskManager::default();
    let swarms = configs
        .iter()
        .flat_map(|config| {
            [None, Some(0), Some(1)]
                .into_iter()
                .map(move |worker: Option<WorkerId>| (config, worker))
        })
        .map(|(config, worker)| {
            let key_config = config.key_config().clone();
            let address = worker
                .map_or_else(|| Some(config.primary_address()), |id| config.worker_address(id))
                .ok_or_else(|| eyre::eyre!("missing swarm address"))?;
            let network_key = worker.map_or_else(
                || key_config.primary_network_keypair().clone(),
                |id| key_config.worker_network_keypair(id),
            );
            let role = worker.map_or(NetworkType::Primary, NetworkType::Worker);
            let (events, receiver) = mpsc::channel(100);
            let network =
                ConsensusNetwork::<TestWorkerRequest, TestWorkerResponse, MemDatabase, _>::new(
                    network_config,
                    events,
                    key_config,
                    network_key,
                    MemDatabase::default(),
                    tasks.get_spawner(),
                    role,
                    address.clone(),
                    None,
                )?;
            let handle = network.network_handle();
            let task = tokio::spawn(network.run());
            Ok((config.key_config().primary_public_key(), worker, address, handle, task, receiver))
        })
        .collect::<eyre::Result<Vec<_>>>()?;
    let members = configs
        .iter()
        .map(|config| config.key_config().primary_public_key())
        .collect::<HashSet<_>>();
    let result = async {
        futures::stream::iter(swarms.iter().map(Ok::<_, eyre::Report>))
            .try_for_each(|(own, worker, address, handle, _, _)| {
                let members = &members;
                async move {
                    let peers = network_config
                        .committee_peers()
                        .iter()
                        .filter(|(key, _)| *key != own)
                        .map(|(key, peer)| {
                            worker
                                .map_or(Some(&peer.primary), |id| peer.worker(id))
                                .cloned()
                                .map(|peer| (*key, peer))
                                .ok_or_else(|| eyre::eyre!("missing worker in inventory"))
                        })
                        .collect::<eyre::Result<BTreeMap<_, _>>>()?;
                    handle.seed_committee_peers(peers).await?;
                    handle
                        .update_committees(HashSet::new(), members.clone(), HashSet::new())
                        .await?;
                    handle.start_listening(address.clone()).await.map(|_| ()).map_err(|error| {
                        eyre::eyre!(
                            "cannot listen for {own:?} worker {worker:?} at {address}: {error}"
                        )
                    })
                }
            })
            .await?;
        futures::stream::iter(swarms.iter().map(Ok::<_, eyre::Report>))
            .try_for_each(|(own, _, _, handle, _, _)| {
                let members = &members;
                async move {
                    futures::stream::iter(
                        members.iter().filter(|key| *key != own).map(Ok::<_, eyre::Report>),
                    )
                    .try_for_each(|key| async move {
                        handle
                            .dial_by_bls(*key)
                            .await
                            .or_else(|error| {
                                if matches!(
                                    error,
                                    NetworkError::AlreadyConnected(_)
                                        | NetworkError::AlreadyDialing(_)
                                        | NetworkError::RedialAttempt
                                        | NetworkError::Dial(_)
                                ) {
                                    Ok(())
                                } else {
                                    Err(error)
                                }
                            })
                            .map_err(Into::into)
                    })
                    .await
                }
            })
            .await?;
        wait_until(
            Duration::from_secs(20),
            "direct committee connectivity on all swarms",
            || async {
                futures::future::try_join_all(
                    swarms.iter().map(|(_, _, _, handle, _, _)| handle.connected_peers()),
                )
                .await
                .map(|connected| {
                    connected.iter().zip(swarms.iter()).all(|(peers, (own, _, _, _, _, _))| {
                        let expected: HashSet<_> =
                            members.iter().filter(|key| *key != own).copied().collect();
                        peers.iter().copied().collect::<HashSet<_>>() == expected
                    })
                })
                .map_err(Into::into)
            },
        )
        .await
    }
    .await;
    let listen_ports: Vec<_> = swarms
        .iter()
        .flat_map(|(_, _, address, _, _, _)| address.iter())
        .filter_map(|protocol| {
            if let libp2p::multiaddr::Protocol::Udp(port) = protocol {
                Some(port)
            } else {
                None
            }
        })
        .collect();
    swarms.iter().for_each(|(_, _, _, _, task, _)| task.abort());
    tasks.abort_all_tasks();
    futures::future::join_all(swarms.into_iter().map(|(_, _, _, _, task, _)| task)).await;
    let cleanup =
        wait_until(Duration::from_secs(5), "fresh swarm listeners to release sockets", || async {
            Ok(listen_ports.iter().all(|port| {
                std::net::UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, *port)).is_ok()
            }))
        })
        .await;
    result.and(cleanup)
}
