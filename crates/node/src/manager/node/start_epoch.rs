//! Epoch-start setup driven by `run_epoch` in [`super`].
//!
//! Everything here runs once per epoch, before consensus begins voting. The
//! committee and epoch-start info are read from the canonical execution tip,
//! then turned into a [`Committee`] and a per-epoch [`ConsensusConfig`]. From
//! that the node's mode is identified (CVV, CVV-inactive, or observer) and the
//! [`PrimaryNode`] and [`WorkerNode`] are created together with their per-epoch
//! [`PrimaryNetwork`]/[`WorkerNetwork`] interfaces.
//!
//! Network setup is split into two scopes. The one-time, per-process swarm init
//! (binding listeners, registering bootstrap peers) is gated on the initial
//! epoch via the `initial_epoch` flag and [`init_network_for_epoch`]. The
//! per-epoch work — refreshing committee membership and gossip publishers,
//! dialing committee peers, and waiting for peers — happens on every epoch so a
//! long-lived swarm tracks the rotating committee.
//!
//! Before voting starts, any consensus that was committed to the chain but not
//! yet executed is replayed to the engine, with a guard that refuses to cross an
//! epoch boundary.

use crate::{
    engine::ExecutionNode, manager::EpochManager, primary::PrimaryNode, worker::WorkerNode,
    EngineToPrimaryRpc,
};
use eyre::{eyre, OptionExt};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tn_config::{Config, ConfigFmt, ConfigTrait as _, ConsensusConfig, NetworkConfig, TelcoinDirs};
use tn_network_libp2p::{error::NetworkError, types::NetworkHandle, TNMessage};
use tn_primary::{
    network::{PrimaryNetwork, PrimaryNetworkHandle},
    ConsensusBus, NodeMode, StateSynchronizer,
};
use tn_reth::{
    system_calls::{
        ConsensusRegistry::{self, EpochInfo},
        EpochState,
    },
    WorkerRpcForwarder,
};
use tn_rpc::RpcNodeInfo;
use tn_types::{
    gas_accumulator::GasAccumulator, BatchValidation, BlsPublicKey, BlsSigner, Committee,
    CommitteeBuilder, ConsensusHeaderDigest, ConsensusOutput, Database as TNDatabase, Epoch,
    Multiaddr, NetworkPublicKey, P2pNode, TaskManager, TaskSpawner, DEFAULT_WORKER_ID,
};
use tn_worker::{WorkerNetwork, WorkerNetworkHandle};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

impl<P, DB> EpochManager<P, DB>
where
    P: TelcoinDirs + Clone + 'static,
    DB: TNDatabase,
{
    /// Re-submit any consensus that reached the consensus chain but was never executed.
    ///
    /// Run once at epoch start to recover the gap between the committed-but-not-executed
    /// tip and the execution tip (e.g. after a crash between commit and execution). The
    /// caller must invoke this only when that gap is genuinely unexecuted: replaying output
    /// the engine already applied causes double execution.
    ///
    /// All missing output must belong to `committee`'s epoch. Encountering a header from a
    /// different leader epoch means execution fell behind across an epoch boundary, which this
    /// recovery path cannot reason about, so it errors rather than replay across the boundary.
    /// If a replayed output is itself the epoch close, the returned [`super::ReplayResult`]
    /// reports its hash and replay stops there.
    pub(super) async fn replay_missed_consensus(
        &mut self,
        committee: Committee,
        to_engine: &mpsc::Sender<ConsensusOutput>,
    ) -> eyre::Result<ReplayResult> {
        let missing =
            state_sync::get_missing_consensus(&self.consensus_bus, &self.consensus_chain).await?;
        let mut last_replayed_hash = None;
        for consensus_header in missing.into_iter() {
            if consensus_header.sub_dag.leader_epoch() != committee.epoch() {
                error!(target: "epoch-manager", "Crossed epoch boundary with missing execution! expected epoch {} got {}",
                    committee.epoch(), consensus_header.sub_dag.leader_epoch());
                return Err(eyre::eyre!(
                    "Crossed epoch boundary with missing execution! expected epoch {} got {}",
                    committee.epoch(),
                    consensus_header.sub_dag.leader_epoch()
                ));
            }
            let consensus_output =
                self.consensus_chain.get_consensus_output_current(consensus_header.number).await?;
            let is_epoch_close = consensus_output.committed_at() >= self.epoch_boundary;
            let output_hash = consensus_output.consensus_header_hash();
            if let Err(e) = self.process_output(to_engine, consensus_output).await {
                error!(target: "epoch-manager", "error sending consensus output to engine: {}", e);
                return Err(e);
            }
            self.metrics.replayed_outputs_total.increment(1);
            last_replayed_hash = Some(output_hash);
            if is_epoch_close {
                return Ok(ReplayResult {
                    epoch_close_hash: Some(output_hash),
                    last_replayed_hash,
                });
            }
        }
        Ok(ReplayResult { epoch_close_hash: None, last_replayed_hash })
    }

    /// Read the canonical execution tip once and derive the current [`Committee`].
    ///
    /// The single `epoch_state_from_canonical_tip` read also yields the `EpochInfo` and
    /// epoch-start timestamp, which are returned alongside the committee so [`configure_consensus`]
    /// can compute the epoch boundary without issuing a second system call against the same tip.
    /// On-chain BLS key bytes are decoded here and a decode failure aborts committee construction.
    pub(super) async fn get_committee_with_epoch_start_info(
        &self,
        engine: &ExecutionNode,
    ) -> eyre::Result<(Committee, EpochInfo, u64)> {
        let EpochState { epoch, epoch_info, validators, bls_pubkeys, epoch_start } =
            engine.epoch_state_from_canonical_tip().await?;
        let validators = validators
            .iter()
            .zip(bls_pubkeys.iter())
            .map(|(v, bls)| {
                let decoded_bls = BlsPublicKey::from_literal_bytes(bls.as_ref());
                decoded_bls.map(|decoded| (decoded, v))
            })
            .collect::<Result<HashMap<_, _>, _>>()
            .map_err(|err| eyre!("failed to create bls key from on-chain bytes: {err:?}"))?;

        Ok((self.create_committee_from_state(epoch, validators).await?, epoch_info, epoch_start))
    }

    /// Build the epoch's [`PrimaryNode`] and [`WorkerNode`] and their per-epoch networks.
    ///
    /// These components are short-lived: they exist only for the current epoch and are torn
    /// down at its close. The node mode is (re)identified first, and the previous epoch's
    /// committee is read from on-chain state so peers from the outgoing committee are not
    /// banned during the handover. `initial_epoch` is threaded down to gate the one-time
    /// per-process network setup (see [`init_network_for_epoch`]).
    ///
    /// After both nodes are up, the next two committees' validator keys are prefetched through
    /// the primary and worker network handles so their network info is already resolved when
    /// those epochs arrive — a best-effort warm-up whose failure is intentionally ignored.
    pub(super) async fn create_consensus(
        &mut self,
        engine: &ExecutionNode,
        epoch_task_manager: &TaskManager,
        initial_epoch: bool,
        gas_accumulator: GasAccumulator,
        consensus_bus: ConsensusBus,
        consensus_config: ConsensusConfig<DB>,
    ) -> eyre::Result<(PrimaryNode<DB>, WorkerNode<DB>)> {
        // create config for consensus
        let _mode = self.identify_node_mode(&consensus_config, &consensus_bus).await?;
        let epoch = consensus_config.committee().epoch();

        // previous committee from on-chain state - canonical source of truth
        let previous_committee_keys: HashSet<BlsPublicKey> = if epoch == 0 {
            HashSet::new() // no previous committee
        } else {
            engine.validators_for_epoch(epoch - 1).await?.into_iter().collect()
        };

        let consensus_bus_app = consensus_bus.app().clone();
        let primary = self
            .create_primary_node_components(
                &consensus_config,
                epoch_task_manager.get_spawner(),
                initial_epoch,
                consensus_bus,
                previous_committee_keys.clone(),
            )
            .await?;

        let public_key = self.key_config.public_key();
        let node_info = RpcNodeInfo {
            chain_id: engine.get_reth_env().await.chainspec().chain_id(),
            name: self.builder.tn_config.node_info.name.clone(),
            bls_public_key: public_key,
            authority_id: public_key.into(),
            execution_address: self.builder.tn_config.node_info.execution_address,
            primary_network_key: self.key_config.primary_network_public_key(),
            worker_network_key: self.key_config.worker_network_public_key(),
            primary_external_address: self
                .builder
                .tn_config
                .node_info
                .primary_network_address()
                .clone(),
            worker_external_address: self
                .builder
                .tn_config
                .node_info
                .worker_network_address()
                .clone(),
            version: self.version_str,
        };
        let engine_to_primary =
            EngineToPrimaryRpc::new(consensus_bus_app, self.consensus_chain.clone(), node_info);
        // only spawns one worker for now
        let worker = self
            .spawn_worker_node_components(
                &consensus_config,
                engine,
                epoch_task_manager.get_spawner(),
                initial_epoch,
                engine_to_primary,
                gas_accumulator,
                previous_committee_keys,
            )
            .await?;

        let primary_handle = primary.network_handle().await;
        let committee = consensus_config.committee();
        let mut prefetches = committee.bls_keys().clone();
        let next_committee_keys = engine.validators_for_epoch(committee.epoch() + 1).await?;
        prefetches.extend(next_committee_keys.iter());
        prefetches.extend(engine.validators_for_epoch(committee.epoch() + 2).await?);
        // Attempt to pre-load the next couple of committee's network info.
        let _ = primary_handle
            .inner_handle()
            .find_authorities(prefetches.iter().copied().collect())
            .await;
        let worker_handle = worker.network_handle().await;
        // Attempt to pre-load the next couple of committee's network info.
        let _ =
            worker_handle.inner_handle().find_authorities(prefetches.into_iter().collect()).await;
        Ok((primary, worker))
    }

    /// Assemble the per-epoch [`ConsensusConfig`] from the canonical execution tip.
    ///
    /// Reads the committee and epoch-start info from the tip, resets `epoch_boundary` to
    /// `epoch_start + epochDuration` (the timestamp at which this epoch closes, used elsewhere
    /// to detect the boundary), and folds in the next committee's keys so the network can
    /// pre-resolve the successor committee. Produces a config scoped to this epoch only.
    pub(super) async fn configure_consensus(
        &mut self,
        engine: &ExecutionNode,
        network_config: &NetworkConfig,
    ) -> eyre::Result<ConsensusConfig<DB>> {
        // retrieve epoch information from canonical tip
        let (committee, epoch_info, epoch_start) =
            self.get_committee_with_epoch_start_info(engine).await?;
        let validators = committee.bls_keys();

        self.epoch_boundary = epoch_start + epoch_info.epochDuration as u64;
        debug!(target: "epoch-manager", new_epoch_boundary=self.epoch_boundary, "resetting epoch boundary");

        debug!(target: "epoch-manager", ?validators, "creating committee for validators");

        let next_committee_keys = engine.validators_for_epoch(committee.epoch() + 1).await?;

        // create config for consensus
        let consensus_config = ConsensusConfig::new_for_epoch(
            self.builder.tn_config.clone(),
            self.consensus_db.clone(),
            self.key_config.clone(),
            committee,
            network_config.clone(),
            next_committee_keys,
        )?;

        Ok(consensus_config)
    }

    /// Resolve the [`Committee`] for `epoch`, the first step of configuring consensus.
    ///
    /// Epoch 0 has no on-chain history, so the genesis committee is loaded from the
    /// committee file on disk. Every later epoch is built with a [`CommitteeBuilder`] from the
    /// statically configured bootstrap servers plus the on-chain validator set for that epoch.
    async fn create_committee_from_state(
        &self,
        epoch: Epoch,
        validators: HashMap<BlsPublicKey, &ConsensusRegistry::ValidatorInfo>,
    ) -> eyre::Result<Committee> {
        info!(target: "epoch-manager", "creating committee from state");

        // the network must be live
        let committee = if epoch == 0 {
            // read from fs for genesis
            Config::load_from_path_or_default::<Committee>(
                self.tn_datadir.committee_path(),
                ConfigFmt::YAML,
            )?
        } else {
            let mut committee_builder = CommitteeBuilder::new(epoch);
            for (key, bootstrap) in &self.bootstrap_servers {
                committee_builder.add_bootstrap_server(
                    *key,
                    bootstrap.primary.clone(),
                    bootstrap.worker.clone(),
                );
            }

            for validator in validators {
                committee_builder.add_authority(validator.0, validator.1.validatorAddress);
            }
            committee_builder.build()
        };

        Ok(committee)
    }

    /// Construct the epoch's [`PrimaryNode`] and bring up its [`PrimaryNetwork`].
    ///
    /// Builds the [`StateSynchronizer`] for the epoch and clones the long-lived primary
    /// [`PrimaryNetworkHandle`] held on the [`EpochManager`] (its absence is a hard error,
    /// since the swarm is created earlier in the process). [`spawn_primary_network_for_epoch`]
    /// wires the per-epoch network onto that handle using `epoch_task_spawner`, so its tasks
    /// abort when the epoch ends, before the node itself is assembled.
    async fn create_primary_node_components(
        &mut self,
        consensus_config: &ConsensusConfig<DB>,
        epoch_task_spawner: TaskSpawner,
        initial_epoch: bool,
        consensus_bus: ConsensusBus,
        previous_committee_keys: HashSet<BlsPublicKey>,
    ) -> eyre::Result<PrimaryNode<DB>> {
        let state_sync = StateSynchronizer::new(
            consensus_config.clone(),
            consensus_bus.clone(),
            epoch_task_spawner.clone(),
        );
        let network_handle = self
            .primary_network_handle
            .as_ref()
            .ok_or_eyre("primary network handle missing from epoch manager")?
            .clone();

        // create the epoch-specific `PrimaryNetwork`
        self.spawn_primary_network_for_epoch(
            consensus_config,
            state_sync.clone(),
            epoch_task_spawner.clone(),
            &network_handle,
            initial_epoch,
            consensus_bus.clone(),
            previous_committee_keys,
        )
        .await?;

        // spawn primary - create node and spawn network
        let primary =
            PrimaryNode::new(consensus_config.clone(), consensus_bus, network_handle, state_sync);

        Ok(primary)
    }

    /// Construct the epoch's [`WorkerNode`] and bring up its [`WorkerNetwork`].
    ///
    /// Only worker id [`tn_types::DEFAULT_WORKER_ID`] is supported. The shared
    /// [`WorkerNetworkHandle`] on the [`EpochManager`] is re-pointed at this epoch's task
    /// spawner and epoch number before anything else, so batch reporting runs under the
    /// epoch-scoped lifetime.
    ///
    /// The engine's worker components are initialized on the initial epoch, and also whenever
    /// the engine reports no workers yet — the latter covers the case where the first epoch
    /// returned early from [`replay_missed_consensus`] (epoch boundary hit) before reaching
    /// [`create_consensus`], leaving the workers uninitialized. Otherwise the worker network
    /// tasks are respawned so they pick up the new epoch task spawner.
    #[allow(clippy::too_many_arguments)]
    async fn spawn_worker_node_components(
        &mut self,
        consensus_config: &ConsensusConfig<DB>,
        engine: &ExecutionNode,
        epoch_task_spawner: TaskSpawner,
        initial_epoch: bool,
        engine_to_primary: EngineToPrimaryRpc,
        gas_accumulator: GasAccumulator,
        previous_committee_keys: HashSet<BlsPublicKey>,
    ) -> eyre::Result<WorkerNode<DB>> {
        // only support one worker for now (with id 0) - otherwise, loop here
        let worker_id = DEFAULT_WORKER_ID;
        // u64 snapshot of the worker's base fee, used by both the transaction pool and the batch
        // validator. Base fee is constant within an epoch, so a snapshot taken at epoch start is
        // valid for the whole epoch.
        let base_fee = gas_accumulator.base_fee(worker_id).base_fee();

        // update the network handle's task spawner for reporting batches in the epoch
        {
            let network_handle = self
                .worker_network_handle
                .as_mut()
                .ok_or_eyre("worker network handle missing from epoch manager")?;

            network_handle.update_task_spawner(epoch_task_spawner.clone());
            network_handle.update_epoch(consensus_config.committee().epoch());
            // initialize worker components on startup
            // This will use the new epoch_task_spawner and epoch on network_handle.
            // Also initialize if workers are empty: this happens when the first epoch returns
            // early from replay_missed_consensus (epoch boundary hit) before create_consensus
            // is reached, leaving workers uninitialized.
            if initial_epoch || !engine.are_workers_initialized().await {
                engine
                    .initialize_worker_components(
                        worker_id,
                        network_handle.clone(),
                        engine_to_primary,
                        base_fee,
                    )
                    .await?;
            } else {
                // We updated our epoch task spawner so make sure worker network tasks are
                // restarted.
                engine.respawn_worker_network_tasks(network_handle.clone()).await;
            }
        }

        // Ensure the worker's transaction pool charges the accumulator's base fee for this epoch.
        // On the init path above the pool was created with this value; this call additionally
        // covers the respawn path, where initialization is skipped.
        engine.set_worker_base_fee(worker_id, base_fee).await;

        let network_handle = self
            .worker_network_handle
            .as_ref()
            .ok_or_eyre("worker network handle missing from epoch manager")?
            .clone();

        let validator = engine
            .new_batch_validator(&worker_id, base_fee, consensus_config.committee().epoch())
            .await;
        self.spawn_worker_network_for_epoch(
            consensus_config,
            &worker_id,
            validator.clone(),
            epoch_task_spawner,
            &network_handle,
            initial_epoch,
            previous_committee_keys,
        )
        .await?;

        // Observer transaction forwarding: a non-committee worker forwards each transaction it
        // accepts to the JSON-RPC endpoint of the validator that owns it, discovered over
        // kademlia (issue #804).
        let forwarder =
            Arc::new(WorkerRpcForwarder::new(network_handle.get_task_spawner().clone()));

        let worker = WorkerNode::new(
            worker_id,
            consensus_config.clone(),
            network_handle.clone(),
            validator,
            forwarder,
            self.consensus_chain.clone(),
        );

        Ok(worker)
    }

    /// Stand up the [`PrimaryNetwork`] interface for this epoch over the shared swarm.
    ///
    /// This operates on the per-epoch interface, not the swarm itself. Every epoch refreshes
    /// the previous/current/next committee membership (via [`init_network_for_epoch`]) and the
    /// gossip publisher set so the network bans and routes against the current committee. The
    /// listener is bound only on the initial epoch.
    ///
    /// Peers are dialed when this node is a CVV (it must reach the other CVVs) or when it has no
    /// connected peers; a non-committee node that already has peers does not pester the
    /// committee. The method then waits for peers before spawning the network on the
    /// epoch-scoped spawner.
    #[allow(clippy::too_many_arguments)]
    async fn spawn_primary_network_for_epoch(
        &mut self,
        consensus_config: &ConsensusConfig<DB>,
        state_sync: StateSynchronizer<DB>,
        epoch_task_spawner: TaskSpawner,
        network_handle: &PrimaryNetworkHandle,
        initial_epoch: bool,
        consensus_bus: ConsensusBus,
        previous_committee_keys: HashSet<BlsPublicKey>,
    ) -> eyre::Result<()> {
        // get event streams for the primary network handler
        let rx_event_stream = self.consensus_bus.subscribe_primary_network_events();

        // set committee for network to prevent banning
        debug!(target: "epoch-manager", auth=?consensus_config.authority_id(), "spawning primary network for epoch");
        let committee_keys: HashSet<BlsPublicKey> = consensus_config
            .committee()
            .authorities()
            .into_iter()
            .map(|a| *a.protocol_key())
            .collect();

        let bootstrap_peers = consensus_config
            .committee()
            .bootstrap_servers()
            .iter()
            .map(|(k, v)| (*k, v.primary.clone()))
            .collect();
        let next_committee_keys: HashSet<BlsPublicKey> =
            consensus_config.next_committee_keys().iter().copied().collect();
        Self::init_network_for_epoch(
            network_handle.inner_handle(),
            bootstrap_peers,
            previous_committee_keys,
            committee_keys.clone(),
            next_committee_keys,
            initial_epoch,
        )
        .await?;

        // start listening if the network needs to be initialized
        if initial_epoch {
            let primary_address = Self::parse_listener_address_for_swarm(
                "PRIMARY_LISTENER_MULTIADDR",
                consensus_config.primary_networkkey(),
                consensus_config.primary_address(),
            )?;
            info!(target: "epoch-manager", ?primary_address, "listening to {primary_address}");
            network_handle.inner_handle().start_listening(primary_address).await?;
        }

        // update the authorized publishers for gossip every epoch
        network_handle
            .inner_handle()
            .subscribe_with_publishers(
                tn_config::LibP2pConfig::primary_topic(consensus_config.chain_id()),
                committee_keys.into_iter().collect(),
            )
            .await?;

        if network_handle.connected_peers_count().await.unwrap_or(0) == 0
            || self.consensus_bus.is_cvv()
        {
            // always dial peers for the new epoch
            // do this if a CVV (may need to connect to the other CVVs) or if we don't have any
            // peers if we are not a committee member and have peers then do not pester
            // the committee
            for (_authority_id, bls_pubkey) in consensus_config
                .committee()
                .others_primaries_by_id(consensus_config.authority_id().as_ref())
            {
                self.dial_peer_bls(
                    network_handle.inner_handle().clone(),
                    bls_pubkey,
                    epoch_task_spawner.clone(),
                );
            }
        }

        Self::wait_for_network_peers(network_handle.inner_handle(), "primary network").await?;

        // re-probe each peer's epoch-pack sync capability this epoch: committees
        // rotate and binaries are upgraded at the boundary, so a peer that could
        // only speak legacy last epoch may now serve the sync protocol (739, step 6)
        network_handle.clear_sync_capability();

        // spawn primary network
        PrimaryNetwork::new(
            rx_event_stream,
            network_handle.clone(),
            consensus_config.clone(),
            consensus_bus.app().clone(),
            state_sync,
            epoch_task_spawner.clone(), // tasks should abort with epoch
            self.consensus_chain.clone(),
        )
        .spawn(&epoch_task_spawner);

        Ok(())
    }

    /// Spawn a long-running task that dials a peer by [`BlsPublicKey`], retrying with backoff.
    ///
    /// Dialing self is skipped. The task runs on the node-lifetime spawner (not the epoch
    /// spawner) so a slow-to-reach peer keeps being retried across epochs. Backoff doubles up to
    /// 120s; an already-connected or already-dialing error is treated as success. The task only
    /// gives up once it has retried enough and at least one other peer is connected — being
    /// unable to reach a single peer is expected, but it will not abandon dialing while isolated.
    pub(super) fn dial_peer_bls<Req: TNMessage, Res: TNMessage>(
        &self,
        handle: NetworkHandle<Req, Res>,
        bls_pubkey: BlsPublicKey,
        node_task_spawner: TaskSpawner,
    ) {
        if bls_pubkey == self.key_config.public_key() {
            // Don't try to dial ourselves.
            return;
        }
        // spawn dials on long-running task manager
        let task_name = format!("DialPeer {bls_pubkey}");
        node_task_spawner.spawn_task(task_name, async move {
            let mut backoff = 1;
            let mut retries = 0;

            debug!(target: "epoch-manager", ?bls_pubkey, "dialing peer");
            while let Err(e) = handle.dial_by_bls(bls_pubkey).await {
                // ignore errors for peers that are already connected or being dialed
                if matches!(e, NetworkError::AlreadyConnected(_))
                    || matches!(e, NetworkError::AlreadyDialing(_))
                {
                    return Ok(());
                }
                retries += 1;

                warn!(target: "epoch-manager", "failed to dial {bls_pubkey}: {e}");
                tokio::time::sleep(Duration::from_secs(backoff)).await;
                if backoff < 120 {
                    backoff += backoff;
                }
                let peers = handle.connected_peer_count().await.unwrap_or(0);
                // We have been trying for a while (at least two max backoffs at 120 secs), if we
                // have any other peers give up.
                if retries > 10 && peers > 0 {
                    warn!(target = "dial_peer", "failed to reach peer {bls_pubkey}, giving up");
                    return Ok(()); // failing to reach a peer is expected now and then
                }
            }
            Ok(())
        });
    }

    /// Stand up the [`WorkerNetwork`] interface for this epoch over the shared swarm.
    ///
    /// The worker analogue of [`spawn_primary_network_for_epoch`]: every epoch refreshes
    /// committee membership (via [`init_network_for_epoch`]) and the gossip subscriptions, while
    /// the listener binds only on the initial epoch. The worker always dials this epoch's
    /// committee peers — the peer manager drops dials to peers already connected — then waits
    /// for peers before spawning the network on the epoch-scoped spawner.
    ///
    /// The batch topic is subscribed, restricted to committee publishers so non-CVVs can
    /// prefetch batches (harmless for CVVs). Non-CVVs push the transactions they accept to
    /// the committee over RPC rather than gossiping them (issue #804).
    #[allow(clippy::too_many_arguments)]
    async fn spawn_worker_network_for_epoch(
        &mut self,
        consensus_config: &ConsensusConfig<DB>,
        worker_id: &u16,
        validator: Arc<dyn BatchValidation>,
        epoch_task_spawner: TaskSpawner,
        network_handle: &WorkerNetworkHandle,
        initial_epoch: bool,
        previous_committee_keys: HashSet<BlsPublicKey>,
    ) -> eyre::Result<()> {
        // get event streams for the worker network handler
        let rx_event_stream = self.worker_event_stream.subscribe();
        debug!(target: "epoch-manager", "spawning worker network for epoch");

        let committee_keys: HashSet<BlsPublicKey> = consensus_config
            .committee()
            .authorities()
            .into_iter()
            .map(|a| *a.protocol_key())
            .collect();

        let bootstrap_peers = consensus_config
            .committee()
            .bootstrap_servers()
            .iter()
            .map(|(k, v)| (*k, v.worker.clone()))
            .collect();
        let next_committee_keys: HashSet<BlsPublicKey> =
            consensus_config.next_committee_keys().iter().copied().collect();
        Self::init_network_for_epoch(
            network_handle.inner_handle(),
            bootstrap_peers,
            previous_committee_keys,
            committee_keys.clone(),
            next_committee_keys,
            initial_epoch,
        )
        .await?;

        // start listening if the network needs to be initialized
        if initial_epoch {
            let worker_address = Self::parse_listener_address_for_swarm(
                "WORKER_LISTENER_MULTIADDR",
                consensus_config.primary_networkkey(),
                consensus_config.worker_address(),
            )?;
            network_handle.inner_handle().start_listening(worker_address).await?;
        }

        let worker_address = consensus_config.worker_address();

        // always attempt to dial peers for the new epoch
        // the network's peer manager will intercept dial attempts for peers that are already
        // connected
        debug!(target: "epoch-manager", ?worker_address, "spawning worker network for epoch");
        for (_, peer) in consensus_config
            .committee()
            .others_primaries_by_id(consensus_config.authority().as_ref().map(|a| a.id()).as_ref())
        {
            self.dial_peer_bls(
                network_handle.inner_handle().clone(),
                peer,
                epoch_task_spawner.clone(),
            );
        }

        Self::wait_for_network_peers(network_handle.inner_handle(), "worker network").await?;

        // Get gossip from committee members about batches every epoch.
        // Useful for non-CVVs to prefetch and harmless for CVVs.
        network_handle
            .inner_handle()
            .subscribe_with_publishers(
                tn_config::LibP2pConfig::worker_batch_topic(consensus_config.chain_id()),
                committee_keys.into_iter().collect(),
            )
            .await?;

        // spawn worker network
        WorkerNetwork::new(
            rx_event_stream,
            network_handle.clone(),
            consensus_config.clone(),
            *worker_id,
            validator,
            self.consensus_chain.clone(),
        )
        .spawn(&epoch_task_spawner);

        Ok(())
    }

    /// Decide this epoch's [`NodeMode`] and publish it to [`ConsensusBus::node_mode`].
    ///
    /// An existing `CvvInactive` state is sticky and returned as-is — a node syncing to rejoin
    /// the committee stays inactive until that resolves elsewhere. Otherwise the node is an
    /// `Observer` if it is not in this committee (or is configured observer-only), and
    /// `CvvActive` if it is. `CvvActive` is optimistic: the node assumes it is caught up and is
    /// demoted to inactive later if that turns out to be false. The chosen mode is written to the
    /// [`ConsensusBus`] before returning.
    async fn identify_node_mode(
        &self,
        consensus_config: &ConsensusConfig<DB>,
        consensus_bus: &ConsensusBus,
    ) -> eyre::Result<NodeMode> {
        if self.consensus_bus.is_cvv_inactive() {
            // If we have an inactive mode then it was set so keep it for now.
            return Ok(NodeMode::CvvInactive);
        }
        debug!(target: "epoch-manager", authority_id=?consensus_config.authority_id(), "identifying node mode..." );
        let in_committee = consensus_config
            .authority_id()
            .map(|id| consensus_config.in_committee(&id))
            .unwrap_or(false);
        state_sync::prime_consensus(
            consensus_bus.app(),
            consensus_config,
            self.consensus_chain.clone(),
        )
        .await;
        let mode = if !in_committee || self.builder.tn_config.observer {
            NodeMode::Observer
        } else {
            // Assume we are caught up, will be demoted to inactive if this is not true...
            NodeMode::CvvActive
        };

        debug!(target: "epoch-manager", ?mode, "node mode identified");
        // update consensus bus
        self.consensus_bus.node_mode().send_modify(|v| *v = mode);

        Ok(mode)
    }

    /// Point a network handle at a new epoch's committee membership.
    ///
    /// Every epoch sets the previous/current/next committee slots directly from authoritative
    /// state via `update_committees`. On the initial epoch only, bootstrap peers are registered
    /// first (the one-time, per-process step gated on `initial_epoch`).
    ///
    /// Ordering matters on that initial epoch: bootstrap peers must be added BEFORE
    /// `update_committees`, so that `known_peers` is already populated when the peer manager
    /// resolves the committees against it.
    async fn init_network_for_epoch<Req: TNMessage, Res: TNMessage>(
        handle: &NetworkHandle<Req, Res>,
        bootstrap_peers: BTreeMap<BlsPublicKey, P2pNode>,
        previous_committee_keys: HashSet<BlsPublicKey>,
        committee_keys: HashSet<BlsPublicKey>,
        next_committee_keys: HashSet<BlsPublicKey>,
        initial_epoch: bool,
    ) -> eyre::Result<()> {
        if initial_epoch {
            handle.add_bootstrap_peers(bootstrap_peers).await?;
        }
        handle
            .update_committees(previous_committee_keys, committee_keys, next_committee_keys)
            .await?;
        Ok(())
    }

    /// Resolve a swarm listener [`Multiaddr`] from an env var, falling back to a default.
    ///
    /// Lets cloud deployments override the primary/worker listen address (e.g. to bind a
    /// container's external address) without changing config. When the env var is set, the
    /// parsed address has the node's [`NetworkPublicKey`] appended as a `/p2p/` component to
    /// match the format produced by keytool generation; an unparseable value or one carrying a
    /// conflicting `/p2p/` key is an error. When unset, `fallback` is returned as-is.
    fn parse_listener_address_for_swarm(
        env_var: &str,
        network_pubkey: NetworkPublicKey,
        fallback: Multiaddr,
    ) -> eyre::Result<Multiaddr> {
        std::env::var(env_var)
            .map(|addr| {
                addr.parse()
                    .map_err(|e| {
                        eyre::eyre!(
                            "Failed to parse listener multiaddr from env {env_var} ({addr})\n{e}"
                        )
                    })
                    // add Protocol::P2p to multiaddr to maintain consistency with
                    // bin/telcoin-network/src/keytool/generate.rs
                    .and_then(|multi: Multiaddr| {
                        multi.with_p2p(network_pubkey.into()).map_err(|_| {
                            eyre::eyre!(
                                "{env_var} multiaddr contains a different P2P protocol {:?}",
                                std::env::var(env_var)
                            )
                        })
                    })
            })
            .unwrap_or(Ok(fallback))
    }

    /// Block until the given [`NetworkHandle`] has at least one connected peer.
    ///
    /// Polls the peer count every 500ms, logging periodically, and gives up after 240 attempts
    /// (~2 minutes) with an error rather than letting epoch startup hang forever on a network that
    /// cannot bootstrap. Generic over the [`TNMessage`] request/response types so it serves both
    /// the primary and worker networks.
    async fn wait_for_network_peers<Req: TNMessage, Res: TNMessage>(
        handle: &NetworkHandle<Req, Res>,
        network_name: &str,
    ) -> eyre::Result<()> {
        let mut peers = handle.connected_peer_count().await.unwrap_or(0);
        let mut retries = 0;
        while peers == 0 {
            retries += 1;
            if retries > 240 {
                return Err(eyre::eyre!(
                    "{network_name} unable to join, cannot connect to any peers!"
                ));
            }
            if retries % 10 == 0 {
                error!(target: "epoch-manager", "failed to join the {network_name}!");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
            peers = handle.connected_peer_count().await.unwrap_or(0);
        }
        Ok(())
    }
}

/// Outcome of replaying consensus output that was validated but not yet executed before a restart.
///
/// Replay happens before consensus is reconfigured, so both hashes describe progress made purely by
/// re-forwarding persisted output to the engine. The two fields are independent: replay can cross
/// the epoch boundary (`epoch_close_hash` set) without ever needing the caller to wait on a
/// mid-epoch `last_replayed_hash`, and vice versa.
pub(super) struct ReplayResult {
    /// Set when replay reached the epoch boundary: the consensus header hash the caller must close
    /// the epoch with before starting the next one. Drives the replay-and-close early return.
    epoch_close_hash: Option<ConsensusHeaderDigest>,
    /// Hash of the last output actually forwarded to the engine during replay, or `None` if
    /// nothing was replayed. The caller waits on this (never on DB-latest, which may have been
    /// persisted but never sent) to confirm execution caught up before live consensus resumes.
    last_replayed_hash: Option<ConsensusHeaderDigest>,
}

impl ReplayResult {
    /// Take `Self::epoch_close_hash` if it exists.
    pub(super) fn take_epoch_close_hash(&mut self) -> Option<ConsensusHeaderDigest> {
        self.epoch_close_hash.take()
    }

    /// Take `Self::last_replayed_hash` if it exists.
    pub(super) fn take_last_replayed_hash(&mut self) -> Option<ConsensusHeaderDigest> {
        self.last_replayed_hash.take()
    }
}
