//! Starting epoch responsibilities

impl<P, DB> EpochManager<P, DB>
where
    P: TelcoinDirs + Clone + 'static,
    DB: TNDatabase,
{
    /// If we have any consensus that made it into the consensus chain but was not executed
    /// then make sure we submit it to the engine for execution now.
    /// Note, this has to be called correctly or it can lead to double execution.
    async fn replay_missed_consensus(
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

    /// Create the current committee from the current execution state.  Also return the epoch info
    /// and epoch start since this will be needed by some callers (avoid extra system calls).
    async fn get_committee_with_epoch_start_info(
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

    /// Helper method to create all consensus-related components for this epoch.
    ///
    /// Consensus components are short-lived and only relevant for the current epoch.
    async fn create_consensus(
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

    /// Configure consensus for the current epoch.
    ///
    /// This method reads the canonical tip to read the epoch information needed
    /// to create the current committee and the consensus config.
    async fn configure_consensus(
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

    /// Create the [Committee] for the current epoch.
    ///
    /// This is the first step for configuring consensus.
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

    /// Create a [PrimaryNode].
    ///
    /// This also creates the [PrimaryNetwork].
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

    /// Create a [WorkerNode].
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
        let base_fee = gas_accumulator.base_fee(worker_id);

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
                    )
                    .await?;
            } else {
                // We updated our epoch task spawner so make sure worker network tasks are
                // restarted.
                engine.respawn_worker_network_tasks(network_handle.clone()).await;
            }
        }

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

        let worker = WorkerNode::new(
            worker_id,
            consensus_config.clone(),
            network_handle.clone(),
            validator,
            self.consensus_chain.clone(),
        );

        Ok(worker)
    }

    /// Create the primary network for the specific epoch.
    ///
    /// This is not the swarm level, but the [PrimaryNetwork] interface.
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
                tn_config::LibP2pConfig::primary_topic(),
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

    /// Dial peer.
    fn dial_peer_bls<Req: TNMessage, Res: TNMessage>(
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

    /// Create the worker network.
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

        // update the authorized publishers for gossip every epoch
        network_handle
            .inner_handle()
            .subscribe(tn_config::LibP2pConfig::worker_txn_topic())
            .await?;
        // Get gossip from committee members about batches.
        // Useful for non-CVVs to prefetch and harmless for CVVs.
        network_handle
            .inner_handle()
            .subscribe_with_publishers(
                tn_config::LibP2pConfig::worker_batch_topic(),
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

    /// Helper method to identify the node's mode:
    /// - "Committee-voting Validator" (CVV)
    /// - "Committee-voting Validator Inactive" (CVVInactive - syncing to rejoin)
    /// - "Observer"
    ///
    /// This method also updates the `ConsensusBus::node_mode()`.
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

    /// Initialize a network handle for a new epoch.
    ///
    /// On the initial epoch this registers bootstrap peers and starts listening (the one-time,
    /// per-process setup gated on `initial_epoch`). Every epoch sets the
    /// previous/current/next committee slots directly from authoritative state via
    /// `update_committees`.
    ///
    /// On the initial epoch, bootstrap peers must be added BEFORE `update_committees` so that
    /// `known_peers` is populated when the peer manager resolves the committees.
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
}
