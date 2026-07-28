//! Methods for node startup and epoch boundaries.

use std::sync::Arc;

use eyre::OptionExt as _;
use jsonrpsee::Methods;
use reth::rpc::{
    builder::{config::RethRpcServerConfig as _, RpcModuleBuilder, RpcServerHandle},
    eth::EthApi,
};
use reth_errors::ProviderError;
use reth_eth_wire::BlockHashNumber;
use reth_evm::{ConfigureEvm as _, Evm as _, EvmFactory as _};
use reth_provider::{
    providers::BlockchainProvider, BlockIdReader as _, BlockNumReader as _,
    ChainStateBlockReader as _, ChainStateBlockWriter as _, DBProvider as _,
    DatabaseProviderFactory as _, HeaderProvider as _, StateProviderFactory as _,
};
use reth_revm::{
    context::result::{EVMError, ExecutionResult, ResultAndState},
    database::StateProviderDatabase,
    State,
};
use reth_transaction_pool::{blobstore::DiskFileBlobStore, EthTransactionPool};
use tn_config::WORKER_CONFIGS_ADDRESS;
use tn_types::{
    gas_accumulator::WorkerFeeConfig, Address, Bytes, Epoch, ExecHeader, SealedHeader,
    SolCall as _, B256,
};
use tracing::{debug, error, warn};

use crate::{
    error::{
        EvmReadError, EvmReadResult, StateReadError, StateReadResult, TnRethError, TnRethResult,
    },
    evm::{TNEvm, TnEvmConfig},
    system_calls::{ConsensusRegistry, EpochState, WorkerConfigs, CONSENSUS_REGISTRY_ADDRESS},
    traits::{TNExecution, TelcoinNode},
    worker::WorkerNetwork,
    RethEnv, RpcServer, WorkerTxPool, SYSTEM_ADDRESS,
};

impl RethEnv {
    /// Advance a lagging finalized marker to the persisted canonical tip.
    ///
    /// Blocks and the finalized/safe markers commit in a single database transaction in
    /// [`Self::finish_executing_output`], so a crash can no longer leave the marker behind the
    /// persisted tip — the two-transaction crash window this heal was written for does not
    /// exist for new writes. The heal is retained as defense-in-depth for databases written by
    /// pre-fix versions, which committed the markers separately in `finalize_block`: a crash
    /// between the two transactions restarted the node with `finalized marker < persisted
    /// canonical tip`, hiding the gap blocks from every marker reader (accumulator catchup,
    /// the tx-pool's last-seen seed, the RPC `finalized`/`safe` tags).
    ///
    /// Advancing the marker is sound because every persisted canonical block is consensus-final
    /// by construction: blocks only enter the canonical database from committed consensus
    /// output, so there is no speculative or reorgable segment the marker could overrun. The
    /// lag is purely a persistence artifact of the pre-fix two-transaction design.
    ///
    /// The heal persists the marker itself ([`Self::finalize_block`] no longer writes the
    /// database) and then routes through `finalize_block` for the in-memory half: the
    /// finalized/safe watches are seeded from the (stale) database at provider construction,
    /// and startup marker readers like the accumulator catchup consult the watch — a DB-only
    /// write would leave them reading the stale value.
    ///
    /// Replay is unaffected: the missed-consensus watermark derives from the persisted
    /// execution tip (which this method never moves), not the finalized marker, so healing
    /// cannot cause `replay_missed_consensus` to re-forward committed output — no
    /// double-execution is possible.
    ///
    /// Call once at startup, before anything reads the marker and before any consensus output
    /// executes. A marker ahead of the tip fails with
    /// [`TnRethError::FinalizedMarkerAheadOfTip`]: the execution database lost blocks this node
    /// already attested as final.
    ///
    /// Like every startup tip reader, this trusts the persisted tip itself; a torn write
    /// between MDBX and static files below the provider is a pre-existing exposure this method
    /// neither adds to nor guards against.
    pub fn heal_finalized_to_persisted_tip(&self) -> TnRethResult<()> {
        // One RO provider = one consistent MDBX snapshot for all three reads. Read errors
        // PROPAGATE: the public last_block_number()/last_finalized_block_number() wrappers
        // unwrap_or(0) and would misdiagnose a failed read as tip=0 here.
        let provider = self.inner.blockchain_provider.database_provider_ro()?;
        let tip = provider.last_block_number()?;
        // None = never finalized (fresh genesis). Some(0) is unreachable in production (the
        // first marker write covers block >= 1), so collapsing None to 0 is safe.
        let finalized = provider.last_finalized_block_number()?.unwrap_or(0);

        if finalized > tip {
            return Err(TnRethError::FinalizedMarkerAheadOfTip { finalized, tip });
        }
        if finalized == tip {
            // Normal restart (marker caught up), or fresh genesis (marker never written and
            // tip = 0): genesis intentionally stays unfinalized so
            // finalized_block_hash_number_for_startup keeps its genesis fallback.
            return Ok(());
        }

        let header =
            provider.sealed_header(tip)?.ok_or(ProviderError::HeaderNotFound(tip.into()))?;
        drop(provider);
        warn!(
            target: "tn::reth",
            finalized,
            tip,
            "finalized marker lags the persisted canonical tip (database written by a pre-fix \
             version that committed blocks and the marker separately); healing marker to tip"
        );
        // durably fix the marker on disk before touching the watches: finalize_block only
        // updates in-memory state, so a lagging pre-fix database needs its rows rewritten here
        let provider_rw = self.inner.blockchain_provider.database_provider_rw()?;
        provider_rw.save_finalized_block_number(tip)?;
        provider_rw.save_safe_block_number(tip)?;
        provider_rw.commit()?;
        self.finalize_block(header)
    }

    /// Return the block number and hash of the finalized block on node startup.
    ///
    /// This method adds additional fallbacks to ensure genesis is used when the network is starting
    /// because the genesis block is not initialized as `finalized`. Nodes that start on genesis
    /// will resync with the network if it exists.
    pub fn finalized_block_hash_number_for_startup(&self) -> TnRethResult<BlockHashNumber> {
        let hash = self
            .inner
            .blockchain_provider
            .finalized_block_hash()?
            .unwrap_or_else(|| self.node_config().chain.sealed_genesis_header().hash());
        let number = self.inner.blockchain_provider.finalized_block_number()?.unwrap_or_default();
        Ok(BlockHashNumber { hash, number })
    }

    /// Build and return the RPC server for the instance.
    /// This probably needs better abstraction.
    pub fn get_rpc_server(
        &self,
        transaction_pool: WorkerTxPool,
        network: WorkerNetwork,
        other: impl Into<Methods>,
    ) -> RpcServer {
        let transaction_pool: EthTransactionPool<
            BlockchainProvider<TelcoinNode>,
            DiskFileBlobStore,
            TnEvmConfig,
        > = transaction_pool.into();
        let tn_execution = Arc::new(TNExecution);
        let rpc_builder = RpcModuleBuilder::default()
            .with_provider(self.inner.blockchain_provider.clone())
            .with_pool(transaction_pool.clone())
            .with_network(network.clone())
            .with_executor(Box::new(self.inner.task_spawner.clone()))
            .with_evm_config(self.inner.evm_config.clone())
            .with_consensus(tn_execution.clone());

        let modules_config = self.node_config().rpc.transport_rpc_module_config();
        let eth_api = EthApi::builder(
            self.inner.blockchain_provider.clone(),
            transaction_pool,
            network,
            self.inner.evm_config.clone(),
        )
        .build();

        let engine_events = reth_tokio_util::EventSender::default();
        let mut server = rpc_builder.build(modules_config, eth_api, engine_events);
        if let Err(e) = server.merge_configured(other) {
            tracing::error!(target: "tn::execution", "Error merging TN rpc module: {e:?}");
        }

        server
    }

    /// Start running the RPC server for this instance.
    pub async fn start_rpc(&self, server: &RpcServer) -> TnRethResult<RpcServerHandle> {
        let server_config = self.node_config().rpc.rpc_server_config();
        Ok(server_config.start(server).await?)
    }

    /// Read the latest committee and epoch information from the [ConsensusRegistry] on-chain.
    ///
    /// The protocol needs the BLS pubkey for the authorities.
    /// - get current epoch info
    /// - getValidator token id by address
    /// - getValidator info by token id
    ///
    /// The committee arrays this returns mutate mid-epoch: a governance `burn` swap-and-pops the
    /// ejected validator out of the CURRENT epoch's stored committees immediately, so tip reads
    /// before and after the burn disagree. Epoch-scoped consensus reads (committee construction,
    /// rewards seeding, quorum) must use [`Self::epoch_state_at_epoch_start`] instead; the tip
    /// read remains correct for point-in-time queries.
    pub fn epoch_state_from_canonical_tip(&self) -> eyre::Result<EpochState> {
        let canonical_tip = self.canonical_tip();
        debug!(target: "engine", ?canonical_tip, "retrieving epoch state from canonical tip");
        self.epoch_state_at_header(&canonical_tip)
    }

    /// Read the committee and epoch information from the [ConsensusRegistry] at `header`.
    ///
    /// The registry state, the EVM environment, and therefore the returned epoch, epoch info,
    /// and committee all derive from this ONE header. Recovery paths that scan
    /// `epoch_info.blockHeight..=header.number` (catchup and epoch-entry base-fee seeding) rely
    /// on this pin: reading the range start from a different header (e.g. the canonical tip)
    /// could yield a silently empty range if finality ever lags the canonical tip.
    pub fn epoch_state_at_header(&self, header: &SealedHeader) -> eyre::Result<EpochState> {
        // create EVM with the state at the pinned header
        let state_provider = self.inner.blockchain_provider.state_by_block_hash(header.hash())?;
        let state = StateProviderDatabase::new(&state_provider);
        let mut db = State::builder().with_database(state).with_bundle_update().build();
        debug!(target: "engine", state=?db.bundle_state, hashes=?db.block_hashes, "retrieving epoch state at header");
        let mut tn_evm = self
            .inner
            .evm_config
            .evm_factory()
            .create_evm(&mut db, self.inner.evm_config.evm_env(header)?);

        // current epoch number
        let epoch = self.get_current_epoch_number(&mut tn_evm)?;

        // current epoch info
        let epoch_info = self.get_current_epoch_info(&mut tn_evm)?;
        debug!(target: "engine", ?epoch, ?epoch_info, "retrieved epoch info at header");

        // retrieve closing timestamp for previous epoch
        let epoch_start = self
            .header_by_number(epoch_info.blockHeight.saturating_sub(1))?
            .ok_or_eyre("failed to retrieve closing epoch information")?
            .timestamp;

        // retrieve the committee
        let validators = self.get_committee_validators_by_epoch(epoch, &mut tn_evm)?;
        let bls_pubkeys = self.get_committee_bls_pubkeys_by_epoch(epoch, &mut tn_evm)?;
        let epoch_state = EpochState { epoch, epoch_info, validators, bls_pubkeys, epoch_start };
        debug!(target: "engine", ?epoch_state, "returning epoch state at header");

        Ok(epoch_state)
    }

    /// Read the CURRENT epoch's [`EpochState`] pinned to the epoch's start state — the previous
    /// epoch's closing block (genesis for epoch 0) — returning the pin header alongside it.
    ///
    /// Bootstrapping reads the current epoch number and its
    /// [`EpochInfo`](ConsensusRegistry::EpochInfo) at the canonical tip. This is deterministic at
    /// ANY tip because `concludeEpoch` writes the epoch number and the `EpochInfo` scalars this
    /// method consumes (`blockHeight`) exactly once at the boundary and never mid-epoch — only
    /// the committee ARRAYS mutate mid-epoch (governance `burn` swap-and-pops immediately,
    /// including the committee embedded in `EpochInfo`), which is why the full state read below
    /// is pinned instead of taken at the tip.
    ///
    /// The pin: `epoch_info.blockHeight` is the entered epoch's first block, so `blockHeight - 1`
    /// is the previous epoch's closing block — the block whose execution ran `concludeEpoch` and
    /// seated the entered committee. For epoch 0 the pin is genesis, whose execution seeds the
    /// registry: genesis state IS epoch 0's start state.
    ///
    /// Every field of the returned state derives from the previous epoch's closing block, so any
    /// node entering (or re-entering) the same epoch — fresh boundary crossing, crash-restart
    /// replay, or ModeChange re-entry, before or after a mid-epoch burn — derives an IDENTICAL
    /// epoch view (committee membership included).
    pub fn epoch_state_at_epoch_start(&self) -> eyre::Result<(EpochState, SealedHeader)> {
        // boundary-written-once identity read: deterministic at any tip
        let (epoch, epoch_info) = self.get_current_epoch_info_at_header(&self.canonical_tip())?;

        let pin_header = if epoch == 0 {
            self.sealed_header_by_number(0)?.ok_or_else(|| eyre::eyre!("missing genesis header"))?
        } else {
            let closing_number = epoch_info
                .blockHeight
                .checked_sub(1)
                .ok_or_else(|| eyre::eyre!("current epoch {epoch} reports blockHeight 0"))?;
            self.sealed_header_by_number(closing_number)?.ok_or_else(|| {
                eyre::eyre!("missing closing block {closing_number} for current epoch {epoch}")
            })?
        };
        debug!(
            target: "engine",
            ?epoch,
            pin_number = pin_header.number,
            pin_hash = ?pin_header.hash(),
            "retrieving epoch state at epoch start"
        );

        let state = self.epoch_state_at_header(&pin_header)?;

        // Tripwire: `concludeEpoch` executes INSIDE the closing block, so the registry at the
        // closing header already reports the entered epoch. This can only fire if the registry's
        // `blockHeight == closing block + 1` convention ever breaks — running a stale committee
        // is a consensus-safety failure, so fail hard instead of returning the mismatched state.
        if state.epoch != epoch {
            error!(
                target: "engine",
                pin_number = pin_header.number,
                pinned_epoch = state.epoch,
                tip_epoch = epoch,
                "epoch-start pin and canonical tip disagree on the current epoch"
            );
            return Err(eyre::eyre!(
                "epoch state pinned to block {} reports epoch {} but the canonical tip reports \
                 epoch {epoch}",
                pin_header.number,
                state.epoch
            ));
        }

        Ok((state, pin_header))
    }

    /// Read the latest committee and epoch information from the [ConsensusRegistry] on-chain.
    pub fn validators_for_epoch(
        &self,
        epoch: u32,
    ) -> eyre::Result<Vec<ConsensusRegistry::ValidatorInfo>> {
        debug!(target: "engine", "retrieving validators for epoch {epoch}");
        let calldata = ConsensusRegistry::getCommitteeValidatorsCall { epoch }.abi_encode().into();
        self.read_consensus_registry(calldata).map_err(Into::into)
    }

    /// Read the BLS pubkeys for the committee of the provided epoch from the [ConsensusRegistry]
    /// on-chain.
    pub fn bls_pubkeys_for_epoch(&self, epoch: u32) -> eyre::Result<Vec<alloy::primitives::Bytes>> {
        let calldata = ConsensusRegistry::getCommitteeBlsPubkeysCall { epoch }.abi_encode().into();
        self.read_consensus_registry(calldata).map_err(Into::into)
    }

    /// Read the BLS pubkeys for the committee of the provided epoch from the
    /// [ConsensusRegistry], pinned to the state of the block identified by `block_hash`.
    ///
    /// Unlike [`Self::bls_pubkeys_for_epoch`], which reads the mutable canonical tip, every node
    /// issuing this read at the same block decodes the identical key set — even after a
    /// mid-epoch governance `burn` swap-and-pops the stored committee arrays.
    pub fn bls_pubkeys_for_epoch_at_block(
        &self,
        epoch: u32,
        block_hash: B256,
    ) -> eyre::Result<Vec<alloy::primitives::Bytes>> {
        let header = self
            .sealed_header_by_hash(block_hash)?
            .ok_or_else(|| eyre::eyre!("sealed header not found for block hash {block_hash:?}"))?;
        let calldata = ConsensusRegistry::getCommitteeBlsPubkeysCall { epoch }.abi_encode().into();
        self.read_consensus_registry_at_header(&header, calldata).map_err(Into::into)
    }

    /// Read the [`ConsensusRegistry`] [`EpochInfo`](ConsensusRegistry::EpochInfo) for `epoch` at
    /// the block identified by `block_hash`.
    ///
    /// Builds an EVM pinned to `block_hash`'s state and issues a single `getEpochInfo(uint32)`
    /// call. The registry keeps a ring buffer of the four most recent epochs and reverts
    /// (`InvalidEpoch`) for anything outside it, so a successful return is guaranteed to be the
    /// requested epoch's record. Used by the epoch manager to recover the previous epoch's block
    /// range from its closing block when deriving next-epoch base fees.
    ///
    /// Fails with a descriptive error if `block_hash` does not resolve to a sealed header or the
    /// registry call does not succeed.
    pub fn get_epoch_info_at_block(
        &self,
        epoch: Epoch,
        block_hash: B256,
    ) -> eyre::Result<ConsensusRegistry::EpochInfo> {
        let header = self
            .sealed_header_by_hash(block_hash)?
            .ok_or_else(|| eyre::eyre!("sealed header not found for block hash {block_hash:?}"))?;
        let state_provider = self.inner.blockchain_provider.state_by_block_hash(header.hash())?;
        let state = StateProviderDatabase::new(&state_provider);
        let mut db = State::builder().with_database(state).with_bundle_update().build();
        let mut tn_evm = self
            .inner
            .evm_config
            .evm_factory()
            .create_evm(&mut db, self.inner.evm_config.evm_env(&header)?);

        let calldata = ConsensusRegistry::getEpochInfoCall { epoch }.abi_encode().into();
        self.call_consensus_registry::<_, ConsensusRegistry::EpochInfo>(&mut tn_evm, calldata)
            .map_err(Into::into)
    }

    /// Read worker fee configs from the [`WorkerConfigs`] contract at the block identified by
    /// `block_hash`.
    ///
    /// Returns the on-chain worker count and one [`WorkerFeeConfig`] per worker. Failures are
    /// classified per [`StateReadError`] (see [`Self::worker_fee_configs_inner`]); `block_hash`
    /// failing to resolve to a sealed header is [`StateReadError::Provider`] — the pinned block
    /// exists on the committee by construction, so a miss reflects this node's local view, not a
    /// chain-global fact.
    pub fn get_worker_fee_configs_at_block(
        &self,
        block_hash: B256,
    ) -> StateReadResult<(usize, Vec<WorkerFeeConfig>)> {
        let header = self
            .sealed_header_by_hash(block_hash)
            .map_err(|e| {
                StateReadError::Provider(format!("header lookup for {block_hash:?}: {e}"))
            })?
            .ok_or_else(|| {
                StateReadError::Provider(format!(
                    "sealed header not found for block hash {block_hash:?}"
                ))
            })?;
        self.worker_fee_configs_inner(&header)
    }

    /// Read fee configs for all workers from the [`WorkerConfigs`] contract at the given header.
    ///
    /// Builds an EVM against `header`'s state and issues a single `getAllWorkerConfigs()` call.
    /// Returns the on-chain worker count alongside the decoded [`WorkerFeeConfig`]s.
    ///
    /// Failures are classified per [`StateReadError`]. The classification boundary: the state
    /// provider construction and any database fault the EVM hits while lazily reading state are
    /// [`StateReadError::Provider`] (node-local — peers reading the same block may succeed);
    /// everything downstream of a successfully-executing EVM — contract absent (empty return
    /// data), revert, halt, ABI decode, arity mismatch — plus EVM environment construction is
    /// [`StateReadError::ChainGlobal`] (a deterministic product of the pinned block, identical on
    /// every node).
    pub(crate) fn worker_fee_configs_inner(
        &self,
        header: &SealedHeader,
    ) -> StateReadResult<(usize, Vec<WorkerFeeConfig>)> {
        let state_provider =
            self.inner.blockchain_provider.state_by_block_hash(header.hash()).map_err(|e| {
                StateReadError::Provider(format!("state provider at {}: {e}", header.hash()))
            })?;
        let state = StateProviderDatabase::new(&state_provider);
        let mut db = State::builder().with_database(state).with_bundle_update().build();
        let evm_env = self.inner.evm_config.evm_env(header).map_err(|e| {
            StateReadError::ChainGlobal(format!("evm env for {}: {e}", header.hash()))
        })?;
        let mut tn_evm = self.inner.evm_config.evm_factory().create_evm(&mut db, evm_env);

        let calldata = WorkerConfigs::getAllWorkerConfigsCall {}.abi_encode().into();
        let result = Self::classified_system_call(
            &mut tn_evm,
            SYSTEM_ADDRESS,
            WORKER_CONFIGS_ADDRESS,
            calldata,
        )?;
        let data = match result.result {
            ExecutionResult::Success { output, .. } => output.into_data(),
            e => {
                return Err(StateReadError::ChainGlobal(format!(
                    "failed to read worker configs: {e:?}"
                )))
            }
        };
        let ret =
            <WorkerConfigs::getAllWorkerConfigsCall as alloy::sol_types::SolCall>::abi_decode_returns(
                &data,
            )
            .map_err(|e| {
                StateReadError::ChainGlobal(format!(
                    "worker configs return decode failed (contract absent at this block?): {e}"
                ))
            })?;

        let num_workers = ret.count as usize;
        if ret.strategies.len() != num_workers
            || ret.values.len() != num_workers
            || ret.datas.len() != num_workers
        {
            return Err(StateReadError::ChainGlobal(format!(
                "worker config arity mismatch: count={num_workers}, strategies={}, values={}, datas={}",
                ret.strategies.len(),
                ret.values.len(),
                ret.datas.len(),
            )));
        }

        let mut configs = Vec::with_capacity(num_workers);
        for (worker_id, (&strategy, &value)) in
            ret.strategies.iter().zip(ret.values.iter()).enumerate()
        {
            let config = match strategy {
                0 => WorkerFeeConfig::Eip1559 { target_gas: value },
                1 => WorkerFeeConfig::Static { fee: value },
                s => {
                    // The contract rejects unknown strategies, so this branch only fires when a
                    // future contract version introduces a strategy this node hasn't been
                    // updated to understand. Fall back to EIP-1559 to preserve liveness instead
                    // of halting all validators.
                    tracing::warn!(
                        target: "tn::reth",
                        worker_id,
                        strategy = s,
                        "unknown fee strategy; falling back to strategy 0 (Eip1559)"
                    );
                    WorkerFeeConfig::Eip1559 { target_gas: value }
                }
            };
            configs.push(config);
        }

        Ok((num_workers, configs))
    }

    /// Build an EVM at the canonical tip, execute a read-only [ConsensusRegistry] call, and
    /// decode the returned data to `T`.
    ///
    /// Convenience wrapper over [`Self::read_consensus_registry_batch`] for the common
    /// single-read case (one pinned EVM, one call).
    pub fn read_consensus_registry<T>(&self, calldata: Bytes) -> EvmReadResult<T>
    where
        T: alloy::sol_types::SolValue,
        T: From<
            <<T as alloy::sol_types::SolValue>::SolType as alloy::sol_types::SolType>::RustType,
        >,
    {
        self.read_consensus_registry_batch(vec![calldata])?.pop().ok_or_else(|| {
            EvmReadError::Internal("consensus registry batch read returned no result".into())
        })
    }

    /// Build an EVM at `header`'s state, execute a read-only [ConsensusRegistry] call, and
    /// decode the returned data to `T`.
    ///
    /// Convenience wrapper over [`Self::read_consensus_registry_batch_at_header`] for the common
    /// single-read case (one pinned EVM, one call).
    pub fn read_consensus_registry_at_header<T>(
        &self,
        header: &SealedHeader,
        calldata: Bytes,
    ) -> EvmReadResult<T>
    where
        T: alloy::sol_types::SolValue,
        T: From<
            <<T as alloy::sol_types::SolValue>::SolType as alloy::sol_types::SolType>::RustType,
        >,
    {
        self.read_consensus_registry_batch_at_header(header, vec![calldata])?.pop().ok_or_else(
            || EvmReadError::Internal("consensus registry batch read returned no result".into()),
        )
    }

    /// Build a single EVM at the canonical tip and execute several read-only [ConsensusRegistry]
    /// calls against it, decoding each result to `T`.
    ///
    /// Thin specialization of [`Self::read_consensus_registry_batch_at_header`] that pins the
    /// batch to the canonical tip.
    pub fn read_consensus_registry_batch<T>(&self, calldatas: Vec<Bytes>) -> EvmReadResult<Vec<T>>
    where
        T: alloy::sol_types::SolValue,
        T: From<
            <<T as alloy::sol_types::SolValue>::SolType as alloy::sol_types::SolType>::RustType,
        >,
    {
        let canonical_tip = self.canonical_tip();
        debug!(target: "engine", ?canonical_tip, "reading consensus registry batch at canonical tip");
        self.read_consensus_registry_batch_at_header(&canonical_tip, calldatas)
    }

    /// Build a single EVM at `header`'s state and execute several read-only [ConsensusRegistry]
    /// calls against it, decoding each result to `T`.
    ///
    /// Every calldata in a batch must decode to the same Solidity type `T` (current caller: five
    /// `getValidatorsInfo(status)` reads → `Vec<ValidatorInfo>`).
    ///
    /// All calls observe ONE pinned state snapshot, so a multi-call query (e.g. unioning
    /// per-status validator sets) cannot straddle a block commit and double-count or drop a
    /// validator that changes status between reads. Each call still runs under its own fresh 30M
    /// gas budget (`transact_system_call`), so splitting a large query across calls keeps gas
    /// bounded per call.
    pub fn read_consensus_registry_batch_at_header<T>(
        &self,
        header: &SealedHeader,
        calldatas: Vec<Bytes>,
    ) -> EvmReadResult<Vec<T>>
    where
        T: alloy::sol_types::SolValue,
        T: From<
            <<T as alloy::sol_types::SolValue>::SolType as alloy::sol_types::SolType>::RustType,
        >,
    {
        // Create EVM with the state at the pinned header.
        //
        // ARCHIVE-MODE ASSUMPTION: this node never constructs a pruner (`PruningArgs` are built
        // with every field disabled and no `PrunerBuilder` exists in the repo), so
        // `state_by_block_hash` always resolves fully indexed history. If pruning is ever
        // enabled, reth's `HistoricalStateProvider` can hit a missing history shard and return
        // `HistoryInfo::MaybeInPlainState`, silently falling back to TIP state for this
        // "pinned" read — exactly the nondeterminism pinning exists to prevent. Revisit every
        // pinned registry read before enabling pruning.
        let state_provider = self
            .inner
            .blockchain_provider
            .state_by_block_hash(header.hash())
            .map_err(|e| EvmReadError::Internal(e.to_string()))?;
        let state = StateProviderDatabase::new(&state_provider);
        let mut db = State::builder().with_database(state).with_bundle_update().build();
        let evm_env = self
            .inner
            .evm_config
            .evm_env(header)
            .map_err(|e| EvmReadError::Internal(e.to_string()))?;
        let mut tn_evm = self.inner.evm_config.evm_factory().create_evm(&mut db, evm_env);

        // reuse the one pinned EVM for every read; `call_consensus_registry` is non-committing,
        // so each read sees the same base state.
        calldatas
            .into_iter()
            .map(|calldata| self.call_consensus_registry(&mut tn_evm, calldata))
            .collect()
    }
    /// Extract the epoch number from a header's nonce.
    pub fn extract_epoch_from_header(header: &ExecHeader) -> Epoch {
        let nonce: u64 = header.nonce.into();
        (nonce >> 32) as u32
    }

    /// Read the curret epoch number from the [ConsensusRegistry] on-chain.
    fn get_current_epoch_number<DB>(&self, evm: &mut TNEvm<DB>) -> EvmReadResult<u32>
    where
        DB: alloy_evm::Database,
    {
        let calldata = ConsensusRegistry::getCurrentEpochCall {}.abi_encode().into();
        self.call_consensus_registry::<_, u32>(evm, calldata)
    }

    /// Read the curret epoch info from the [ConsensusRegistry] on-chain.
    fn get_current_epoch_info<DB>(
        &self,
        evm: &mut TNEvm<DB>,
    ) -> EvmReadResult<ConsensusRegistry::EpochInfo>
    where
        DB: alloy_evm::Database,
    {
        let calldata = ConsensusRegistry::getCurrentEpochInfoCall {}.abi_encode().into();
        self.call_consensus_registry::<_, ConsensusRegistry::EpochInfo>(evm, calldata)
    }

    /// Retrieve all `ValidatorInfo` in the committee for the provided epoch.
    fn get_committee_validators_by_epoch<DB>(
        &self,
        epoch: Epoch,
        evm: &mut TNEvm<DB>,
    ) -> EvmReadResult<Vec<ConsensusRegistry::ValidatorInfo>>
    where
        DB: alloy_evm::Database,
    {
        let calldata = ConsensusRegistry::getCommitteeValidatorsCall { epoch }.abi_encode().into();
        self.call_consensus_registry::<_, Vec<ConsensusRegistry::ValidatorInfo>>(evm, calldata)
    }

    /// Retrieve BLS pubkeys for the committee of the provided epoch.
    fn get_committee_bls_pubkeys_by_epoch<DB>(
        &self,
        epoch: Epoch,
        evm: &mut TNEvm<DB>,
    ) -> EvmReadResult<Vec<alloy::primitives::Bytes>>
    where
        DB: alloy_evm::Database,
    {
        let calldata = ConsensusRegistry::getCommitteeBlsPubkeysCall { epoch }.abi_encode().into();
        self.call_consensus_registry::<_, Vec<alloy::primitives::Bytes>>(evm, calldata)
    }
    /// Read the CURRENT epoch number and [`EpochInfo`](ConsensusRegistry::EpochInfo) from the
    /// [`ConsensusRegistry`] at `header`, with failures classified per [`StateReadError`].
    ///
    /// This is the close-time identity read for the epoch manager's `adjust_base_fees`: at an
    /// epoch's closing block the registry state has already crossed to the entered epoch
    /// (`concludeEpoch` ran inside that block), so the returned info is the entered epoch's record
    /// and its `blockHeight` must equal `header.number + 1`. It reads exactly what
    /// [`Self::epoch_state_at_header`] reads for the same check but skips the committee/BLS/
    /// epoch-start lookups (a gating check needs only the epoch identity) and — unlike that
    /// method, whose failures collapse into `eyre` strings — keeps node-local provider faults
    /// (NOT committee-deterministic: retry or halt) distinguishable from chain-global failures
    /// (committee-deterministic: fail-open stays consistent).
    pub fn get_current_epoch_info_at_header(
        &self,
        header: &SealedHeader,
    ) -> StateReadResult<(Epoch, ConsensusRegistry::EpochInfo)> {
        let state_provider =
            self.inner.blockchain_provider.state_by_block_hash(header.hash()).map_err(|e| {
                StateReadError::Provider(format!("state provider at {}: {e}", header.hash()))
            })?;
        let state = StateProviderDatabase::new(&state_provider);
        let mut db = State::builder().with_database(state).with_bundle_update().build();
        let evm_env = self.inner.evm_config.evm_env(header).map_err(|e| {
            StateReadError::ChainGlobal(format!("evm env for {}: {e}", header.hash()))
        })?;
        let mut tn_evm = self.inner.evm_config.evm_factory().create_evm(&mut db, evm_env);

        // both reads observe the ONE pinned EVM state
        let epoch: Epoch = Self::classified_registry_read(
            &mut tn_evm,
            ConsensusRegistry::getCurrentEpochCall {}.abi_encode().into(),
        )?;
        let epoch_info: ConsensusRegistry::EpochInfo = Self::classified_registry_read(
            &mut tn_evm,
            ConsensusRegistry::getCurrentEpochInfoCall {}.abi_encode().into(),
        )?;
        Ok((epoch, epoch_info))
    }

    /// Execute a read-only [`ConsensusRegistry`] call on `evm` and decode the result, classifying
    /// failures per [`StateReadError`].
    ///
    /// The [`StateReadError`]-typed sibling of [`Self::call_consensus_registry`]: revert, halt,
    /// and decode failures are all deterministic products of the pinned chain state
    /// ([`StateReadError::ChainGlobal`]); only a database fault inside the EVM (via
    /// [`Self::classified_system_call`]) is node-local ([`StateReadError::Provider`]).
    fn classified_registry_read<DB, T>(evm: &mut TNEvm<DB>, calldata: Bytes) -> StateReadResult<T>
    where
        DB: alloy_evm::Database,
        T: alloy::sol_types::SolValue,
        T: From<
            <<T as alloy::sol_types::SolValue>::SolType as alloy::sol_types::SolType>::RustType,
        >,
    {
        let state = Self::classified_system_call(
            evm,
            SYSTEM_ADDRESS,
            CONSENSUS_REGISTRY_ADDRESS,
            calldata,
        )?;
        match state.result {
            ExecutionResult::Success { output, .. } => {
                alloy::sol_types::SolValue::abi_decode(&output.into_data()).map_err(|e| {
                    StateReadError::ChainGlobal(format!(
                        "registry return decode failed (contract absent at this block?): {e}"
                    ))
                })
            }
            ExecutionResult::Revert { output, .. } => Err(StateReadError::ChainGlobal(format!(
                "registry call reverted: {:?}",
                alloy::sol_types::decode_revert_reason(&output)
            ))),
            ExecutionResult::Halt { reason, gas_used } => Err(StateReadError::ChainGlobal(
                format!("registry call halted: {reason:?} (gas {gas_used})"),
            )),
        }
    }

    /// Execute a read-only system call on `evm`, classifying failures per [`StateReadError`].
    ///
    /// An [`EVMError::Database`] is a node-local provider fault surfaced by the EVM's lazy state
    /// reads (the state provider is only CONSTRUCTED up front; account/storage/bytecode loads
    /// happen during execution, so an MDBX/provider I/O fault lands here) — classified
    /// [`StateReadError::Provider`]. Every other transact failure derives deterministically from
    /// the pinned block and calldata, so it is [`StateReadError::ChainGlobal`].
    fn classified_system_call<DB>(
        evm: &mut TNEvm<DB>,
        caller: Address,
        contract: Address,
        calldata: Bytes,
    ) -> StateReadResult<ResultAndState>
    where
        DB: alloy_evm::Database,
    {
        evm.transact_system_call(caller, contract, calldata).map_err(|e| match e {
            EVMError::Database(db_err) => {
                StateReadError::Provider(format!("system call state read failed: {db_err}"))
            }
            other => {
                StateReadError::ChainGlobal(format!("system call failed reading state: {other}"))
            }
        })
    }

    /// Helper function to call `ConsensusRegistry` state on-chain.
    pub(crate) fn call_consensus_registry<DB, T>(
        &self,
        evm: &mut TNEvm<DB>,
        calldata: Bytes,
    ) -> EvmReadResult<T>
    where
        DB: alloy_evm::Database,
        T: alloy::sol_types::SolValue,
        T: From<
            <<T as alloy::sol_types::SolValue>::SolType as alloy::sol_types::SolType>::RustType,
        >,
    {
        let state = self
            .read_state_on_chain(evm, SYSTEM_ADDRESS, CONSENSUS_REGISTRY_ADDRESS, calldata)
            .map_err(|e| EvmReadError::Internal(e.to_string()))?;

        // retrieve data from state, distinguishing user-triggerable reverts from node faults
        match state.result {
            ExecutionResult::Success { output, .. } => {
                let data = output.into_data();
                // use SolValue to decode the result
                alloy::sol_types::SolValue::abi_decode(&data).map_err(|e| {
                    EvmReadError::Internal(format!("registry return decode failed: {e}"))
                })
            }
            ExecutionResult::Revert { output, .. } => Err(EvmReadError::Revert {
                reason: alloy::sol_types::decode_revert_reason(&output),
                output,
            }),
            ExecutionResult::Halt { reason, gas_used } => Err(EvmReadError::Internal(format!(
                "registry call halted: {reason:?} (gas {gas_used})"
            ))),
        }
    }

    /// Read state on-chain.
    pub(crate) fn read_state_on_chain<DB>(
        &self,
        evm: &mut TNEvm<DB>,
        caller: Address,
        contract: Address,
        calldata: Bytes,
    ) -> TnRethResult<ResultAndState>
    where
        DB: alloy_evm::Database,
    {
        // read from state
        let res = match evm.transact_system_call(caller, contract, calldata) {
            Ok(res) => res,
            Err(e) => {
                // fatal error
                error!(target: "engine", ?caller, ?contract, "failed to read state: {}", e);
                return Err(TnRethError::EVMCustom(format!(
                    "system call failed reading state: {e}"
                )));
            }
        };

        Ok(res)
    }
}
