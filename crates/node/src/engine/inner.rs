//! Inner-execution node components for both Worker and Primary execution.
//!
//! This module contains the logic for execution.

use crate::error::ExecutionError;
use eyre::OptionExt;
use jsonrpsee::http_client::HttpClient;
use std::{net::SocketAddr, sync::Arc};
use tn_batch_builder::BatchBuilder;
use tn_batch_validator::BatchValidator;
use tn_config::Config;
use tn_engine::ExecutorEngine;
use tn_faucet::{FaucetArgs, FaucetRpcExtApiServer as _};
use tn_reth::{
    system_calls::EpochState,
    worker::{WorkerComponents, WorkerNetwork},
    RethEnv, RpcServerHandle, WorkerTxPool,
};
use tn_rpc::{EngineToPrimary, TelcoinNetworkRpcExt, TelcoinNetworkRpcExtApiServer};
use tn_types::{
    gas_accumulator::{BaseFeeContainer, GasAccumulator},
    Address, BatchSender, BatchValidation, ConsensusOutput, ExecHeader, Noticer, SealedHeader,
    TaskSpawner, WorkerId, B256, MIN_PROTOCOL_BASE_FEE,
};
use tn_worker::WorkerNetworkHandle;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

/// Inner type for holding execution layer types.
#[derive(Debug)]
pub(super) struct ExecutionNodeInner {
    /// The [Address] for the authority used as the suggested beneficiary.
    ///
    /// The address refers to the execution layer's address
    /// based on the authority's secp256k1 public key.
    pub(super) address: Address,
    /// The validator node config.
    pub(super) tn_config: Config,
    /// Reth execution environment.
    pub(super) reth_env: RethEnv,
    /// Optional args to turn on faucet (for testnet only).
    pub(super) opt_faucet_args: Option<FaucetArgs>,
    /// Collection of execution components by worker.
    /// Index of vec is worker id.
    pub(super) workers: Vec<WorkerComponents>,
}

impl ExecutionNodeInner {
    /// Spawn tasks associated with executing output from consensus.
    ///
    /// The method is consumed by [PrimaryNodeInner::start].
    /// All tasks are spawned with the [ExecutionNodeInner]'s [TaskManager].
    pub(super) async fn start_engine(
        &self,
        rx_output: mpsc::Receiver<ConsensusOutput>,
        rx_shutdown: Noticer,
        gas_accumulator: GasAccumulator,
    ) -> eyre::Result<()> {
        let parent_header = self.reth_env.lookup_head()?;

        // spawn execution engine to extend canonical tip
        let tn_engine = ExecutorEngine::new(
            self.reth_env.clone(),
            self.reth_env.get_debug_max_round(),
            rx_output,
            parent_header,
            rx_shutdown,
            self.reth_env.get_task_spawner().clone(),
            gas_accumulator,
        );

        // spawn tn engine
        self.reth_env.get_task_spawner().spawn_critical_task("consensus engine", async move {
            let res = tn_engine.await;
            match res {
                Ok(_) => info!(target: "engine", "TN Engine exited gracefully"),
                Err(e) => error!(target: "engine", ?e, "TN Engine error"),
            }
        });

        Ok(())
    }

    /// The worker's RPC, TX pool, and block builder
    pub(super) async fn start_batch_builder(
        &mut self,
        worker_id: WorkerId,
        block_provider_sender: BatchSender,
        epoch_task_spawner: &TaskSpawner,
        base_fee: BaseFeeContainer,
    ) -> eyre::Result<()> {
        // check for worker components and initialize if they're missing
        let transaction_pool = self
            .workers
            .get(worker_id as usize)
            .ok_or_eyre("worker components missing for {worker_id}")?
            .pool();

        // create the batch builder for this epoch
        let batch_builder = BatchBuilder::new(
            &self.reth_env,
            transaction_pool.clone(),
            block_provider_sender,
            self.address,
            self.tn_config.parameters.max_batch_delay,
            epoch_task_spawner.clone(),
            worker_id,
            base_fee,
        );

        // spawn block builder task
        epoch_task_spawner.spawn_critical_task("batch builder", async move {
            let res = batch_builder.await;
            info!(target: "tn::execution", ?res, "batch builder task exited");
        });

        Ok(())
    }

    /// Initialize the worker's transaction pool and public RPC.
    /// Must call this function in accending worker_id order or will panic,
    /// for instance call for worker id 0, then 1, etc.
    pub(super) async fn initialize_worker_components<EP>(
        &mut self,
        worker_id: WorkerId,
        network_handle: WorkerNetworkHandle,
        engine_to_primary: EP,
    ) -> eyre::Result<()>
    where
        EP: EngineToPrimary + Send + Sync + 'static,
    {
        let transaction_pool = self.reth_env.init_txn_pool()?;

        let network =
            WorkerNetwork::new(self.reth_env.chainspec(), network_handle, self.tn_config.version);
        let mut tx_pool_latest = transaction_pool.block_info();
        tx_pool_latest.pending_basefee = MIN_PROTOCOL_BASE_FEE;
        let last_seen = self.reth_env.finalized_block_hash_number_for_startup()?;
        tx_pool_latest.last_seen_block_hash = last_seen.hash;
        tx_pool_latest.last_seen_block_number = last_seen.number;
        transaction_pool.set_block_info(tx_pool_latest);

        // extend TN namespace
        let tn_ext = TelcoinNetworkRpcExt::new(self.reth_env.chainspec(), engine_to_primary);
        let mut server = self.reth_env.get_rpc_server(
            transaction_pool.clone(),
            network.clone(),
            tn_ext.into_rpc(),
        );

        info!(target: "tn::execution", "tn rpc extension successfully merged");

        // extend faucet namespace if included
        if let Some(faucet_args) = self.opt_faucet_args.take() {
            // create extension from CLI args
            match faucet_args.create_rpc_extension(self.reth_env.clone(), transaction_pool.clone())
            {
                Ok(faucet_ext) => {
                    // add faucet module
                    if let Err(e) = server.merge_configured(faucet_ext.into_rpc()) {
                        error!(target: "faucet", "Error merging faucet rpc module: {e:?}");
                    }

                    info!(target: "tn::execution", "faucet rpc extension successfully merged");
                }
                Err(e) => {
                    error!(target: "faucet", "Error creating faucet rpc module: {e:?}");
                }
            }
        }

        // start the RPC server
        let rpc_handle = self.reth_env.start_rpc(&server).await?;

        // take ownership of worker components
        let components = WorkerComponents::new(rpc_handle, transaction_pool, network);
        // Must call this function in accending worker_id order or will panic.
        if worker_id as usize != self.workers.len() {
            panic!("initialize_worker_components not called with sequencial worker ids!")
        }
        self.workers.push(components);
        Ok(())
    }

    /// Respawn any tasks on the worker network when we get a new epoch task manager.
    ///
    /// This method should be called on epoch rollover.
    /// Will take care of all workers.
    pub async fn respawn_worker_network_tasks(&self, network_handle: WorkerNetworkHandle) {
        for worker in &self.workers {
            worker.worker_network().respawn_peer_count(network_handle.clone());
        }
    }

    /// Create a new block validator.
    pub(super) fn new_batch_validator(
        &self,
        worker_id: &WorkerId,
        base_fee: BaseFeeContainer,
    ) -> Arc<dyn BatchValidation> {
        // retrieve handle to transaction pool to submit gossip transactions to validators
        let tx_pool = self.workers.get(*worker_id as usize).map(|w| w.pool());

        Arc::new(BatchValidator::new(self.reth_env.clone(), tx_pool, *worker_id, base_fee))
    }

    /// Fetch the last executed state from the database.
    ///
    /// This method is called when the primary spawns to retrieve
    /// the last committed sub dag from it's database in the case
    /// of the node restarting.
    ///
    /// This returns the hash of the last executed ConsensusHeader on the consensus chain.
    /// since the execution layer is confirming the last executing block.
    pub(super) fn last_executed_output(&self) -> eyre::Result<B256> {
        // NOTE: The payload_builder only extends canonical tip and sets finalized after
        // entire output is successfully executed. This ensures consistent recovery state.
        //
        // For example: consensus round 8 sends an output with 5 blocks, but only 2 blocks are
        // executed before the node restarts. The provider never finalized the round, so the
        // `finalized_block_number` would point to the last block of round 7. The primary
        // would then re-send consensus output for round 8.
        //
        // recover finalized block's nonce: this is the last subdag index from consensus (round)
        let finalized_block_num = self.reth_env.last_finalized_block_number()?;
        let last_round_of_consensus = self
            .reth_env
            .header_by_number(finalized_block_num)?
            .map(|opt| opt.parent_beacon_block_root.unwrap_or_default())
            .unwrap_or_else(Default::default);

        Ok(last_round_of_consensus)
    }

    /// Return a vector of the last 'number' executed block headers.
    pub(super) fn last_executed_blocks(&self, number: u64) -> eyre::Result<Vec<ExecHeader>> {
        let finalized_block_num = self.reth_env.last_finalized_block_number()?;
        let start_num = finalized_block_num.saturating_sub(number);
        let mut result = Vec::with_capacity(number as usize);
        if start_num < finalized_block_num {
            for block_num in start_num + 1..=finalized_block_num {
                if let Some(header) = self.reth_env.header_by_number(block_num)? {
                    result.push(header);
                }
            }
        }

        Ok(result)
    }

    /// Return a vector of the last 'number' executed block headers.
    /// These are the execution blocks finalized after consensus output, i.e. it
    /// skips all the "intermediate" blocks and is just the final block from a consensus output.
    pub(super) fn last_executed_output_blocks(
        &self,
        number: u64,
    ) -> eyre::Result<Vec<SealedHeader>> {
        let last_block_number = self.reth_env.last_block_number()?;
        debug!(target: "epoch-manager", ?last_block_number, "restoring last executed output blocks");
        let mut result = Vec::with_capacity(number as usize);
        if number > 0 {
            let mut block_num = last_block_number;
            let mut last_nonce;
            if let Some(header) = self.reth_env.sealed_header_by_number(block_num)? {
                last_nonce = header.nonce;
                result.push(header);
            } else {
                return Err(eyre::Error::msg(format!("Unable to read block {block_num}")));
            }
            let mut blocks = 1;
            while blocks < number {
                if block_num == 0 {
                    break;
                }
                block_num -= 1;
                if let Some(header) = self.reth_env.sealed_header_by_number(block_num)? {
                    if header.nonce != last_nonce {
                        last_nonce = header.nonce;
                        result.push(header);
                        blocks += 1;
                    }
                } else {
                    return Err(eyre::Error::msg(format!("Unable to read block {block_num}")));
                }
            }
        }
        result.reverse();
        Ok(result)
    }

    /// Return an database provider.
    pub(super) fn get_reth_env(&self) -> RethEnv {
        self.reth_env.clone()
    }

    /// Return a worker's RpcServerHandle if the RpcServer exists.
    pub(super) fn worker_rpc_handle(&self, worker_id: &WorkerId) -> eyre::Result<&RpcServerHandle> {
        let handle = self
            .workers
            .get(*worker_id as usize)
            .ok_or(ExecutionError::WorkerNotFound(worker_id.to_owned()))?
            .rpc_handle();
        Ok(handle)
    }

    /// Return a worker's HttpClient if the RpcServer exists.
    pub(super) fn worker_http_client(
        &self,
        worker_id: &WorkerId,
    ) -> eyre::Result<Option<HttpClient>> {
        let handle = self.worker_rpc_handle(worker_id)?.http_client();
        Ok(handle)
    }

    /// Return a worker's transaction pool if it exists.
    pub(super) fn get_worker_transaction_pool(
        &self,
        worker_id: &WorkerId,
    ) -> eyre::Result<WorkerTxPool> {
        let tx_pool = self
            .workers
            .get(*worker_id as usize)
            .ok_or(ExecutionError::WorkerNotFound(worker_id.to_owned()))?
            .pool();

        Ok(tx_pool)
    }

    /// Return a worker's local Http address if the RpcServer exists.
    pub(super) fn worker_http_local_address(
        &self,
        worker_id: &WorkerId,
    ) -> eyre::Result<Option<SocketAddr>> {
        let addr = self.worker_rpc_handle(worker_id)?.http_local_addr();
        Ok(addr)
    }

    /// Read [EpochState] from the canonical tip.
    pub fn epoch_state_from_canonical_tip(&self) -> eyre::Result<EpochState> {
        self.reth_env.epoch_state_from_canonical_tip()
    }
}
