//! Inner-execution node components for both Worker and Primary execution.
//!
//! This module contains the logic for execution.

use crate::error::ExecutionError;
use jsonrpsee::http_client::HttpClient;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tn_batch_builder::BatchBuilder;
use tn_batch_validator::BatchValidator;
use tn_config::Config;
use tn_engine::ExecutorEngine;
use tn_faucet::{FaucetArgs, FaucetRpcExtApiServer as _};
use tn_reth::{
    worker::{WorkerComponents, WorkerNetwork},
    RethEnv, RpcServerHandle, WorkerTxPool,
};
use tn_rpc::{TelcoinNetworkRpcExt, TelcoinNetworkRpcExtApiServer};
use tn_types::{
    Address, BatchSender, BatchValidation, ConsensusOutput, ExecHeader, Noticer, SealedHeader,
    TaskManager, WorkerId, B256, MIN_PROTOCOL_BASE_FEE,
};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tracing::{error, info};

/// Inner type for holding execution layer types.
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
    /// TODO: temporary solution until upstream reth supports public rpc hooks
    pub(super) opt_faucet_args: Option<FaucetArgs>,
    /// Collection of execution components by worker.
    pub(super) workers: HashMap<WorkerId, WorkerComponents>,
    // TODO: add Pool to self.workers for direct access (tests)
}

impl ExecutionNodeInner {
    /// Spawn tasks associated with executing output from consensus.
    ///
    /// The method is consumed by [PrimaryNodeInner::start].
    /// All tasks are spawned with the [ExecutionNodeInner]'s [TaskManager].
    pub(super) async fn start_engine(
        &self,
        from_consensus: broadcast::Receiver<ConsensusOutput>,
        task_manager: &TaskManager,
        rx_shutdown: Noticer,
    ) -> eyre::Result<()> {
        let parent_header = self.reth_env.lookup_head()?;

        // spawn execution engine to extend canonical tip
        let tn_engine = ExecutorEngine::new(
            self.reth_env.clone(),
            self.reth_env.get_debug_max_round(),
            BroadcastStream::new(from_consensus),
            parent_header,
            rx_shutdown,
        );

        // spawn tn engine
        task_manager.spawn_task("consensus engine", async move {
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
        task_manager: &TaskManager,
        rx_shutdown: Noticer,
    ) -> eyre::Result<()> {
        // inspired by reth's default eth tx pool:
        // - `EthereumPoolBuilder::default()`
        // - `components_builder.build_components()`
        // - `pool_builder.build_pool(&ctx)`
        let transaction_pool = self.reth_env.worker_txn_pool().clone();

        // TODO: WorkerNetwork is basically noop and missing some functionality
        let network = WorkerNetwork::new(self.reth_env.chainspec());
        let mut tx_pool_latest = transaction_pool.block_info();
        tx_pool_latest.pending_basefee = MIN_PROTOCOL_BASE_FEE;
        let last_seen = self.reth_env.finalized_block_hash_number()?;
        tx_pool_latest.last_seen_block_hash = last_seen.hash;
        tx_pool_latest.last_seen_block_number = last_seen.number;
        transaction_pool.set_block_info(tx_pool_latest);

        let batch_builder = BatchBuilder::new(
            &self.reth_env,
            transaction_pool.clone(),
            block_provider_sender,
            self.address,
            self.tn_config.parameters.max_batch_delay,
        );

        // spawn block builder task
        task_manager.spawn_task("batch builder", async move {
            tokio::select!(
                _ = &rx_shutdown => {
                }
                res = batch_builder => {
                    info!(target: "tn::execution", ?res, "batch builder task exited");
                }
            )
        });

        // extend TN namespace
        let engine_to_primary = (); // TODO: pass client/server here
        let tn_ext = TelcoinNetworkRpcExt::new(self.reth_env.chainspec(), engine_to_primary);
        let mut server = self.reth_env.get_rpc_server(
            transaction_pool.clone(),
            network,
            task_manager,
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
        let components = WorkerComponents::new(rpc_handle, transaction_pool);
        self.workers.insert(worker_id, components);

        Ok(())
    }

    /// Create a new block validator.
    pub(super) fn new_batch_validator(&self) -> Arc<dyn BatchValidation> {
        // batch validator
        Arc::new(BatchValidator::new(self.reth_env.clone()))
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
        let finalized_block_num = self.reth_env.last_block_number()?;
        let mut result = Vec::with_capacity(number as usize);
        if number > 0 {
            let mut block_num = finalized_block_num;
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
            .get(worker_id)
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
            .get(worker_id)
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
}
