//! Inner-execution node components for both Worker and Primary execution.
//!
//! This module contains the logic for execution.

use crate::error::ExecutionError;
use eyre::{eyre, OptionExt};
use jsonrpsee::http_client::HttpClient;
use std::{net::SocketAddr, ops::ControlFlow, sync::Arc};
use tn_batch_builder::BatchBuilder;
use tn_batch_validator::BatchValidator;
use tn_config::Config;
use tn_engine::ExecutorEngine;
use tn_reth::{
    error::{StateReadError, StateReadResult},
    system_calls::EpochState,
    worker::{WorkerComponents, WorkerNetwork},
    RethEnv, RpcServerHandle, WorkerTxPool,
};
use tn_rpc::{EngineToPrimary, TelcoinNetworkRpcExt, TelcoinNetworkRpcExtApiServer};
use tn_types::{
    gas_accumulator::{BaseFeeContainer, GasAccumulator},
    Address, BatchSender, BatchValidation, BlockHeader, BlsPublicKey, Bytes, ConsensusHeaderDigest,
    ConsensusOutput, EngineUpdate, Epoch, ExecHeader, Noticer, SealedHeader, TaskSpawner, WorkerId,
    B256,
};
use tn_worker::WorkerNetworkHandle;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

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
        engine_update_tx: mpsc::Sender<EngineUpdate>,
    ) -> eyre::Result<()> {
        let parent_header = self.reth_env.lookup_head()?;

        let block_num = parent_header.number();
        let block_hash = parent_header.hash();
        let consensus_header = parent_header.parent_beacon_block_root();
        // spawn execution engine to extend canonical tip
        let tn_engine = ExecutorEngine::new(
            self.reth_env.clone(),
            self.reth_env.get_debug_max_round(),
            rx_output,
            parent_header,
            rx_shutdown,
            self.reth_env.get_task_spawner().clone(),
            gas_accumulator,
            engine_update_tx,
        );

        // spawn tn engine
        self.reth_env.get_task_spawner().spawn_critical_task("consensus engine", async move {
            info!("Engine stated from block {block_num}/{block_hash}, consensus output {consensus_header:?}");
            let res = tn_engine.run().await;
            match &res {
                Ok(_) => {
                    info!(target: "engine", "TN Engine exited gracefully");
                }
                Err(e) => {
                    error!(target: "engine", ?e, "TN Engine error");
                }
            }
            Ok(res?)
        });

        Ok(())
    }

    /// The worker's RPC, TX pool, and block builder
    pub(super) async fn start_batch_builder(
        &mut self,
        worker_id: WorkerId,
        block_provider_sender: BatchSender,
        epoch_task_spawner: &TaskSpawner,
        base_fee: u64,
        epoch: Epoch,
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
            epoch,
        )?;

        // spawn batch builder task
        epoch_task_spawner.spawn_critical_task("batch builder", async move {
            let res = batch_builder.run().await;
            info!(target: "tn::execution", ?res, "batch builder task exited");
            Ok(res?)
        });

        Ok(())
    }

    /// Initialize the worker's transaction pool and public RPC.
    /// Must call this function in accending worker_id order or will panic,
    /// for instance call for worker id 0, then 1, etc.
    ///
    /// The pool receives the shared [`BaseFeeContainer`] so canonical updates always charge
    /// the current epoch's fee (issue #1262).
    pub(super) async fn initialize_worker_components<EP>(
        &mut self,
        worker_id: WorkerId,
        network_handle: WorkerNetworkHandle,
        engine_to_primary: EP,
        base_fee: BaseFeeContainer,
    ) -> eyre::Result<()>
    where
        EP: EngineToPrimary + Send + Sync + 'static,
    {
        let transaction_pool = self.reth_env.init_txn_pool(base_fee.clone())?;

        let network = WorkerNetwork::new(
            self.reth_env.chainspec(),
            network_handle,
            self.tn_config.version,
            self.reth_env.clone(),
        );
        let mut tx_pool_latest = transaction_pool.block_info();
        tx_pool_latest.pending_basefee = base_fee.base_fee();
        let last_seen = self.reth_env.finalized_block_hash_number_for_startup()?;
        tx_pool_latest.last_seen_block_hash = last_seen.hash;
        tx_pool_latest.last_seen_block_number = last_seen.number;
        transaction_pool.set_block_info(tx_pool_latest);

        // extend TN namespace
        let tn_ext = TelcoinNetworkRpcExt::new(self.reth_env.clone(), engine_to_primary);
        let server = self.reth_env.get_rpc_server(
            transaction_pool.clone(),
            network.clone(),
            base_fee,
            tn_ext.into_rpc(),
        )?;

        info!(target: "tn::execution", "tn rpc extension successfully merged");

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

    /// Update the pending base fee on a worker's transaction pool.
    ///
    /// Called every epoch so the pool charges the base fee the accumulator currently holds for
    /// the worker. This covers the respawn path, where `initialize_worker_components` is skipped
    /// but the base fee for the new epoch must still take effect. If the worker's components have
    /// not been initialized, the update is dropped with a warning and forces node shutdown: the
    /// worker's pool would keep charging the previous epoch's base fee, admitting underpriced
    /// transactions that waste space in accepted batches (the batch validator checks only the
    /// batch-level declared fee, never per-transaction fees).
    pub(super) fn set_worker_base_fee(
        &self,
        worker_id: WorkerId,
        base_fee: u64,
    ) -> eyre::Result<()> {
        if let Some(worker) = self.workers.get(worker_id as usize) {
            let pool = worker.pool();
            let mut block_info = pool.block_info();
            block_info.pending_basefee = base_fee;
            pool.set_block_info(block_info);
        } else {
            warn!(
                target: "tn::execution",
                worker_id,
                initialized_workers = self.workers.len(),
                "set_worker_base_fee: dropping base-fee update for uninitialized worker"
            );
            return Err(eyre!(
                "set_worker_base_fee: worker {worker_id} uninitialized ({} workers initialized)",
                self.workers.len()
            ));
        }
        Ok(())
    }

    /// Respawn any tasks on the worker network when we get a new epoch task manager.
    ///
    /// This method should be called on epoch rollover.
    /// Will take care of all workers.
    pub(super) async fn respawn_worker_network_tasks(&self, network_handle: WorkerNetworkHandle) {
        for worker in &self.workers {
            worker.worker_network().respawn_peer_count(network_handle.clone());
        }
    }

    /// Push the node's consensus catch-up state into every worker's RPC network shim.
    ///
    /// The epoch manager's node-mode watch task drives this on every mode change so the
    /// stock `eth_syncing` handler answers from live consensus state (issue #1231).
    pub(super) fn set_workers_syncing(&self, syncing: bool) {
        self.workers.iter().for_each(|worker| worker.worker_network().set_syncing(syncing));
    }

    /// Create a new block validator.
    pub(super) fn new_batch_validator(
        &self,
        worker_id: &WorkerId,
        base_fee: u64,
        epoch: Epoch,
    ) -> Arc<dyn BatchValidation> {
        // retrieve handle to transaction pool to submit gossip transactions to validators
        let tx_pool = self.workers.get(*worker_id as usize).map(|w| w.pool());

        Arc::new(BatchValidator::new(self.reth_env.clone(), tx_pool, *worker_id, base_fee, epoch))
    }

    /// Fetch the last executed state from the database.
    ///
    /// This method is called when the primary spawns to retrieve
    /// the last committed sub dag from it's database in the case
    /// of the node restarting.
    ///
    /// This returns the hash of the last executed ConsensusHeader on the consensus chain.
    /// since the execution layer is confirming the last executing block.
    pub(super) fn last_executed_output(&self) -> eyre::Result<ConsensusHeaderDigest> {
        // The payload builder commits an output's blocks and the finalized marker in ONE
        // transaction, so a crash can no longer leave the marker lagging the persisted tip.
        // Startup additionally heals databases written by pre-fix versions that committed the
        // marker separately (`RethEnv::heal_finalized_to_persisted_tip`) before anything reads
        // the marker, so the finalized block read here is the last block of the last consensus
        // output whose execution was fully committed — the digest recovered below names
        // exactly the last executed output, and the primary re-requests everything after it.
        //
        // recover finalized block's nonce: this is the last subdag index from consensus (round)
        let finalized_block_num = self.reth_env.last_finalized_block_number()?;
        let last_round_of_consensus = self
            .reth_env
            .header_by_number(finalized_block_num)?
            .map(|opt| opt.parent_beacon_block_root.unwrap_or_default())
            .unwrap_or_else(Default::default);

        Ok(last_round_of_consensus.into())
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
        collect_last_output_blocks(
            |block_num| self.reth_env.sealed_header_by_number(block_num).map_err(Into::into),
            last_block_number,
            number,
            self.reth_env.real_header_floor(),
        )
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

    /// Return all worker's transaction pools.
    pub(super) fn get_worker_transaction_pools(&self) -> Vec<WorkerTxPool> {
        self.workers.iter().map(|w| w.pool()).collect()
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
    pub(super) fn epoch_state_from_canonical_tip(&self) -> eyre::Result<EpochState> {
        self.reth_env.epoch_state_from_canonical_tip()
    }

    /// Read the current epoch's [EpochState] pinned to the previous epoch's closing block
    /// (genesis for epoch 0), returning the pin header alongside it.
    ///
    /// `tip` is the bootstrap sample the pin derives from, supplied by the caller so a retried
    /// read can prove every attempt resolves the same pin (see
    /// [`RethEnv::epoch_state_at_epoch_start_from_tip`]).
    pub(super) fn epoch_state_at_epoch_start_from_tip(
        &self,
        tip: &SealedHeader,
    ) -> StateReadResult<(EpochState, SealedHeader)> {
        self.reth_env.epoch_state_at_epoch_start_from_tip(tip)
    }

    /// Read committee validator keys for epoch, pinned to `header`'s state.
    ///
    /// On-chain BLS key bytes are decoded through [`decode_committee_keys`], so a decode failure
    /// is a hard error, mirroring the epoch-entry committee read.
    pub(super) fn validators_for_epoch_at_header(
        &self,
        epoch: u32,
        header: &SealedHeader,
    ) -> StateReadResult<Vec<BlsPublicKey>> {
        decode_committee_keys(
            epoch,
            header.hash(),
            self.reth_env.bls_pubkeys_for_epoch_at_header(epoch, header)?,
        )
    }

    /// Read several epochs' committee validator keys, pinned to `header`'s state — ONE pinned
    /// EVM for the whole batch, results ordered to match `epochs`.
    ///
    /// Each set decodes through [`decode_committee_keys`] under its own epoch, so an
    /// undecodable key fails the whole batch and names the epoch it came from.
    pub(super) fn validators_for_epochs_at_header(
        &self,
        epochs: &[Epoch],
        header: &SealedHeader,
    ) -> StateReadResult<Vec<Vec<BlsPublicKey>>> {
        self.reth_env
            .bls_pubkeys_for_epochs_at_header(epochs, header)?
            .into_iter()
            .zip(epochs)
            .map(|(raw, &epoch)| decode_committee_keys(epoch, header.hash(), raw))
            .collect()
    }

    /// Read several epochs' committee validator keys, pinned to the block identified by
    /// `block_hash` — the by-hash sibling of [`Self::validators_for_epochs_at_header`].
    ///
    /// ONE header lookup and ONE pinned EVM serve the whole batch, so every returned set provably
    /// derives from the same block. Serves callers holding only a `BlockNumHash` (the
    /// epoch-record close).
    pub(super) fn validators_for_epochs_at_block(
        &self,
        epochs: &[Epoch],
        block_hash: B256,
    ) -> StateReadResult<Vec<Vec<BlsPublicKey>>> {
        self.reth_env
            .bls_pubkeys_for_epochs_at_block(epochs, block_hash)?
            .into_iter()
            .zip(epochs)
            .map(|(raw, &epoch)| decode_committee_keys(epoch, block_hash, raw))
            .collect()
    }
}

/// Decode on-chain BLS key bytes into committee keys for `epoch`, read at pin block `pin`.
///
/// A decode failure is a hard error: a silently short committee is a consensus-safety failure,
/// while halting is a single-node liveness failure.
///
/// The failure is [`StateReadError::ChainGlobal`], never `Provider`: the decode is a pure function
/// of the bytes at the pin plus this node's own code, so every node reading the same block fails
/// identically and no retry can clear it. That classification is what lets the decode stay fused
/// inside a caller's retried closure — `retry_provider_faults` short-circuits `ChainGlobal`, so a
/// bad key set halts on the first attempt rather than after three.
fn decode_committee_keys(
    epoch: Epoch,
    pin: B256,
    raw: Vec<Bytes>,
) -> StateReadResult<Vec<BlsPublicKey>> {
    raw.iter()
        .map(|bls| {
            BlsPublicKey::from_literal_bytes(bls.as_ref()).map_err(|err| {
                StateReadError::ChainGlobal(format!(
                    "failed to create bls key from on-chain bytes for epoch {epoch} at block \
                     {pin:?}: {err:?}"
                ))
            })
        })
        .collect()
}

/// Walk headers backward from `tip`, keeping the last block of each consensus output, and never
/// reading below `scan_floor`.
///
/// Every block of one consensus output carries the same header nonce, and the nonce changes at
/// each output, so a nonce change marks an output's final block. The walk collects up to `number`
/// of them (returned oldest first) and stops early at `scan_floor`: `0` (genesis) on a
/// normally-synced node, [`RethEnv::real_header_floor`] on a snapshot-restored datadir. The floor
/// is what keeps the walk inside real history: below it the datadir holds scaffold dummy headers
/// whose nonce is always `0`, so the nonce-change condition could never fire again and the walk
/// would read every header down to block 0 (O(chain-height) synchronous reads inside the startup
/// path), then hand the caller a synthetic header (issue #1321). The header AT `scan_floor` is
/// real and stays eligible for collection.
fn collect_last_output_blocks(
    read_header: impl Fn(u64) -> eyre::Result<Option<SealedHeader>>,
    tip: u64,
    number: u64,
    scan_floor: u64,
) -> eyre::Result<Vec<SealedHeader>> {
    let read = |block_num: u64| {
        read_header(block_num).and_then(|header| {
            header.ok_or_else(|| eyre::Error::msg(format!("Unable to read block {block_num}")))
        })
    };
    let want = usize::try_from(number).unwrap_or(usize::MAX);
    let collected = (number > 0)
        .then(|| {
            let tip_header = read(tip)?;
            let last_nonce = tip_header.nonce;
            let outcome = (scan_floor..tip).rev().try_fold(
                (vec![tip_header], last_nonce),
                |(heads, last_nonce), block_num| {
                    if heads.len() >= want {
                        ControlFlow::Break(Ok(heads))
                    } else {
                        read(block_num).map_or_else(
                            |err| ControlFlow::Break(Err(err)),
                            |header| {
                                // Only track each output's "finalized" block, not all the extra
                                // batches: the nonce is shared by every batch of one consensus
                                // output and changes at the next output (composed of epoch and
                                // round).
                                let step = if header.nonce == last_nonce {
                                    (heads, last_nonce)
                                } else {
                                    let nonce = header.nonce;
                                    (
                                        heads.into_iter().chain(std::iter::once(header)).collect(),
                                        nonce,
                                    )
                                };
                                ControlFlow::Continue(step)
                            },
                        )
                    }
                },
            );
            match outcome {
                ControlFlow::Continue((heads, _)) => Ok(heads),
                ControlFlow::Break(result) => result,
            }
        })
        .transpose()?
        .unwrap_or_default();
    Ok(collected.into_iter().rev().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    /// A synthetic chain reader: real headers in `real_floor..=tip` where each consensus output
    /// spans `span` blocks (nonce = 1 + output index, never colliding with the dummy nonce),
    /// scaffold dummies (zero nonce, like `ExecHeader::default()`) below `real_floor`. `reads`
    /// counts every header fetch.
    fn reader(
        tip: u64,
        real_floor: u64,
        span: u64,
        reads: &Cell<u64>,
    ) -> impl Fn(u64) -> eyre::Result<Option<SealedHeader>> + '_ {
        move |number| {
            reads.set(reads.get() + 1);
            let nonce =
                number.checked_sub(real_floor).map_or(0u64, |offset| 1 + offset / span.max(1));
            Ok((number <= tip).then(|| {
                SealedHeader::new(
                    ExecHeader { number, nonce: nonce.into(), ..Default::default() },
                    B256::ZERO,
                )
            }))
        }
    }

    /// Regression test for issue #1321: when the real-header window holds fewer distinct nonces
    /// than requested, the walk must stop at the floor: never read the dummy region below it and
    /// never push a synthetic (dummy) header into the result.
    #[test]
    fn scan_stops_at_restored_floor_and_excludes_dummies() -> eyre::Result<()> {
        let reads = Cell::new(0u64);
        // 10_000-block chain, real headers only in 9_745..=10_000, 8 blocks per output => 32
        // distinct nonces in the window, fewer than the 50 requested
        let (tip, floor) = (10_000u64, 9_745u64);
        let result = collect_last_output_blocks(reader(tip, floor, 8, &reads), tip, 50, floor)?;

        // every read stayed inside real history (the window itself, at most)
        assert!(reads.get() <= tip - floor + 1, "walk read below the floor: {} reads", reads.get());
        // no synthetic header escaped: every returned block is at or above the floor
        assert!(result.iter().all(|h| h.number >= floor), "dummy header in result");
        // the walk still collected the real output heads (one per distinct nonce in the window)
        assert_eq!(result.len(), 32);
        let ordered = result.iter().zip(result.iter().skip(1)).all(|(a, b)| a.number < b.number);
        assert!(ordered, "result is not oldest-first");
        Ok(())
    }

    /// Baseline: with no restore floor the walk behaves as before: it collects `number` output
    /// heads (newest is the tip), one per nonce change, oldest first.
    #[test]
    fn scan_collects_output_heads_without_floor() -> eyre::Result<()> {
        let reads = Cell::new(0u64);
        // whole chain real, 4 blocks per output
        let tip = 100u64;
        let result = collect_last_output_blocks(reader(tip, 0, 4, &reads), tip, 5, 0)?;

        assert_eq!(result.len(), 5);
        assert_eq!(result.last().map(|h| h.number), Some(tip));
        let distinct = result.iter().zip(result.iter().skip(1)).all(|(a, b)| a.nonce != b.nonce);
        assert!(distinct, "collected two blocks of the same consensus output");
        Ok(())
    }

    /// A walk that reaches genesis on a short, normally-synced chain stops there: the pre-#1321
    /// termination, preserved by `scan_floor == 0`.
    #[test]
    fn scan_stops_at_genesis_on_short_chain() -> eyre::Result<()> {
        let reads = Cell::new(0u64);
        let tip = 6u64;
        let result = collect_last_output_blocks(reader(tip, 0, 2, &reads), tip, 50, 0)?;

        // blocks 0..=6 at 2 blocks per output hold 4 distinct nonces; the walk reads each block
        // exactly once, terminates at genesis, and keeps one head per output
        assert_eq!(reads.get(), tip + 1);
        assert_eq!(result.len(), 4);
        Ok(())
    }
}
