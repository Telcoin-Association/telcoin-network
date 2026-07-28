//! Methods that execute transactions.

use std::sync::Arc;

use alloy::primitives::FixedBytes;
use rayon::iter::{IntoParallelRefIterator as _, ParallelIterator as _};
use reth_chain_state::{DeferredTrieData, ExecutedBlock, NewCanonicalChain};
use reth_errors::{BlockExecutionError, BlockValidationError};
use reth_evm::{
    execute::{BlockBuilder as _, BlockBuilderOutcome},
    ConfigureEvm as _,
};
use reth_provider::{
    BlockExecutionOutput, CanonChainTracker as _, ChainStateBlockWriter as _, DBProvider as _,
    DatabaseProviderFactory as _, StateProviderFactory as _,
};
use reth_revm::{cached::CachedReads, database::StateProviderDatabase, State};
use reth_rpc_eth_types::utils::recover_raw_transaction as reth_recover_raw_transaction;
use tn_types::{
    deconstruct_nonce, ConsensusNumHash, EngineUpdate, Round, SealedHeader, TransactionSigned, B256,
};
use tracing::{debug, error, info, warn};

use crate::{
    error::{TnRethError, TnRethResult},
    metrics::RETH_METRICS,
    payload::TNPayload,
    TNPrimitives,
};

use super::RethEnv;

impl RethEnv {
    /// Construct a canonical block from a worker's block that reached consensus.
    pub fn build_block_from_batch_payload(
        &self,
        payload: TNPayload,
        transactions: &Vec<Vec<u8>>,
        anchor_hash: B256,
        ancestors: &[DeferredTrieData],
    ) -> TnRethResult<ExecutedBlock> {
        let parent_header = payload.parent_header.clone();
        debug!(target: "engine", ?parent_header, "retrieving state for next block");
        let state_provider =
            self.inner.blockchain_provider.state_by_block_hash(parent_header.hash())?;
        let state = StateProviderDatabase::new(&state_provider);
        let mut cached_reads = CachedReads::default();
        let mut db = State::builder()
            .with_database(cached_reads.as_db_mut(state))
            .with_bundle_update()
            .build();

        debug!(
            target: "engine",
            parent = ?parent_header.num_hash(),
            "building new payload"
        );

        // copy in case of error
        let batch_digest = payload.batch_digest;

        let mut builder =
            self.inner.evm_config.builder_for_next_block(&mut db, &parent_header, payload)?;

        builder.apply_pre_execution_changes().inspect_err(|err| {
            warn!(target: "engine", %err, "failed to apply pre-execution changes");
        })?;

        // Phase 1: Recover all transactions (ECDSA ecrecover) in parallel via rayon.
        // Always use par_iter — the slight overhead on small batches is negligible
        // compared to the savings on large ones, and avoids an extra code path.
        //
        // A transaction whose signer cannot be recovered cannot be executed, but a
        // certified sub-DAG is fixed and identical on every honest node, so returning
        // an error here would deterministically halt (and, on restart-replay,
        // crash-loop) the whole network on a single undecodable transaction. Instead
        // drop the unrecoverable transactions and continue, mirroring the `InvalidTx`
        // tolerance of the execute phase below: every node drops the same
        // transactions from the same certified bytes, so the resulting block stays
        // deterministic (issue #933). The primary defense is validating batches before
        // they can be certified; this only bounds the blast radius if one ever is.
        let recovered_txs = transactions
            .par_iter()
            .filter_map(|tx_bytes| {
                reth_recover_raw_transaction::<TransactionSigned>(tx_bytes)
                    .inspect_err(|e| {
                        error!(
                            target: "engine",
                            batch=?batch_digest,
                            ?tx_bytes,
                            "failed to recover signer, dropping transaction: {e}"
                        )
                    })
                    .ok()
            })
            .collect::<Vec<_>>();

        // Recovery failures are the signal that a batch carrying undecodable transaction bytes
        // was certified, so make them alertable instead of log-only. Count once, after the
        // parallel phase: incrementing inside the rayon closure would contend a single atomic on
        // every transaction, and the batch width that sets the cost of that contention is
        // attacker-chosen. The difference is exact because `filter_map` is the only thing above
        // that removes elements. Unconditional, including the overwhelmingly common zero: one
        // relaxed atomic add per block is free next to executing the block, and a guard here
        // would leave the series unregistered on a healthy node.
        let unrecoverable = transactions.len().saturating_sub(recovered_txs.len());
        RETH_METRICS
            .unrecoverable_txs_dropped_total
            .increment(u64::try_from(unrecoverable).unwrap_or(u64::MAX));

        // Phase 2: Execute recovered transactions sequentially.
        for recovered in recovered_txs {
            // forks are impossible
            match builder.execute_transaction(recovered.clone()) {
                Ok(_gas_used) => (),
                Err(BlockExecutionError::Validation(BlockValidationError::InvalidTx {
                    error,
                    ..
                })) => {
                    // allow transaction errors (ie - duplicates)
                    //
                    // it's possible that another worker's batch included this transaction
                    warn!(target: "engine", %error,  "skipping invalid transaction: {:#?}", recovered);
                    // expected in normal operation - see the field docs, this is not an alert
                    RETH_METRICS.invalid_txs_skipped_total.increment(1);
                    continue;
                }
                // this is an error that we should treat as fatal for this attempt
                Err(err) => return Err(err.into()),
            }
        }

        let BlockBuilderOutcome { execution_result, block, hashed_state, trie_updates } =
            builder.finish(&state_provider)?;

        debug!(target: "engine", hash=?block.hash(), "block builder outcome");
        let block_execution_output =
            BlockExecutionOutput { result: execution_result, state: db.take_bundle() };
        let computed_trie_data = DeferredTrieData::sort_and_build_trie_input(
            Arc::new(hashed_state),
            Arc::new(trie_updates),
            anchor_hash,
            ancestors,
        );
        let res: ExecutedBlock<TNPrimitives> = ExecutedBlock::new(
            Arc::new(block),
            Arc::new(block_execution_output),
            computed_trie_data,
        );

        Ok(res)
    }

    /// Finalize the block (header) executed from consensus output in memory.
    ///
    /// The finalized/safe database markers are persisted atomically with the blocks in
    /// [`Self::finish_executing_output`]; this method only updates the in-memory
    /// finalized/safe watches (consumed by components like RPC) and prunes persisted blocks
    /// from the canonical in-memory state.
    pub fn finalize_block(&self, header: SealedHeader) -> TnRethResult<()> {
        let num_hash = header.num_hash();
        // this clears up old blocks in-memory
        self.inner.blockchain_provider.set_finalized(header.clone());

        // update safe block last because this is less time sensitive but still needs to happen
        self.inner.blockchain_provider.set_safe(header);

        // cleanup chain state in memory
        // this returns the `canonical_chain().count()` back to 0
        self.inner
            .blockchain_provider
            .canonical_in_memory_state()
            .remove_persisted_blocks(num_hash);

        Ok(())
    }

    /// This makes all blocks canonical, commits them to the database,
    /// broadcasts new chain on `canon_state_notification_sender`
    /// and set last executed header as the tracked header.
    ///
    /// The finalized/safe markers commit in the same database transaction as the blocks, so a
    /// crash can never leave the persisted marker behind the persisted tip. The in-memory
    /// finalized/safe watches are updated afterwards by [`Self::finalize_block`].
    ///
    /// It also clears the canonical in-memory state.
    pub fn finish_executing_output(
        &self,
        blocks: Vec<ExecutedBlock>,
        engine_update: Option<(Round, ConsensusNumHash, tokio::sync::mpsc::Sender<EngineUpdate>)>,
    ) -> TnRethResult<()> {
        // NOTE: this makes all blocks canonical, commits them to the database,
        // and broadcasts new chain on `canon_state_notification_sender`
        //
        // the canon_state_notifications include every block executed in this round
        //
        // the worker's pool maintenance task subcribes to these events
        debug!(
            target: "engine",
            first=?blocks.first().map(|b| b.recovered_block.num_hash()),
            last=?blocks.last().map(|b| b.recovered_block.num_hash()),
            "storing range of blocks",
        );

        // insert blocks to db
        let provider_rw = self.inner.blockchain_provider.database_provider_rw()?;
        provider_rw.save_blocks(blocks.clone(), reth_provider::SaveBlocksMode::Full)?;
        // advance the finalized/safe markers in the same transaction as the blocks: every
        // canonical block comes from committed consensus output, so the last saved block is
        // final by construction, and the single commit leaves no crash window where the
        // persisted tip outruns the marker
        if let Some(last) = blocks.last() {
            let last_number = last.recovered_block.number;
            provider_rw.save_finalized_block_number(last_number)?;
            provider_rw.save_safe_block_number(last_number)?;
        }
        provider_rw.commit()?;

        // process update
        //
        // see reth::EngineApiTreeHandler::on_canonical_chain_update
        let chain_update = NewCanonicalChain::Commit { new: blocks };
        let canonical_head = chain_update.tip();
        let (epoch, round) =
            deconstruct_nonce(<FixedBytes<8> as Into<u64>>::into(canonical_head.nonce));
        info!(
            target: "engine",
            "canonical head for epoch {:?} round {:?}: {:?} - {:?}",
            epoch,
            round,
            canonical_head.number,
            canonical_head.hash(),
        );

        if let Some((leader_round, consensus_num_hash, engine_update_tx)) = engine_update {
            engine_update_tx
                .blocking_send((
                    leader_round,
                    consensus_num_hash,
                    Some(canonical_head.clone_sealed_header()),
                ))
                .map_err(|e| {
                    error!(target: "engine", ?e, "engine update channel send failed");
                    TnRethError::EngineUpdateChannelClosed
                })?;
        }

        // broadcast canonical update
        let notification = chain_update.to_chain_notification();
        self.canonical_in_memory_state().notify_canon_state(notification);

        Ok(())
    }
}
