//! Execution and ATOMIC CANONICALIZATION of consensus output.
//!
//! This module owns the path from certified consensus output to the canonical chain:
//! executing a worker batch's payload into a block
//! ([`RethEnv::build_block_from_batch_payload`]), committing a round's blocks to the
//! database ([`RethEnv::finish_executing_output`]), and updating the in-memory
//! finalized/safe watches ([`RethEnv::finalize_block`]). Consensus output is the ONLY
//! source of canonical blocks: there is no fork choice and no reorg path, and every
//! committed block is final by construction.
//!
//! # Deliberate transaction drops (fork safety)
//!
//! Block building silently drops transactions on exactly two paths:
//!
//! 1. unrecoverable signers — a transaction whose signer cannot be recovered is dropped (and
//!    counted) instead of failing the block, because a certified sub-DAG is fixed: erroring would
//!    deterministically halt — and crash-loop on restart replay — every honest node over one
//!    undecodable transaction (issue #933);
//! 2. execution-level `BlockValidationError::InvalidTx` — expected in normal operation (e.g. two
//!    workers' batches carrying the same transaction); skipped and counted.
//!
//! Dropping instead of halting is fork-safe because both paths are deterministic
//! functions of the certified bytes: every honest node recovers, drops, and skips the
//! exact same transactions, so all nodes still build byte-identical blocks. Any other
//! execution error fails the attempt instead.
//!
//! # Atomicity contract
//!
//! [`RethEnv::persist_executed_output`] persists the blocks and the finalized/safe
//! block-number markers in ONE `provider_rw` transaction with a single commit — a
//! crash can never leave the persisted tip and the markers out of sync, and every
//! error it returns leaves that transaction uncommitted, so no block became canonical
//! in the database. Durability is NOT all-or-nothing, though: `save_blocks` also
//! appends to the process-wide static-file writers, and that progress is fsynced
//! outside the transaction, so a fault after the append leaves the writers advanced
//! for reth's startup consistency check to reconcile on restart. Read that method's
//! error contract before retrying it. Only after that commit
//! does [`RethEnv::announce_executed_output`] report the new tip over the engine
//! update channel (`blocking_send`) and then broadcast the canonical-state
//! notification (`notify_canon_state`); its errors are post-commit and must never
//! trigger a rollback. [`RethEnv::finish_executing_output`] composes the two for
//! callers that do not need the commit boundary. [`RethEnv::finalize_block`] is
//! in-memory only (the finalized/safe watches plus pruning persisted blocks from
//! the in-memory state); its database half happens inside the atomic commit above.

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
    metrics::{record_invalid_tx_skipped, InvalidTxSkipReason, RETH_METRICS},
    payload::TNPayload,
    TNPrimitives,
};

use super::RethEnv;

impl RethEnv {
    /// Construct a canonical block from a worker's block that reached consensus.
    ///
    /// The returned block may contain FEWER transactions than the certified batch:
    /// transactions with unrecoverable signers and transactions failing execution
    /// validation (`InvalidTx`, e.g. duplicates across workers' batches) are dropped
    /// deterministically — see the module docs for why dropping is fork-safe. Any
    /// other execution error fails the whole attempt.
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
                    debug!(target: "engine", %error, tx_hash = ?recovered.hash(), "skipping invalid transaction");
                    // expected in normal operation - see the reason docs, this is not an alert
                    record_invalid_tx_skipped(InvalidTxSkipReason::classify(error.as_ref()));
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

    /// Atomically persist an executed round of consensus output.
    ///
    /// The blocks AND the finalized/safe markers commit in the same database
    /// transaction (one `provider_rw` commit), so a crash can never leave the
    /// persisted marker behind the persisted tip — every block here comes from
    /// committed consensus output and is final by construction. The in-memory
    /// finalized/safe watches are updated afterwards by [`Self::finalize_block`].
    ///
    /// # Error contract
    ///
    /// Every error return leaves the DATABASE transaction uncommitted: the last fallible
    /// step is `provider_rw.commit()`, so on any `Err` no block became canonical in the
    /// database and the caller must compensate the speculative in-memory advance.
    ///
    /// Durability is NOT all-or-nothing, though: `save_blocks` also appends to the
    /// process-wide static-file writers, and that progress is fsynced outside the
    /// database transaction. A fault after that append (marker writes, the commit
    /// itself) aborts the database transaction but leaves the static-file writer
    /// advanced, so a repeat call trips over it with
    /// `ProviderError::UnexpectedStaticFileBlockNumber`; reth's startup consistency
    /// check reconciles the divergence on restart. Callers retrying this method must
    /// treat that error as terminal (see the engine's `persist_output_with_retry`).
    /// Post-commit reporting lives in [`Self::announce_executed_output`] so the commit
    /// boundary is a function boundary rather than an error-classification exercise
    /// (issue #1090).
    pub fn persist_executed_output(&self, blocks: &[ExecutedBlock]) -> TnRethResult<()> {
        #[cfg(any(feature = "test-utils", test))]
        self.inner.persist_attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        #[cfg(any(feature = "test-utils", test))]
        self.consume_injected_persist_fault()?;

        debug!(
            target: "engine",
            first=?blocks.first().map(|b| b.recovered_block.num_hash()),
            last=?blocks.last().map(|b| b.recovered_block.num_hash()),
            "storing range of blocks",
        );

        // insert blocks to db
        let provider_rw = self.inner.blockchain_provider.database_provider_rw()?;
        provider_rw.save_blocks(blocks.to_vec(), reth_provider::SaveBlocksMode::Full)?;
        // advance the finalized/safe markers in the same transaction as the blocks: every
        // canonical block comes from committed consensus output, so the last saved block is
        // final by construction, and the single commit leaves no crash window where the
        // persisted tip outruns the marker
        if let Some(last) = blocks.last() {
            let last_number = last.recovered_block.number;
            provider_rw.save_finalized_block_number(last_number)?;
            provider_rw.save_safe_block_number(last_number)?;
        }
        #[cfg(any(feature = "test-utils", test))]
        self.consume_injected_late_persist_fault()?;

        provider_rw.commit()?;
        Ok(())
    }

    /// Announce a durably committed round of consensus output: report the new tip over
    /// the engine update channel, then broadcast the canonical-state notification.
    ///
    /// POST-commit half of the split (issue #1090): by the time this runs the blocks
    /// are canonical for real, so a failure here (e.g.
    /// [`TnRethError::EngineUpdateChannelClosed`]) must NOT trigger any rollback of the
    /// in-memory advance; the in-memory state correctly names the committed tip.
    ///
    /// Ordering: first the new tip is reported over the engine update channel
    /// (`blocking_send`, feeding the consensus layer's `recent_blocks` watch, so this
    /// method must run on a blocking thread, and a closed channel errors out before any
    /// broadcast); only then is the canonical-state notification broadcast via
    /// `notify_canon_state` to in-process subscribers such as the worker's pool
    /// maintenance task.
    pub fn announce_executed_output(
        &self,
        blocks: Vec<ExecutedBlock>,
        engine_update: Option<(Round, ConsensusNumHash, tokio::sync::mpsc::Sender<EngineUpdate>)>,
    ) -> TnRethResult<()> {
        // process update
        //
        // the canon_state_notifications include every block executed in this round
        //
        // the worker's pool maintenance task subcribes to these events
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

    /// Atomically persist an executed round of consensus output and announce the new
    /// canonical tip.
    ///
    /// Composes [`Self::persist_executed_output`] (the atomic durable write; every
    /// error leaves the database transaction uncommitted) and
    /// [`Self::announce_executed_output`] (post-commit reporting). Callers that must
    /// distinguish the commit boundary (to compensate or retry only the durable
    /// write, as the engine's `execute_consensus_output` does) call the two halves
    /// directly.
    pub fn finish_executing_output(
        &self,
        blocks: Vec<ExecutedBlock>,
        engine_update: Option<(Round, ConsensusNumHash, tokio::sync::mpsc::Sender<EngineUpdate>)>,
    ) -> TnRethResult<()> {
        self.persist_executed_output(&blocks)?;
        self.announce_executed_output(blocks, engine_update)
    }

    /// TEST-ONLY: arm `count` injected pre-commit persist faults.
    ///
    /// The next `count` calls to [`Self::persist_executed_output`] fail with a
    /// [`TnRethError::Provider`] before touching the database, simulating a node-local
    /// storage fault (disk full, an MDBX write error) on the durable write. Lets
    /// integration tests exercise the engine's bounded persist retry and its
    /// pre-commit rollback without a real storage fault (issue #1090).
    #[cfg(any(feature = "test-utils", test))]
    pub fn inject_persist_provider_faults(&self, count: u32) {
        self.inner.persist_fault_injections.store(count, std::sync::atomic::Ordering::SeqCst);
    }

    /// TEST-ONLY: consume one armed injected persist fault, if any.
    ///
    /// Returns the injected [`TnRethError::Provider`] while a fault is armed and `Ok`
    /// otherwise. The inner variant (`HeaderNotFound(0)`) is a stand-in: the engine's
    /// retry policy classifies on the `Provider` class alone, never on the variant.
    #[cfg(any(feature = "test-utils", test))]
    fn consume_injected_persist_fault(&self) -> TnRethResult<()> {
        use std::sync::atomic::Ordering;
        self.inner
            .persist_fault_injections
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |armed| armed.checked_sub(1))
            .map_or(Ok(()), |_| {
                Err(TnRethError::Provider(reth_provider::ProviderError::HeaderNotFound(
                    0u64.into(),
                )))
            })
    }

    /// TEST-ONLY: arm `count` injected LATE persist faults.
    ///
    /// The next `count` calls to [`Self::persist_executed_output`] fail with a
    /// [`TnRethError::Provider`] AFTER `save_blocks` has advanced the process-wide
    /// static-file writers, immediately before `provider_rw.commit()`, simulating a
    /// node-local fault on the finalized/safe marker writes or the commit itself.
    /// That ordering is the one that makes an in-process retry impossible: the failed
    /// attempt's static-file progress is already fsynced, so a repeat call trips
    /// `ProviderError::UnexpectedStaticFileBlockNumber` (issue #1090).
    #[cfg(any(feature = "test-utils", test))]
    pub fn inject_late_persist_provider_faults(&self, count: u32) {
        self.inner.persist_late_fault_injections.store(count, std::sync::atomic::Ordering::SeqCst);
    }

    /// TEST-ONLY: consume one armed injected LATE persist fault, if any.
    ///
    /// Returns the injected [`TnRethError::Provider`] while a fault is armed and `Ok`
    /// otherwise. The inner variant (`HeaderNotFound(0)`) is a stand-in for the marker
    /// write or commit failing; the defining property is the POSITION (after
    /// `save_blocks` advanced the static-file writers), not the variant.
    #[cfg(any(feature = "test-utils", test))]
    fn consume_injected_late_persist_fault(&self) -> TnRethResult<()> {
        use std::sync::atomic::Ordering;
        self.inner
            .persist_late_fault_injections
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |armed| armed.checked_sub(1))
            .map_or(Ok(()), |_| {
                Err(TnRethError::Provider(reth_provider::ProviderError::HeaderNotFound(
                    0u64.into(),
                )))
            })
    }

    /// TEST-ONLY: how many times [`Self::persist_executed_output`] has been entered.
    ///
    /// Counts ATTEMPTS, not failures: the counter increments on entry, before any injected
    /// fault is consumed, so a test can pin exactly how much of the engine's bounded retry
    /// budget a given fault ordering spent. Telling "failed fast at attempt 2" apart from
    /// "burned the whole budget" otherwise takes a wall-clock measurement of the retry
    /// backoff, which is not a sound assertion (issue #1090).
    #[cfg(any(feature = "test-utils", test))]
    pub fn persist_attempt_count(&self) -> u32 {
        self.inner.persist_attempts.load(std::sync::atomic::Ordering::SeqCst)
    }
}
