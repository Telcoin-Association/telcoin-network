//! Execution and ATOMIC CANONICALIZATION of consensus output.
//!
//! This module owns the path from certified consensus output to the canonical chain:
//! executing a worker batch's payload into a block
//! ([`RethEnv::build_block_from_batch_payload`], or, with the signers recovered ahead of
//! time by [`RethEnv::recover_output_signers`],
//! [`RethEnv::build_block_from_recovered_payload`]), committing a round's blocks to the
//! database ([`RethEnv::finish_executing_output`]), and updating the in-memory
//! finalized/safe watches ([`RethEnv::finalize_block`]). Consensus output is the ONLY
//! source of canonical blocks: there is no fork choice and no reorg path, and every
//! committed block is final by construction.
//!
//! # State roots for multi-block output (#1301)
//!
//! Within one `ConsensusOutput`, each block's state root is computed by TN's own
//! layered path (`OutputTrieOverlay` in `output_overlay.rs`) rather than reth's
//! `MemoryOverlayStateProvider`: an output-scoped overlay accumulates the built
//! blocks' sorted deltas in place, and the root runs directly against a read-only
//! database transaction with the current block's deltas layered over the overlay.
//! This bypasses (does not fix upstream) reth's per-block re-merge/copy/re-sort of
//! every in-memory ancestor - see `build_block_from_batch_payload` for the honest
//! cost bound. All other state reads still resolve through reth's memory overlay.
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
//! for reth's startup consistency check (run by `RethEnv::init_provider_factory`,
//! issue #1238) to reconcile on restart. Read that method's
//! error contract before retrying it. Only after that commit
//! does [`RethEnv::announce_executed_output`] report the new tip over the engine
//! update channel (`blocking_send`) and then broadcast the canonical-state
//! notification (`notify_canon_state`); its errors are post-commit and must never
//! trigger a rollback. [`RethEnv::finish_executing_output`] composes the two for
//! callers that do not need the commit boundary. [`RethEnv::finalize_block`] is
//! in-memory only (the finalized/safe watches plus pruning persisted blocks from
//! the in-memory state); its database half happens inside the atomic commit above.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use alloy::primitives::FixedBytes;
use rayon::iter::{
    IndexedParallelIterator as _, IntoParallelRefIterator as _, ParallelIterator as _,
};
use reth_chain_state::{ComputedTrieData, ExecutedBlock, NewCanonicalChain};
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
    deconstruct_nonce, ConsensusNumHash, ConsensusOutput, EngineUpdate, Recovered, Round,
    SealedHeader, TransactionSigned, B256,
};
use tracing::{debug, error, info, trace, warn};

use crate::{
    error::{TnRethError, TnRethResult},
    metrics::{record_invalid_tx_skipped, InvalidTxSkipReason, RETH_METRICS},
    payload::TNPayload,
    TNPrimitives,
};

use super::{
    output_overlay::{OutputTrieOverlay, OverlayRootStateProvider},
    RethEnv,
};

impl RethEnv {
    /// Construct a canonical block from a worker's block that reached consensus.
    ///
    /// The returned block may contain FEWER transactions than the certified batch:
    /// transactions with unrecoverable signers and transactions failing execution
    /// validation (`InvalidTx`, e.g. duplicates across workers' batches) are dropped
    /// deterministically — see the module docs for why dropping is fork-safe. Any
    /// other execution error fails the whole attempt.
    ///
    /// The returned block's trie bundle holds only this block's own sorted state and
    /// trie-update deltas. It never carries the cumulative ancestor overlay, so
    /// `anchored_trie_input` is `None`. No consumer on the node's path reads that
    /// overlay: persistence and canonical-chain notifications use the per-block deltas.
    /// Building the overlay was also not O(1) here: the parent's bundle keeps the
    /// overlay `Arc`s alive, so reth's parent-reuse fast path (`Arc::make_mut`)
    /// deep-copies the whole cumulative overlay for every block - one `ConsensusOutput`
    /// of `N` blocks with `M` state updates each paid `O(N^2 * M)` copy work and
    /// transient memory (#1266).
    ///
    /// # State-root path (#1301)
    ///
    /// The state root does NOT go through reth's `MemoryOverlayStateProvider`, whose
    /// `state_root_with_updates` re-merges, deep-copies, and re-sorts EVERY in-memory
    /// ancestor's deltas per block - the second, reth-side `O(N^2 * M)` term, which is
    /// bypassed here rather than fixed upstream. Instead the caller-supplied `overlay`
    /// geometrically compacts the output's already-sorted per-block deltas, and the root
    /// runs against a fresh read-only database transaction with layered in-memory
    /// cursors: current block's sorted deltas over the accumulated overlay over the
    /// database, prefix sets from the current block only (see
    /// [`OutputTrieOverlay::layered_root_with_updates`]). All other provider reads
    /// still resolve through reth's memory overlay, unchanged.
    ///
    /// For `N` blocks with at most `M` delta entries each, sorting remains
    /// `O(M log M)` per block. Equal-level sorted runs compact with cumulative
    /// `O(N M log N)` merge work. General Reth merges allocate replacement vectors;
    /// geometric compaction bounds this work without merging the entire accumulated
    /// overlay each block. Root cursors traverse at most `O(log N)` ancestor layers,
    /// with prefix sets still derived only from the current block's delta. This bound
    /// describes overlay compaction, not the total cost of block execution or trie walks.
    ///
    /// Callers that persist every block before building the next (tests, e2e helpers)
    /// pass a fresh empty overlay per block: the database then already holds the
    /// parent state, so the empty overlay is exact.
    pub fn build_block_from_batch_payload(
        &self,
        payload: TNPayload,
        transactions: &Vec<Vec<u8>>,
        overlay: &mut OutputTrieOverlay,
    ) -> TnRethResult<ExecutedBlock> {
        self.build_block_from_batch_payload_timed(payload, transactions, overlay)
            .map(|(block, _timings)| block)
    }

    /// [`Self::build_block_from_batch_payload`], also returning the wall time of each build
    /// phase as [`BlockBuildTimings`].
    ///
    /// The block is the one the untimed call builds: the laps only read the clock between
    /// phases, and the root lap is read off the state-root provider wrapper after `finish`
    /// returns. The signers are recovered first, exactly as [`Self::recover_output_signers`]
    /// recovers one batch, and the recovered transactions go through
    /// [`Self::build_block_from_recovered_payload`], so both entry points share one build.
    pub fn build_block_from_batch_payload_timed(
        &self,
        payload: TNPayload,
        transactions: &Vec<Vec<u8>>,
        overlay: &mut OutputTrieOverlay,
    ) -> TnRethResult<(ExecutedBlock, BlockBuildTimings)> {
        // observability only: the recover lap; the recovered build times the other phases
        let stamp = Instant::now();
        let recovered = recover_batch_signers(transactions, payload.batch_digest);
        record_unrecoverable(transactions.len(), recovered.len());
        let recover = stamp.elapsed();

        let (block, timings) =
            self.build_block_from_recovered_payload(payload, recovered, overlay)?;
        Ok((block, BlockBuildTimings { recover, ..timings }))
    }

    /// Recover the signer of every transaction in `output`, one `Vec` per batch.
    ///
    /// The outer `Vec` follows [`ConsensusOutput::flatten_batches`] order (certificates in
    /// output order, then each certificate's batches), so entry `i` holds the transactions of
    /// the block built from batch `i`; a single flat list would shift transactions across
    /// blocks. Each inner `Vec` keeps its batch's transaction order.
    ///
    /// The drop semantics are those of the block build: a transaction whose signer cannot be
    /// recovered is logged at error (with its batch digest and byte length, never the bytes),
    /// counted in `tn_reth.unrecoverable_txs_dropped_total` and left out, because a certified
    /// sub-DAG is fixed and erroring would halt every honest node on the same bytes (issue
    /// #933). Every node drops the same transactions from the same certified bytes, so the
    /// blocks built from the result through [`Self::build_block_from_recovered_payload`] equal
    /// those [`Self::build_block_from_batch_payload`] builds from the bytes.
    ///
    /// Batches and the transactions within each batch are recovered in parallel on the global
    /// rayon pool. The engine calls this on a blocking job when an output enters its queue, so
    /// the work overlaps the previous output's execution.
    pub fn recover_output_signers(
        output: &ConsensusOutput,
    ) -> Vec<Vec<Recovered<TransactionSigned>>> {
        // the flattened order without the index pairs: certificate order, then batch order
        let batches: Vec<&Vec<Vec<u8>>> = output
            .batches()
            .iter()
            .flat_map(|cert| cert.batches.iter().map(|batch| &batch.transactions))
            .collect();
        let recovered: Vec<Vec<Recovered<TransactionSigned>>> = batches
            .par_iter()
            .enumerate()
            .map(|(batch_index, transactions)| {
                // the digest only labels the error line of a dropped transaction; a digest
                // deque shorter than the batches fails the engine's own length check later
                let batch_digest = output.get_batch_digest(batch_index).unwrap_or_default();
                recover_batch_signers(transactions, batch_digest)
            })
            .collect();

        // counted once per output, after the parallel phase (see `record_unrecoverable`)
        record_unrecoverable(
            batches.iter().map(|transactions| transactions.len()).sum(),
            recovered.iter().map(Vec::len).sum(),
        );
        recovered
    }

    /// Construct a canonical block from a batch whose signers are already recovered.
    ///
    /// `recovered` is the batch's entry of [`Self::recover_output_signers`]: the transactions
    /// that survived signer recovery, in batch order. The build is the one
    /// [`Self::build_block_from_batch_payload`] runs after its own recovery (see its docs for
    /// the drop semantics and the state-root path), so the same recovered transactions build
    /// the same block. The returned timings' `recover` lap is zero: no recovery runs here.
    pub fn build_block_from_recovered_payload(
        &self,
        payload: TNPayload,
        recovered: Vec<Recovered<TransactionSigned>>,
        overlay: &mut OutputTrieOverlay,
    ) -> TnRethResult<(ExecutedBlock, BlockBuildTimings)> {
        let parent_header = payload.parent_header.clone();
        trace!(target: "engine::dump", ?parent_header, "retrieving state for next block");
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

        // observability only: each `lap` closes one phase of the returned timings
        let mut stamp = Instant::now();

        let mut builder =
            self.inner.evm_config.builder_for_next_block(&mut db, &parent_header, payload)?;

        builder.apply_pre_execution_changes().inspect_err(|err| {
            warn!(target: "engine", %err, "failed to apply pre-execution changes");
        })?;
        let setup = lap(&mut stamp);

        // Execute the recovered transactions sequentially, in batch order.
        for tx in recovered {
            // the hash is cached at decode and is not an execution input: reading it first lets
            // the transaction move into the builder without a per-transaction clone
            let tx_hash = *tx.tx_hash();
            // forks are impossible
            match builder.execute_transaction(tx) {
                Ok(_gas_used) => (),
                Err(BlockExecutionError::Validation(BlockValidationError::InvalidTx {
                    error,
                    ..
                })) => {
                    // allow transaction errors (ie - duplicates)
                    //
                    // it's possible that another worker's batch included this transaction
                    debug!(target: "engine", %error, ?tx_hash, "skipping invalid transaction");
                    // expected in normal operation - see the reason docs, this is not an alert
                    record_invalid_tx_skipped(InvalidTxSkipReason::classify(error.as_ref()));
                    continue;
                }
                // this is an error that we should treat as fatal for this attempt
                Err(err) => return Err(err.into()),
            }
        }
        let evm = setup.saturating_add(lap(&mut stamp));

        // Scope the wrapper so its borrows of the overlay are dropped before the
        // overlay is extended below (`Arc::make_mut` must not see a live borrow).
        // `finish` runs the state root through the wrapper, which times it; the root lap is
        // read off the wrapper before it drops and taken out of the finish lap below.
        let (BlockBuilderOutcome { execution_result, block, hashed_state, trie_updates }, root) = {
            let root_provider = OverlayRootStateProvider::new(
                &state_provider,
                overlay,
                &self.inner.blockchain_provider,
            );
            let outcome = builder.finish(&root_provider)?;
            (outcome, root_provider.root_elapsed())
        };

        debug!(target: "engine", hash=?block.hash(), "block builder outcome");
        let block_execution_output =
            BlockExecutionOutput { result: execution_result, state: db.take_bundle() };
        let (sorted_hashed_state, sorted_trie_updates) =
            rayon::join(|| hashed_state.into_sorted(), || trie_updates.into_sorted());
        let computed_trie_data = ComputedTrieData::without_trie_input(
            Arc::new(sorted_hashed_state),
            Arc::new(sorted_trie_updates),
        );
        // Accumulate this block's sorted deltas into the output-scoped overlay so the
        // NEXT block's layered state root sees them without re-merging its ancestors
        // (#1301). One linear merge pass; the `Arc`s are shared with the block's
        // `ComputedTrieData`, so nothing is recomputed.
        overlay.extend_from_block(&computed_trie_data);
        let res: ExecutedBlock<TNPrimitives> = ExecutedBlock::new(
            Arc::new(block),
            Arc::new(block_execution_output),
            computed_trie_data,
        );
        let finish = lap(&mut stamp).saturating_sub(root);

        Ok((res, BlockBuildTimings { recover: Duration::ZERO, evm, root, finish }))
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
    /// check, which `RethEnv::init_provider_factory` runs on every construction
    /// (issue #1238), reconciles the divergence on restart. Callers retrying this
    /// method must
    /// treat that error as terminal (see the engine's `persist_output_with_retry`).
    /// Post-commit reporting lives in [`Self::announce_executed_output`] so the commit
    /// boundary is a function boundary rather than an error-classification exercise
    /// (issue #1090).
    ///
    /// # Observability
    ///
    /// A successful commit logs one `persisted output` debug line (target `engine`) with the
    /// block range, what was written and the per-phase wall time (`open_ms`, `save_blocks_ms`,
    /// `markers_ms`, `commit_ms`), and records the `tn_reth.persist_*` series (see
    /// [`record_persisted_output`]). A failed attempt records nothing.
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

        // observability only: each `lap` closes one phase of the `persisted output` line
        let mut stamp = Instant::now();

        // insert blocks to db
        let provider_rw = self.inner.blockchain_provider.database_provider_rw()?;
        let open = lap(&mut stamp);
        provider_rw.save_blocks(blocks.to_vec(), reth_provider::SaveBlocksMode::Full)?;
        let save_blocks = lap(&mut stamp);
        // advance the finalized/safe markers in the same transaction as the blocks: every
        // canonical block comes from committed consensus output, so the last saved block is
        // final by construction, and the single commit leaves no crash window where the
        // persisted tip outruns the marker
        if let Some(last) = blocks.last() {
            let last_number = last.recovered_block.number;
            provider_rw.save_finalized_block_number(last_number)?;
            provider_rw.save_safe_block_number(last_number)?;
        }
        let markers = lap(&mut stamp);
        #[cfg(any(feature = "test-utils", test))]
        self.consume_injected_late_persist_fault()?;

        provider_rw.commit()?;
        let commit = lap(&mut stamp);

        record_persisted_output(blocks, PersistPhases { open, save_blocks, markers, commit });
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

/// `tn_reth.persist_save_blocks_seconds`: `save_blocks` time per successful persist.
const PERSIST_SAVE_BLOCKS_SECONDS: &str = "tn_reth.persist_save_blocks_seconds";
/// `tn_reth.persist_commit_seconds`: database commit time per successful persist.
const PERSIST_COMMIT_SECONDS: &str = "tn_reth.persist_commit_seconds";
/// `tn_reth.persisted_accounts_total`: changed accounts written, summed per block.
const PERSISTED_ACCOUNTS_TOTAL: &str = "tn_reth.persisted_accounts_total";
/// `tn_reth.persisted_storage_slots_total`: changed storage slots written, summed per block.
const PERSISTED_STORAGE_SLOTS_TOTAL: &str = "tn_reth.persisted_storage_slots_total";

/// Wall time of the phases of one [`RethEnv::build_block_from_batch_payload_timed`] call.
///
/// The four laps are disjoint; opening the parent state provider falls outside all of them.
/// Observability only: nothing here feeds block contents.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BlockBuildTimings {
    /// Parallel signer recovery of the batch's transactions; zero from
    /// [`RethEnv::build_block_from_recovered_payload`], which takes recovered signers.
    pub recover: Duration,
    /// Builder setup, the pre-execution system calls and the sequential transaction loop.
    pub evm: Duration,
    /// The layered state root inside `builder.finish`, including the read-only database
    /// transaction it opens.
    pub root: Duration,
    /// The rest of `builder.finish` (closing system calls, bundle merge, hashed post-state and
    /// block assembly), plus the bundle take, the delta sorts and the overlay extend.
    pub finish: Duration,
}

/// Wall time of each phase of one successful [`RethEnv::persist_executed_output`] call.
#[derive(Debug, Clone, Copy)]
struct PersistPhases {
    /// Opening the read-write database provider.
    open: Duration,
    /// `save_blocks` (database writes plus the static-file appends).
    save_blocks: Duration,
    /// Writing the finalized and safe block-number markers.
    markers: Duration,
    /// Committing the database transaction.
    commit: Duration,
}

/// What one [`RethEnv::persist_executed_output`] call wrote, summed over its blocks.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct PersistedCounts {
    /// Transactions in the persisted blocks.
    txs: usize,
    /// Receipts in the persisted blocks.
    receipts: usize,
    /// Accounts in each block's bundle state (an account changed in two blocks counts twice).
    accounts: usize,
    /// Storage slots in each block's bundle state, summed the same way.
    storage_slots: usize,
}

impl PersistedCounts {
    /// Count what `blocks` carry into the database.
    ///
    /// Summing map lengths is independent of `HashMap` iteration order; this feeds only logs and
    /// metrics, never block contents.
    fn of(blocks: &[ExecutedBlock]) -> Self {
        blocks.iter().fold(Self::default(), |counts, block| {
            let bundle = &block.execution_output.state;
            Self {
                txs: counts.txs + block.recovered_block.transaction_count(),
                receipts: counts.receipts + block.execution_output.result.receipts.len(),
                accounts: counts.accounts + bundle.state.len(),
                storage_slots: counts.storage_slots
                    + bundle.state.values().map(|account| account.storage.len()).sum::<usize>(),
            }
        })
    }
}

/// Log and record one successful persist of `blocks`.
///
/// The series use per-call `metrics::histogram!`/`counter!` macros rather than handles on
/// `RethEnvMetrics`: a persist runs about once per consensus output (~1/s), so resolving the
/// handle per call is cheap, and the macros bind to whichever recorder is current, which keeps
/// the unit test independent of test ordering (see the `metrics` module docs).
fn record_persisted_output(blocks: &[ExecutedBlock], phases: PersistPhases) {
    let counts = PersistedCounts::of(blocks);
    ::metrics::histogram!(PERSIST_SAVE_BLOCKS_SECONDS).record(phases.save_blocks);
    ::metrics::histogram!(PERSIST_COMMIT_SECONDS).record(phases.commit);
    ::metrics::counter!(PERSISTED_ACCOUNTS_TOTAL)
        .increment(u64::try_from(counts.accounts).unwrap_or(u64::MAX));
    ::metrics::counter!(PERSISTED_STORAGE_SLOTS_TOTAL)
        .increment(u64::try_from(counts.storage_slots).unwrap_or(u64::MAX));
    debug!(
        target: "engine",
        first = blocks.first().map(|block| block.recovered_block.number).unwrap_or_default(),
        last = blocks.last().map(|block| block.recovered_block.number).unwrap_or_default(),
        blocks = blocks.len(),
        txs = counts.txs,
        receipts = counts.receipts,
        accounts = counts.accounts,
        storage_slots = counts.storage_slots,
        open_ms = duration_ms(phases.open),
        save_blocks_ms = duration_ms(phases.save_blocks),
        markers_ms = duration_ms(phases.markers),
        commit_ms = duration_ms(phases.commit),
        "persisted output"
    );
}

/// Recover the signers of one batch's transactions in parallel, dropping the unrecoverable ones.
///
/// A transaction whose signer cannot be recovered cannot be executed, but a certified sub-DAG
/// is fixed and identical on every honest node, so returning an error here would
/// deterministically halt (and, on restart-replay, crash-loop) the whole network on a single
/// undecodable transaction. Instead the unrecoverable transactions are dropped, mirroring the
/// `InvalidTx` tolerance of the execute phase: every node drops the same transactions from the
/// same certified bytes, so the resulting block stays deterministic (issue #933). The primary
/// defense is validating batches before they can be certified; this only bounds the blast
/// radius if one ever is. Always `par_iter`: the overhead on small batches is negligible next
/// to the savings on large ones, and it avoids an extra code path.
///
/// The error line carries the batch digest and the transaction's byte length; the bytes
/// themselves are attacker-chosen and up to a batch in size, so they are never logged. The
/// caller counts the drops with [`record_unrecoverable`].
fn recover_batch_signers(
    transactions: &Vec<Vec<u8>>,
    batch_digest: B256,
) -> Vec<Recovered<TransactionSigned>> {
    transactions
        .par_iter()
        .filter_map(|tx_bytes| {
            reth_recover_raw_transaction::<TransactionSigned>(tx_bytes)
                .inspect_err(|e| {
                    error!(
                        target: "engine",
                        batch = ?batch_digest,
                        tx_len = tx_bytes.len(),
                        "failed to recover signer, dropping transaction: {e}"
                    )
                })
                .ok()
        })
        .collect()
}

/// Count the transactions signer recovery dropped: `handed` went in, `recovered` came out.
///
/// Recovery failures are the signal that a batch carrying undecodable transaction bytes was
/// certified, so they are alertable instead of log-only. Callers count once, after the parallel
/// phase: incrementing inside the rayon closure would contend a single atomic on every
/// transaction, and the batch width that sets the cost of that contention is attacker-chosen.
/// The difference is exact because `filter_map` is the only thing in
/// [`recover_batch_signers`] that removes elements. Unconditional, including the
/// overwhelmingly common zero: one relaxed atomic add is free next to executing the
/// transactions, and a guard would leave the series unregistered on a healthy node.
fn record_unrecoverable(handed: usize, recovered: usize) {
    RETH_METRICS
        .unrecoverable_txs_dropped_total
        .increment(u64::try_from(handed.saturating_sub(recovered)).unwrap_or(u64::MAX));
}

/// Return the time since `stamp` and move `stamp` to now, so consecutive calls time
/// consecutive phases.
fn lap(stamp: &mut Instant) -> Duration {
    let now = Instant::now();
    let elapsed = now.saturating_duration_since(*stamp);
    *stamp = now;
    elapsed
}

/// Whole milliseconds in `elapsed`, saturating at `u64::MAX`.
fn duration_ms(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{Address, U256};
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use reth_revm::{db::BundleState, state::AccountInfo};

    /// One block's account reverts, in the shape `BundleState::new` takes.
    type AccountReverts = Vec<(Address, Option<Option<AccountInfo>>, Vec<(U256, U256)>)>;

    /// A block whose bundle changes `accounts` accounts with `slots_each` storage slots apiece,
    /// carrying `receipts` receipts.
    fn block_with_state(accounts: u8, slots_each: u8, receipts: usize) -> ExecutedBlock {
        let state = (0..accounts).map(|a| {
            let storage = (0..slots_each)
                .map(|slot| (U256::from(slot), (U256::ZERO, U256::from(1u8))))
                .collect();
            (Address::with_last_byte(a), None, Some(AccountInfo::default()), storage)
        });
        let mut block = ExecutedBlock::default();
        let mut output = (*block.execution_output).clone();
        output.state = BundleState::new(state, Vec::<AccountReverts>::new(), Vec::new());
        output.result.receipts = vec![Default::default(); receipts];
        block.execution_output = Arc::new(output);
        block
    }

    /// Counts sum the bundle accounts, their storage slots and the receipts across blocks.
    #[test]
    fn test_persisted_counts_sum_over_blocks() {
        let blocks =
            [block_with_state(2, 3, 4), block_with_state(1, 1, 0), ExecutedBlock::default()];
        assert_eq!(
            PersistedCounts::of(&blocks),
            PersistedCounts { txs: 0, receipts: 4, accounts: 3, storage_slots: 7 }
        );
        assert_eq!(PersistedCounts::of(&[]), PersistedCounts::default());
    }

    /// A recorded persist registers both phase histograms and both write counters.
    #[test]
    fn test_record_persisted_output_metrics() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let blocks = [block_with_state(2, 3, 1), block_with_state(1, 2, 1)];

        ::metrics::with_local_recorder(&recorder, || {
            record_persisted_output(
                &blocks,
                PersistPhases {
                    open: Duration::from_millis(1),
                    save_blocks: Duration::from_millis(250),
                    markers: Duration::from_millis(2),
                    commit: Duration::from_millis(500),
                },
            );
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let find = |name: &str| {
            snapshot
                .iter()
                .find(|(key, ..)| key.key().name() == name)
                .map(|(.., value)| value)
                .unwrap_or_else(|| panic!("metric {name} not registered"))
        };
        assert!(matches!(
            find(PERSIST_SAVE_BLOCKS_SECONDS),
            DebugValue::Histogram(h) if h.len() == 1 && h[0].0 == 0.25
        ));
        assert!(matches!(
            find(PERSIST_COMMIT_SECONDS),
            DebugValue::Histogram(h) if h.len() == 1 && h[0].0 == 0.5
        ));
        assert!(matches!(find(PERSISTED_ACCOUNTS_TOTAL), DebugValue::Counter(3)));
        assert!(matches!(find(PERSISTED_STORAGE_SLOTS_TOTAL), DebugValue::Counter(8)));
    }

    /// Consecutive laps time consecutive phases and advance the stamp.
    #[test]
    fn test_lap_advances_stamp() {
        let start = Instant::now();
        let mut stamp = start;
        let first = lap(&mut stamp);
        assert!(stamp >= start);
        assert_eq!(stamp.saturating_duration_since(start), first);
        assert_eq!(duration_ms(Duration::from_micros(2_500)), 2);
    }
}
