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
//! cost bound. All other state reads still resolve through reth's memory overlay,
//! behind a read cache that the engine keeps for the whole output (see
//! [`OutputBuildScope`]).
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
use reth_revm::{
    cached::{CachedAccount, CachedReads},
    database::StateProviderDatabase,
    db::BundleState,
    State,
};
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
    mdbx_advice::PersistStorage,
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
    ///
    /// # Callers
    ///
    /// The engine never calls this: it recovers signers with [`Self::recover_output_signers`]
    /// and builds through [`Self::build_block_from_recovered_payload`]. Every remaining caller of
    /// this and [`Self::build_block_from_batch_payload_timed`] is a test, a `#[cfg(test)]` module
    /// or a `test-utils` helper. Both are kept as the differential oracle for the recovered path:
    /// they recover each batch themselves and build from an empty read cache, so a test can
    /// compare their blocks with the recovered build's.
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
    /// recovers one batch, and the recovered transactions go through the build that
    /// [`Self::build_block_from_recovered_payload`] runs, so both entry points share one build.
    /// Every call starts from an empty read cache; only the caller's `overlay` carries over from
    /// one block of an output to the next.
    ///
    /// Like the untimed call, this has no production caller: tests keep it as the differential
    /// oracle for the recovered path (see the callers note on
    /// [`Self::build_block_from_batch_payload`]).
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

        // a fresh read cache per call: these callers build one block at a time, often persisting
        // between builds, so no read is kept past the block it was made for
        let (block, timings) =
            self.build_recovered_block(payload, recovered, overlay, &mut CachedReads::default())?;
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
    ///
    /// `scope` is the consensus output's [`OutputBuildScope`]: the block's state root layers over
    /// its trie overlay, and its account, storage, code and block-hash reads go through its read
    /// cache before the parent state provider. A successful build leaves both describing the
    /// returned block's post-state, ready for the next block of the output. After an error the
    /// scope no longer matches any block the caller will build next: drop it.
    ///
    /// # Errors
    ///
    /// [`TnRethError::ScopeParentMismatch`] when the scope was advanced before and the payload's
    /// parent is not the block it was advanced to; nothing is built or advanced. Otherwise the
    /// errors of the build itself.
    pub fn build_block_from_recovered_payload(
        &self,
        payload: TNPayload,
        recovered: Vec<Recovered<TransactionSigned>>,
        scope: &mut OutputBuildScope,
    ) -> TnRethResult<(ExecutedBlock, BlockBuildTimings)> {
        scope.check_parent(payload.parent_header.hash())?;
        let OutputBuildScope { overlay, reads, tip } = scope;
        let (block, timings) = self.build_recovered_block(payload, recovered, overlay, reads)?;
        *tip = Some(block.recovered_block.hash());
        Ok((block, timings))
    }

    /// The block build behind every entry point: execute `recovered` on the parent state read
    /// through `reads`, root it over `overlay`, then advance `overlay` and `reads` to the built
    /// block's post-state.
    ///
    /// Neither is advanced on an error: every fallible step runs before the bundle is taken.
    fn build_recovered_block(
        &self,
        payload: TNPayload,
        recovered: Vec<Recovered<TransactionSigned>>,
        overlay: &mut OutputTrieOverlay,
        reads: &mut CachedReads,
    ) -> TnRethResult<(ExecutedBlock, BlockBuildTimings)> {
        let parent_header = payload.parent_header.clone();
        trace!(target: "engine::dump", ?parent_header, "retrieving state for next block");
        let state_provider =
            self.inner.blockchain_provider.state_by_block_hash(parent_header.hash())?;
        let state = StateProviderDatabase::new(&state_provider);
        let mut db =
            State::builder().with_database(reads.as_db_mut(state)).with_bundle_update().build();

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
            // copy the hash (cached at decode) and the recovered signer out for the skip line, so
            // the transaction moves into the builder without a per-transaction clone
            let tx_hash = *tx.tx_hash();
            let sender = tx.signer();
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
                    debug!(
                        target: "engine",
                        %error,
                        ?tx_hash,
                        %sender,
                        "skipping invalid transaction"
                    );
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
        // `finish` merged every transition of the block, the closing system calls included, so
        // the bundle is the block's whole state change; the build cannot fail past this point
        let bundle = db.take_bundle();
        // end the state's borrow of the read cache before the cache is advanced
        drop(db);
        advance_cached_reads(reads, &bundle);
        let block_execution_output =
            BlockExecutionOutput { result: execution_result, state: bundle };
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
    /// `markers_ms`, `commit_ms`), and records four series (see [`record_persisted_output`]):
    /// the histograms `tn_reth.persist_save_blocks_seconds` and `tn_reth.persist_commit_seconds`
    /// and the counters `tn_reth.persisted_accounts_total` and
    /// `tn_reth.persisted_storage_slots_total`. The line also carries what the persist did to the
    /// MDBX file (page-operation deltas, the process's `write_bytes` delta, the used size, the GC
    /// table; see `PersistStorage` in `env/mdbx_advice.rs`), read in microseconds without a second
    /// open of the environment. A failed attempt records nothing.
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
        // observability only: the storage counters start once this persist holds the write lock
        let mut probe = self.inner.mdbx.begin_persist();
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
        probe.before_commit();
        let markers = lap(&mut stamp);
        #[cfg(any(feature = "test-utils", test))]
        self.consume_injected_late_persist_fault()?;

        provider_rw.commit()?;
        let commit = lap(&mut stamp);

        let storage = self.inner.mdbx.finish_persist(probe);
        record_persisted_output(
            blocks,
            PersistPhases { open, save_blocks, markers, commit },
            &storage,
        );
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

/// The build state one consensus output's blocks share: the trie overlay their state roots layer
/// over and the cache their state reads go through.
///
/// The engine creates one scope per consensus output, before its first block, and passes it to
/// [`RethEnv::build_block_from_recovered_payload`] for each block of the output in order. A
/// successful build advances both halves to the built block's post-state; the scope is dropped
/// with the output.
///
/// # Invariant
///
/// The read cache is a subset of the true pre-block state for the next block of this output:
/// every cached account, storage slot, bytecode and block hash equals what that block's parent
/// state provider returns for it, and anything absent is read from the provider. A hit therefore
/// returns exactly what a fresh read would, and every block is byte-identical to the block an
/// empty cache builds; the differential tests in `tests/it/trie_overlay.rs` and
/// `tests/it/cached_reads_props.rs` pin that. Each build advances the cache from the block's
/// bundle state, the change set the next block's provider resolves the parent from (see
/// `advance_cached_reads`), so senders, recipients and contracts read by several blocks of one
/// output are loaded from the database once.
///
/// One revm `State` kept across the blocks would also save the reads but breaks the roots: an
/// account's `DestroyedChanged` status stays sticky across blocks, and reth rebuilds such an
/// account's storage as wiped, so an account created and self-destructed in one block, recreated
/// with a storage write in the next and written again in a third would lose the second block's
/// slot from the state root.
///
/// # Errors
///
/// A failed build advances neither half, so the scope still describes the failed block's
/// parent, which the caller will not build on again. Drop it: the engine aborts the whole output
/// on any build error.
///
/// The scope remembers the block it was last advanced to and refuses a payload on any other
/// parent ([`TnRethError::ScopeParentMismatch`]), so a scope kept past its output or reused on
/// another branch fails closed instead of serving reads of a different state.
#[derive(Debug, Default)]
pub struct OutputBuildScope {
    /// The output's accumulated trie overlay (see [`OutputTrieOverlay`]).
    overlay: OutputTrieOverlay,
    /// Reads of the next block's parent state, advanced past every built block.
    reads: CachedReads,
    /// The hash of the block the scope was last advanced to: the only parent it builds on.
    /// `None` until the first block of the output is built.
    tip: Option<B256>,
}

impl OutputBuildScope {
    /// An empty scope for the first block of an output: no overlay (the database holds the
    /// parent state) and no cached reads.
    pub fn new() -> Self {
        Self::default()
    }

    /// A scope for the first block of an output that builds on `tip`, an unpersisted block,
    /// with `overlay` holding the trie deltas between the database anchor and `tip`.
    ///
    /// The scope starts with no cached reads and refuses a first payload on any parent other
    /// than `tip` ([`TnRethError::ScopeParentMismatch`]), so an output seeded on one block cannot
    /// build on another. The caller vouches for the state behind the scope:
    /// - `overlay` holds exactly the deltas of every block after the database anchor up to `tip`,
    ///   in order, each added with [`OutputTrieOverlay::extend_from_block`]. A block whose persist
    ///   is still in flight is unpersisted: leaving it out leaves a gap, and every state root built
    ///   over the overlay is wrong.
    /// - A persist of those blocks may commit while the scope builds, so later roots can read a
    ///   database that already holds some of the overlay's runs. That reads the same state only
    ///   because each run holds absolute values (accounts, slots, tombstones, wipes and trie
    ///   nodes); a caller that lets a persist commit mid-output relies on it and must keep it
    ///   pinned by a differential test.
    /// - The blocks up to `tip` stay in reth's in-memory canonical state until their persist
    ///   commits, since the build reads `tip`'s state through it.
    pub fn seeded(overlay: OutputTrieOverlay, tip: B256) -> Self {
        Self { overlay, reads: CachedReads::default(), tip: Some(tip) }
    }

    /// The read cache the next block of the output starts from (tests inspect it).
    #[cfg(any(feature = "test-utils", test))]
    pub fn reads(&self) -> &CachedReads {
        &self.reads
    }

    /// Fail closed unless `parent` is the block the scope was last advanced to (any parent
    /// passes before the first build).
    fn check_parent(&self, parent: B256) -> TnRethResult<()> {
        match self.tip {
            Some(expected) if expected != parent => {
                Err(TnRethError::ScopeParentMismatch { expected, got: parent })
            }
            _ => Ok(()),
        }
    }
}

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
    /// block assembly), plus the bundle take, the read-cache advance, the delta sorts and the
    /// overlay extend.
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
///
/// The `page_*` fields, `proc_write_bytes` and `prefault_wchar_bytes` are deltas over the persist;
/// `mdbx_used_bytes`, `mdbx_advised_bytes`, `mdbx_last_pgno`, `gc_entries` and `gc_large_pages`
/// are values after the commit. A field whose source could not be read is left off the line.
fn record_persisted_output(
    blocks: &[ExecutedBlock],
    phases: PersistPhases,
    storage: &PersistStorage,
) {
    let counts = PersistedCounts::of(blocks);
    let ops = storage.page_ops;
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
        page_newly = ops.map(|ops| ops.newly),
        page_cow = ops.map(|ops| ops.cow),
        page_spill = ops.map(|ops| ops.spill),
        page_unspill = ops.map(|ops| ops.unspill),
        page_prefault = ops.map(|ops| ops.prefault),
        page_wops = ops.map(|ops| ops.wops),
        page_msync = ops.map(|ops| ops.msync),
        proc_write_bytes = storage.proc_write_bytes,
        mdbx_used_bytes = storage.mdbx_used_bytes,
        mdbx_advised_bytes = storage.mdbx_advised_bytes,
        mdbx_last_pgno = storage.mdbx_last_pgno,
        gc_entries = storage.gc.map(|gc| gc.entries),
        gc_large_pages = storage.gc.map(|gc| gc.large_pages),
        prefault_wchar_bytes = storage.prefault_wchar_bytes,
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

/// Advance an output's read cache from a built block's parent state to its post-state.
///
/// `bundle` is the block's whole state change. The next block's parent state provider answers
/// every address in it from it (`BundleAccount::account_info`, `BundleAccount::storage_slot`)
/// and every other address from the state below, which the block left unchanged, so cache
/// entries of addresses outside the bundle stay valid. Each address in the bundle gets the
/// provider's answer:
///
/// - destroyed in the block (`Destroyed`, `DestroyedChanged` when recreated after the destruct,
///   `DestroyedAgain`) or left without info (deleted, or cleared as empty under EIP-161): the entry
///   is evicted with its storage, and the next read reaches the provider, which applies the wipe;
/// - otherwise the new info without its code and its `account_id` (revm's hint for faster access to
///   the account's storage; a provider read carries neither, and revm loads code by hash) and each
///   written slot's present value. A storage-known status (an account the block created) also drops
///   the account's other cached slots, which the provider answers as zero.
///
/// Cached bytecode is keyed by its own hash, and cached block hashes belong to blocks below the
/// one that read them (revm asks the database only for the 256 before the current block), so
/// both name the same values for every later block of the output: `contracts` and
/// `block_hashes` are kept as they are.
fn advance_cached_reads(reads: &mut CachedReads, bundle: &BundleState) {
    // each bundle account updates only its own entry, so the map's iteration order cannot change
    // the result
    for (address, account) in &bundle.state {
        // evicting a recreated account (`DestroyedChanged`) is a deliberate, conservative choice:
        // it is storage-known, so it could be kept like `InMemoryChange`, but eviction costs one
        // re-read of a rare account and keeps the advance to the cases the differential tests pin
        let info = match &account.info {
            Some(info) if !account.status.was_destroyed() => info,
            _ => {
                reads.accounts.remove(address);
                continue;
            }
        };
        // `copy_without_code` keeps the storage hint, which a provider read never sets
        let mut info = info.copy_without_code();
        info.account_id = None;
        let cached = reads
            .accounts
            .entry(*address)
            .or_insert_with(|| CachedAccount { info: None, storage: Default::default() });
        cached.info = Some(info);
        if account.status.is_storage_known() {
            cached.storage.clear();
        }
        cached
            .storage
            .extend(account.storage.iter().map(|(slot, value)| (*slot, value.present_value)));
    }
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
    use reth_revm::{
        db::{states::StorageSlot, AccountStatus, BundleAccount, BundleState},
        primitives::KECCAK_EMPTY,
        state::{AccountInfo, Bytecode},
    };

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
                &PersistStorage::default(),
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

    /// Account info carrying code and an `account_id` storage hint, neither of which a cache entry
    /// may keep.
    fn info_with_code(balance: u64, nonce: u64) -> AccountInfo {
        let code = Bytecode::new_raw(alloy::primitives::Bytes::from_static(&[0x60, 0x00]));
        AccountInfo {
            balance: U256::from(balance),
            nonce,
            code_hash: code.hash_slow(),
            account_id: Some(3),
            code: Some(code),
        }
    }

    /// A cache entry with `info` and the `(slot, value)` pairs of `storage`.
    fn cached(info: Option<AccountInfo>, storage: &[(u64, u64)]) -> CachedAccount {
        let storage = storage.iter().map(|(slot, value)| (U256::from(*slot), U256::from(*value)));
        CachedAccount { info, storage: storage.collect() }
    }

    /// A bundle account with `info`, `status` and the `(slot, before, after)` writes of `storage`.
    fn bundled(
        info: Option<AccountInfo>,
        storage: &[(u64, u64, u64)],
        status: AccountStatus,
    ) -> BundleAccount {
        let storage = storage.iter().map(|(slot, before, after)| {
            (U256::from(*slot), StorageSlot::new_changed(U256::from(*before), U256::from(*after)))
        });
        BundleAccount::new(None, info, storage.collect(), status)
    }

    /// The read cache moves to the block's post-state: destroyed (also destroyed-then-changed and
    /// destroyed again), EIP-161-cleared and info-less accounts are evicted with their storage; a
    /// changed account takes its new info without code or `account_id` and its written slots
    /// and keeps its other cached slots; a created account keeps only the slots the block
    /// wrote; an account the cache did not hold is added; untouched accounts, code and block
    /// hashes stay as they were.
    #[test]
    fn test_advance_cached_reads_follows_bundle() {
        let address = Address::with_last_byte;
        let (destroyed, destroyed_changed, cleared, absent, destroyed_again) =
            (address(1), address(2), address(3), address(4), address(9));
        let (changed, created, uncached, untouched) =
            (address(5), address(6), address(7), address(8));
        let old_info =
            |balance: u64| AccountInfo { balance: U256::from(balance), ..Default::default() };

        let mut reads = CachedReads::default();
        reads.accounts.insert(destroyed, cached(Some(old_info(1)), &[(1, 5)]));
        reads.accounts.insert(destroyed_changed, cached(Some(old_info(2)), &[(1, 5)]));
        reads.accounts.insert(cleared, cached(Some(AccountInfo::default()), &[]));
        reads.accounts.insert(absent, cached(None, &[]));
        reads.accounts.insert(changed, cached(Some(old_info(10)), &[(1, 5), (3, 8)]));
        reads.accounts.insert(destroyed_again, cached(Some(old_info(3)), &[(1, 5)]));
        // stale non-zero slots under an absent account (orphaned storage): the block creates the
        // account, after which the provider answers zero for every slot the block did not write
        reads.accounts.insert(created, cached(None, &[(4, 7), (9, 3)]));
        reads.accounts.insert(untouched, cached(Some(old_info(1)), &[(1, 1)]));
        reads.contracts.insert(B256::with_last_byte(1), Bytecode::default());
        reads.block_hashes.insert(7, B256::with_last_byte(7));
        let contracts = reads.contracts.clone();
        let block_hashes = reads.block_hashes.clone();

        let bundle = BundleState {
            state: [
                (destroyed, bundled(None, &[], AccountStatus::Destroyed)),
                (
                    destroyed_changed,
                    bundled(
                        Some(info_with_code(2, 1)),
                        &[(2, 0, 7)],
                        AccountStatus::DestroyedChanged,
                    ),
                ),
                // the transition EIP-161 makes of a touched empty account
                (cleared, bundled(None, &[], AccountStatus::Destroyed)),
                (absent, bundled(None, &[], AccountStatus::LoadedNotExisting)),
                // destroyed, recreated and cleared again within the block
                (destroyed_again, bundled(None, &[], AccountStatus::DestroyedAgain)),
                (
                    changed,
                    bundled(Some(info_with_code(11, 2)), &[(1, 5, 6)], AccountStatus::Changed),
                ),
                (
                    created,
                    bundled(
                        Some(info_with_code(0, 1)),
                        &[(2, 0, 3)],
                        AccountStatus::InMemoryChange,
                    ),
                ),
                (
                    uncached,
                    bundled(Some(info_with_code(4, 0)), &[(5, 0, 6)], AccountStatus::Changed),
                ),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        advance_cached_reads(&mut reads, &bundle);

        for evicted in [destroyed, destroyed_changed, cleared, absent, destroyed_again] {
            assert!(!reads.accounts.contains_key(&evicted), "{evicted} must be evicted");
        }
        // `AccountInfo`'s `PartialEq` ignores code and `account_id`: compare every field
        let fields = |address: Address| {
            let account = &reads.accounts[&address];
            let info = account.info.as_ref().expect("cached info");
            let mut storage: Vec<(U256, U256)> =
                account.storage.iter().map(|(slot, value)| (*slot, *value)).collect();
            storage.sort();
            (
                info.balance,
                info.nonce,
                info.code_hash,
                info.code.is_none(),
                info.account_id,
                storage,
            )
        };
        let slots = |pairs: &[(u64, u64)]| -> Vec<(U256, U256)> {
            pairs.iter().map(|(slot, value)| (U256::from(*slot), U256::from(*value))).collect()
        };
        let code_hash = info_with_code(0, 0).code_hash;
        assert_eq!(
            fields(changed),
            (U256::from(11), 2, code_hash, true, None, slots(&[(1, 6), (3, 8)]))
        );
        assert_eq!(fields(created), (U256::ZERO, 1, code_hash, true, None, slots(&[(2, 3)])));
        assert_eq!(fields(uncached), (U256::from(4), 0, code_hash, true, None, slots(&[(5, 6)])));
        assert_eq!(
            fields(untouched),
            (U256::from(1), 0, KECCAK_EMPTY, false, None, slots(&[(1, 1)]))
        );
        assert_eq!(reads.accounts.len(), 4, "only the changed, created, uncached and untouched");
        assert_eq!(reads.contracts, contracts, "cached code is kept");
        assert_eq!(reads.block_hashes, block_hashes, "cached block hashes are kept");
    }

    /// A scope builds only on the block it was last advanced to; before its first build any
    /// parent passes.
    #[test]
    fn test_scope_builds_only_on_its_tip() {
        let (first, second) = (B256::with_last_byte(1), B256::with_last_byte(2));
        let mut scope = OutputBuildScope::new();
        assert!(scope.check_parent(first).is_ok(), "a fresh scope takes any parent");
        scope.tip = Some(second);
        assert!(scope.check_parent(second).is_ok(), "the tip is the next parent");
        assert!(matches!(
            scope.check_parent(first),
            Err(TnRethError::ScopeParentMismatch { expected, got })
                if expected == second && got == first
        ));
    }

    // ---- bench6 pipelined-persist ----

    /// A seeded scope keeps the overlay it was handed, starts with no cached reads, and builds
    /// only on the tip it was seeded with, even for its first block.
    #[test]
    fn test_seeded_scope_keeps_its_overlay_and_builds_only_on_its_tip() {
        let (tip, other) = (B256::with_last_byte(7), B256::with_last_byte(8));
        let mut overlay = OutputTrieOverlay::new();
        overlay.extend_from_block(&ComputedTrieData::default());
        let handed = format!("{overlay:?}");
        assert_ne!(
            handed,
            format!("{:?}", OutputTrieOverlay::new()),
            "test setup: a non-empty run"
        );

        let scope = OutputBuildScope::seeded(overlay, tip);
        assert_eq!(format!("{:?}", scope.overlay), handed, "the overlay is kept as handed");
        assert!(scope.reads.accounts.is_empty() && scope.reads.contracts.is_empty());
        assert!(scope.reads.block_hashes.is_empty());
        assert!(scope.check_parent(tip).is_ok(), "the seeded tip is the first parent");
        assert!(matches!(
            scope.check_parent(other),
            Err(TnRethError::ScopeParentMismatch { expected, got }) if expected == tip && got == other
        ));
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

    // ---- bench6 mdbx-advice ----

    /// The `persisted output` field names after the message, in order. FROZEN: the harness
    /// sampler, the per-arm summary tool and the analysis parse them.
    const PERSISTED_OUTPUT_FIELDS: [&str; 25] = [
        "first",
        "last",
        "blocks",
        "txs",
        "receipts",
        "accounts",
        "storage_slots",
        "open_ms",
        "save_blocks_ms",
        "markers_ms",
        "commit_ms",
        "page_newly",
        "page_cow",
        "page_spill",
        "page_unspill",
        "page_prefault",
        "page_wops",
        "page_msync",
        "proc_write_bytes",
        "mdbx_used_bytes",
        "mdbx_advised_bytes",
        "mdbx_last_pgno",
        "gc_entries",
        "gc_large_pages",
        "prefault_wchar_bytes",
    ];

    /// Golden: the `persisted output` line carries exactly the frozen fields, in order; a field
    /// whose source could not be read is left off and the rest stay.
    #[test]
    fn test_persisted_output_field_names() {
        use super::super::mdbx_advice::{
            test_support::persisted_output_lines, GcStat, PageOpCounts,
        };

        let phases = PersistPhases {
            open: Duration::ZERO,
            save_blocks: Duration::ZERO,
            markers: Duration::ZERO,
            commit: Duration::ZERO,
        };
        let names = |storage: PersistStorage| {
            let ((), lines) =
                persisted_output_lines(|| record_persisted_output(&[], phases, &storage));
            let [line] = lines.as_slice() else { panic!("one line: {lines:?}") };
            line.iter()
                .map(|(name, _)| name.clone())
                .filter(|name| name != "message")
                .collect::<Vec<_>>()
        };

        let full = PersistStorage {
            page_ops: Some(PageOpCounts::default()),
            proc_write_bytes: Some(0),
            mdbx_used_bytes: Some(0),
            mdbx_advised_bytes: 0,
            mdbx_last_pgno: Some(0),
            gc: Some(GcStat::default()),
            prefault_wchar_bytes: Some(0),
        };
        assert_eq!(names(full), PERSISTED_OUTPUT_FIELDS);

        // nothing readable: the timing fields and the advised bytes, which are always known
        let always: Vec<&str> =
            PERSISTED_OUTPUT_FIELDS[..11].iter().copied().chain(["mdbx_advised_bytes"]).collect();
        assert_eq!(names(PersistStorage::default()), always);
    }
}
