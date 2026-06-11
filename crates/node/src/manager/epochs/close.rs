//! Epoch teardown.
//!
//! The methods here form the closing half of [`EpochManager`]'s per-epoch
//! lifecycle. `run_epoch` (in the sibling `epochs` module) calls them in
//! sequence once an epoch ends, whether that end was a clean epoch boundary or
//! an early exit.
//!
//! Teardown recovers our own batches that never reached consensus — orphaned by
//! the epoch change or a restart — back into the mempool so their transactions
//! are not lost. On a non-boundary exit it also drains any consensus output that
//! was committed but not yet forwarded, sending it to the engine to avoid
//! orphaning finalized blocks. It then writes the finalized [`EpochRecord`] for
//! the just-closed epoch and clears the epoch-scoped consensus DB tables so the
//! next epoch starts from a clean slate. Historic data survives in the
//! `ConsensusChain` store; only the per-epoch working tables are cleared.

use crate::{engine::ExecutionNode, manager::EpochManager, primary::PrimaryNode};
use eyre::eyre;
use tn_config::TelcoinDirs;
use tn_reth::recover_raw_transaction;
use tn_storage::tables::{
    CertificateDigestByOrigin, CertificateDigestByRound, Certificates, LastProposed,
    NodeBatchesCache, OurNodeBatchesCache, Payload, ProposedCertificates, Votes,
};
use tn_types::{
    Batch, BlockHash, ConsensusHeaderDigest, ConsensusNumHash, ConsensusOutput,
    Database as TNDatabase, Epoch, EpochDigest, EpochRecord, TaskManager, TnReceiver,
};
use tn_worker::{quorum_waiter::QuorumWaiterTrait, Worker};
use tokio::sync::mpsc;
use tracing::{error, info, info_span, warn, Instrument};

impl<P, DB> EpochManager<P, DB>
where
    P: TelcoinDirs + Clone + 'static,
    DB: TNDatabase,
{
    /// Recover our own batches that never reached the consensus chain.
    ///
    /// Batches get orphaned when the epoch changes — or the node restarts —
    /// before they are included in a certificate. Their transactions would be
    /// lost otherwise, so we reintroduce them. An active CVV re-injects each
    /// transaction into its worker's mempool to be repackaged next epoch; a
    /// non-CVV is not building batches, so it disburses the batch directly.
    ///
    /// [`OurNodeBatchesCache`] is read and then immediately cleared: the cached
    /// batches are now defunct since their contents are back in flight. Recovery
    /// runs as a spawned task on the epoch [`TaskManager`] so it does not block
    /// the rest of teardown.
    pub(super) async fn orphan_batches<QuorumWaiter: QuorumWaiterTrait>(
        &mut self,
        epoch_task_manager: &TaskManager,
        engine: ExecutionNode,
        worker: Worker<DB, QuorumWaiter>,
        epoch: Epoch,
    ) -> eyre::Result<()> {
        // Collect any batches from this epoch that never made it to the consensus chain.
        let mut orphan_batches: Vec<(BlockHash, Batch)> =
            // Any batches in this table were created by us but never made it to consensus.
            self.consensus_db.iter::<OurNodeBatchesCache>().collect();
        // We have what we need so clear our Batch cache now.
        // We are reintroducing the transactions so these batches are now defunct.
        self.consensus_db.clear_table::<OurNodeBatchesCache>()?;
        if !orphan_batches.is_empty() {
            let consensus_bus = self.consensus_bus.clone();
            let span =
                info_span!(target: "telcoin", "orphan-batches", epoch = tracing::field::Empty);
            span.record("epoch", epoch.to_string());
            epoch_task_manager.spawn_task("Orphaned Batches", async move {
                info!(target: "epoch-manager", "Re-introducing orphaned batches {} transactions", orphan_batches.len());
                let pools = engine.get_all_worker_transaction_pools().await;
                let is_cvv = consensus_bus.is_active_cvv();
                for (digest, batch) in orphan_batches.drain(..) {
                    // Loop through any orphaned batches and resubmit it's transactions.
                    // This is most likely because of epoch changes but could be caused by a restart as
                    // well.
                    if is_cvv {
                        for tx_bytes in batch.transactions() {
                            // Put txn back into the mempool.
                            if let Ok(recovered) = recover_raw_transaction(tx_bytes) {
                                if let Some(pool) = pools.get(batch.worker_id as usize) {
                                    let _ = pool.add_recovered_transaction_external(recovered).await;
                                }
                            }
                        }
                    } else {
                        // If we are not a CVV then go ahead and disburse the txns from the batch directly.
                        let _ = worker.disburse_txns(batch.seal(digest)).await;
                    }
                }
                Ok(())
            }.instrument(span));
        } else {
            info!(target: "epoch-manager", "No batches leftover");
        }
        Ok(())
    }

    /// Flush any committed-but-unforwarded consensus output to the engine on a
    /// non-boundary exit.
    ///
    /// When the epoch ends early (e.g. a CVV that is behind), output may have
    /// been committed without yet reaching the engine; forwarding it here keeps
    /// it from being orphaned. Two phases cover the two ways output can be left
    /// behind. Phase 1 drains whatever is still queued in the broadcast channel.
    /// Phase 2 backfills the gap the channel cannot cover: during subscriber
    /// shutdown, output is saved to the pack-file DB but may never be
    /// broadcast, so we load every entry between `last_forwarded_consensus_number`
    /// and the DB latest and forward those too.
    ///
    /// Returns the [`ConsensusHeaderDigest`] of the first output committed at or
    /// past the epoch boundary, signalling that an epoch-boundary output was
    /// reached; `None` if no such output was found.
    pub(super) async fn send_leftover_consensus_output_to_engine(
        &mut self,
        consensus_output: &mut impl TnReceiver<ConsensusOutput>,
        to_engine: &mpsc::Sender<ConsensusOutput>,
    ) -> Option<ConsensusHeaderDigest> {
        // Phase 1: Drain broadcast channel (existing behavior)
        while let Ok(output) = consensus_output.try_recv() {
            let result = if output.committed_at() >= self.epoch_boundary {
                Some(output.consensus_header_hash())
            } else {
                None
            };
            // only forward the output to the engine
            let _ = self.process_output(to_engine, output).await;
            if result.is_some() {
                return result;
            }
        }

        // Phase 2: Check DB for outputs saved during subscriber shutdown drain.
        // During shutdown, the subscriber saves outputs to the pack file DB but may not
        // broadcast them through the channel. Scan for any gap between the last output
        // we forwarded and the DB latest, loading missing entries from the pack file.
        let latest_db = self.consensus_chain.latest_consensus_number();
        let last_sent = self.last_forwarded_consensus_number;
        if latest_db > last_sent {
            for number in (last_sent + 1)..=latest_db {
                match self.consensus_chain.get_consensus_output_current(number).await {
                    Ok(output) => {
                        let result = if output.committed_at() >= self.epoch_boundary {
                            Some(output.consensus_header_hash())
                        } else {
                            None
                        };
                        let _ = self.process_output(to_engine, output).await;
                        if result.is_some() {
                            return result;
                        }
                    }
                    Err(e) => {
                        warn!(target: "epoch-manager", number, ?e, "failed to load gap consensus from DB");
                        break;
                    }
                }
            }
        }

        None
    }

    /// Persist the finalized [`EpochRecord`] for the just-completed epoch and
    /// publish it on `epoch_record_watch` for downstream signing or signature
    /// collection.
    ///
    /// Epoch 0 is a special case: it starts with an unsigned "dummy" record so
    /// the initial committee is available before any certificate exists. Once a
    /// real cert is present we must overwrite that filler, which is why epoch 0
    /// only short-circuits on a record that already carries a certificate
    /// (`Some(_)`) — skipping this would leave the dummy in place and break sync.
    /// For later epochs an existing record short-circuits unconditionally.
    ///
    /// When building a fresh record, the previous epoch's `next_committee` must
    /// match this epoch's committee; a mismatch means the committee handoff is
    /// inconsistent and is treated as an error rather than silently recorded.
    pub(super) async fn write_epoch_record(
        &mut self,
        primary: &PrimaryNode<DB>,
        engine: &ExecutionNode,
    ) -> eyre::Result<()> {
        let committee = primary.current_committee().await;
        let epoch = committee.epoch();
        if epoch == 0 {
            // Epoch 0 will have a "dummy" epoch record to make the initial committee avaliable to
            // code using these records. In this case there will not be a cert so we
            // want to overwrite this with the correct record. That is why we need to
            // use Some(_) (this means we have a certificate) instead of _ like in the general case.
            // Without this we never overwrite the dummy epoch 0 record with the proper record and
            // would break sync.
            if let Some((epoch_rec, Some(_))) =
                self.consensus_chain.epochs().get_epoch_by_number(epoch).await
            {
                // We already have this record...
                self.consensus_bus.epoch_record_watch().send_replace(Some(epoch_rec));
                return Ok(());
            }
        } else if let Some((epoch_rec, _)) =
            self.consensus_chain.epochs().get_epoch_by_number(epoch).await
        {
            // We already have this record...
            self.consensus_bus.epoch_record_watch().send_replace(Some(epoch_rec));
            return Ok(());
        }

        let committee_keys = engine.validators_for_epoch(epoch).await?;
        let next_committee_keys = engine.validators_for_epoch(epoch + 1).await?;
        let parent_hash = if epoch == 0 {
            EpochDigest::default()
        } else if let Some(prev) = self.consensus_chain.epochs().record_by_epoch(epoch - 1).await {
            if committee_keys != prev.next_committee {
                error!(
                    target: "epoch-manager",
                    "Last epochs next committee not equal to this epochs committee! previous {:?}, current {:?}",
                    prev.next_committee,
                    committee_keys
                );
                return Err(eyre!(
                    "Last epochs next committee not equal to this epochs committee!"
                ));
            }
            prev.digest()
        } else {
            error!(
                target: "epoch-manager",
                "failed to find previous epoch record when starting epoch",
            );
            return Err(eyre!("failed to find previous epoch record when starting epoch"));
        };
        let last_consensus_header = self
            .last_consensus_header
            .take()
            .expect("epoch was finished with last consensus header");
        let target_hash = last_consensus_header.digest();
        let parent_state = self.consensus_bus.latest_execution_block_num_hash();

        let epoch_rec = EpochRecord {
            epoch,
            committee: committee_keys,
            next_committee: next_committee_keys,
            parent_hash,
            final_state: parent_state,
            final_consensus: ConsensusNumHash::new(last_consensus_header.number, target_hash),
        };

        self.consensus_chain.epochs().save_record(epoch_rec.clone()).await?;
        self.consensus_bus.epoch_record_watch().send_replace(Some(epoch_rec));
        Ok(())
    }

    /// Clear the epoch-scoped consensus tables so the next epoch starts clean.
    ///
    /// These tables hold only the working state of a single epoch — proposals,
    /// votes, certificates and their indexes, and the batch cache — so they are
    /// reset at every boundary. Complete historic data lives in the
    /// `ConsensusChain` store and is unaffected.
    ///
    /// [`OurNodeBatchesCache`] is deliberately left intact: `orphan_batches`
    /// still reads it to recover our un-consensed batches and clears it itself
    /// once recovery is done. Clearing it here would drop those batches.
    pub(super) fn clear_consensus_db_for_next_epoch(&self) -> eyre::Result<()> {
        self.consensus_db.clear_table::<LastProposed>()?;
        self.consensus_db.clear_table::<Votes>()?;
        self.consensus_db.clear_table::<Certificates>()?;
        self.consensus_db.clear_table::<CertificateDigestByRound>()?;
        self.consensus_db.clear_table::<CertificateDigestByOrigin>()?;
        self.consensus_db.clear_table::<ProposedCertificates>()?;
        self.consensus_db.clear_table::<Payload>()?;
        self.consensus_db.clear_table::<NodeBatchesCache>()?;
        // Note do not clear OurNodeBatchesCache here- we need to keep those until we process the
        // orphans and clear then.
        Ok(())
    }
}
