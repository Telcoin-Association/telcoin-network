//! Epoch teardown.
//!
//! The methods here form the closing half of [`EpochManager`]'s per-epoch
//! lifecycle. `run_epoch` (in the sibling `run_epoch` module) calls them in
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

use crate::{engine::ExecutionNode, manager::EpochManager};
use eyre::eyre;
use std::collections::BTreeSet;
use tn_config::TelcoinDirs;
use tn_reth::recover_raw_transaction;
use tn_storage::tables::{
    CertificateDigestByOrigin, CertificateDigestByRound, Certificates, LastProposed,
    NodeBatchesCache, OurNodeBatchesCache, Payload, ProposedCertificates, Votes,
};
use tn_types::{
    Batch, BlockHash, BlockNumHash, BlsPublicKey, ConsensusHeaderDigest, ConsensusNumHash,
    ConsensusOutput, Database as TNDatabase, Epoch, EpochDigest, EpochRecord, TaskManager,
    TnReceiver,
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
    /// reached; `None` if no such output was found. That boundary output's header
    /// is also stashed in `last_consensus_header` so the caller's close-and-write
    /// sequence (`close_epoch`, then `write_epoch_record`) can commit the epoch's
    /// record.
    pub(super) async fn send_leftover_consensus_output_to_engine(
        &mut self,
        consensus_output: &mut impl TnReceiver<ConsensusOutput>,
        to_engine: &mpsc::Sender<ConsensusOutput>,
    ) -> Option<ConsensusHeaderDigest> {
        // Phase 1: Drain broadcast channel (existing behavior)
        while let Ok(output) = consensus_output.try_recv() {
            let result = if output.committed_at() >= self.epoch_boundary {
                // stash the boundary header for the caller's close-and-write sequence
                self.last_consensus_header = Some(output.clone().into());
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
                            // stash the boundary header for the caller's close-and-write sequence
                            self.last_consensus_header = Some(output.clone().into());
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
    /// collection. The record is flushed durably to disk before returning: the
    /// certificate-quorum path persists asynchronously, so without the explicit
    /// flush a crash between this write and that persist would lose the record —
    /// fatal for epoch 0, which has no peer-fetch anchor to heal from.
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
        epoch: Epoch,
        engine: &ExecutionNode,
    ) -> eyre::Result<()> {
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

        // CLOSE-TIME READS, INTENTIONALLY POST-BURN: these committee reads see the on-chain
        // state at epoch CLOSE, not the epoch-start pin used by the entry path. That is by
        // design: a mid-epoch governance burn swap-and-pops the ejected validator out of the
        // ConsensusRegistry's committee arrays immediately, and the epoch record is meant to
        // commit that shrunken committee (`build_epoch_record` tolerates the shrink against
        // the previous record's `next_committee` promise). Do NOT "fix" these reads to the
        // epoch-start pin.
        //
        // They ARE pinned, though — to `parent_state`, the epoch-closing block this record
        // commits as `final_state`. `parent_state` is the bus's `recent_blocks` watch value,
        // not the reth canonical tip: the engine publishes each executed output's last block
        // and its consensus hash in ONE watch write, so the bus can lag the true tip
        // mid-execution but never lead it. At every call site (the live boundary arm and the
        // two replay-and-close recovery arms of `run_epoch`, each running after `close_epoch`'s
        // `wait_for_consensus_execution`) the wait resolved on the exact write that carries the
        // closing block, so this read IS that block; the pin makes the record's committee reads
        // and its `final_state` derive from the same header by construction instead of by
        // timing.
        let parent_state = self.consensus_bus.latest_execution_block_num_hash();
        let committee_keys = engine.validators_for_epoch_at_block(epoch, parent_state.hash).await?;
        let next_committee_keys =
            engine.validators_for_epoch_at_block(epoch + 1, parent_state.hash).await?;
        let prev_record = if epoch == 0 {
            None
        } else {
            self.consensus_chain.epochs().record_by_epoch(epoch - 1).await
        };
        let last_consensus_header = self.last_consensus_header.take().ok_or_else(|| {
            eyre!("epoch {epoch} reached write_epoch_record without its boundary consensus header")
        })?;
        let target_hash = last_consensus_header.digest();

        let epoch_rec = build_epoch_record(
            epoch,
            committee_keys,
            next_committee_keys,
            prev_record.as_ref(),
            parent_state,
            ConsensusNumHash::new(last_consensus_header.number, target_hash),
        )?;

        self.consensus_chain.epochs().save_record(epoch_rec.clone()).await?;
        // the cert-quorum path persists asynchronously; a crash before that flush would lose
        // the record just saved — fatal for epoch 0, which has no peer-fetch anchor — so
        // flush it durably here before publishing
        self.consensus_chain.epochs().persist().await?;
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

/// Build the finalized [`EpochRecord`] for a just-completed epoch.
///
/// The committee keys are the fresh on-chain reads for `epoch` and `epoch + 1`
/// pinned to the epoch-closing block the record commits as `final_state`;
/// `prev` is the stored record for `epoch - 1`, required
/// for every epoch after 0. The new record chains to `prev` via `parent_hash`,
/// and the committee read for this epoch must be compatible with the previous
/// record's `next_committee` under [`EpochRecord::committee_compatible`]: the
/// same set (in any stored order), or a tolerated shrink when governance ejects
/// a validator mid-epoch (`burn` / slash-to-zero swap-and-pops the on-chain
/// committee array). A committee that grew, swapped in an unknown member, or
/// shrank below the sync tolerance is still an error rather than silently
/// recorded — sync would reject such a record, so producing it would only hide
/// the divergence.
pub fn build_epoch_record(
    epoch: Epoch,
    committee_keys: Vec<BlsPublicKey>,
    next_committee_keys: Vec<BlsPublicKey>,
    prev: Option<&EpochRecord>,
    final_state: BlockNumHash,
    final_consensus: ConsensusNumHash,
) -> eyre::Result<EpochRecord> {
    let parent_hash = if epoch == 0 {
        EpochDigest::default()
    } else if let Some(prev) = prev {
        prev.digest()
    } else {
        error!(
            target: "epoch-manager",
            "failed to find previous epoch record when starting epoch",
        );
        return Err(eyre!("failed to find previous epoch record when starting epoch"));
    };

    let record = EpochRecord {
        epoch,
        committee: committee_keys,
        next_committee: next_committee_keys,
        parent_hash,
        final_state,
        final_consensus,
    };

    if let Some(prev) = prev {
        let expected: BTreeSet<BlsPublicKey> = prev.next_committee.iter().copied().collect();
        if !record.committee_compatible(&expected) {
            error!(
                target: "epoch-manager",
                "Last epochs next committee not compatible with this epochs committee! previous {:?}, current {:?}",
                prev.next_committee,
                record.committee
            );
            return Err(eyre!(
                "Last epochs next committee not compatible with this epochs committee!"
            ));
        }
        if record.committee.len() < expected.len() {
            let ejected: Vec<_> =
                expected.iter().filter(|key| !record.committee.contains(key)).collect();
            warn!(
                target: "epoch-manager",
                ?ejected,
                epoch,
                "committee shrank mid-epoch: validator(s) ejected on-chain; recording shrunken committee"
            );
        }
    }

    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::StdRng, SeedableRng as _};
    use tn_types::BlsKeypair;

    /// Deterministic BLS public keys, one per seed byte.
    fn keys(seeds: std::ops::Range<u8>) -> Vec<BlsPublicKey> {
        seeds.map(|i| *BlsKeypair::generate(&mut StdRng::from_seed([i; 32])).public()).collect()
    }

    /// A previous-epoch record promising `next_committee` for the epoch under test.
    fn prev_record(next_committee: Vec<BlsPublicKey>) -> EpochRecord {
        EpochRecord {
            epoch: 0,
            committee: next_committee.clone(),
            next_committee,
            ..Default::default()
        }
    }

    fn final_state() -> BlockNumHash {
        BlockNumHash::new(42, BlockHash::repeat_byte(7))
    }

    fn final_consensus() -> ConsensusNumHash {
        ConsensusNumHash::new(9, ConsensusHeaderDigest::default())
    }

    /// identical committee handoff succeeds and preserves the read order.
    #[test]
    fn build_epoch_record_unchanged_committee() {
        let five = keys(0..5);
        let prev = prev_record(five.clone());
        let rec = build_epoch_record(
            1,
            five.clone(),
            five.clone(),
            Some(&prev),
            final_state(),
            final_consensus(),
        )
        .expect("unchanged committee handoff must succeed");
        assert_eq!(rec.epoch, 1);
        assert_eq!(rec.parent_hash, prev.digest());
        assert_eq!(rec.committee, five, "stored order must be exactly the on-chain read order");
        assert_eq!(rec.next_committee, five);
        assert_eq!(rec.final_state, final_state());
        assert_eq!(rec.final_consensus, final_consensus());
    }

    /// a governance `burn` mid-epoch swap-and-pops the ejected key out of the on-chain
    /// committee array, so the closing read is a 4-member subset of the 5 the previous
    /// record promised. The record must still build or every node halts at the boundary.
    #[test]
    fn build_epoch_record_tolerates_mid_epoch_ejection() {
        let five = keys(0..5);
        let prev = prev_record(five.clone());
        // swap-and-pop of five[2]: the last element moves into its slot
        let shrunken = vec![five[0], five[1], five[4], five[3]];
        let rec = build_epoch_record(
            1,
            shrunken.clone(),
            shrunken.clone(),
            Some(&prev),
            final_state(),
            final_consensus(),
        )
        .expect("mid-epoch ejection must not prevent the epoch record from building");
        assert_eq!(rec.committee, shrunken);
        assert_eq!(rec.parent_hash, prev.digest());
    }

    /// the same five keys in a different stored order are the same committee.
    #[test]
    fn build_epoch_record_reordered_only() {
        let five = keys(0..5);
        let prev = prev_record(five.clone());
        let mut rotated = five.clone();
        rotated.rotate_left(2);
        let rec = build_epoch_record(
            1,
            rotated.clone(),
            five.clone(),
            Some(&prev),
            final_state(),
            final_consensus(),
        )
        .expect("same committee in a different stored order must succeed");
        assert_eq!(rec.committee, rotated);
        assert_eq!(rec.parent_hash, prev.digest());
    }

    /// shrinking past the tolerance floor or bound stays an error — producing a record
    /// sync would reject only hides the divergence.
    #[test]
    fn build_epoch_record_rejects_below_tolerance() {
        // 4 -> 3 violates the minimum-committee floor of 4
        let four = keys(0..4);
        let prev = prev_record(four.clone());
        let three = four[..3].to_vec();
        assert!(build_epoch_record(
            1,
            three.clone(),
            three,
            Some(&prev),
            final_state(),
            final_consensus()
        )
        .is_err());

        // 8 -> 5 violates the ceil(2n/3) tolerance bound (needs >= 6 of 8)
        let eight = keys(0..8);
        let prev = prev_record(eight.clone());
        let five = eight[..5].to_vec();
        assert!(build_epoch_record(
            1,
            five.clone(),
            five,
            Some(&prev),
            final_state(),
            final_consensus()
        )
        .is_err());
    }

    /// a committee containing a key the previous record never promised is rejected —
    /// current committees cannot legitimately grow or swap members mid-epoch.
    #[test]
    fn build_epoch_record_rejects_unknown_member() {
        let five = keys(0..5);
        let prev = prev_record(five.clone());
        let fresh = *BlsKeypair::generate(&mut StdRng::from_seed([99; 32])).public();
        let committee = vec![five[0], five[1], five[2], five[3], fresh];
        assert!(build_epoch_record(
            1,
            committee.clone(),
            committee,
            Some(&prev),
            final_state(),
            final_consensus()
        )
        .is_err());
    }

    /// epoch 0 has no previous record to hand off from.
    #[test]
    fn build_epoch_record_epoch_zero_skips_handoff() {
        let five = keys(0..5);
        let rec = build_epoch_record(
            0,
            five.clone(),
            five.clone(),
            None,
            final_state(),
            final_consensus(),
        )
        .expect("epoch 0 must build without a previous record");
        assert_eq!(rec.parent_hash, EpochDigest::default());
        assert_eq!(rec.committee, five);
    }

    /// any later epoch without its previous record is an error.
    #[test]
    fn build_epoch_record_missing_prev_errors() {
        let five = keys(0..5);
        assert!(build_epoch_record(1, five.clone(), five, None, final_state(), final_consensus())
            .is_err());
    }
}
