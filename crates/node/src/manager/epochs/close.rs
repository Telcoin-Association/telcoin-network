//! Closing epoch responsibilities
//!

impl<P, DB> EpochManager<P, DB>
where
    P: TelcoinDirs + Clone + 'static,
    DB: TNDatabase,
{
    /// Collect any of our batches that never got into consensus (at epoch change or node restart)
    /// and Re-introduce them into the mempool for inclusion in future batches.
    async fn orphan_batches<QuorumWaiter: QuorumWaiterTrait>(
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

    /// We stopped waiting on the epoch boundary so lets make sure that the consensus queue
    /// is sent to the engine. If we don't do this it is possible that a quick
    /// exit could orphan output (for instance a CVV that is behind).
    /// We need to go until all the consensus output in DB has been sent to the engine (if it was
    /// saved it should have been sent).
    async fn send_leftover_consensus_output_to_engine(
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

    /// Record the epoch record for just completed epoch in our DB.
    /// Also record this in the manager for posible signing/collection of signatures.
    async fn write_epoch_record(
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

    /// Clear the epoch-related tables for consensus.
    ///
    /// These tables are epoch-specific. Complete historic data is stored
    /// in the `ConsensusChain` data store.
    fn clear_consensus_db_for_next_epoch(&self) -> eyre::Result<()> {
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
