//! Recover sender slots from canonical execution before replay or availability voting.

use crate::manager::EpochManager;
use futures::TryStreamExt;
use tn_config::TelcoinDirs;
use tn_reth::RethEnv;
use tn_types::{
    BatchSlotChainId, BatchSlotOutput, BatchSlotParent, BatchSlotVoteStore, BatchSlots, BlsSigner,
    Committee, Database, Hash as _, SealedHeader, SignedBatchSlotRecord, B256,
};

impl<P, DB> EpochManager<P, DB>
where
    P: TelcoinDirs + Clone + 'static,
    DB: Database,
{
    /// Install the epoch's canonical slot view before any output is forwarded to execution.
    ///
    /// Mid-epoch mode changes retain the live controller, including an in-flight publication
    /// and local retry demand. A fresh controller is reconstructed from committed execution
    /// headers and archived consensus bodies, so epoch snapshots need no slot sidecar.
    pub(super) async fn initialize_batch_slots(
        &self,
        reth_env: &RethEnv,
        committee: &Committee,
        epoch_start: &SealedHeader,
    ) -> eyre::Result<()> {
        let control = reth_env.batch_slots();
        if !tn_types::forks::batch_slots_active(committee.epoch()) {
            control.disable();
            Ok(())
        } else if control.snapshot().is_some_and(|slots| slots.epoch() == committee.epoch()) {
            Ok(())
        } else {
            let store = BatchSlotVoteStore::new(self.consensus_db.clone(), committee.epoch());
            store.initialize().await?;
            let opening_consensus = if epoch_start.number == 0 {
                epoch_start.parent_beacon_block_root.unwrap_or_default()
            } else {
                epoch_start
                    .parent_beacon_block_root
                    .ok_or_else(|| eyre::eyre!("epoch opening has no consensus anchor"))?
            };
            let slots = BatchSlots::new(
                BatchSlotChainId::new(reth_env.chainspec().chain_id()),
                committee.clone(),
                BatchSlotParent::new(opening_consensus, epoch_start.hash()),
            )?;
            let first = epoch_start
                .number
                .checked_add(1)
                .ok_or_else(|| eyre::eyre!("epoch opening block number overflow"))?;
            let tip = reth_env.last_block_number()?;
            eyre::ensure!(tip >= epoch_start.number, "execution tip precedes epoch opening");
            // Each changed output has at least one block. Consecutive blocks with the same
            // consensus root share an atomic commit; only the final hash opens successors.
            let anchors =
                (first..=tip).try_fold(Vec::<(B256, B256)>::new(), |mut anchors, number| {
                    let header = reth_env.sealed_header_by_number(number)?.ok_or_else(|| {
                        eyre::eyre!("missing canonical slot recovery block {number}")
                    })?;
                    let consensus = header.parent_beacon_block_root.ok_or_else(|| {
                        eyre::eyre!("slot recovery block {number} has no consensus root")
                    })?;
                    if let Some((_, execution)) =
                        anchors.last_mut().filter(|(root, _)| *root == consensus)
                    {
                        *execution = header.hash();
                    } else {
                        anchors.push((consensus, header.hash()));
                    }
                    Ok::<_, eyre::Report>(anchors)
                })?;
            let (slots, history) =
                futures::stream::iter(anchors.into_iter().map(Ok::<_, eyre::Report>))
                    .try_fold(
                        (slots, Vec::new()),
                        |(slots, mut history), (consensus, execution)| async move {
                            let header = self
                                .consensus_chain
                                .consensus_header_by_digest(committee.epoch(), consensus.into())
                                .await?
                                .ok_or_else(|| {
                                    eyre::eyre!(
                                        "missing consensus header for slot recovery {consensus}"
                                    )
                                })?;
                            let output = self
                                .consensus_chain
                                .get_consensus_output_current(header.number)
                                .await?;
                            eyre::ensure!(
                                output.leader().epoch() == committee.epoch()
                                    && B256::from(output.digest()) == consensus,
                                "slot recovery output disagrees with canonical execution"
                            );
                            let records = output
                                .flatten_batches()
                                .into_iter()
                                .map(|(certificate, batch)| {
                                    let batch = output
                                        .batches()
                                        .get(certificate)
                                        .and_then(|certificate| certificate.batches.get(batch))
                                        .ok_or_else(|| {
                                            eyre::eyre!("missing batch in slot recovery output")
                                        })?;
                                    let bytes = batch
                                        .transactions
                                        .iter()
                                        .try_fold(0_usize, |size, tx| size.checked_add(tx.len()));
                                    eyre::ensure!(
                                        bytes.is_some_and(|bytes| bytes
                                            <= tn_types::max_batch_size(committee.epoch())),
                                        "oversized native recovery envelope"
                                    );
                                    SignedBatchSlotRecord::from_envelope(batch).map_err(Into::into)
                                })
                                .collect::<eyre::Result<Vec<_>>>()?;
                            let (slots, closed) =
                                BatchSlotOutput::recover(slots, consensus, &records, execution)?;
                            history.extend(closed);
                            Ok((slots, history))
                        },
                    )
                    .await?;
            store.publish_authorizations(&history).await?;
            let key = self.key_config.public_key();
            let authority = committee
                .authorities()
                .iter()
                .any(|authority| authority.protocol_key() == &key)
                .then_some(key);
            let receiver = control.install(slots, authority);
            let publication = control.clone();
            // Epoch task teardown precedes the final output drain. The publisher therefore
            // lives until its channel closes when the controller is replaced at the boundary.
            // Dropping this task's handle detaches it; replacement closes the old receiver.
            let _publisher = tokio::spawn(async move { publication.serve(store, receiver).await });
            Ok(())
        }
    }
}
