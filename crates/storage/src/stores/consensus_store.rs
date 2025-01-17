//! NOTE: tests for this module are in test-utils storage_tests.rs to avoid circular dependancies.

use crate::{
    tables::{CommittedSubDag as CommittedSubDagTable, LastCommitted},
    traits::{Database, DbTxMut},
    StoreResult, ROUNDS_TO_KEEP,
};
use std::collections::HashMap;
use tn_types::{AuthorityIdentifier, CommittedSubDag, ConsensusCommit, Round, SequenceNumber};
use tracing::debug;

/// The persistent storage of the sequencer.
/// Uses DB tables:
///   - LastCommitted<AuthorityIdentifier, Round>: The latest committed round of each validator.
///   - CommittedSubDag<SequenceNumber, ConsensusCommit>: The global consensus sequence
pub struct ConsensusStore<DB> {
    /// The Consensus DB store.
    db: DB,
}

impl<DB: Database> ConsensusStore<DB> {
    /// Create a new consensus store structure by using already loaded maps.
    pub fn new(db: DB) -> Self {
        Self { db }
    }

    /// Clear the store.
    pub fn clear(&self) -> StoreResult<()> {
        let mut txn = self.db.write_txn()?;
        txn.clear_table::<LastCommitted>()?;
        txn.clear_table::<CommittedSubDagTable>()?;
        txn.commit()?;
        Ok(())
    }

    /// Persist the consensus state.
    pub fn write_consensus_state(
        &self,
        last_committed: &HashMap<AuthorityIdentifier, Round>,
        sub_dag: &CommittedSubDag,
    ) -> eyre::Result<()> {
        let mut txn = self.db.write_txn()?;
        let commit = ConsensusCommit::from_sub_dag(sub_dag);

        for (id, round) in last_committed.iter() {
            txn.insert::<LastCommitted>(id, round)?;
        }
        txn.insert::<CommittedSubDagTable>(&sub_dag.sub_dag_index, &commit)?;
        txn.commit()?;
        self.gc_rounds(sub_dag.sub_dag_index)?;
        Ok(())
    }

    /// Deletes all sub dags for a seq number before target_seq.
    fn gc_rounds(&self, target_seq: SequenceNumber) -> StoreResult<()> {
        if target_seq <= ROUNDS_TO_KEEP {
            return Ok(());
        }
        let target_seq = target_seq - ROUNDS_TO_KEEP;
        let mut dags = Vec::new();
        for (seq, _) in self.db.iter::<CommittedSubDagTable>() {
            if seq < target_seq {
                dags.push(seq);
            } else {
                // We are done, all following seq will be greater.
                break;
            }
        }
        let mut txn = self.db.write_txn()?;
        for seq in dags {
            txn.remove::<CommittedSubDagTable>(&seq)?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Load the last committed round of each validator.
    pub fn read_last_committed(&self) -> HashMap<AuthorityIdentifier, Round> {
        self.db.iter::<LastCommitted>().collect()
    }

    /// Gets the latest sub dag index from the store
    pub fn get_latest_sub_dag_index(&self) -> SequenceNumber {
        self.db.last_record::<CommittedSubDagTable>().map(|(seq, _)| seq).unwrap_or_default()
    }

    /// Returns thet latest subdag committed. If none is committed yet, then
    /// None is returned instead.
    pub fn get_latest_sub_dag(&self) -> Option<ConsensusCommit> {
        self.db.last_record::<CommittedSubDagTable>().map(|(_, sub_dag)| sub_dag)
    }

    /// Load all the sub dags committed with sequence number of at least `from`.
    pub fn read_committed_sub_dags_from(
        &self,
        from: &SequenceNumber,
    ) -> StoreResult<Vec<ConsensusCommit>> {
        Ok(self
            .db
            .skip_to::<CommittedSubDagTable>(from)?
            .map(|(_, sub_dag)| sub_dag)
            .collect::<Vec<ConsensusCommit>>())
    }

    /// Load consensus commit with a given sequence number.
    pub fn read_consensus_commit(
        &self,
        seq: &SequenceNumber,
    ) -> StoreResult<Option<ConsensusCommit>> {
        self.db.get::<CommittedSubDagTable>(seq)
    }

    /// Reads from storage the latest commit sub dag where its ReputationScores are marked as
    /// "final". If none exists yet then this method will return None.
    pub fn read_latest_commit_with_final_reputation_scores(&self) -> Option<ConsensusCommit> {
        for commit in self.db.reverse_iter::<CommittedSubDagTable>().map(|(_, sub_dag)| sub_dag) {
            // found a final of schedule score, so we'll return that
            if commit.reputation_score().final_of_schedule {
                debug!(
                    "Found latest final reputation scores: {:?} from commit {:?}",
                    commit.reputation_score(),
                    commit.sub_dag_index()
                );
                return Some(commit);
            }
        }
        debug!("No final reputation scores have been found");
        None
    }
}

// NOTE: tests for this module are in test-utils storage_tests.rs to avoid circular dependancies.
