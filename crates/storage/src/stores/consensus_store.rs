//! NOTE: tests for this module are in test-utils storage_tests.rs to avoid circular dependancies.

use crate::{
    tables::{ConsensusBlockNumbersByDigest, ConsensusBlocks},
    StoreResult,
};
use std::{cmp::max, collections::HashMap};
use tn_types::{
    AuthorityIdentifier, CommittedSubDag, ConsensusHeader, Database, DbTxMut, Round, SequenceNumber,
};
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

    /// Persist the sub dag to the consensus chain for some storage tests.
    /// This uses garbage parent hash and number and is ONLY for testing.
    /// As a test only function this will panic if unable to write the sub dag
    /// to the consensus chain
    pub fn write_subdag_for_test(&self, number: u64, sub_dag: CommittedSubDag) {
        let header = ConsensusHeader { number, sub_dag, ..Default::default() };
        let mut txn = self.db.write_txn().expect("failed to get DB txn");
        txn.insert::<ConsensusBlocks>(&header.number, &header)
            .expect("error saving a consensus header to persistant storage!");
        txn.insert::<ConsensusBlockNumbersByDigest>(&header.digest(), &header.number)
            .expect("error saving a consensus header to persistant storage!");
        txn.commit().expect("error saving a consensus header to persistant storage!");
    }

    /// Clear the consesus chain, ONLY for testing.
    /// Will panic on an error.
    pub fn clear_consensus_chain_for_test(&self) {
        let mut txn = self.db.write_txn().expect("failed to get txn");

        txn.clear_table::<ConsensusBlocks>().expect("failed to clear consensus blocks");
        txn.clear_table::<ConsensusBlockNumbersByDigest>()
            .expect("failed to clear consensus block indexes");

        txn.commit().expect("failed to clear consensus blocks");
    }

    /// Load the last committed round of each validator.
    pub fn read_last_committed(&self) -> HashMap<AuthorityIdentifier, Round> {
        let mut res = HashMap::new();
        for (id, round, certs) in
            self.db.reverse_iter::<ConsensusBlocks>().take(50).map(|(_, block)| {
                (
                    block.sub_dag.leader.origin(),
                    block.sub_dag.leader_round(),
                    block.sub_dag.certificates,
                )
            })
        {
            res.entry(id).and_modify(|r| *r = max(*r, round)).or_insert_with(|| round);
            for c in &certs {
                res.entry(c.origin())
                    .and_modify(|r| *r = max(*r, c.round()))
                    .or_insert_with(|| c.round());
            }
        }
        res
    }

    /// Returns the latest subdag committed. If none is committed yet, then
    /// None is returned instead.
    pub fn get_latest_sub_dag(&self) -> Option<CommittedSubDag> {
        self.db.last_record::<ConsensusBlocks>().map(|(_, block)| block.sub_dag)
    }

    /// Load all the sub dags committed with sequence number of at least `from`.
    pub fn read_committed_sub_dags_from(
        &self,
        from: &SequenceNumber,
    ) -> StoreResult<Vec<CommittedSubDag>> {
        Ok(self
            .db
            .skip_to::<ConsensusBlocks>(from)?
            .map(|(_, block)| block.sub_dag)
            .collect::<Vec<CommittedSubDag>>())
    }

    /// Reads from storage the latest commit sub dag where its ReputationScores are marked as
    /// "final". If none exists yet then this method will return None.
    pub fn read_latest_commit_with_final_reputation_scores(&self) -> Option<CommittedSubDag> {
        for commit in self.db.reverse_iter::<ConsensusBlocks>().map(|(_, block)| block.sub_dag) {
            // found a final of schedule score, so we'll return that
            if commit.reputation_score.final_of_schedule {
                debug!(
                    "Found latest final reputation scores: {:?} from commit",
                    commit.reputation_score,
                );
                return Some(commit);
            }
        }
        debug!("No final reputation scores have been found");
        None
    }
}

// NOTE: tests for this module are in test-utils storage_tests.rs to avoid circular dependancies.
