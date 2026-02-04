//! NOTE: tests for this module are in test-utils storage_tests.rs to avoid circular dependancies.

use crate::tables::{ConsensusBlockNumbersByDigest, ConsensusBlocks, ConsensusBlocksCache};
use std::{cmp::max, collections::HashMap, sync::Arc};
use tn_types::{
    AuthorityIdentifier, BlockHash, CommittedSubDag, ConsensusHeader, Database, DbTxMut, Epoch,
    Round,
};
use tracing::debug;

/// Implement persistent storage of the consensus chain.
/// Uses DB tables:
///   - ConsensusBlocks
///   - ConsensusBlockNumbersByDigest
///   - ConsensusBlocksCache
pub trait ConsensusStore: Clone {
    /// Persist the sub dag to the consensus chain for some storage tests.
    /// This uses garbage parent hash and number and is ONLY for testing.
    /// As a test only function this will panic if unable to write the sub dag
    /// to the consensus chain
    fn write_subdag_for_test(&self, number: u64, sub_dag: CommittedSubDag);

    /// Clear the consensus chain, ONLY for testing.
    /// Will panic on an error.
    fn clear_consensus_chain_for_test(&self);

    /// Load the last committed round of each validator.
    fn read_last_committed(&self, epoch: Epoch) -> HashMap<AuthorityIdentifier, Round>;

    /// Returns the latest subdag committed. If none is committed yet, then
    /// None is returned instead.
    fn get_latest_sub_dag(&self) -> Option<Arc<CommittedSubDag>>;

    /// Reads from storage the latest commit sub dag from the epoch where its
    /// ReputationScores are marked as "final". If none exists then this
    /// method returns `None`.
    fn read_latest_commit_with_final_reputation_scores(
        &self,
        epoch: Epoch,
    ) -> Option<Arc<CommittedSubDag>>;

    /// Get a ConsensusHeader by hash.
    fn get_consensus_by_hash(&self, hash: BlockHash) -> Option<ConsensusHeader>;

    /// Get a ConsensusHeader by number.
    fn get_consensus_by_number(&self, number: u64) -> Option<ConsensusHeader>;

    /// If there is consensus for hash in the consensus chain remove it.
    /// This is used in case output is added after epoch end.
    fn remove_consensus_by_hash(&self, hash: BlockHash);
}

impl<DB: Database> ConsensusStore for DB {
    fn write_subdag_for_test(&self, number: u64, sub_dag: CommittedSubDag) {
        let header = ConsensusHeader { number, sub_dag: Arc::new(sub_dag), ..Default::default() };
        let mut txn = self.write_txn().expect("failed to get DB txn");
        txn.insert::<ConsensusBlocks>(&header.number, &header)
            .expect("error saving a consensus header to persistant storage!");
        txn.insert::<ConsensusBlockNumbersByDigest>(&header.digest(), &header.number)
            .expect("error saving a consensus header to persistant storage!");
        txn.commit().expect("error saving a consensus header to persistant storage!");
    }

    fn clear_consensus_chain_for_test(&self) {
        let mut txn = self.write_txn().expect("failed to get txn");

        txn.clear_table::<ConsensusBlocks>().expect("failed to clear consensus blocks");
        txn.clear_table::<ConsensusBlockNumbersByDigest>()
            .expect("failed to clear consensus block indexes");

        txn.commit().expect("failed to clear consensus blocks");
    }

    fn read_last_committed(&self, epoch: Epoch) -> HashMap<AuthorityIdentifier, Round> {
        let mut res = HashMap::new();
        for block in self.reverse_iter::<ConsensusBlocks>().take(50).filter_map(|(_, block)| {
            if block.sub_dag.leader_epoch() == epoch {
                Some(block)
            } else {
                None
            }
        }) {
            let id = block.sub_dag.leader.origin().clone();
            let round = block.sub_dag.leader_round();
            let certs = block.sub_dag.certificates();
            res.entry(id).and_modify(|r| *r = max(*r, round)).or_insert_with(|| round);
            for c in certs {
                res.entry(c.origin().clone())
                    .and_modify(|r| *r = max(*r, c.round()))
                    .or_insert_with(|| c.round());
            }
        }
        res
    }

    fn get_latest_sub_dag(&self) -> Option<Arc<CommittedSubDag>> {
        self.last_record::<ConsensusBlocks>().map(|(_, block)| block.sub_dag)
    }

    fn read_latest_commit_with_final_reputation_scores(
        &self,
        epoch: Epoch,
    ) -> Option<Arc<CommittedSubDag>> {
        for commit in self.reverse_iter::<ConsensusBlocks>().map(|(_, block)| block.sub_dag) {
            // ignore previous epochs
            if commit.leader_epoch() < epoch {
                return None;
            }

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

    fn get_consensus_by_hash(&self, hash: BlockHash) -> Option<ConsensusHeader> {
        if let Ok(Some(number)) = self.get::<ConsensusBlockNumbersByDigest>(&hash) {
            if let Ok(Some(block)) = self.get::<ConsensusBlocks>(&number) {
                Some(block)
            } else if let Ok(Some(block)) = self.get::<ConsensusBlocksCache>(&number) {
                Some(block)
            } else {
                None
            }
        } else {
            None
        }
    }

    fn get_consensus_by_number(&self, number: u64) -> Option<ConsensusHeader> {
        if let Ok(Some(block)) = self.get::<ConsensusBlocks>(&number) {
            Some(block)
        } else if let Ok(Some(block)) = self.get::<ConsensusBlocksCache>(&number) {
            Some(block)
        } else {
            None
        }
    }

    fn remove_consensus_by_hash(&self, hash: BlockHash) {
        if let Ok(Some(number)) = self.get::<ConsensusBlockNumbersByDigest>(&hash) {
            let _ = self.remove::<ConsensusBlockNumbersByDigest>(&hash);
            let _ = self.remove::<ConsensusBlocks>(&number);
        }
    }
}

// NOTE: tests for this module are in test-utils storage_tests.rs to avoid circular dependancies.
