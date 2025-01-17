//! Utils for consensus tests

use std::sync::Arc;
use tn_storage::{traits::Database, ConsensusStore};

pub const NUM_SUB_DAGS_PER_SCHEDULE: u32 = 100;

pub fn make_consensus_store<DB: Database>(db: DB) -> Arc<ConsensusStore<DB>> {
    Arc::new(ConsensusStore::new(db))
}
