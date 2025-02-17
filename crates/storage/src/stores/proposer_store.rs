//! NOTE: tests for this module are in test-utils storage_tests.rs to avoid circular dependancies.

use crate::{tables::LastProposed, ProposerKey, StoreResult};
use tn_types::{Database, Header};
use tn_utils::fail_point;

pub const LAST_PROPOSAL_KEY: ProposerKey = 0;

/// The storage for the proposer
#[derive(Clone)]
pub struct ProposerStore<DB> {
    /// Holds the Last Header that was proposed by the Proposer.
    last_proposed: DB,
}

impl<DB: Database> ProposerStore<DB> {
    pub fn new(last_proposed: DB) -> ProposerStore<DB> {
        Self { last_proposed }
    }

    /// Inserts a proposed header into the store
    #[allow(clippy::let_and_return)]
    pub fn write_last_proposed(&self, header: &Header) -> StoreResult<()> {
        fail_point!("proposer-store-before-write");

        let result = self.last_proposed.insert::<LastProposed>(&LAST_PROPOSAL_KEY, header);

        fail_point!("proposer-store-after-write");
        result
    }

    /// Get the last header
    pub fn get_last_proposed(&self) -> StoreResult<Option<Header>> {
        self.last_proposed.get::<LastProposed>(&LAST_PROPOSAL_KEY)
    }
}

// NOTE: tests for this module are in test-utils storage_tests.rs to avoid circular dependancies.
