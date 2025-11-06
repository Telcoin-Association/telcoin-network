//! NOTE: tests for this module are in test-utils storage_tests.rs to avoid circular dependancies.

use crate::{tables::LastProposed, ProposerKey, StoreResult};
use tn_types::{Database, Header};

/// The last proposal key - always 0.
pub const LAST_PROPOSAL_KEY: ProposerKey = 0;

/// Database trait for proposals (primary headers).
pub trait ProposerStore {
    /// Inserts a proposed header into the store
    fn write_last_proposed(&self, header: &Header) -> StoreResult<()>;

    /// Get the last header
    fn get_last_proposed(&self) -> StoreResult<Option<Header>>;
}

impl<DB: Database> ProposerStore for DB {
    #[allow(clippy::let_and_return)]
    fn write_last_proposed(&self, header: &Header) -> StoreResult<()> {
        let result = self.insert::<LastProposed>(&LAST_PROPOSAL_KEY, header);
        result
    }

    fn get_last_proposed(&self) -> StoreResult<Option<Header>> {
        self.get::<LastProposed>(&LAST_PROPOSAL_KEY)
    }
}

// NOTE: tests for this module are in test-utils storage_tests.rs to avoid circular dependancies.
