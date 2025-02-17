use crate::tables::Votes;
use tn_types::{AuthorityIdentifier, Database, Vote, VoteInfo};
use tn_utils::fail_point;

/// The storage for the last votes digests per authority
#[derive(Clone)]
pub struct VoteDigestStore<DB> {
    store: DB,
}

impl<DB: Database> VoteDigestStore<DB> {
    pub fn new(store: DB) -> VoteDigestStore<DB> {
        Self { store }
    }

    /// Insert the vote's basic details into the database for the corresponding
    /// header author key.
    #[allow(clippy::let_and_return)]
    pub fn write(&self, vote: &Vote) -> eyre::Result<()> {
        fail_point!("vote-digest-store-before-write");

        let result = self.store.insert::<Votes>(&vote.origin(), &vote.into());

        fail_point!("vote-digest-store-after-write");
        result
    }

    /// Read the vote info based on the provided corresponding header author key
    pub fn read(&self, header_author: &AuthorityIdentifier) -> eyre::Result<Option<VoteInfo>> {
        self.store.get::<Votes>(header_author)
    }
}
