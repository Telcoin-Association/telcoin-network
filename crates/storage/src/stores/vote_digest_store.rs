use crate::tables::Votes;
use tn_types::{AuthorityIdentifier, Database, Vote, VoteInfo};
use tn_utils::fail_point;

/// The impl for the last votes digests per authority
pub trait VoteDigestStore {
    /// Insert the vote's basic details into the database for the corresponding
    /// header author key.
    fn write_vote(&self, vote: &Vote) -> eyre::Result<()>;

    /// Read the vote info based on the provided corresponding header author key
    fn read_vote_info(&self, header_author: &AuthorityIdentifier)
        -> eyre::Result<Option<VoteInfo>>;
}

impl<DB: Database> VoteDigestStore for DB {
    /// Insert the vote's basic details into the database for the corresponding
    /// header author key.
    #[allow(clippy::let_and_return)]
    fn write_vote(&self, vote: &Vote) -> eyre::Result<()> {
        fail_point!("vote-digest-store-before-write");

        let result = self.insert::<Votes>(vote.origin(), &vote.into());

        fail_point!("vote-digest-store-after-write");
        result
    }

    /// Read the vote info based on the provided corresponding header author key
    fn read_vote_info(
        &self,
        header_author: &AuthorityIdentifier,
    ) -> eyre::Result<Option<VoteInfo>> {
        self.get::<Votes>(header_author)
    }
}
