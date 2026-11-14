//! TN(TN-0202): reading the `Transactions` rows a jar with tagged values stores as positions
//! ([`TxRow::Ref`](super::TxRow::Ref)).
//!
//! Such a row holds the transaction's position in the consensus batch its block executed, not the
//! transaction. A [`DatabaseProvider`](crate::DatabaseProvider) resolves it through the
//! [`TxBodyResolver`] installed on the static-file provider (`set_tx_body_resolver`): it knows the
//! block of a transaction number (`TransactionBlocks`) and the block's header, which names the
//! batch. [`StaticFileProvider`](super::StaticFileProvider)'s and its jars' own
//! transaction reads know neither, so they return [`TxBodyReadError::InConsensusPack`] for such a
//! row. A row that cannot be resolved is an error on every read path, never a missing
//! transaction.

use super::TxRowError;
use alloy_primitives::{BlockNumber, TxHash, TxNumber};
use reth_node_types::NodePrimitives;
use reth_storage_errors::provider::ProviderResult;
use std::path::PathBuf;

/// TN(TN-0202): resolves the `Transactions` rows a version-3 jar stores as batch positions.
pub trait TxBodyResolver<N: NodePrimitives>: Send + Sync + core::fmt::Debug {
    /// The transactions at `positions` of the batch that block `number`'s `header` names, in
    /// order: exactly `positions.len()` of them, each the one at its position. Any failure is an
    /// error.
    fn resolve(
        &self,
        number: BlockNumber,
        header: &N::BlockHeader,
        positions: &[u32],
    ) -> ProviderResult<Vec<N::SignedTx>>;
}

/// TN(TN-0202): a `Transactions` row a read could not turn into its transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxBodyReadError {
    /// The row of transaction `tx` in static file `path` does not decode.
    Row {
        /// The jar's data file.
        path: PathBuf,
        /// The transaction number.
        tx: TxNumber,
        /// Why the row does not decode.
        error: TxRowError,
    },
    /// The row of transaction `tx` stores its position in its batch, and this reader has no block
    /// to resolve it with: a `StaticFileProvider` or jar read. A `DatabaseProvider` resolves it.
    InConsensusPack {
        /// The jar's data file.
        path: PathBuf,
        /// The transaction number.
        tx: TxNumber,
        /// The position the row stores.
        position: u32,
    },
    /// The row of transaction `tx` stores a position, and no [`TxBodyResolver`] is installed.
    NoResolver {
        /// The block of the transaction.
        block: BlockNumber,
        /// The transaction number.
        tx: TxNumber,
    },
    /// The row of transaction `tx` stores a position, and no block, or no block whose body holds
    /// `tx`, is indexed for it.
    NoBlock {
        /// The transaction number.
        tx: TxNumber,
        /// The block `TransactionBlocks` names for it, if any.
        block: Option<BlockNumber>,
    },
    /// Block `block` has transactions stored as positions, and its header is missing.
    NoHeader {
        /// The block.
        block: BlockNumber,
    },
    /// The resolver returned another number of transactions than the positions it was asked.
    WrongCount {
        /// The block.
        block: BlockNumber,
        /// The positions asked.
        asked: usize,
        /// The transactions returned.
        returned: usize,
    },
    /// A read by hash resolved transaction `tx`, whose hash is not the one asked.
    TxBodyMismatch {
        /// The hash asked.
        hash: TxHash,
        /// The transaction number the hash index gave.
        tx: TxNumber,
        /// The hash of the transaction resolved.
        resolved: TxHash,
    },
}

impl core::fmt::Display for TxBodyReadError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Row { path, tx, error } => {
                write!(f, "static file {}: transaction {tx}: {error}", path.display())
            }
            Self::InConsensusPack { path, tx, position } => write!(
                f,
                "transaction {tx} is stored in a consensus pack, as position {position} of its \
                 block's batch (static file {}); only a database provider resolves it",
                path.display()
            ),
            Self::NoResolver { block, tx } => write!(
                f,
                "transaction {tx} of block {block} is stored in a consensus pack and no resolver \
                 is installed"
            ),
            Self::NoBlock { tx, block: None } => write!(
                f,
                "transaction {tx} is stored in a consensus pack, and no block is indexed for it"
            ),
            Self::NoBlock { tx, block: Some(block) } => write!(
                f,
                "transaction {tx} is stored in a consensus pack, and the block indexed for it, \
                 {block}, has no body holding it"
            ),
            Self::NoHeader { block } => write!(
                f,
                "block {block} has transactions stored in a consensus pack, and its header is \
                 missing"
            ),
            Self::WrongCount { block, asked, returned } => write!(
                f,
                "the resolver returned {returned} transactions for {asked} positions of block \
                 {block}"
            ),
            Self::TxBodyMismatch { hash, tx, resolved } => write!(
                f,
                "transaction {tx}, indexed for hash {hash}, resolves to a transaction with hash \
                 {resolved}"
            ),
        }
    }
}

impl core::error::Error for TxBodyReadError {}

/// TN(TN-0202): a second [`TxBodyResolver`] was offered to a static-file provider that has one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxBodyResolverInstalled;

impl core::fmt::Display for TxBodyResolverInstalled {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "a transaction body resolver is already installed")
    }
}

impl core::error::Error for TxBodyResolverInstalled {}
