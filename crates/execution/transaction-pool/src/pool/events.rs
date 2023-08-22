use crate::{traits::PropagateKind, PoolTransaction, ValidPoolTransaction};
use std::sync::Arc;
use tn_types::{execution::{TxHash, H256}, consensus::BatchDigest};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// An event that happened to a transaction and contains its full body where possible.
#[derive(Debug)]
pub enum FullTransactionEvent<T: PoolTransaction> {
    /// Transaction has been added to the pending pool.
    Pending(TxHash),
    /// Transaction has been added to the queued pool.
    Queued(TxHash),
    /// Transaction has been included in the block belonging to this hash.
    Mined {
        /// The hash of the mined transaction.
        tx_hash: TxHash,
        /// The hash of the mined block that contains the transaction.
        block_hash: H256,
    },
    /// Transaction has been replaced by the transaction belonging to the hash.
    ///
    /// E.g. same (sender + nonce) pair
    Replaced {
        /// The transaction that was replaced.
        transaction: Arc<ValidPoolTransaction<T>>,
        /// The transaction that replaced the event subject.
        replaced_by: TxHash,
    },
    /// Transaction was dropped due to configured limits.
    Discarded(TxHash),
    /// Transaction became invalid indefinitely.
    Invalid(TxHash),
    /// Transaction was propagated to peers.
    Propagated(Arc<Vec<PropagateKind>>),
    /// Transaction was sealed in a batch.
    Sealed {
        /// The hash of the sealed transaction.
        tx_hash: TxHash,
        /// The digest for the batch that sealed the transaction.
        batch_digest: BatchDigest,
    }
}

impl<T: PoolTransaction> Clone for FullTransactionEvent<T> {
    fn clone(&self) -> Self {
        match self {
            Self::Replaced { transaction, replaced_by } => {
                Self::Replaced { transaction: Arc::clone(transaction), replaced_by: *replaced_by }
            }
            other => other.clone(),
        }
    }
}

/// Various events that describe status changes of a transaction.
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum TransactionEvent {
    /// Transaction has been added to the pending pool.
    Pending,
    /// Transaction has been added to the queued pool.
    Queued,
    /// Transaction has been included in the block belonging to this hash.
    Mined(H256),
    /// Transaction has been replaced by the transaction belonging to the hash.
    ///
    /// E.g. same (sender + nonce) pair
    Replaced(TxHash),
    /// Transaction was dropped due to configured limits.
    Discarded,
    /// Transaction became invalid indefinitely.
    Invalid,
    /// Transaction was propagated to peers.
    Propagated(Arc<Vec<PropagateKind>>),
    /// Transaction was sealed in a batch.
    Sealed(BatchDigest),
}

impl TransactionEvent {
    /// Returns `true` if the event is final and no more events are expected for this transaction
    /// hash.
    pub fn is_final(&self) -> bool {
        matches!(
            self,
            TransactionEvent::Replaced(_) |
                TransactionEvent::Mined(_) |
                TransactionEvent::Discarded
        )
    }
}
