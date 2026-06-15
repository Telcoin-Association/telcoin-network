//! Code to support various chain forks.

#[cfg(feature = "adiri")]
use crate::Epoch;

#[cfg(feature = "adiri")]
/// The epoch below which Adiri testnet may have had duplicate batches.
pub const ADIRI_DUP_BATCH_EPOCH: Epoch = 153;
