//! Batch implementation for consensus.
//!
//! Batches hold transactions and other data needed to
// Copyright (c) Telcoin, LLC
use crate::{crypto, MetadataAPI, VersionedMetadata};
use base64::{engine::general_purpose, Engine};
use enum_dispatch::enum_dispatch;
use fastcrypto::hash::{Digest, Hash, HashFunction};
use mem_utils::MallocSizeOf;
#[cfg(any(test, feature = "arbitrary"))]
use proptest_derive::Arbitrary;
use reth_primitives::{SealedBlock, SealedBlockWithSenders, TransactionSigned, Withdrawals};
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;
use tokio::sync::oneshot;

/// Type that batches contain.
pub type Transaction = Vec<u8>;

/// Type for sending ack back to EL once a batch is sealed.
/// TODO: support propagating errors from the worker to the primary.
pub type BatchResponse = oneshot::Sender<BatchDigest>;

/// Convenince error type for casting batches into SealedBlocks with senders.

/// Batch validation error types
#[derive(Error, Debug, Clone)]
pub enum BatchConversionError {
    /// Errors from BlockExecution
    #[error("Failed to recover signers for sealed block:\n{0:?}\n")]
    RecoverSigners(SealedBlock),
    /// Failed to decode transaction bytes
    #[error("RLP error decoding transaction: {0}")]
    DecodeTransaction(#[from] alloy_rlp::Error),
}

/// The message type for EL to CL when a new batch is made.
#[derive(Debug)]
pub struct NewBatch {
    /// A batch that was constructed by the EL.
    pub batch: Batch,
    /// Reply to the EL once the batch is stored.
    pub ack: BatchResponse,
    // TODO: add reason for sealing batch here
    // for metrics: `timeout`, 'gas', or 'bytes/size'
}

/// The batch for workers to communicate for consensus.
///
/// TODO: Batch is just another term for `SealedBlock` in Ethereum.
/// I think it would better to use `SealedBlock` instead of a redundant type.
#[cfg_attr(any(test, feature = "arbitrary"), derive(Arbitrary))]
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[enum_dispatch(BatchAPI)]
pub enum Batch {
    /// Version 1 - based on sui V2
    V1(BatchV1),
}
impl Batch {
    /// Create a new batch for testing only!
    ///
    /// This is not a valid batch for consensus. Metadata uses defaults.
    pub fn new(transactions: Vec<Transaction>) -> Self {
        Self::V1(BatchV1::new(transactions, VersionedMetadata::default()))
    }

    /// Create a new batch with versioned metadata.
    ///
    /// This should be used when the batch was fully executed.
    pub fn new_with_metadata(
        transactions: Vec<Transaction>,
        versioned_metadata: VersionedMetadata,
    ) -> Self {
        Self::V1(BatchV1::new(transactions, versioned_metadata))
    }

    /// Size of the batch variant's inner data.
    pub fn size(&self) -> usize {
        match self {
            Batch::V1(data) => data.size(),
        }
    }
}

impl TryFrom<&Batch> for SealedBlockWithSenders {
    type Error = BatchConversionError;

    fn try_from(batch: &Batch) -> Result<Self, Self::Error> {
        let header = batch.versioned_metadata().sealed_header().clone();
        // decode transactions
        let tx_signed: Result<Vec<TransactionSigned>, alloy_rlp::Error> = batch
            .transactions_owned()
            .map(|tx| TransactionSigned::decode_enveloped(&mut tx.as_ref()))
            .collect();
        let body = tx_signed?;
        // seal block
        let block = SealedBlock {
            header,
            body,
            ommers: vec![],
            withdrawals: Some(Withdrawals::new(vec![])),
            requests: None,
        };
        block.try_seal_with_senders().map_err(Self::Error::RecoverSigners)
    }
}

impl From<Vec<Vec<u8>>> for Batch {
    fn from(value: Vec<Vec<u8>>) -> Self {
        Batch::new(value)
    }
}

impl Hash<{ crypto::DIGEST_LENGTH }> for Batch {
    type TypedDigest = BatchDigest;

    fn digest(&self) -> BatchDigest {
        match self {
            Batch::V1(data) => data.digest(),
        }
    }
}

/// API for access data from versioned Batch variants.
///
/// TODO: update comments once EL data is finalized between Batch and VersionedMetadata
#[enum_dispatch]
pub trait BatchAPI {
    /// TODO
    fn transactions(&self) -> &Vec<Transaction>;
    /// TODO
    fn transactions_mut(&mut self) -> &mut Vec<Transaction>;
    /// TODO
    fn versioned_metadata(&self) -> &VersionedMetadata;
    /// TODO
    fn versioned_metadata_mut(&mut self) -> &mut VersionedMetadata;
    /// TODO
    fn owned_metadata(self) -> VersionedMetadata;
    /// Return an owned iteration of transactions.
    fn transactions_owned(&self) -> std::vec::IntoIter<Vec<u8>>;
}

/// The batch version.
///
/// akin to BatchV2 in sui
#[cfg_attr(any(test, feature = "arbitrary"), derive(Arbitrary))]
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct BatchV1 {
    /// List of transactions.
    ///
    /// A batch `Transaction` is an encoded `TransactionSigned` from the EL.
    /// Recovering senders is CPU intensive, but so it network bandwidth. For this
    /// protocol, network bandwidth is more costly so only the signed version is sent.
    /// It is the responsibility of peers to recover signers and verify transactions.
    pub transactions: Vec<Transaction>,

    /// Metadata for batch.
    ///
    /// This field is not included as part of the batch digest
    pub versioned_metadata: VersionedMetadata,
}

impl BatchAPI for BatchV1 {
    fn transactions(&self) -> &Vec<Transaction> {
        &self.transactions
    }

    fn transactions_mut(&mut self) -> &mut Vec<Transaction> {
        &mut self.transactions
    }

    fn versioned_metadata(&self) -> &VersionedMetadata {
        &self.versioned_metadata
    }

    fn versioned_metadata_mut(&mut self) -> &mut VersionedMetadata {
        &mut self.versioned_metadata
    }

    fn owned_metadata(self) -> VersionedMetadata {
        self.versioned_metadata
    }

    fn transactions_owned(&self) -> std::vec::IntoIter<Vec<u8>> {
        self.transactions.clone().into_iter()
    }
}

impl BatchV1 {
    /// Create a new BatchV1
    pub fn new(transactions: Vec<Transaction>, versioned_metadata: VersionedMetadata) -> Self {
        Self {
            transactions,
            // Default metadata uses defaults for ExecutionPayload
            versioned_metadata,
        }
    }

    /// The size of the BatchV1 transactions body and sealed header.
    pub fn size(&self) -> usize {
        let tx_size: usize = self.transactions.iter().map(|t| t.len()).sum();
        let header_size = self.versioned_metadata.size();
        tx_size + header_size
    }
}

/// Digest of the batch.
#[cfg_attr(any(test, feature = "arbitrary"), derive(Arbitrary))]
#[derive(
    Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq, Hash, PartialOrd, Ord, MallocSizeOf,
)]
pub struct BatchDigest(pub [u8; crypto::DIGEST_LENGTH]);

impl fmt::Debug for BatchDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", general_purpose::STANDARD.encode(self.0))
    }
}

impl fmt::Display for BatchDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", general_purpose::STANDARD.encode(self.0).get(0..16).ok_or(fmt::Error)?)
    }
}

impl AsRef<[u8]> for BatchDigest {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<BatchDigest> for Digest<{ crypto::DIGEST_LENGTH }> {
    fn from(digest: BatchDigest) -> Self {
        Digest::new(digest.0)
    }
}

impl BatchDigest {
    /// New BatchDigest
    pub fn new(val: [u8; crypto::DIGEST_LENGTH]) -> BatchDigest {
        BatchDigest(val)
    }
}

impl Hash<{ crypto::DIGEST_LENGTH }> for BatchV1 {
    type TypedDigest = BatchDigest;

    fn digest(&self) -> Self::TypedDigest {
        BatchDigest::new(
            crypto::DefaultHashFunction::digest_iterator(self.transactions.iter()).into(),
        )
    }
}
