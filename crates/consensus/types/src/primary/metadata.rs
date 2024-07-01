//! Metadata to include with each Batch.
//!
//! The [VersionedMetadata] includes the output
//! from the execution layer in the form of a
//! [SealedHeader]. The Batch itself contains the
//! raw transactions, which are used during validation
//! to recreate the included [SealedHeader].
use crate::{now, TimestampSec};
use enum_dispatch::enum_dispatch;
use reth_primitives::SealedHeader;
use serde::{Deserialize, Serialize};

/// Additional metadata information for an entity.
///
/// The structure as a whole is not signed. As a result this data
/// should not be treated as trustworthy data and should be used
/// for NON CRITICAL purposes only. For example should not be used
/// for any processes that are part of our protocol that can affect
/// safety or liveness.
///
/// This is a versioned `Metadata` type
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq /* , MallocSizeOf */)]
#[enum_dispatch(MetadataAPI)]
pub enum VersionedMetadata {
    /// version 1
    V1(MetadataV1),
}

impl VersionedMetadata {
    /// Create a new instance of [VersionedMetadata]
    pub fn new(sealed_header: SealedHeader) -> Self {
        Self::V1(MetadataV1 { created_at: now(), received_at: None, sealed_header })
    }

    /// Create a new instance of [VersionedMetadata] with a timestamp
    pub fn new_with_timestamp(created_at: TimestampSec, sealed_header: SealedHeader) -> Self {
        Self::V1(MetadataV1 { created_at, received_at: None, sealed_header })
    }

    /// Calculates a heuristic for the in-memory size of the [SealedHeader].
    pub fn size(&self) -> usize {
        self.sealed_header().size()
    }
}

impl Default for VersionedMetadata {
    /// Default implementation for `VersionedMetadata` used for testing only.
    fn default() -> Self {
        Self::V1(MetadataV1 {
            created_at: now(),
            received_at: None,
            sealed_header: SealedHeader::default(),
        })
    }
}

impl From<SealedHeader> for VersionedMetadata {
    fn from(sealed_header: SealedHeader) -> Self {
        VersionedMetadata::new(sealed_header)
    }
}

/// API for accessing fields for [VersionedMetadata] variants
///
/// TODO: update comments if leaving these fields in here.
#[enum_dispatch]
pub trait MetadataAPI {
    /// The time the [Batch] was created.
    ///
    /// TODO: should this be set by the Worker instead of EL?
    fn created_at(&self) -> &TimestampSec;
    /// Set the time when the [Batch] was received from a peer.
    fn set_created_at(&mut self, ts: TimestampSec);
    /// Retrieve the optional timestamp for when the [Batch] was received.
    fn received_at(&self) -> Option<TimestampSec>;
    /// Update the time the [Batch] was received.
    fn set_received_at(&mut self, ts: TimestampSec);

    /// The [SealedHeader] from executing the [Batch].
    fn sealed_header(&self) -> &SealedHeader;
}

/// Metadata for batches.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Default, Eq /* , MallocSizeOf */)]
pub struct MetadataV1 {
    /// Timestamp of when the entity created. This is generated
    /// by the node which creates the entity.
    pub created_at: TimestampSec,
    /// Timestamp of when the entity was received by another node. This will help
    /// calculate latencies that are not affected by clock drift or network
    /// delays. This field is not set for own batches.
    pub received_at: Option<TimestampSec>,
    /// [SealedHeader] from execution.
    pub sealed_header: SealedHeader,
}

impl MetadataAPI for MetadataV1 {
    fn created_at(&self) -> &TimestampSec {
        &self.created_at
    }

    fn set_created_at(&mut self, ts: TimestampSec) {
        self.created_at = ts;
    }

    fn received_at(&self) -> Option<TimestampSec> {
        self.received_at
    }

    fn set_received_at(&mut self, ts: TimestampSec) {
        self.received_at = Some(ts);
    }

    fn sealed_header(&self) -> &SealedHeader {
        &self.sealed_header
    }
}
