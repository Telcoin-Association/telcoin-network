//! Fixtures used in multiple tests.

use crate::TNMessage;
use serde::{Deserialize, Serialize};
use tn_types::{BlockHash, Certificate, CertificateDigest, Header, SealedBatch, Vote};

// impl TNMessage trait for types
impl TNMessage for TestWorkerRequest {}
impl TNMessage for TestWorkerResponse {}
impl TNMessage for TestPrimaryRequest {}
impl TNMessage for TestPrimaryResponse {}

/// Requests between workers.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TestWorkerRequest {
    /// Broadcast a newly produced worker block.
    ///
    /// NOTE: expect no response
    NewBatch(SealedBatch),
    /// The collection of missing [BlockHash]es for this peer.
    MissingBatches(Vec<BlockHash>),
}

/// Response to worker requests.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TestWorkerResponse {
    /// Return the missing blocks requested by the peer.
    ///
    /// TODO: anemo included `size_limit_reached: bool` field
    /// but this should be trustless. See `RequestBlocksResponse` message.
    MissingBatches {
        /// The collection of requested blocks.
        batches: Vec<SealedBatch>,
    },
}

/// Requests from Primary.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TestPrimaryRequest {
    NewCertificate { certificate: Certificate },
    Vote { header: Header, parents: Vec<Certificate> },
}

/// Response to primary requests.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TestPrimaryResponse {
    Vote(Vote),
    MissingCertificates(Vec<Certificate>),
    MissingParents(Vec<CertificateDigest>),
}
