//! Remote calls to the server

use super::{
    request::{
        FetchBlocksRequest, MissingBlocksRequest, MissingCertificatesRequest, PrimaryVoteRequest,
    },
    response::{
        FetchBlocksResponse, FetchCertificatesResponse, RequestVoteResponse, StatusMessage,
    },
};
use ssz::Encode;
use tn_types::Certificate;

/// The RPC response indicating success or failure.
#[derive(Debug, Clone)]
pub enum RPCCodedResponse {
    /// The request was successfully executed.
    Success(RPCResponse),
    /// The server encountered an error while trying to fulfill the request.
    Error(RPCErrorCode),
}

#[derive(Debug, Clone, PartialEq)]
pub enum RPCResponse {
    /// HELLO "status" message
    Status(StatusMessage),
    /// Response from peers after receiving a certificate.
    CertificateAccepted,
    /// Response to voting request.
    ///
    /// TODO: this should be a vote or an error.
    VoteStatus(RequestVoteResponse),
    /// Response for fetched certificates.
    FetchCertificates(FetchCertificatesResponse),
    /// Response for fetched blocks from worker.
    _FetchBlocks(FetchBlocksResponse),
}

impl RPCResponse {
    /// Convenience method for retrieving serialized bytes.
    pub fn as_ssz_bytes(&self) -> Vec<u8> {
        match self {
            RPCResponse::Status(msg) => msg.as_ssz_bytes(),
            RPCResponse::CertificateAccepted => true.as_ssz_bytes(),
            RPCResponse::VoteStatus(request_vote_response) => todo!(),
            RPCResponse::FetchCertificates(fetch_certificates_response) => todo!(),
            RPCResponse::_FetchBlocks(fetch_blocks_response) => todo!(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RPCErrorCode {
    /// Too many requests
    RateLimited,
    /// Invalid request
    InvalidRequest,
    /// Internal error
    ServerError,
    /// Error spec'd to indicate that a peer does not have blocks on a requested range.
    /// Requested resource not available (blocks, etc)
    ResourceUnavailable,
    /// Unknown error type
    Unknown,
}

/// Requests from other peers.
pub enum InboundRequest {
    /// A new certificate broadcast from peer.
    Certificate(Certificate),
    /// Primary request for vote on new header.
    PrimaryVote(PrimaryVoteRequest),
    /// Request for missing certificates.
    FetchCertificates(MissingCertificatesRequest),
    /// Request for missing worker blocks.
    ///
    /// TODO: this is Primary -> Worker only
    FetchBlocks(FetchBlocksRequest),
    /// Worker is missing blocks
    ///
    /// TODO: this is Worker <-> Worker,
    /// need a way to separate worker/primary requests.
    MissingBlocks(MissingBlocksRequest),
}
