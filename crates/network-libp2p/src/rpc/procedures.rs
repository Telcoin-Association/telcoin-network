//! Remote calls to the server

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
    /// A HELLO message.
    Status,
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
