// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use std::fmt::Debug;
use thiserror::Error;
use tn_storage::StoreError;
use tn_types::{CertificateDigest, WorkerId};

/// Return an error if the condition is false.
#[macro_export(local_inner_macros)]
macro_rules! ensure {
    ($cond:expr, $e:expr) => {
        if !($cond) {
            return Err($e);
        }
    };
}

pub type SubscriberResult<T> = Result<T, SubscriberError>;

#[derive(Debug, Error)]
pub enum SubscriberError {
    #[error("channel {0} closed unexpectedly")]
    ClosedChannel(String),

    #[error("Storage failure: {0}")]
    StoreError(#[from] StoreError),

    #[error("Error occurred while retrieving certificate {0} payload: {1}")]
    PayloadRetrieveError(CertificateDigest, String),

    #[error("Consensus referenced unexpected worker id {0}")]
    UnexpectedWorkerId(WorkerId),

    #[error("Connection with the transaction executor dropped")]
    ExecutorConnectionDropped,

    #[error("Deserialization of consensus message failed: {0}")]
    SerializationError(String),

    #[error("Received unexpected protocol message from consensus")]
    UnexpectedProtocolMessage,

    #[error("There can only be a single consensus client at the time")]
    OnlyOneConsensusClientPermitted,

    #[error("Execution engine failed: {0}")]
    NodeExecutionError(String),

    #[error("Client transaction invalid: {0}")]
    ClientExecutionError(String),
}
