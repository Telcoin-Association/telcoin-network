// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use std::time::Duration;
use tonic::Code;

// pub(crate) static GRPC_ENDPOINT_PATH_HEADER: HeaderName =
// HeaderName::from_static("grpc-path-req");

/// The trait to be implemented when want to be notified about
/// a new request and related metrics around it.
///
/// When a request is performed (up to the point that a response is created)
/// the on_response method is called with the corresponding metrics
/// details. The on_request method will be called when the request
/// is received, but not further processing has happened at this
/// point.
pub trait MetricsCallbackProvider: Send + Sync + Clone + 'static {
    /// Method will be called when a request has been received.
    /// `path`: the endpoint uri path
    fn on_request(&self, path: String);

    /// Method to be called from the server when a request is performed.
    /// `path`: the endpoint uri path
    /// `latency`: the time when the request was received and when the response was created
    /// `status`: the http status code of the response
    /// `grpc_status_code`: the grpc status code (see <https://github.com/grpc/grpc/blob/master/doc/statuscodes.md#status-codes-and-their-use-in-grpc>)
    fn on_response(&self, path: String, latency: Duration, status: u16, grpc_status_code: Code);

    /// Called when request call is started
    fn on_start(&self, _path: &str) {}

    /// Called when request call is dropped.
    /// It is guaranteed that for each on_start there will be corresponding on_drop
    fn on_drop(&self, _path: &str) {}
}
