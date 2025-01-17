//! DEPRECATED
//!
//! Network library - swapping in favor of libp2p.
#![warn(future_incompatible, nonstandard_style, rust_2018_idioms, rust_2021_compatibility)]
#![allow(clippy::async_yields_async)]

pub mod admin;
pub mod anemo_ext;
pub mod connectivity;
pub mod epoch_filter;
mod error;
pub mod failpoints;
pub mod local;
pub mod metrics;
mod p2p;
mod retry;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
mod traits;

pub use crate::{
    retry::RetryConfig,
    traits::{
        PrimaryToPrimaryRpc, PrimaryToWorkerClient, ReliableNetwork, WorkerRpc,
        WorkerToPrimaryClient,
    },
};

/// This adapter will make a [`tokio::task::JoinHandle`] abort its handled task when the handle is
/// dropped.
#[derive(Debug)]
#[must_use]
pub struct CancelOnDropHandler<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for CancelOnDropHandler<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<T> std::future::Future for CancelOnDropHandler<T> {
    type Output = T;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        use futures::future::FutureExt;
        // If the task panics just propagate it up
        self.0.poll_unpin(cx).map(Result::unwrap)
    }
}
