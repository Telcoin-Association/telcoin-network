// SPDX-License-Identifier: Apache-2.0
//! Network messages for anemo communication

pub mod local;
mod notify;
mod request;
mod response;
pub use notify::*;
pub use request::*;
pub use response::*;
use std::collections::{HashMap, HashSet};
use tn_types::{Batch, BlockHash};

// async_trait for object safety, get rid of when possible.
#[async_trait::async_trait]
pub trait WorkerToPrimaryClient: Send + Sync + 'static {
    async fn report_own_batch(&self, request: WorkerOwnBatchMessage) -> eyre::Result<()>;

    async fn report_others_batch(&self, request: WorkerOthersBatchMessage) -> eyre::Result<()>;
}

/// Dumb mock to just return Ok on calls for tests.
pub struct MockWorkerToPrimary();

#[async_trait::async_trait]
impl WorkerToPrimaryClient for MockWorkerToPrimary {
    async fn report_own_batch(&self, _request: WorkerOwnBatchMessage) -> eyre::Result<()> {
        Ok(())
    }

    async fn report_others_batch(&self, _request: WorkerOthersBatchMessage) -> eyre::Result<()> {
        Ok(())
    }
}

/// Dumb mock to just pends forever on calls for tests.
pub struct MockWorkerToPrimaryHang();

#[async_trait::async_trait]
impl WorkerToPrimaryClient for MockWorkerToPrimaryHang {
    async fn report_own_batch(&self, _request: WorkerOwnBatchMessage) -> eyre::Result<()> {
        std::future::pending().await
    }

    async fn report_others_batch(&self, _request: WorkerOthersBatchMessage) -> eyre::Result<()> {
        std::future::pending().await
    }
}

// async_trait for object safety, get rid of when possible.
#[async_trait::async_trait]
pub trait PrimaryToWorkerClient: Send + Sync + 'static {
    async fn synchronize(&self, message: WorkerSynchronizeMessage) -> eyre::Result<()>;

    async fn fetch_batches(&self, digests: HashSet<BlockHash>) -> eyre::Result<FetchBatchResponse>;
}

/// Type that can return batches.
#[derive(Default)]
pub struct MockPrimaryToWorkerClient {
    pub batches: HashMap<BlockHash, Batch>,
}

#[async_trait::async_trait]
impl PrimaryToWorkerClient for MockPrimaryToWorkerClient {
    async fn synchronize(&self, _message: WorkerSynchronizeMessage) -> eyre::Result<()> {
        Ok(())
    }

    async fn fetch_batches(
        &self,
        _digests: HashSet<BlockHash>,
    ) -> eyre::Result<FetchBatchResponse> {
        Ok(FetchBatchResponse { batches: self.batches.clone() })
    }
}
