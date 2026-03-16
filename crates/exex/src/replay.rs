//! Historical block replay for ExEx catch-up.
//!
//! When an ExEx starts (or restarts after a crash), it may need to catch up
//! to the current chain tip. Since TN has no reorgs, this is simply reading
//! finalized blocks from the database in ascending order.

use crate::TnExExNotification;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tn_reth::RethEnv;
use tn_types::BlockNumber;

/// Stream that replays historical blocks from the database as ExEx notifications.
///
/// Reads finalized blocks from `current_block` to `end_block` (inclusive) and yields
/// `TnExExNotification::ChainExecuted` for each block.
///
/// If `current_block > end_block`, the stream is immediately empty.
#[derive(Debug)]
pub struct ReplayStream {
    reth_env: RethEnv,
    current_block: BlockNumber,
    end_block: BlockNumber,
}

impl ReplayStream {
    /// Create a new replay stream from `start_block` to `end_block` (inclusive).
    pub fn new(reth_env: RethEnv, start_block: BlockNumber, end_block: BlockNumber) -> Self {
        Self { reth_env, current_block: start_block, end_block }
    }
}

impl futures::Stream for ReplayStream {
    type Item = eyre::Result<TnExExNotification>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.current_block > this.end_block {
            return Poll::Ready(None);
        }

        let block_number = this.current_block;
        this.current_block += 1;

        match this.reth_env.replay_block_as_chain(block_number) {
            Ok(Some(chain)) => {
                Poll::Ready(Some(Ok(TnExExNotification::ChainExecuted { new: chain })))
            }
            Ok(None) => Poll::Ready(Some(Err(eyre::eyre!(
                "block {block_number} not found in database during replay"
            )))),
            Err(e) => Poll::Ready(Some(Err(e.into()))),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.current_block > self.end_block {
            (0, Some(0))
        } else {
            let remaining = (self.end_block - self.current_block + 1) as usize;
            (remaining, Some(remaining))
        }
    }
}
