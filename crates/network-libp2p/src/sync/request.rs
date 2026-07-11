//! Typed requests carried in the opening frame of a bulk-sync exchange.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use tn_types::{AuthorityIdentifier, BlockHash, Epoch, Round};

/// A bulk-sync request from a worker, carried in the opening
/// [`SyncFrame::Req`](crate::sync::SyncFrame::Req) of a
/// `/tn-worker-{id}-sync/0.0.1` exchange.
///
/// The variant order is the wire format: this enum's BCS discriminant rides on
/// the wire inside the `Req` frame, so variants must not be reordered or removed
/// before a protocol-version bump (`request_tags_are_stable` pins it).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkerSyncRequest {
    /// Request the sealed batches identified by `batch_digests`, produced in
    /// `epoch`.
    ///
    /// Folds the legacy `RequestBatchesStream` ack-plus-digest handshake into a
    /// single open: the digests travel in the request frame, so the responder
    /// needs no `(peer, digest)` pending map to correlate a follow-up stream.
    Batches {
        /// The batch digests being requested.
        batch_digests: BTreeSet<BlockHash>,
        /// The epoch that produced these batches.
        epoch: Epoch,
    },
}

/// A bulk-sync request from a primary, carried in the opening
/// [`SyncFrame::Req`](crate::sync::SyncFrame::Req) of a `/tn-primary-sync/0.0.1`
/// exchange.
///
/// The variant order is the wire format: this enum's BCS discriminant rides on
/// the wire inside the `Req` frame, so variants must not be reordered or removed
/// before a protocol-version bump (`request_tags_are_stable` pins it).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PrimarySyncRequest {
    /// Request the consensus pack file for `epoch`.
    ///
    /// Folds the legacy `StreamEpoch` ack-plus-digest handshake into a single
    /// open.
    EpochPack {
        /// The epoch whose consensus data is requested.
        epoch: Epoch,
    },
    /// Request certificates the requester is missing.
    ///
    /// Replaces the legacy `MissingCertificates` request. Because the response
    /// is streamed and ended by the responder, this carries no
    /// `max_response_size`: the request-response codec's fixed cap no longer
    /// bounds the reply, so the requester need not pre-declare a budget.
    MissingCertificates {
        /// Certificates after this round (exclusive) are requested.
        exclusive_lower_bound: Round,
        /// Per-authority rounds to skip, each serialized as a RoaringBitmap.
        skip_rounds: Vec<(AuthorityIdentifier, Vec<u8>)>,
    },
    /// Request a verifiable PREFIX of an epoch's consensus pack file, stopping
    /// after the consensus output with `last_consensus_number`.
    ///
    /// Folds the legacy `StreamEpochPartial` ack-plus-digest handshake into a
    /// single open. Unlike [`Self::EpochPack`] (a complete epoch), this streams
    /// the in-progress current epoch up to an already-committed, verifiable
    /// point; `last_consensus_number` is a chain consensus number, not a
    /// pack-relative index.
    EpochPackPartial {
        /// The epoch whose consensus data is requested.
        epoch: Epoch,
        /// The final (inclusive) consensus header number to stream up to.
        last_consensus_number: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::{read_frame, write_frame, SyncFrame};

    const MAX_FRAME: usize = 1024 * 1024;

    /// Round-trip a typed request through the opening `Req` frame, returning the
    /// decoded frame so the caller can assert it survived unchanged.
    async fn roundtrip_req<R>(request: R) -> SyncFrame<R>
    where
        R: Serialize + serde::de::DeserializeOwned + Sync,
    {
        let frame = SyncFrame::Req(request);
        let (mut enc, mut comp) = (Vec::new(), Vec::new());
        let mut wire = Vec::new();
        write_frame(&mut wire, &frame, &mut enc, &mut comp, MAX_FRAME).await.expect("write req");

        let (mut dec, mut comp2) = (Vec::new(), Vec::new());
        read_frame(&mut wire.as_ref(), &mut dec, &mut comp2, MAX_FRAME).await.expect("read req")
    }

    #[tokio::test]
    async fn worker_batches_request_round_trips() {
        let request = WorkerSyncRequest::Batches {
            batch_digests: BTreeSet::from([BlockHash::from([3u8; 32]), BlockHash::from([9u8; 32])]),
            epoch: 42,
        };
        assert_eq!(roundtrip_req(request.clone()).await, SyncFrame::Req(request));
    }

    #[tokio::test]
    async fn primary_epoch_pack_request_round_trips() {
        let request = PrimarySyncRequest::EpochPack { epoch: 7 };
        assert_eq!(roundtrip_req(request.clone()).await, SyncFrame::Req(request));
    }

    #[tokio::test]
    async fn primary_partial_epoch_pack_request_round_trips() {
        let request = PrimarySyncRequest::EpochPackPartial { epoch: 7, last_consensus_number: 41 };
        assert_eq!(roundtrip_req(request.clone()).await, SyncFrame::Req(request));
    }

    #[tokio::test]
    async fn primary_missing_certificates_request_round_trips() {
        let request = PrimarySyncRequest::MissingCertificates {
            exclusive_lower_bound: 12,
            skip_rounds: vec![
                (AuthorityIdentifier::from_bytes([1u8; 32]), vec![0xDE, 0xAD]),
                (AuthorityIdentifier::from_bytes([2u8; 32]), vec![0xBE, 0xEF]),
            ],
        };
        assert_eq!(roundtrip_req(request.clone()).await, SyncFrame::Req(request));
    }

    /// The request enums' BCS discriminants ride on the wire (inside the `Req`
    /// frame), so pin them exactly as `frame_tags_are_stable` pins the frame
    /// tags. A reorder would keep the round-trip tests green while changing the
    /// 0.0.1 framing seen by a deployed peer; this catches it.
    #[test]
    fn request_tags_are_stable() {
        let worker =
            bcs::to_bytes(&WorkerSyncRequest::Batches { batch_digests: BTreeSet::new(), epoch: 0 })
                .expect("encode worker request");
        assert_eq!(worker.first(), Some(&0));

        let epoch_pack =
            bcs::to_bytes(&PrimarySyncRequest::EpochPack { epoch: 0 }).expect("encode epoch pack");
        assert_eq!(epoch_pack.first(), Some(&0));

        let missing = bcs::to_bytes(&PrimarySyncRequest::MissingCertificates {
            exclusive_lower_bound: 0,
            skip_rounds: Vec::new(),
        })
        .expect("encode missing certificates");
        assert_eq!(missing.first(), Some(&1));

        let partial = bcs::to_bytes(&PrimarySyncRequest::EpochPackPartial {
            epoch: 0,
            last_consensus_number: 0,
        })
        .expect("encode partial epoch pack");
        assert_eq!(partial.first(), Some(&2));
    }
}
