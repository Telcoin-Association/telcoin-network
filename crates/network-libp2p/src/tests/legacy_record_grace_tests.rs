//! The adiri grace for pre-#1751 node records.
//!
//! v0.16 nodes publish and relay node records without the network-key co-signature. A v0.17 node
//! drops them on both kad paths without penalizing the sender, while every other invalid record
//! stays fatal. A record-exchange response that fails to decode is not scored either.
// TODO(v0.18-adiri): remove the legacy node-record grace once the network is closed to pre-#1751
// records.

use super::*;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};

/// The counter for records dropped by the grace.
const LEGACY_DROPS: &str = "tn_network.legacy_node_records_dropped_total";

/// The two kad paths that check a peer-supplied node record with
/// [`ConsensusNetwork::peer_record_valid`].
#[derive(Clone, Copy, Debug)]
enum RecordPath {
    /// An inbound PUT, which charges the transport source.
    Put,
    /// A GET result, which charges the responding peer.
    Lookup,
}

impl RecordPath {
    /// Hand `record` to `network` as if `peer` sent it on this path.
    fn deliver(
        self,
        network: &mut ConsensusNetworkMemoryDB<TestWorkerRequest, TestWorkerResponse>,
        peer: PeerId,
        record: kad::Record,
    ) -> eyre::Result<()> {
        match self {
            Self::Put => network.process_kad_put_request(peer, record)?,
            Self::Lookup => {
                let id = network.swarm.behaviour_mut().kademlia.get_record(record.key.clone());
                network.process_kad_event(kad::Event::OutboundQueryProgressed {
                    id,
                    result: kad::QueryResult::GetRecord(Ok(kad::GetRecordOk::FoundRecord(
                        kad::PeerRecord { peer: Some(peer), record },
                    ))),
                    stats: kad::QueryStats::empty(),
                    step: kad::ProgressStep { count: NonZeroUsize::MIN, last: true },
                })?
            }
        }
        Ok(())
    }
}

/// The primary-network grace counts by `path` label, sorted, from one snapshot.
fn legacy_drops(snapshotter: &Snapshotter) -> Vec<(String, u64)> {
    let mut drops: Vec<_> = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter_map(|(key, _, _, value)| {
            let key = key.key();
            let primary =
                key.labels().any(|label| label.key() == "network" && label.value() == "primary");
            let path = key.labels().find(|label| label.key() == "path")?.value().to_owned();
            match value {
                DebugValue::Counter(count) if primary && key.name() == LEGACY_DROPS => {
                    Some((path, count))
                }
                _ => None,
            }
        })
        .collect();
    drops.sort();
    drops
}

/// A record in the pre-#1751 layout is dropped on both paths without scoring the sender, and
/// each drop is counted under its path.
#[tokio::test]
async fn legacy_node_record_is_dropped_without_penalty() -> eyre::Result<()> {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        let TestTypes { peer1, peer2, .. } =
            create_test_types::<TestWorkerRequest, TestWorkerResponse>();
        let mut network = peer1.network;
        let mut record = peer2.network.get_peer_record();
        // BCS flattens the tuple, so these are the bytes a v0.16 node publishes for this record
        let signed = &peer2.network.node_record;
        record.value = encode(&(&signed.info, &signed.signature));
        assert!(NodeRecord::decodes_as_legacy(&record.value));

        [RecordPath::Put, RecordPath::Lookup].into_iter().try_for_each(|path| {
            let sender = register_untrusted_put_record_source(&mut network)?;
            path.deliver(&mut network, sender, record.clone())?;
            let manager = &network.swarm.behaviour().peer_manager;
            assert_eq!(manager.peer_score(&sender), Some(0.0), "{path:?}: sender was scored");
            assert!(!manager.peer_banned(&sender), "{path:?}: sender was banned");
            eyre::Ok(())
        })
    })?;

    assert_eq!(legacy_drops(&snapshotter), [("lookup".to_owned(), 1), ("put".to_owned(), 1)]);
    Ok(())
}

/// Bytes in no known layout, a current record whose BLS signature does not cover its contents,
/// and a current record whose publisher does not match its network key all stay fatal on both
/// paths, and none of them is counted as a legacy drop.
#[tokio::test]
async fn invalid_node_records_stay_fatal() -> eyre::Result<()> {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        let TestTypes { peer1, peer2, .. } =
            create_test_types::<TestWorkerRequest, TestWorkerResponse>();
        let mut network = peer1.network;
        let key = peer2.config.key_config().primary_public_key();
        let valid = peer2.network.get_peer_record();

        let mut garbage = valid.clone();
        garbage.value = vec![0xff; 64];
        let mut unsigned = valid.clone();
        let mut changed = peer2.network.node_record.clone();
        changed.info.timestamp = changed.info.timestamp.saturating_add(1);
        unsigned.value = encode(&changed);
        [&garbage, &unsigned].into_iter().for_each(|record| {
            assert!(
                NodeRecord::decode_and_verify(&record.value, network.record_domain, &key).is_none()
            );
            assert!(!NodeRecord::decodes_as_legacy(&record.value));
        });
        assert!(NodeRecord::decode_and_verify(&valid.value, network.record_domain, &key).is_some());

        [RecordPath::Put, RecordPath::Lookup].into_iter().try_for_each(|path| {
            [garbage.clone(), unsigned.clone()].into_iter().try_for_each(|record| {
                let sender = register_untrusted_put_record_source(&mut network)?;
                path.deliver(&mut network, sender, record)?;
                assert!(
                    network.swarm.behaviour().peer_manager.peer_banned(&sender),
                    "{path:?}: an undecodable or unsigned record must be fatal"
                );
                eyre::Ok(())
            })?;

            // the bytes verify, but the publisher is the sender rather than the record's key
            let sender = register_untrusted_put_record_source(&mut network)?;
            let mut mismatched = valid.clone();
            mismatched.publisher = Some(sender);
            path.deliver(&mut network, sender, mismatched)?;
            assert!(
                network.swarm.behaviour().peer_manager.peer_banned(&sender),
                "{path:?}: a publisher mismatch must be fatal"
            );
            eyre::Ok(())
        })
    })?;

    assert!(legacy_drops(&snapshotter).is_empty());
    Ok(())
}

/// A v0.16 record-exchange response fails inside the codec as invalid data. That failure is not
/// scored and is not retried, and a committee peer's record is looked up over kad instead.
#[tokio::test]
async fn undecodable_record_response_is_not_scored() -> eyre::Result<()> {
    use crate::codec::{decode_message, encode_message};

    let TestTypes { peer1, peer2, .. } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let mut network = peer1.network;
    let key = peer2.config.key_config().primary_public_key();

    // the response a v0.16 node serves: BCS flattens the inner tuple into a pre-#1751 record
    let signed = &peer2.network.node_record;
    let response = Some((key, (&signed.info, &signed.signature)));
    let mut wire = Vec::new();
    encode_message(&mut wire, &response, &mut Vec::new(), &mut Vec::new(), 1024 * 1024).await?;
    let error = decode_message::<_, RecordResponse>(
        &mut wire.as_slice(),
        &mut Vec::new(),
        &mut Vec::new(),
        1024 * 1024,
    )
    .await
    .expect_err("a v0.16 record response must not decode");
    assert_eq!(error.kind(), ErrorKind::InvalidData);

    let validator = *peer2.network.swarm.local_peer_id();
    network.swarm.behaviour_mut().peer_manager.update_committees(
        Default::default(),
        [key].into_iter().collect(),
        Default::default(),
    );
    network.process_kad_put_request(validator, peer2.network.get_peer_record())?;
    let ordinary = register_untrusted_put_record_source(&mut network)?;

    [validator, ordinary].into_iter().try_for_each(|peer| {
        let before = network.swarm.behaviour().peer_manager.peer_score(&peer);
        assert!(network.record_exchange.allow_request(peer));
        let request = network.swarm.behaviour_mut().record_exchange.send_request(&peer, ());
        network.record_exchange.track(peer, request);
        network.process_record_exchange_event(ReqResEvent::OutboundFailure {
            peer,
            connection_id: libp2p::swarm::ConnectionId::new_unchecked(1),
            request_id: request,
            error: ReqResOutboundFailure::Io(std::io::Error::new(error.kind(), error.to_string())),
        })?;
        let manager = &network.swarm.behaviour().peer_manager;
        assert_eq!(manager.peer_score(&peer), before, "{peer}: the failure was scored");
        assert!(!manager.peer_banned(&peer), "{peer}: the failure was fatal");
        assert!(!network.record_exchange.take_deferred().contains(&peer), "{peer}: retried");
        eyre::Ok(())
    })?;

    // only the committee peer has a known identity to look up
    assert_eq!(network.kad_record_queries.len(), 1);
    Ok(())
}
