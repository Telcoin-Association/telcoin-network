use super::*;
use crate::record::store::MemoryStore;

struct Received {
    behaviour: Behaviour<MemoryStore>,
    original: Record,
    source: PeerId,
    connection: ConnectionId,
    request: RequestId,
}

fn peer_id(seed: u8) -> PeerId {
    let bytes = [vec![0x12, 0x20], vec![seed; 32]].concat();
    PeerId::from_bytes(&bytes).expect("valid fixed SHA256 peer identifier")
}

fn receive_expired(filtering: StoreInserts, expires: Option<Instant>) -> Received {
    let local = peer_id(1);
    let source = peer_id(2);
    let config = Config {
        record_filtering: filtering,
        record_ttl: Some(Duration::ZERO),
        ..Config::new(StreamProtocol::new("/test/kad/expiry"))
    };
    let mut behaviour = Behaviour::with_config(local, MemoryStore::new(local), config);
    let original = Record {
        key: record::Key::new(&[7_u8]),
        value: vec![3, 2, 1],
        publisher: Some(source),
        expires,
    };
    let connection = ConnectionId::new_unchecked(1);
    let request = RequestId::for_test();
    behaviour.record_received(source, connection, request, original.clone());
    Received { behaviour, original, source, connection, request }
}

fn assert_ack(received: &Received) {
    let acknowledgments = received
        .behaviour
        .queued_events
        .iter()
        .filter(|event| {
            if let ToSwarm::NotifyHandler {
                peer_id,
                handler: NotifyHandler::One(connection),
                event: HandlerIn::PutRecordRes { key, value, request_id },
            } = event
            {
                assert_eq!(peer_id, &received.source);
                assert_eq!(connection, &received.connection);
                assert_eq!(key, &received.original.key);
                assert_eq!(value, &received.original.value);
                assert_eq!(request_id, &received.request);
                true
            } else {
                false
            }
        })
        .count();
    assert_eq!(acknowledgments, 1);
    assert!(received.behaviour.store.get(&received.original.key).is_none());
}

#[test]
fn filtered_expired_record_reaches_application_without_storage() {
    [None, Some(Instant::now() - Duration::from_secs(1))].into_iter().for_each(|expires| {
        let received = receive_expired(StoreInserts::FilterBoth, expires);
        let records: Vec<_> = received
            .behaviour
            .queued_events
            .iter()
            .filter_map(|event| {
                if let ToSwarm::GenerateEvent(Event::InboundRequest {
                    request: InboundRequest::PutRecord { source, connection, record: Some(record) },
                }) = event
                {
                    assert_eq!(source, &received.source);
                    assert_eq!(connection, &received.connection);
                    Some(record)
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(records.len(), 1);
        let record = records[0];
        assert_eq!(record.key, received.original.key);
        assert_eq!(record.value, received.original.value);
        assert_eq!(record.publisher, received.original.publisher);
        assert!(record.is_expired(Instant::now()));
        assert!(record.expires.is_some());
        received.original.expires.into_iter().for_each(|original| {
            assert_eq!(record.expires, Some(original));
        });
        assert_ack(&received);
    });
}

#[test]
fn unfiltered_expired_record_has_no_event_or_storage() {
    [None, Some(Instant::now() - Duration::from_secs(1))].into_iter().for_each(|expires| {
        let received = receive_expired(StoreInserts::Unfiltered, expires);
        assert!(!received
            .behaviour
            .queued_events
            .iter()
            .any(|event| { matches!(event, ToSwarm::GenerateEvent(_)) }));
        assert_ack(&received);
    });
}
