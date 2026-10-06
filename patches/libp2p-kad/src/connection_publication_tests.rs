use super::*;
use crate::record::store::MemoryStore;
use libp2p_core::{Endpoint, transport::PortUse};

fn connected_behaviour(peer: PeerId, connections: &[ConnectionId]) -> Behaviour<MemoryStore> {
    let local = PeerId::random();
    let mut behaviour = Behaviour::new(local, MemoryStore::new(local));
    behaviour.connected_peers.insert(peer);
    connections.iter().for_each(|connection| {
        behaviour.connections.insert(
            *connection,
            ConnectionState {
                peer,
                publication: ConnectionPublication::Unpublished,
            },
        );
    });
    behaviour
}

fn record() -> Record {
    Record::new(
        record::Key::new(&b"connection-publication"),
        b"signed-record".to_vec(),
    )
}

fn actions(behaviour: &mut Behaviour<MemoryStore>) -> Vec<ToSwarm<Event, HandlerIn>> {
    let waker = futures::task::noop_waker();
    let mut context = Context::from_waker(&waker);
    std::iter::from_fn(|| match behaviour.poll(&mut context) {
        Poll::Ready(event) => Some(event),
        Poll::Pending => None,
    })
    .take(256)
    .collect()
}

fn close(behaviour: &mut Behaviour<MemoryStore>, peer: PeerId, connection: ConnectionId) {
    let endpoint = ConnectedPoint::Dialer {
        address: "/memory/1".parse().expect("valid memory address"),
        role_override: Endpoint::Dialer,
        port_use: PortUse::Reuse,
    };
    behaviour.on_connection_closed(ConnectionClosed {
        peer_id: peer,
        connection_id: connection,
        endpoint: &endpoint,
        cause: None,
        remaining_established: 1,
    });
}

#[test]
fn new_connection_publication_uses_its_handler_and_ignores_old_handler_reply() {
    let peer = PeerId::random();
    let old = ConnectionId::new_unchecked(1);
    let new = ConnectionId::new_unchecked(2);
    let mut behaviour = connected_behaviour(peer, &[old, new]);
    behaviour.queue_record_to_connection(peer, new);
    let query_id = behaviour
        .put_record_to_connection(record(), peer, new)
        .expect("query admitted");
    let dispatched = actions(&mut behaviour);
    assert!(dispatched.iter().any(|event| matches!(event,
        ToSwarm::NotifyHandler { peer_id, handler: NotifyHandler::One(connection), event: HandlerIn::PutRecord { query_id: id, .. } }
            if *peer_id == peer && *connection == new && *id == query_id
    )));
    assert!(!dispatched.iter().any(|event| matches!(
        event,
        ToSwarm::NotifyHandler {
            handler: NotifyHandler::Any,
            ..
        } | ToSwarm::Dial { .. }
    )));

    behaviour.on_connection_handler_event(
        peer,
        old,
        HandlerEvent::PutRecordRes {
            query_id,
            key: record().key,
            value: record().value,
        },
    );
    assert!(actions(&mut behaviour).is_empty());
    assert_eq!(behaviour.active_connection_publications(), 1);

    behaviour.on_connection_handler_event(
        peer,
        new,
        HandlerEvent::PutRecordRes {
            query_id,
            key: record().key,
            value: record().value,
        },
    );
    assert!(actions(&mut behaviour).iter().any(|event| matches!(event,
        ToSwarm::GenerateEvent(Event::OutboundQueryProgressed { id, result: QueryResult::PutRecord(Ok(_)), .. })
            if *id == query_id
    )));
    behaviour.queue_record_to_connection(peer, new);
    assert_eq!(behaviour.next_pending_record_connection(), None);
    assert_eq!(behaviour.active_connection_publications(), 0);
}

#[test]
fn closed_target_fails_before_or_after_dispatch_without_using_surviving_connection() {
    [false, true].into_iter().for_each(|dispatch_first| {
        let peer = PeerId::random();
        let old = ConnectionId::new_unchecked(1);
        let target = ConnectionId::new_unchecked(2);
        let mut behaviour = connected_behaviour(peer, &[old, target]);
        behaviour.queue_record_to_connection(peer, target);
        let query_id = behaviour.put_record_to_connection(record(), peer, target).expect("query admitted");
        if dispatch_first {
            assert!(actions(&mut behaviour).iter().any(|event| matches!(event,
                ToSwarm::NotifyHandler { handler: NotifyHandler::One(connection), .. } if *connection == target
            )));
        }
        close(&mut behaviour, peer, target);
        behaviour.on_connection_handler_event(
            peer,
            target,
            HandlerEvent::PutRecordRes {
                query_id,
                key: record().key,
                value: record().value,
            },
        );
        let completed = actions(&mut behaviour);
        assert!(behaviour.connection_matches(&peer, old));
        assert!(!behaviour.connection_matches(&peer, target));
        assert!(completed.iter().any(|event| matches!(event,
            ToSwarm::GenerateEvent(Event::OutboundQueryProgressed { id, result: QueryResult::PutRecord(Err(_)), .. })
                if *id == query_id
        )));
        assert!(!completed.iter().any(|event| matches!(event,
            ToSwarm::NotifyHandler { .. } | ToSwarm::Dial { .. }
        )));
        assert_eq!(behaviour.active_connection_publications(), 0);
    });
}

#[test]
fn finished_query_rejects_a_late_response_on_its_still_live_target() {
    let peer = PeerId::random();
    let target = ConnectionId::new_unchecked(1);
    let mut behaviour = connected_behaviour(peer, &[target]);
    behaviour.queue_record_to_connection(peer, target);
    let query_id = behaviour
        .put_record_to_connection(record(), peer, target)
        .expect("query admitted");
    assert!(actions(&mut behaviour).iter().any(|event| matches!(event,
        ToSwarm::NotifyHandler { handler: NotifyHandler::One(connection), .. } if *connection == target
    )));
    behaviour
        .queries
        .get_mut(&query_id)
        .expect("active query")
        .finish();
    behaviour.on_connection_handler_event(
        peer,
        target,
        HandlerEvent::PutRecordRes {
            query_id,
            key: record().key,
            value: record().value,
        },
    );
    assert!(behaviour.connection_matches(&peer, target));
    assert!(actions(&mut behaviour).iter().any(|event| matches!(event,
        ToSwarm::GenerateEvent(Event::OutboundQueryProgressed { id, result: QueryResult::PutRecord(Err(_)), .. })
            if *id == query_id
    )));
}

#[test]
fn saturated_publications_remain_pending_and_deliver_after_a_slot_frees() {
    let peer = PeerId::random();
    let connections = (0..MAX_ACTIVE_CONNECTION_PUBLICATIONS + 2)
        .map(ConnectionId::new_unchecked)
        .collect::<Vec<_>>();
    let mut behaviour = connected_behaviour(peer, &connections);
    let active = connections
        .iter()
        .take(MAX_ACTIVE_CONNECTION_PUBLICATIONS)
        .map(|connection| {
            behaviour.queue_record_to_connection(peer, *connection);
            behaviour
                .put_record_to_connection(record(), peer, *connection)
                .expect("slot available")
        })
        .collect::<Vec<_>>();
    let waiting = connections[MAX_ACTIVE_CONNECTION_PUBLICATIONS];
    let closing = connections[MAX_ACTIVE_CONNECTION_PUBLICATIONS + 1];
    behaviour.queue_record_to_connection(peer, waiting);
    behaviour.queue_record_to_connection(peer, closing);
    behaviour.queue_record_to_connection(peer, waiting);
    behaviour.queue_record_to_connection(peer, connections[0]);
    assert_eq!(
        behaviour.active_connection_publications(),
        MAX_ACTIVE_CONNECTION_PUBLICATIONS
    );
    assert_eq!(behaviour.next_pending_record_connection(), None);
    assert_eq!(
        behaviour.put_record_to_connection(record(), peer, waiting),
        Err(ConnectionPublicationError::AtCapacity)
    );
    close(&mut behaviour, peer, closing);
    assert_eq!(
        behaviour.put_record_to_connection(record(), peer, closing),
        Err(ConnectionPublicationError::StaleConnection)
    );

    behaviour
        .queries
        .get_mut(&active[0])
        .expect("active query")
        .finish();
    assert!(actions(&mut behaviour).iter().any(|event| matches!(event,
        ToSwarm::GenerateEvent(Event::OutboundQueryProgressed { id, .. }) if *id == active[0]
    )));
    assert_eq!(
        behaviour.next_pending_record_connection(),
        Some((peer, waiting))
    );
    let resumed = behaviour
        .put_record_to_connection(record(), peer, waiting)
        .expect("freed slot");
    assert_eq!(
        behaviour.active_connection_publications(),
        MAX_ACTIVE_CONNECTION_PUBLICATIONS
    );
    assert!(actions(&mut behaviour).iter().any(|event| matches!(event,
        ToSwarm::NotifyHandler { handler: NotifyHandler::One(connection), event: HandlerIn::PutRecord { query_id, .. }, .. }
            if *connection == waiting && *query_id == resumed
    )));
    assert_eq!(
        behaviour.put_record_to_connection(record(), peer, waiting),
        Err(ConnectionPublicationError::NotPending)
    );
    behaviour.on_connection_handler_event(
        peer,
        waiting,
        HandlerEvent::PutRecordRes {
            query_id: resumed,
            key: record().key,
            value: record().value,
        },
    );
    assert!(actions(&mut behaviour).iter().any(|event| matches!(event,
        ToSwarm::GenerateEvent(Event::OutboundQueryProgressed { id, result: QueryResult::PutRecord(Ok(_)), .. })
            if *id == resumed
    )));
}

#[test]
fn publication_admission_rejects_a_connection_owned_by_another_peer() {
    let peer = PeerId::random();
    let other = PeerId::random();
    let connection = ConnectionId::new_unchecked(1);
    let mut behaviour = connected_behaviour(peer, &[connection]);
    behaviour.queue_record_to_connection(other, connection);
    assert_eq!(behaviour.next_pending_record_connection(), None);
    assert_eq!(
        behaviour.put_record_to_connection(record(), other, connection),
        Err(ConnectionPublicationError::StaleConnection)
    );
    assert_eq!(behaviour.active_connection_publications(), 0);
    behaviour.queue_record_to_connection(peer, connection);
    assert_eq!(
        behaviour.next_pending_record_connection(),
        Some((peer, connection))
    );
}

#[test]
fn admission_order_saturation_preserves_every_waiting_publication() {
    let peer = PeerId::random();
    let connections = [
        ConnectionId::new_unchecked(1),
        ConnectionId::new_unchecked(2),
        ConnectionId::new_unchecked(3),
    ];
    let mut behaviour = connected_behaviour(peer, &connections);
    behaviour.publication_order = PublicationOrder(u64::MAX - 1);
    connections.iter().for_each(|connection| {
        behaviour.queue_record_to_connection(peer, *connection);
    });
    let started = (0..connections.len())
        .map(|_| {
            let (target_peer, connection) = behaviour
                .next_pending_record_connection()
                .expect("each waiting target remains selectable");
            assert_eq!(target_peer, peer);
            behaviour
                .put_record_to_connection(record(), peer, connection)
                .expect("query capacity available");
            connection
        })
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(started, connections.into_iter().collect());
    assert_eq!(behaviour.next_pending_record_connection(), None);
    assert_eq!(
        behaviour.active_connection_publications(),
        connections.len()
    );
}
