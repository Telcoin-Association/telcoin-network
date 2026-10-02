//! Regression tests for runtime public discovery suspension.

use super::*;
use crate::store::MemoryStore;

/// Construct a behavior with immediate automatic bootstrap and no periodic background work.
fn behavior() -> Behaviour<MemoryStore> {
    let local = PeerId::random();
    let mut config = Config::default();
    config.set_periodic_bootstrap_interval(None);
    config.set_automatic_bootstrap_throttle(Some(Duration::ZERO));
    Behaviour::with_config(local, MemoryStore::new(local), config)
}

/// Drain only immediately ready behavior outputs, without wall-clock assumptions.
fn drain(kad: &mut Behaviour<MemoryStore>) -> Vec<ToSwarm<Event, HandlerIn>> {
    let mut cx = Context::from_waker(Waker::noop());
    std::iter::from_fn(|| match kad.poll(&mut cx) {
        Poll::Ready(event) => Some(event),
        Poll::Pending => None,
    })
    .take(64)
    .collect()
}

/// New routing contacts cannot initiate automatic bootstrap while public discovery is suspended.
#[test]
fn closed_contacts_do_not_bootstrap_and_recovery_restores_it() -> Result<(), String> {
    let mut kad = behavior();
    kad.set_public_discovery_enabled(false);
    kad.add_address(
        &PeerId::random(),
        "/memory/1".parse().map_err(|err| format!("{err}"))?,
    );
    let outputs = drain(&mut kad);
    assert!(
        !outputs
            .iter()
            .any(|event| matches!(event, ToSwarm::Dial { .. }))
    );
    assert_eq!(kad.iter_queries().count(), 0);
    kad.set_public_discovery_enabled(true);
    kad.add_address(
        &PeerId::random(),
        "/memory/2".parse().map_err(|err| format!("{err}"))?,
    );
    let outputs = drain(&mut kad);
    assert!(
        outputs
            .iter()
            .any(|event| matches!(event, ToSwarm::Dial { .. }))
    );
    assert!(
        kad.iter_queries()
            .any(|query| matches!(query.info(), QueryInfo::Bootstrap { .. }))
    );
    Ok(())
}

/// Closure removes public queries and their queued dials/RPCs, preserving a record query's dial.
#[test]
fn closed_cancels_public_queries_but_preserves_record_resolution() -> Result<(), String> {
    let mut kad = behavior();
    let hub = PeerId::random();
    kad.add_address(&hub, "/memory/1".parse().map_err(|err| format!("{err}"))?);
    let bootstrap = kad.bootstrap().map_err(|err| format!("{err}"))?;
    let closest = kad.get_closest_peers(PeerId::random());
    let finished = kad.get_closest_peers(PeerId::random());
    let finished_request = kad
        .queries
        .get(&finished)
        .ok_or("missing finishing query")?
        .info
        .to_request(finished);
    kad.queries.remove(&finished);
    kad.queued_events.push_back(ToSwarm::NotifyHandler {
        peer_id: hub,
        handler: NotifyHandler::Any,
        event: finished_request,
    });
    let record = kad.get_record(crate::RecordKey::new(&b"committee"));
    let unauthorized = kad.get_record(crate::RecordKey::new(&b"public-record"));
    kad.queued_events.push_back(ToSwarm::NotifyHandler {
        peer_id: hub,
        handler: NotifyHandler::Any,
        event: HandlerIn::GetRecord {
            key: crate::RecordKey::new(&b"public-record"),
            query_id: unauthorized,
        },
    });
    let request = kad
        .queries
        .get(&closest)
        .ok_or("missing public query")?
        .info
        .to_request(closest);
    kad.queued_events.push_back(ToSwarm::NotifyHandler {
        peer_id: hub,
        handler: NotifyHandler::Any,
        event: request,
    });
    let public_peer = PeerId::random();
    kad.queued_events.push_back(ToSwarm::Dial {
        opts: DialOpts::peer_id(public_peer).build(),
    });
    let record_request = kad
        .queries
        .get(&record)
        .ok_or("missing record query")?
        .info
        .to_request(record);
    kad.queries
        .get_mut(&record)
        .ok_or("missing record query")?
        .pending_rpcs
        .push((hub, record_request));
    kad.queued_events.push_back(ToSwarm::Dial {
        opts: DialOpts::peer_id(hub).build(),
    });
    kad.set_public_discovery_enabled(false);
    assert!(kad.cancel_query(&unauthorized));
    assert!(kad.query(&unauthorized).is_none());
    assert!(kad.query(&bootstrap).is_none());
    assert!(kad.query(&closest).is_none());
    assert!(kad.query(&record).is_some());
    let outputs = drain(&mut kad);
    assert!(!outputs.iter().any(|event| matches!(event,
        ToSwarm::NotifyHandler { event: HandlerIn::GetRecord { query_id, .. }, .. }
        if *query_id == unauthorized
    )));
    assert!(!outputs.iter().any(|event| matches!(
        event,
        ToSwarm::NotifyHandler {
            event: HandlerIn::FindNodeReq { .. },
            ..
        }
    )));
    assert!(!outputs.iter().any(
        |event| matches!(event, ToSwarm::Dial { opts } if opts.get_peer_id() == Some(public_peer))
    ));
    assert!(
        outputs.iter().any(
            |event| matches!(event, ToSwarm::Dial { opts } if opts.get_peer_id() == Some(hub))
        )
    );
    kad.set_public_discovery_enabled(true);
    assert!(kad.query(&closest).is_none());
    assert!(kad.query(&bootstrap).is_none());
    assert!(kad.query(&record).is_some());
    Ok(())
}
