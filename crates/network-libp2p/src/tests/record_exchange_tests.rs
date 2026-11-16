//! Deterministic request, response and cache budget regressions.
use super::*;
use libp2p::{request_response, StreamProtocol};

/// Construct a behaviour solely to obtain real outbound request identifiers.
fn behaviour() -> request_response::Behaviour<RecordCodec> {
    request_response::Behaviour::with_codec(
        RecordCodec::new(16 * 1024),
        [(StreamProtocol::new("/record-test/1"), request_response::ProtocolSupport::Full)],
        request_response::Config::default(),
    )
}

/// Pending requests survive reconnect churn and only the matching terminal event releases them.
#[tokio::test(start_paused = true)]
async fn pending_requests_coalesce_and_release() {
    let mut state = RecordExchange::new(NonZeroUsize::MIN, 2, Duration::from_secs(1));
    let peer = PeerId::random();
    let mut rpc = behaviour();
    let request = rpc.send_request(&peer, ());
    let other = rpc.send_request(&peer, ());
    assert!(state.allow_request(peer));
    state.track(peer, request);
    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(!state.allow_request(peer));
    assert!(!state.finish(peer, other));
    let second_peer = PeerId::random();
    assert!(state.allow_request(second_peer));
    state.track(second_peer, rpc.send_request(&second_peer, ()));
    assert!(!state.allow_request(PeerId::random()), "global live-request cap");
    state.defer(peer);
    assert!(state.take_deferred().is_empty(), "pending reconnects coalesce");
    assert!(state.finish(peer, request));
    assert!(state.allow_request(peer));
}

/// Refused requests neither extend the cooldown nor consume an outbound attempt.
#[tokio::test(start_paused = true)]
async fn response_cooldown_does_not_slide() {
    let mut state = RecordExchange::new(NonZeroUsize::MIN, 1, Duration::from_secs(1));
    let peer = PeerId::random();
    assert!(state.allow_response(peer));
    tokio::time::advance(Duration::from_millis(500)).await;
    assert!(!state.allow_response(peer));
    assert!(state.allow_request(peer), "inbound and outbound budgets are independent");
    tokio::time::advance(Duration::from_millis(500)).await;
    assert!(state.allow_response(peer));
}

/// Churn cannot grow either cooldown cache or the deferred retry set past its budget.
#[tokio::test]
async fn churn_history_and_deferred_work_are_bounded() {
    let mut state = RecordExchange::new(NonZeroUsize::MIN, 1, Duration::from_secs(1));
    (0..100).for_each(|_| {
        let peer = PeerId::random();
        assert!(state.allow_request(peer));
        assert!(state.allow_response(peer));
        state.defer(peer);
        assert_eq!(state.attempts.len(), 1);
        assert_eq!(state.served.len(), 1);
        assert_eq!(state.deferred.len(), 1);
    });
    assert_eq!(state.take_deferred().len(), 1);
    assert!(state.take_deferred().is_empty());
}
