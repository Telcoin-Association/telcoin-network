//! Tests for the explicit gossipsub frame limits (issue #1655).

use super::*;
use eyre::eyre;
use futures::{future, AsyncReadExt as _, AsyncWriteExt as _, StreamExt as _};
use libp2p::{
    core::{transport::PortUse, upgrade::ReadyUpgrade, Endpoint},
    identity::Keypair,
    swarm::{
        handler::{ConnectionEvent, FullyNegotiatedInbound, FullyNegotiatedOutbound},
        ConnectionDenied, ConnectionHandler, ConnectionHandlerEvent, ConnectionId, FromSwarm,
        NetworkBehaviour, SubstreamProtocol, SwarmEvent, THandler, THandlerInEvent,
        THandlerOutEvent, ToSwarm,
    },
    Multiaddr, PeerId, Stream, StreamProtocol, SwarmBuilder,
};
use std::{
    convert::Infallible,
    task::{ready, Context, Poll},
};
use tokio::sync::{mpsc, oneshot};

/// Chain id used by the tests.
const TEST_CHAIN_ID: u64 = 2017;

/// Longest time the wire test waits for one step.
const WIRE_TIMEOUT: Duration = Duration::from_secs(20);

/// Requests from the test for a raw gossip substream. Each carries the channel for the substream.
type OpenRequests = mpsc::UnboundedReceiver<oneshot::Sender<Stream>>;

/// The test side of [`OpenRequests`].
type SubstreamOpener = mpsc::UnboundedSender<oneshot::Sender<Stream>>;

/// Signing prefix gossipsub 0.50.0 puts before the signed message bytes (`protocol.rs` line 47).
const SIGNING_PREFIX: &[u8] = b"libp2p-pubsub:";

/// Encode `value` as an unsigned LEB128 varint (protobuf varints and the frame length prefix).
fn varint(value: usize) -> Vec<u8> {
    std::iter::successors(Some(value), |rest| Some(rest >> 7).filter(|next| *next > 0))
        .map(|rest| {
            let low = u8::try_from(rest & 0x7f).expect("seven bits fit in a byte");
            if rest >> 7 > 0 {
                low | 0x80
            } else {
                low
            }
        })
        .collect()
}

/// Encode one length-delimited protobuf field (wire type 2) with a tag below 16.
fn field(tag: u8, bytes: &[u8]) -> Vec<u8> {
    [vec![(tag << 3) | 2], varint(bytes.len()), bytes.to_vec()].concat()
}

/// Encode the signed fields of a gossipsub `Message` (`from` = 1, `data` = 2, `seqno` = 3,
/// `topic` = 4) in tag order, as the codec re-encodes them to check the signature.
fn unsigned_message(keypair: &Keypair, seqno: u64, topic: &str, data: &[u8]) -> Vec<u8> {
    [
        field(1, &keypair.public().to_peer_id().to_bytes()),
        field(2, data),
        field(3, &seqno.to_be_bytes()),
        field(4, topic.as_bytes()),
    ]
    .concat()
}

/// Encode a gossipsub `Message` with a valid signature (`signature` = 5).
fn signed_message(keypair: &Keypair, seqno: u64, topic: &str, data: &[u8]) -> Vec<u8> {
    let unsigned = unsigned_message(keypair, seqno, topic, data);
    let signature =
        keypair.sign(&[SIGNING_PREFIX, &unsigned].concat()).expect("ed25519 signing succeeds");
    [unsigned, field(5, &signature)].concat()
}

/// Encode a gossipsub `Message` whose signature does not verify.
fn badly_signed_message(keypair: &Keypair, seqno: u64, topic: &str, data: &[u8]) -> Vec<u8> {
    [unsigned_message(keypair, seqno, topic, data), field(5, &[0u8; 64])].concat()
}

/// Encode one length-prefixed RPC frame whose `publish` entries (tag 2) are `messages`.
fn publish_frame(messages: &[Vec<u8>]) -> Vec<u8> {
    let body: Vec<u8> = messages.iter().flat_map(|message| field(2, message)).collect();
    [varint(body.len()), body].concat()
}

/// The longest topic name the node uses: `tn-consensus-output-{chain_id}` with the widest
/// chain id, checked against the worker topic `tn-worker-{chain_id}-{worker_id}`.
fn longest_topic() -> String {
    let consensus_output = format!("tn-consensus-output-{}", u64::MAX);
    let worker = format!("tn-worker-{}-{}", u64::MAX, u16::MAX);
    std::cmp::max_by_key(consensus_output, worker, String::len)
}

/// The configured limits reach the gossipsub config, and each one equals a pinned number. A
/// dependency update or an edit that changes a limit the node relies on fails here.
#[test]
fn test_gossipsub_config_pins_frame_limits() -> Result<(), ConfigBuilderError> {
    let config = gossipsub_config(TEST_CHAIN_ID)?;

    // the builder wrote every limit (the setters write the codec and behaviour values)
    assert_eq!(config.max_transmit_size(), GOSSIP_MAX_TRANSMIT_SIZE);
    assert_eq!(config.max_publish_messages(), GOSSIP_MAX_PUBLISH_PER_FRAME);
    assert_eq!(config.max_control_message_size(), GOSSIP_CONTROL_SIZE);

    // pinned values: a change here must be a reviewed decision
    assert_eq!(MAX_GOSSIP_MESSAGE_SIZE, 12_000);
    assert_eq!(GOSSIP_PUBLISH_ENVELOPE_BOUND, 512);
    assert_eq!(GOSSIP_CONTROL_SIZE, 16_384);
    assert_eq!(GOSSIP_READ_CHUNK, 8_192);
    assert_eq!(GOSSIP_FRAME_BOUND, 16_387);
    assert_eq!(GOSSIP_MAX_TRANSMIT_SIZE, 24_579);
    assert_eq!(GOSSIP_MAX_PUBLISH_PER_FRAME, 8);

    // the transmit size is one honest frame plus one codec read chunk of slack
    assert_eq!(GOSSIP_MAX_TRANSMIT_SIZE, GOSSIP_FRAME_BOUND + GOSSIP_READ_CHUNK);
    // the frame bound covers both honest frame shapes
    assert_eq!(
        GOSSIP_FRAME_BOUND,
        std::cmp::max(
            MAX_GOSSIP_MESSAGE_SIZE + GOSSIP_PUBLISH_ENVELOPE_BOUND,
            GOSSIP_CONTROL_SIZE + GOSSIP_LENGTH_PREFIX_BOUND,
        )
    );
    Ok(())
}

/// A worst-case honest publish frame (maximum payload, longest topic, ed25519 identity, and a
/// `key` field the library never sends for ed25519) fits in the payload bound plus the envelope.
#[test]
fn test_gossip_publish_envelope_bound_covers_worst_case_frame() {
    let keypair = Keypair::generate_ed25519();
    let topic = longest_topic();
    assert_eq!(topic.len(), 40);
    let data = vec![0xa5u8; MAX_GOSSIP_MESSAGE_SIZE];
    let message = signed_message(&keypair, u64::MAX, &topic, &data);
    let key = keypair.public().encode_protobuf();
    assert_eq!(key.len(), 36);
    let with_key = [message.clone(), field(6, &key)].concat();

    let frame = publish_frame(&[message]);
    let frame_with_key = publish_frame(&[with_key]);

    // the envelope is about 210 bytes for this topic, well inside the 512-byte bound
    assert!(frame.len() <= MAX_GOSSIP_MESSAGE_SIZE + GOSSIP_PUBLISH_ENVELOPE_BOUND);
    assert!(frame_with_key.len() <= MAX_GOSSIP_MESSAGE_SIZE + GOSSIP_PUBLISH_ENVELOPE_BOUND);
    assert!(frame_with_key.len() <= GOSSIP_FRAME_BOUND);
    // two back-to-back maximum frames exceed one frame bound, which is why the slack exists
    assert!(2 * frame.len() > GOSSIP_FRAME_BOUND);
}

/// The largest one-entry publish frame that fits in [`GOSSIP_FRAME_BOUND`], and its payload.
///
/// No honest publish frame is this large (the largest honest frame is a full control field), but
/// the codec size limit counts bytes only, so a publish entry of this size stands in for it.
fn largest_frame(keypair: &Keypair, seqno: u64, topic: &str, fill: u8) -> (Vec<u8>, Vec<u8>) {
    // a bad signature has the same length as a valid one, and costs no signing work
    let frame_len = |len: usize| {
        publish_frame(&[badly_signed_message(keypair, seqno, topic, &vec![fill; len])]).len()
    };
    // the envelope at the payload bound; above it the varint widths grow by a few bytes at most
    let envelope = frame_len(MAX_GOSSIP_MESSAGE_SIZE) - MAX_GOSSIP_MESSAGE_SIZE;
    let guess = GOSSIP_FRAME_BOUND - envelope;
    let len = (0..=GOSSIP_LENGTH_PREFIX_BOUND)
        .map(|back| guess - back)
        .find(|len| frame_len(*len) <= GOSSIP_FRAME_BOUND)
        .unwrap_or(MAX_GOSSIP_MESSAGE_SIZE);
    let payload = vec![fill; len];
    let frame = publish_frame(&[signed_message(keypair, seqno, topic, &payload)]);
    (payload, frame)
}

/// Connection handler for the raw sender in the wire test.
///
/// It opens raw gossipsub substreams when the test asks, so the test can write hand-built
/// frames. It also accepts the receiver's own gossip substream and holds it open: the receiver
/// disables gossipsub on a connection whose peer refuses that substream.
struct RawGossipHandler {
    /// The chain-scoped gossipsub protocol.
    protocol: StreamProtocol,
    /// Substream requests from the test (only the first connection gets them).
    requests: Option<OpenRequests>,
    /// The receiver's gossip substream, held open and never read.
    _inbound: Option<Stream>,
}

impl ConnectionHandler for RawGossipHandler {
    type FromBehaviour = Infallible;
    type ToBehaviour = Infallible;
    type InboundProtocol = ReadyUpgrade<StreamProtocol>;
    type OutboundProtocol = ReadyUpgrade<StreamProtocol>;
    type InboundOpenInfo = ();
    type OutboundOpenInfo = oneshot::Sender<Stream>;

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(ReadyUpgrade::new(self.protocol.clone()), ())
    }

    fn on_behaviour_event(&mut self, event: Self::FromBehaviour) {
        match event {}
    }

    fn on_connection_event(
        &mut self,
        event: ConnectionEvent<
            '_,
            Self::InboundProtocol,
            Self::OutboundProtocol,
            Self::InboundOpenInfo,
            Self::OutboundOpenInfo,
        >,
    ) {
        // `ConnectionEvent` is non-exhaustive; only the two negotiated substreams matter here
        if let ConnectionEvent::FullyNegotiatedInbound(FullyNegotiatedInbound {
            protocol: stream,
            ..
        }) = event
        {
            self._inbound = Some(stream);
        } else if let ConnectionEvent::FullyNegotiatedOutbound(FullyNegotiatedOutbound {
            protocol: stream,
            info: reply,
            ..
        }) = event
        {
            // the test drops its receiver only after it has failed
            let _ = reply.send(stream);
        }
    }

    fn connection_keep_alive(&self) -> bool {
        true
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<
        ConnectionHandlerEvent<Self::OutboundProtocol, Self::OutboundOpenInfo, Self::ToBehaviour>,
    > {
        let next =
            ready!(self.requests.as_mut().map_or(Poll::Pending, |requests| requests.poll_recv(cx)));
        let protocol = self.protocol.clone();
        next.map_or(Poll::Pending, |reply| {
            Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest {
                protocol: SubstreamProtocol::new(ReadyUpgrade::new(protocol), reply),
            })
        })
    }
}

/// Network behaviour for the raw sender in the wire test: one [`RawGossipHandler`] per
/// connection.
struct RawGossipSender {
    /// The chain-scoped gossipsub protocol.
    protocol: StreamProtocol,
    /// Substream requests from the test, given to the first connection.
    requests: Option<OpenRequests>,
}

impl RawGossipSender {
    /// Create the handler for a new connection.
    fn handler(&mut self) -> RawGossipHandler {
        RawGossipHandler {
            protocol: self.protocol.clone(),
            requests: self.requests.take(),
            _inbound: None,
        }
    }
}

impl NetworkBehaviour for RawGossipSender {
    type ConnectionHandler = RawGossipHandler;
    type ToSwarm = Infallible;

    fn handle_established_inbound_connection(
        &mut self,
        _: ConnectionId,
        _: PeerId,
        _: &Multiaddr,
        _: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(self.handler())
    }

    fn handle_established_outbound_connection(
        &mut self,
        _: ConnectionId,
        _: PeerId,
        _: &Multiaddr,
        _: Endpoint,
        _: PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(self.handler())
    }

    fn on_swarm_event(&mut self, _: FromSwarm<'_>) {}

    fn on_connection_handler_event(
        &mut self,
        _: PeerId,
        _: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        match event {}
    }

    fn poll(&mut self, _: &mut Context<'_>) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        Poll::Pending
    }
}

/// Open a raw gossip substream from the sender to the receiver.
async fn open_substream(opener: &SubstreamOpener) -> eyre::Result<Stream> {
    let (reply, opened) = oneshot::channel();
    opener.send(reply).map_err(|_| eyre!("the sender connection is gone"))?;
    tokio::time::timeout(WIRE_TIMEOUT, opened)
        .await
        .map_err(|_| eyre!("the sender did not open a substream in time"))
        .and_then(|opened| opened.map_err(|_| eyre!("the sender dropped a substream request")))
}

/// Write `frame` on a new substream and require the receiver to close that substream.
///
/// The receiver never writes on a substream the sender opened, and the sender never closes its
/// side, so the read below ends only when the receiver closes or resets the substream.
async fn expect_refused(opener: &SubstreamOpener, frame: &[u8]) -> eyre::Result<()> {
    let mut substream = open_substream(opener).await?;
    // a write can fail after the receiver stops reading; the read below is the check
    let _ = substream.write_all(frame).await;
    let _ = substream.flush().await;
    let read = tokio::time::timeout(WIRE_TIMEOUT, substream.read(&mut [0u8; 1]))
        .await
        .map_err(|_| eyre!("the receiver kept a substream open after a frame over a limit"))?;
    // end of stream (0 bytes) and a reset (an error) both mean the receiver closed it
    assert_eq!(read.unwrap_or(0), 0, "the receiver wrote on a substream the sender opened");
    Ok(())
}

/// The listen address from one swarm event, if the event reports one.
fn listen_address<E>(event: SwarmEvent<E>) -> future::Ready<Option<Multiaddr>> {
    future::ready(if let SwarmEvent::NewListenAddr { address, .. } = event {
        Some(address)
    } else {
        None
    })
}

/// On the wire, through the gossipsub 0.50.0 handler and codec, a frame that breaks a limit
/// closes its substream and delivers none of its entries, and frames at the limits are
/// delivered in full.
///
/// A raw sender writes hand-built frames to a receiver that runs [`gossipsub_config`]. Each
/// refused frame goes on its own substream, because the receiver accepts only a few new
/// substreams on one connection (`MAX_SUBSTREAM_ATTEMPTS` in the gossipsub handler).
#[tokio::test]
async fn test_gossip_frame_limits_on_the_wire() -> eyre::Result<()> {
    let topic = longest_topic();
    let limit = u64::try_from(GOSSIP_MAX_PUBLISH_PER_FRAME)?;
    let protocol = StreamProtocol::try_from_owned(format!(
        "{}/1.1.0",
        gossip_protocol_id_prefix(TEST_CHAIN_ID)
    ))
    .map_err(|e| eyre!("gossip protocol: {e:?}"))?;

    // receiver: the node's gossipsub configuration, subscribed to the test topic
    let receiver_key = Keypair::generate_ed25519();
    let mut gossip: gossipsub::Behaviour = gossipsub::Behaviour::new(
        gossipsub::MessageAuthenticity::Signed(receiver_key.clone()),
        gossipsub_config(TEST_CHAIN_ID)?,
    )
    .map_err(|e| eyre!("gossipsub behaviour: {e}"))?;
    gossip
        .subscribe(&gossipsub::IdentTopic::new(topic.clone()))
        .map_err(|e| eyre!("subscribe: {e:?}"))?;
    let mut receiver = SwarmBuilder::with_existing_identity(receiver_key)
        .with_tokio()
        .with_quic()
        .with_behaviour(|_| gossip)
        .map_err(|e| eyre!("receiver behaviour: {e:?}"))?
        .with_swarm_config(|c| c.with_idle_connection_timeout(WIRE_TIMEOUT))
        .build();
    receiver.listen_on("/ip4/127.0.0.1/udp/0/quic-v1".parse()?)?;
    let address = (&mut receiver)
        .filter_map(listen_address)
        .next()
        .await
        .ok_or_else(|| eyre!("the receiver stopped before it listened"))?;

    // forward every delivered payload to the test, in delivery order
    let (delivered_tx, mut delivered_rx) = mpsc::unbounded_channel();
    tokio::spawn(receiver.for_each(move |event| {
        if let SwarmEvent::Behaviour(gossipsub::Event::Message { message, .. }) = event {
            let _ = delivered_tx.send(message.data);
        }
        future::ready(())
    }));

    // sender: raw substreams on the same gossipsub protocol
    let sender_key = Keypair::generate_ed25519();
    let (opener, requests) = mpsc::unbounded_channel();
    let raw_sender = RawGossipSender { protocol, requests: Some(requests) };
    let mut sender = SwarmBuilder::with_existing_identity(sender_key.clone())
        .with_tokio()
        .with_quic()
        .with_behaviour(|_| raw_sender)
        .map_err(|e| eyre!("sender behaviour: {e:?}"))?
        .with_swarm_config(|c| c.with_idle_connection_timeout(WIRE_TIMEOUT))
        .build();
    sender.dial(address)?;
    tokio::spawn(sender.for_each(|_| future::ready(())));

    // every entry has its own sequence number, so the duplicate cache never hides one
    let valid = |seqno: u64, data: &[u8]| signed_message(&sender_key, seqno, &topic, data);
    let invalid = |seqno: u64, data: &[u8]| badly_signed_message(&sender_key, seqno, &topic, data);

    // one entry over the count limit, all with bad signatures: the codec refuses the frame
    // before any signature check (a bad signature alone drops only its own entry)
    let over_count_bad: Vec<Vec<u8>> =
        (1..=limit + 1).map(|seqno| invalid(seqno, b"refused".as_slice())).collect();
    expect_refused(&opener, &publish_frame(&over_count_bad)).await?;

    // one entry over the count limit, all valid: refused, and none of them is delivered
    let over_count: Vec<Vec<u8>> =
        (101..=101 + limit).map(|seqno| valid(seqno, b"refused".as_slice())).collect();
    expect_refused(&opener, &publish_frame(&over_count)).await?;

    // one valid entry in a frame larger than the transmit size: refused
    let oversized = valid(200, vec![0x5a; GOSSIP_MAX_TRANSMIT_SIZE].as_slice());
    expect_refused(&opener, &publish_frame(&[oversized])).await?;

    // the honest traffic, on one substream
    let mut honest = open_substream(&opener).await?;
    // 1. the count limit, all with bad signatures: each entry is dropped, the substream stays
    let at_count_bad: Vec<Vec<u8>> =
        (301..=300 + limit).map(|seqno| invalid(seqno, b"dropped".as_slice())).collect();
    honest.write_all(&publish_frame(&at_count_bad)).await?;
    // 2. a sentinel: it is the first delivery, so no entry of a refused frame was delivered
    honest.write_all(&publish_frame(&[valid(400, b"sentinel".as_slice())])).await?;
    // 3. the count limit, all valid: every entry is delivered
    let at_count_payloads: Vec<Vec<u8>> =
        (501..=500 + limit).map(|seqno| format!("at-count-{seqno}").into_bytes()).collect();
    let at_count: Vec<Vec<u8>> = at_count_payloads
        .iter()
        .zip(501..)
        .map(|(data, seqno)| valid(seqno, data.as_slice()))
        .collect();
    honest.write_all(&publish_frame(&at_count)).await?;
    // 4. two frames at the honest frame bound, back to back in one write: the read buffer can
    // hold the first frame less one byte plus a read chunk of the second, which the slack covers
    let (first_payload, first_frame) = largest_frame(&sender_key, 601, &topic, 0x01);
    let (second_payload, second_frame) = largest_frame(&sender_key, 602, &topic, 0x02);
    assert!(first_frame.len() <= GOSSIP_FRAME_BOUND);
    assert!(first_frame.len() + GOSSIP_LENGTH_PREFIX_BOUND >= GOSSIP_FRAME_BOUND);
    honest.write_all(&[first_frame, second_frame].concat()).await?;
    honest.flush().await?;

    let expected: Vec<Vec<u8>> = std::iter::once(b"sentinel".to_vec())
        .chain(at_count_payloads)
        .chain([first_payload, second_payload])
        .collect();
    let delivered: Vec<Vec<u8>> = tokio::time::timeout(
        WIRE_TIMEOUT,
        futures::stream::poll_fn(|cx| delivered_rx.poll_recv(cx))
            .take(expected.len())
            .collect::<Vec<_>>(),
    )
    .await
    .map_err(|_| eyre!("the receiver did not deliver every entry of the honest frames"))?;
    // compare the sizes first, so a failure does not print the large payloads
    assert_eq!(
        delivered.iter().map(Vec::len).collect::<Vec<_>>(),
        expected.iter().map(Vec::len).collect::<Vec<_>>()
    );
    assert!(delivered == expected, "the delivered payloads differ from the honest payloads");
    Ok(())
}
