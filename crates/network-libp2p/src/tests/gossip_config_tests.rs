//! Tests for the explicit gossipsub frame limits (issue #1655) and the cap on message ids per
//! `IHAVE` (issue #1700).

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

/// A sequence number like the ones gossipsub 0.50.0 publishes: a Unix time in nanoseconds
/// (19 digits).
const NANOSECOND_SEQNO: u64 = 1_760_000_000_000_000_000;

/// Encode one length-prefixed RPC frame whose control field holds one `IHAVE` for `topic` with
/// `ids` (`Rpc.control` = 3, `ControlMessage.ihave` = 1, `topic_id` = 1, `message_ids` = 2).
///
/// Return the frame and the control bytes the codec counts for it: the whole `Rpc.control`
/// field, with its tag and length.
fn ihave_frame(topic: &str, ids: &[Vec<u8>]) -> (Vec<u8>, usize) {
    let ihave =
        [field(1, topic.as_bytes()), ids.iter().flat_map(|id| field(2, id)).collect()].concat();
    let control = field(3, &field(1, &ihave));
    let control_bytes = control.len();
    ([varint(control_bytes), control].concat(), control_bytes)
}

/// The control bytes the codec counts for one `IWANT` of `ids` (`ControlMessage.iwant` = 2,
/// `message_ids` = 1).
fn iwant_control_bytes(ids: &[Vec<u8>]) -> usize {
    let iwant: Vec<u8> = ids.iter().flat_map(|id| field(1, id)).collect();
    field(3, &field(2, &iwant)).len()
}

/// The default message id the node's gossipsub configuration gives a message from `source`
/// with sequence number `seqno`.
fn default_message_id(config: &gossipsub::Config, source: PeerId, seqno: u64) -> Vec<u8> {
    config
        .message_id(&gossipsub::Message {
            source: Some(source),
            data: Vec::new(),
            sequence_number: Some(seqno),
            topic: gossipsub::TopicHash::from_raw("unused"),
        })
        .0
}

/// `count` distinct default message ids of one ed25519 publisher with nanosecond sequence
/// numbers. Each id is 71 bytes.
fn ed25519_ids(config: &gossipsub::Config, count: usize) -> eyre::Result<Vec<Vec<u8>>> {
    let source = Keypair::generate_ed25519().public().to_peer_id();
    Ok((0..u64::try_from(count)?)
        .map(|offset| default_message_id(config, source, NANOSECOND_SEQNO + offset))
        .collect())
}

/// `count` distinct message ids of exactly [`GOSSIP_MESSAGE_ID_BOUND`] bytes.
fn bound_ids(count: usize) -> Vec<Vec<u8>> {
    let width = GOSSIP_MESSAGE_ID_BOUND;
    (0..count).map(|index| format!("{index:0width$}").into_bytes()).collect()
}

/// A topic of exactly [`GOSSIP_TOPIC_BOUND`] bytes: [`longest_topic`] padded with `-`.
fn bound_topic() -> String {
    let topic = longest_topic();
    let width = GOSSIP_TOPIC_BOUND;
    format!("{topic:-<width$}")
}

/// The cap on message ids per `IHAVE` reaches the gossipsub config, and each bound equals a
/// pinned number.
#[test]
fn test_gossipsub_config_pins_ihave_id_cap() -> Result<(), ConfigBuilderError> {
    let config = gossipsub_config(TEST_CHAIN_ID)?;

    // the setter writes the IHAVE cap and the IWANT cap per peer per heartbeat
    assert_eq!(config.max_control_messages_sent(), GOSSIP_MAX_IHAVE_IDS);

    // pinned values: a change here must be a reviewed decision
    assert_eq!(GOSSIP_MAX_IHAVE_IDS, 198);
    assert_eq!(GOSSIP_MESSAGE_ID_BOUND, 80);
    assert_eq!(GOSSIP_TOPIC_BOUND, 64);

    // one IHAVE lists the ids of the last three heartbeats
    assert_eq!(config.history_gossip(), 3);
    // the longest topic the node uses is within the topic bound
    assert!(longest_topic().len() <= GOSSIP_TOPIC_BOUND);
    Ok(())
}

/// [`GOSSIP_MESSAGE_ID_BOUND`] covers every default message id: an ed25519 publisher gives 71
/// bytes, and the longest peer id with the widest sequence number gives exactly the bound.
#[test]
fn test_gossip_message_id_bound_covers_default_ids() -> eyre::Result<()> {
    let config = gossipsub_config(TEST_CHAIN_ID)?;

    // ed25519 peer id (52 base58 characters) and a nanosecond sequence number (19 digits)
    let ed25519 = Keypair::generate_ed25519().public().to_peer_id();
    assert_eq!(default_message_id(&config, ed25519, NANOSECOND_SEQNO).len(), 71);

    // the longest peer id is an identity multihash of a 42-byte key (44 bytes, 60 base58
    // characters), and u64::MAX has 20 digits
    let longest_bytes: Vec<u8> = [0x00, 0x2a].into_iter().chain([0xff; 42]).collect();
    let longest = PeerId::from_bytes(&longest_bytes)?;
    assert_eq!(longest.to_bytes().len(), 44);
    assert_eq!(default_message_id(&config, longest, u64::MAX).len(), GOSSIP_MESSAGE_ID_BOUND);
    Ok(())
}

/// Without the cap, honest gossip passes the control limit. An `IHAVE` of 71-byte ids passes
/// it at 225 ids on a 15-byte topic and at 224 ids on the longest topic, and the library
/// default lets the node send up to 5000 ids.
#[test]
fn test_ihave_of_default_ids_passes_the_control_limit() -> eyre::Result<()> {
    let config = gossipsub_config(TEST_CHAIN_ID)?;
    let limit = config.max_control_message_size();
    let control_bytes = |topic: &str, count: usize| -> eyre::Result<usize> {
        Ok(ihave_frame(topic, &ed25519_ids(&config, count)?).1)
    };

    let short_topic = "t".repeat(15);
    assert!(control_bytes(&short_topic, 224)? <= limit);
    assert!(control_bytes(&short_topic, 225)? > limit);

    let topic = longest_topic();
    assert_eq!(topic.len(), 40);
    assert!(control_bytes(&topic, 223)? <= limit);
    assert!(control_bytes(&topic, 224)? > limit);

    // the cap is below both counts
    assert!(config.max_control_messages_sent() < 223);
    Ok(())
}

/// An `IHAVE` at the cap fits the control limit with the largest ids on the largest topic, and
/// one more id does not. An `IWANT` at the cap fits too.
#[test]
fn test_ihave_id_cap_fits_the_control_limit() -> eyre::Result<()> {
    let config = gossipsub_config(TEST_CHAIN_ID)?;
    let limit = config.max_control_message_size();
    let cap = config.max_control_messages_sent();
    let topic = bound_topic();
    assert_eq!(topic.len(), GOSSIP_TOPIC_BOUND);

    // ids at the id bound on a topic at the topic bound
    let (frame, at_cap) = ihave_frame(&topic, &bound_ids(cap));
    assert!(at_cap <= limit);
    assert!(frame.len() <= GOSSIP_FRAME_BOUND);
    assert!(ihave_frame(&topic, &bound_ids(cap + 1)).1 > limit);

    // ids of today's size on the longest topic the node uses
    assert!(ihave_frame(&longest_topic(), &ed25519_ids(&config, cap)?).1 <= limit);

    // the IWANT the node sends one peer in one heartbeat
    assert!(iwant_control_bytes(&bound_ids(cap)) <= limit);
    Ok(())
}

/// Start a receiver that runs [`gossipsub_config`] subscribed to `topic`, and a raw sender
/// connected to it.
///
/// Return the sender's substream opener, the payloads the receiver delivers (in delivery
/// order) and the sender's identity.
async fn raw_gossip_pair(
    topic: &str,
) -> eyre::Result<(SubstreamOpener, mpsc::UnboundedReceiver<Vec<u8>>, Keypair)> {
    let protocol = StreamProtocol::try_from_owned(format!(
        "{}/1.1.0",
        gossip_protocol_id_prefix(TEST_CHAIN_ID)
    ))
    .map_err(|e| eyre!("gossip protocol: {e:?}"))?;

    // receiver: the node's gossipsub configuration, subscribed to the topic
    let receiver_key = Keypair::generate_ed25519();
    let mut gossip: gossipsub::Behaviour = gossipsub::Behaviour::new(
        gossipsub::MessageAuthenticity::Signed(receiver_key.clone()),
        gossipsub_config(TEST_CHAIN_ID)?,
    )
    .map_err(|e| eyre!("gossipsub behaviour: {e}"))?;
    gossip.subscribe(&gossipsub::IdentTopic::new(topic)).map_err(|e| eyre!("subscribe: {e:?}"))?;
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

    // forward every delivered payload, in delivery order
    let (delivered_tx, delivered_rx) = mpsc::unbounded_channel();
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
    Ok((opener, delivered_rx, sender_key))
}

/// On the wire, through the gossipsub 0.50.0 handler and codec, an `IHAVE` at the cap keeps its
/// substream open, and one more id closes the substream.
///
/// The ids are at the id bound and the topic is at the topic bound. A sentinel publish frame
/// follows the `IHAVE` on the same substream, and the receiver delivers it only if it did not
/// close that substream.
#[tokio::test]
async fn test_ihave_id_cap_on_the_wire() -> eyre::Result<()> {
    let topic = bound_topic();
    let cap = gossipsub_config(TEST_CHAIN_ID)?.max_control_messages_sent();
    let (opener, mut delivered_rx, sender_key) = raw_gossip_pair(&topic).await?;

    // an IHAVE at the cap, then a sentinel, on one substream
    let mut honest = open_substream(&opener).await?;
    honest.write_all(&ihave_frame(&topic, &bound_ids(cap)).0).await?;
    honest
        .write_all(&publish_frame(&[signed_message(&sender_key, 1, &topic, b"sentinel")]))
        .await?;
    honest.flush().await?;
    let delivered = tokio::time::timeout(WIRE_TIMEOUT, delivered_rx.recv())
        .await
        .map_err(|_| eyre!("the receiver closed the substream of an IHAVE at the cap"))?;
    assert_eq!(delivered, Some(b"sentinel".to_vec()));

    // one id over the cap: the receiver closes the substream
    expect_refused(&opener, &ihave_frame(&topic, &bound_ids(cap + 1)).0).await?;
    Ok(())
}
