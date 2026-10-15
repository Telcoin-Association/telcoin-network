//! Gossipsub configuration for the consensus network, with explicit frame limits.
//!
//! Every gossip message is signed, and the gossipsub codec verifies one signature for each
//! publish entry in a received frame. That work runs before the duplicate cache and before
//! `verify_gossip` applies [`MAX_GOSSIP_MESSAGE_SIZE`]. The frame limits below bound it. They are
//! derived from the node's own gossip message bound, not taken from the library defaults, so a
//! dependency upgrade cannot silently change them.

use crate::types::gossip_protocol_id_prefix;
use libp2p::gossipsub::{self, ConfigBuilderError};
use std::time::Duration;
use tn_config::MAX_GOSSIP_MESSAGE_SIZE;

#[cfg(test)]
#[path = "tests/gossip_config_tests.rs"]
mod gossip_config_tests;

/// Upper bound, in bytes, on everything one honest publish frame carries besides its payload.
///
/// gossipsub 0.50.0 encodes one `RpcOut` per frame, so an honest frame holds one `Message` in
/// one `Rpc.publish` entry. With an ed25519 identity the framing is: frame length varint (3),
/// publish tag and length (3), `from` peer id (2 + 38), `data` tag and length (3), `seqno`
/// (2 + 8), `topic` (3 + topic bytes), `signature` (2 + 64) and `key` (2 + 36, sent only when the
/// key cannot be inlined in the peer id, which never happens for ed25519). That is 166 bytes
/// plus the topic name, so 512 covers any topic name up to 346 bytes.
pub(crate) const GOSSIP_PUBLISH_ENVELOPE_BOUND: usize = 512;

/// Byte limit on the control and subscription fields of one frame.
///
/// Equal to the gossipsub 0.50.0 default (16 KiB). It is set explicitly so that a library
/// upgrade cannot change it, and because it sizes [`GOSSIP_FRAME_BOUND`].
pub(crate) const GOSSIP_CONTROL_SIZE: usize = 16 * 1024;

/// Bytes the codec reader adds to its buffer for each read.
///
/// asynchronous-codec 0.7.0 `FramedRead` reads in chunks of `INITIAL_CAPACITY` (8 KiB) and calls
/// the decoder after each read. The gossipsub size check measures the whole read buffer, not one
/// frame, so the buffer can hold one frame less one byte plus a full chunk of the next frame.
pub(crate) const GOSSIP_READ_CHUNK: usize = 8 * 1024;

/// Upper bound, in bytes, on the unsigned varint length prefix of one frame.
///
/// A frame shorter than 2 MiB (2^21 bytes) has a length prefix of at most three bytes.
const GOSSIP_LENGTH_PREFIX_BOUND: usize = 3;

/// Largest frame an honest peer sends, in bytes.
///
/// An honest frame is either one publish entry (payload bound plus envelope) or one control or
/// subscription field (control limit plus length prefix). This is the larger of the two.
pub(crate) const GOSSIP_FRAME_BOUND: usize = larger(
    MAX_GOSSIP_MESSAGE_SIZE + GOSSIP_PUBLISH_ENVELOPE_BOUND,
    GOSSIP_CONTROL_SIZE + GOSSIP_LENGTH_PREFIX_BOUND,
);

/// The gossipsub `max_transmit_size`, in bytes.
///
/// One honest frame plus one read chunk. Without the chunk, two honest maximum frames sent back
/// to back can fill the read buffer past the limit, and the receiver closes the stream.
pub(crate) const GOSSIP_MAX_TRANSMIT_SIZE: usize = GOSSIP_FRAME_BOUND + GOSSIP_READ_CHUNK;

/// The gossipsub `max_publish_messages`: the publish entries accepted in one frame.
///
/// An honest peer sends one entry per frame (gossipsub 0.50.0 encodes one `RpcOut` per frame).
/// The codec default of 500 lets one frame force 500 signature checks; 8 caps that work at 8
/// checks per frame and leaves room for a future sender that batches a few entries. This is
/// not a rate bound. Per-peer and per-topic rate budgets are separate work.
pub(crate) const GOSSIP_MAX_PUBLISH_PER_FRAME: usize = 8;

/// Return the larger of two sizes in a const context.
const fn larger(a: usize, b: usize) -> usize {
    if a > b {
        a
    } else {
        b
    }
}

/// Build the gossipsub configuration for a consensus network on `chain_id`.
///
/// Order of the limits in the resolved libp2p-gossipsub 0.50.0 (with asynchronous-codec 0.7.0
/// as the frame reader). A version bump must re-check this order and the bounds above.
///
/// 1. Codec frame gate (`protocol.rs` `validate_rpc_limits`, lines 295-347, called first in
///    `GossipsubCodec::decode`, line 353). On the raw read buffer it checks the buffer size against
///    `max_transmit_size`, then the publish entry count against `max_publish_messages`, then the
///    control and subscription bytes against `max_control_message_size`. A violation is a decode
///    error, and the handler closes the inbound stream (`handler.rs` lines 362-368). This gate
///    needs no peer identity and runs before any signature work.
/// 2. Protobuf decode of the frame (`protocol.rs` line 363).
/// 3. Per-topic size check (`protocol.rs` line 379). No topic has a per-topic size: the
///    [`MAX_GOSSIP_MESSAGE_SIZE`] bound applies to every topic.
/// 4. Signature check of each entry (`ValidationMode::Strict`, `protocol.rs` line 460). A bad
///    signature drops that entry only; the stream stays open.
/// 5. Behaviour checks in `handle_received_message`: message validity (`behaviour.rs` line 1979),
///    then the duplicate cache (line 1983), then the subscribed-topic check (line 2020).
/// 6. `verify_gossip` in this crate: the [`MAX_GOSSIP_MESSAGE_SIZE`] payload check and the
///    publisher checks. That check is a separate control and stays in place.
pub fn gossipsub_config(chain_id: u64) -> Result<gossipsub::Config, ConfigBuilderError> {
    gossipsub::ConfigBuilder::default()
        // explicitly set default
        .heartbeat_interval(Duration::from_secs(1))
        // explicitly set default
        .validation_mode(gossipsub::ValidationMode::Strict)
        // TN specific: filter against authorized_publishers for certain topics
        .validate_messages()
        // Gossipsub negotiates its own `/meshsub` protocol, independent of the
        // req-res/kad/stream names below, so without this it is the one wire
        // protocol two chains still share: namespacing the topics keeps their
        // messages apart but still lets cross-chain peers negotiate a gossip
        // substream. Folding the chain id into the protocol id closes that gap.
        // The builder appends `/1.1.0` and `/1.0.0`, yielding
        // `/tn-meshsub-{chain_id}/1.1.0` and `/tn-meshsub-{chain_id}/1.0.0`.
        .protocol_id_prefix(gossip_protocol_id_prefix(chain_id))
        // Frame limits (issue #1655). See the order of checks above.
        .max_transmit_size(GOSSIP_MAX_TRANSMIT_SIZE)
        // The setter writes both the codec value and the behaviour value.
        .max_publish_messages(GOSSIP_MAX_PUBLISH_PER_FRAME)
        // explicitly set default
        .max_control_message_size(GOSSIP_CONTROL_SIZE)
        .build()
}
