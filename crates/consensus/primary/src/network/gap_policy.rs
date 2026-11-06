//! The gap policy of the primary network event queue.
//!
//! The primary network event queue lives for the whole process, but its consumer, the epoch's
//! event loop, does not. A live event loop drains the queue as events arrive, so the queue can
//! fill only in the gap with no event loop: between two epochs, and at start before the first
//! epoch. This policy decides what the queue holds in that gap.

use crate::{
    consensus_bus::GapPolicy,
    network::{Req, Res},
};
use tn_network_libp2p::types::NetworkEvent;

/// One mebibyte.
const MIB: usize = 1024 * 1024;

/// The most gossip messages that the primary network event queue holds in one gap.
///
/// The honest burst is about 40 rounds of certificates from a 100-member committee, plus the
/// epoch votes of the committee. The cap is below [`tn_types::CHANNEL_CAPACITY`], so the held
/// gossip alone never fills the queue.
const PRIMARY_GAP_MAX_ITEMS: usize = 4_000;

/// The most gossip bytes that the primary network event queue holds in one gap: the item cap at
/// 8 KiB each.
const PRIMARY_GAP_MAX_BYTES: usize = 32 * MIB;

/// The gap policy of the primary network event queue.
///
/// In a gap the queue holds only gossip, at its data length, because the epoch votes arrive as
/// gossip between two epochs and epoch-record voting depends on them. It drops every other event:
/// a dropped request drops its response channel, and the peer retries. The certifier retries a
/// vote request until a new proposal supersedes it, and peers retry epoch-record and stream
/// requests.
pub(crate) fn primary_event_gap_policy() -> GapPolicy<NetworkEvent<Req, Res>> {
    GapPolicy::new(
        "primary_network_events",
        primary_event_hold,
        PRIMARY_GAP_MAX_ITEMS,
        PRIMARY_GAP_MAX_BYTES,
    )
}

/// The byte weight of a primary network event to hold in a gap, or `None` for an event to drop.
fn primary_event_hold(event: &NetworkEvent<Req, Res>) -> Option<usize> {
    match event {
        NetworkEvent::Gossip(gossip) => Some(gossip.message.data.len()),
        NetworkEvent::Request { .. }
        | NetworkEvent::Error(..)
        | NetworkEvent::InboundStream { .. } => None,
    }
}
