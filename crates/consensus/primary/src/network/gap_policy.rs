//! The gap policy of the primary network event queue.
//!
//! The primary network event queue lives for the whole process, but its consumer, the epoch's
//! event loop, does not. A live event loop drains the queue as events arrive, so the queue can
//! fill only in the gap with no event loop: between two epochs, and at start before the first
//! epoch. This policy decides what the queue holds in that gap.

use crate::{
    consensus_bus::{GapAuthor, GapPolicy, GapWeight},
    network::{Req, Res},
};
use tn_config::{EPOCH_VOTE_TOPIC_PREFIX, MAX_GOSSIP_MESSAGE_SIZE};
use tn_network_libp2p::types::{GossipPayload, NetworkEvent};

/// The committee size that the gap caps are sized for.
const PRIMARY_GAP_COMMITTEE_SIZE: usize = 100;

/// The committees whose members can publish on the epoch-vote topic at a boundary: the previous,
/// the current and the next committee.
const PRIMARY_GAP_VOTE_COMMITTEES: usize = 3;

/// The most epoch votes from one author that the primary network event queue holds in one gap.
///
/// An honest member publishes one vote for the epoch record that closes, and can publish one more
/// for the previous record when that record has no certificate. The vote collector republishes
/// the same vote after a receive timeout, and gossipsub delivers such a copy again after its
/// duplicate cache forgets the first one. The cap allows two of these copies. One member cannot
/// take the space of the other members past this cap.
const PRIMARY_GAP_MAX_VOTES_PER_AUTHOR: usize = 4;

/// The most epoch votes that the primary network event queue holds in one gap: the per-author cap
/// for each member of the three committees, so 1,200.
///
/// The cap is below [`tn_types::CHANNEL_CAPACITY`], so the held votes alone never fill the queue.
const PRIMARY_GAP_MAX_ITEMS: usize = PRIMARY_GAP_VOTE_COMMITTEES
    .saturating_mul(PRIMARY_GAP_COMMITTEE_SIZE)
    .saturating_mul(PRIMARY_GAP_MAX_VOTES_PER_AUTHOR);

/// The most epoch-vote bytes that the primary network event queue holds in one gap: the item cap
/// at [`MAX_GOSSIP_MESSAGE_SIZE`] each, so 14,400,000 bytes (less than 14 MiB).
const PRIMARY_GAP_MAX_BYTES: usize = PRIMARY_GAP_MAX_ITEMS.saturating_mul(MAX_GOSSIP_MESSAGE_SIZE);

/// The gap policy of the primary network event queue.
///
/// In a gap the queue holds only the gossip on the epoch-vote topic, at its data length, because
/// the epoch votes arrive as gossip between two epochs and epoch-record voting depends on them.
/// Each author has a share of [`PRIMARY_GAP_MAX_VOTES_PER_AUTHOR`] votes, so certificate gossip,
/// consensus-output gossip and the votes of one member cannot take the space of the other votes.
///
/// The policy drops every other event. The certificate fetcher of the new epoch gets the dropped
/// certificates again when a later certificate names them as parents. State sync gets the dropped
/// consensus output again. A dropped request drops its response channel, and the peer retries. The
/// certifier retries a vote request until a new proposal supersedes it, and peers retry
/// epoch-record and stream requests.
pub(crate) fn primary_event_gap_policy() -> GapPolicy<NetworkEvent<Req, Res>> {
    GapPolicy::new(
        "primary_network_events",
        primary_event_hold,
        PRIMARY_GAP_MAX_ITEMS,
        PRIMARY_GAP_MAX_BYTES,
        PRIMARY_GAP_MAX_VOTES_PER_AUTHOR,
    )
}

/// The weight of a primary network event to hold in a gap, or `None` for an event to drop.
fn primary_event_hold(event: &NetworkEvent<Req, Res>) -> Option<GapWeight> {
    match event {
        NetworkEvent::Gossip(gossip) => epoch_vote_weight(gossip),
        NetworkEvent::Request { .. }
        | NetworkEvent::Error(..)
        | NetworkEvent::InboundStream { .. } => None,
    }
}

/// The weight of gossip on the epoch-vote topic, with its author, or `None` for gossip on another
/// topic or with no author.
///
/// The topic prefix has no chain id. The swarm subscribes only to the topics of this chain. The
/// swarm forwards only gossip that an authorized publisher signed, so forwarded gossip on this
/// topic has an author.
fn epoch_vote_weight(gossip: &GossipPayload) -> Option<GapWeight> {
    let message = &gossip.message;
    message
        .source
        .filter(|_| message.topic.as_str().starts_with(EPOCH_VOTE_TOPIC_PREFIX))
        .map(|source| GapWeight::new(message.data.len(), GapAuthor::new(source.to_bytes())))
}

#[cfg(test)]
mod tests {
    use super::{
        primary_event_gap_policy, PRIMARY_GAP_MAX_ITEMS, PRIMARY_GAP_MAX_VOTES_PER_AUTHOR,
    };
    use crate::{
        consensus_bus::QueChannel,
        network::{Req, Res},
    };
    use tn_config::LibP2pConfig;
    use tn_network_libp2p::{
        types::{GossipPayload, NetworkEvent},
        GossipMessage, PeerId, TopicHash,
    };
    use tn_types::{TnSender as _, TrySendOutcome, CHANNEL_CAPACITY};

    /// A primary network event channel with the primary gap policy and no event loop.
    fn primary_channel() -> QueChannel<NetworkEvent<Req, Res>> {
        QueChannel::new_always_subscribed_held(primary_event_gap_policy())
    }

    /// A gossip event on `topic` from `source`.
    fn gossip(topic: String, source: Option<PeerId>) -> NetworkEvent<Req, Res> {
        NetworkEvent::Gossip(Box::new(GossipPayload {
            message: GossipMessage {
                source,
                data: vec![0; 100],
                sequence_number: None,
                topic: TopicHash::from_raw(topic),
            },
            relayer: None,
            author: None,
        }))
    }

    /// What `try_send_outcome` reported for `event`, or `None` for an error.
    fn outcome(
        channel: &QueChannel<NetworkEvent<Req, Res>>,
        event: NetworkEvent<Req, Res>,
    ) -> Option<TrySendOutcome> {
        channel.try_send_outcome(event).ok()
    }

    /// The reproduction of issue #1762: certificate gossip that arrives first in a gap does not
    /// take the space of the epoch votes. The gap drops certificate and consensus-output gossip.
    #[test]
    fn certificate_gossip_does_not_crowd_out_epoch_votes() {
        let channel = primary_channel();
        let certificate = || gossip(LibP2pConfig::primary_topic(0), Some(PeerId::random()));
        let output = gossip(LibP2pConfig::consensus_output_topic(0), Some(PeerId::random()));
        let vote = gossip(LibP2pConfig::epoch_vote_topic(0), Some(PeerId::random()));

        (0..4_000).for_each(|_| {
            assert_eq!(outcome(&channel, certificate()), Some(TrySendOutcome::Unsubscribed));
        });
        assert_eq!(outcome(&channel, output), Some(TrySendOutcome::Unsubscribed));
        assert_eq!(outcome(&channel, vote), Some(TrySendOutcome::Queued));
    }

    /// One author holds at most its share of the votes in a gap. The votes of other authors
    /// still queue.
    #[test]
    fn one_author_cannot_take_the_vote_space() {
        let channel = primary_channel();
        let member = PeerId::random();
        let vote = |source| gossip(LibP2pConfig::epoch_vote_topic(0), Some(source));

        (0..PRIMARY_GAP_MAX_VOTES_PER_AUTHOR).for_each(|_| {
            assert_eq!(outcome(&channel, vote(member)), Some(TrySendOutcome::Queued));
        });
        assert_eq!(outcome(&channel, vote(member)), Some(TrySendOutcome::OverBudget));
        assert_eq!(outcome(&channel, vote(PeerId::random())), Some(TrySendOutcome::Queued));
    }

    /// The gap does not hold a vote with no author.
    #[test]
    fn vote_with_no_author_is_not_held() {
        let channel = primary_channel();
        let vote = gossip(LibP2pConfig::epoch_vote_topic(0), None);
        assert_eq!(outcome(&channel, vote), Some(TrySendOutcome::Unsubscribed));
    }

    /// The gap item cap of the primary network event queue is at most the channel capacity, so
    /// the held votes alone never fill the queue.
    #[test]
    fn primary_gap_item_cap_fits_the_channel() {
        assert_eq!(primary_event_gap_policy().max_items(), PRIMARY_GAP_MAX_ITEMS);
        assert!(PRIMARY_GAP_MAX_ITEMS <= CHANNEL_CAPACITY, "{PRIMARY_GAP_MAX_ITEMS}");
    }
}
