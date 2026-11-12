//! The gap policy of the primary network event queue.
//!
//! The primary network event queue lives for the whole process, but its consumer, the epoch's
//! event loop, does not. A live event loop drains the queue as events arrive, so the queue can
//! fill only in the gap with no event loop: between two epochs, and at start before the first
//! epoch. This policy decides what the queue holds in that gap.

use crate::{
    consensus_bus::{GapAuthor, GapPolicy, GapWeight},
    network::{message::PrimaryGossip, Req, Res},
};
use tn_config::{EPOCH_VOTE_TOPIC_PREFIX, MAX_GOSSIP_MESSAGE_SIZE};
use tn_network_libp2p::types::{GossipPayload, NetworkEvent};
use tn_types::{try_decode, Epoch};

/// The committee size that the gap caps are sized for.
const PRIMARY_GAP_COMMITTEE_SIZE: usize = 100;

/// The committees whose members can publish on the epoch-vote topic at a boundary: the previous,
/// the current and the next committee.
const PRIMARY_GAP_VOTE_COMMITTEES: usize = 3;

/// The epoch records that a gap holds votes for: the record of the epoch of the last event loop
/// and the record before it, which can still have no certificate.
const PRIMARY_GAP_VOTE_RECORDS: usize = 2;

/// The most copies of the vote of one author for one epoch record that the primary network event
/// queue holds in one gap.
///
/// The vote collector republishes the same vote after a receive timeout, and gossipsub delivers
/// such a copy again after its duplicate cache forgets the first one. The cap allows the vote and
/// one copy. One member cannot take the space of the other members past this cap.
const PRIMARY_GAP_MAX_COPIES_PER_VOTE: usize = 2;

/// The most epoch votes that the primary network event queue holds in one gap: the copy cap for
/// each record and each member of the three committees, so 1,200.
///
/// The cap is below [`tn_types::CHANNEL_CAPACITY`], so the held votes alone never fill the queue.
const PRIMARY_GAP_MAX_ITEMS: usize = PRIMARY_GAP_VOTE_COMMITTEES
    .saturating_mul(PRIMARY_GAP_COMMITTEE_SIZE)
    .saturating_mul(PRIMARY_GAP_VOTE_RECORDS)
    .saturating_mul(PRIMARY_GAP_MAX_COPIES_PER_VOTE);

/// The most epoch-vote bytes that the primary network event queue holds in one gap: the item cap
/// at [`MAX_GOSSIP_MESSAGE_SIZE`] each, so 14,400,000 bytes (less than 14 MiB).
const PRIMARY_GAP_MAX_BYTES: usize = PRIMARY_GAP_MAX_ITEMS.saturating_mul(MAX_GOSSIP_MESSAGE_SIZE);

/// The gap policy of the primary network event queue.
///
/// In a gap the queue holds only the gossip on the epoch-vote topic, at its data length, because
/// the epoch votes arrive as gossip between two epochs and epoch-record voting depends on them.
/// The queue holds only the votes for the epoch of the last event loop and the epoch before it, at
/// most [`PRIMARY_GAP_MAX_COPIES_PER_VOTE`] copies for each author and epoch. Thus certificate
/// gossip, consensus-output gossip, replayed old votes and the votes of one member cannot take the
/// space of the other votes.
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
        PRIMARY_GAP_MAX_COPIES_PER_VOTE,
    )
}

/// The weight of a primary network event to hold in the gap after the event loop of `epoch`, or
/// `None` for an event to drop.
fn primary_event_hold(event: &NetworkEvent<Req, Res>, epoch: Option<Epoch>) -> Option<GapWeight> {
    match event {
        NetworkEvent::Gossip(gossip) => epoch_vote_weight(gossip, epoch),
        NetworkEvent::Request { .. }
        | NetworkEvent::Error(..)
        | NetworkEvent::InboundStream { .. } => None,
    }
}

/// The weight of an epoch vote to hold in the gap after the event loop of `epoch`, or `None` for
/// other gossip.
///
/// The queue holds gossip on the epoch-vote topic with an author that decodes to a vote for `epoch`
/// or the epoch before it. These are the records that the vote handler accepts at this boundary.
/// Before the first event loop, `epoch` is `None` and the queue holds no vote. The share key is the
/// author and the epoch of the vote. Thus a peer that replays old vote frames of a member does not
/// take the share of the current vote of that member.
///
/// The topic prefix has no chain id. The swarm subscribes only to the topics of this chain. The
/// swarm forwards only gossip that an authorized publisher signed, so forwarded gossip on this
/// topic has an author. The policy does not check the vote signature, so the epoch window also
/// limits the shares of one author: one share for each epoch of the window.
fn epoch_vote_weight(gossip: &GossipPayload<Req, Res>, epoch: Option<Epoch>) -> Option<GapWeight> {
    let message = &gossip.message;
    let source =
        message.source.filter(|_| message.topic.as_str().starts_with(EPOCH_VOTE_TOPIC_PREFIX))?;
    let window = epoch.map(|epoch| epoch.saturating_sub(1)..=epoch)?;
    try_decode::<PrimaryGossip>(&message.data)
        .ok()
        .and_then(|gossip| match gossip {
            PrimaryGossip::EpochVote(vote) => Some(vote.epoch),
            PrimaryGossip::Certificate(_) | PrimaryGossip::Consensus(_) => None,
        })
        .filter(|vote_epoch| window.contains(vote_epoch))
        .map(|vote_epoch| {
            let author = source.to_bytes().into_iter().chain(vote_epoch.to_le_bytes()).collect();
            GapWeight::new(message.data.len(), GapAuthor::new(author))
        })
}

#[cfg(test)]
mod tests {
    use super::{primary_event_gap_policy, PRIMARY_GAP_MAX_COPIES_PER_VOTE, PRIMARY_GAP_MAX_ITEMS};
    use crate::{
        consensus_bus::QueChannel,
        network::{message::PrimaryGossip, Req, Res},
    };
    use tn_config::LibP2pConfig;
    use tn_network_libp2p::{
        types::{
            GossipPayload, GossipValidation, GossipVerdict, MessageId, NetworkCommand, NetworkEvent,
        },
        GossipMessage, PeerId, TopicHash,
    };
    use tn_types::{
        encode, Epoch, EpochVote, TnReceiver as _, TnSender as _, TrySendOutcome, CHANNEL_CAPACITY,
    };

    /// The epoch of the last event loop in the tests.
    const EPOCH: Epoch = 10;

    /// A primary network event channel with the primary gap policy, in the gap after the event
    /// loop of [`EPOCH`].
    fn primary_channel() -> QueChannel<NetworkEvent<Req, Res>> {
        let channel = QueChannel::new_always_subscribed_held(primary_event_gap_policy());
        drop(channel.subscribe_in_epoch(Some(EPOCH)));
        channel
    }

    /// A gossip event on `topic` from `source` with 100 bytes of data.
    fn gossip(topic: String, source: Option<PeerId>) -> NetworkEvent<Req, Res> {
        gossip_with(topic, source, vec![0; 100])
    }

    /// An epoch-vote gossip event for `epoch` from `source`.
    fn vote_gossip(epoch: Epoch, source: Option<PeerId>) -> NetworkEvent<Req, Res> {
        let vote = EpochVote { epoch, ..EpochVote::default() };
        let data = encode(&PrimaryGossip::EpochVote(Box::new(vote)));
        gossip_with(LibP2pConfig::epoch_vote_topic(0), source, data)
    }

    /// A gossip event on `topic` from `source` with `data` and a detached validation ticket.
    fn gossip_with(topic: String, source: Option<PeerId>, data: Vec<u8>) -> NetworkEvent<Req, Res> {
        gossip_with_ticket(topic, source, data, GossipValidation::detached())
    }

    /// A gossip event on `topic` from `source` with `data` and the validation ticket `validation`.
    fn gossip_with_ticket(
        topic: String,
        source: Option<PeerId>,
        data: Vec<u8>,
        validation: GossipValidation<Req, Res>,
    ) -> NetworkEvent<Req, Res> {
        NetworkEvent::Gossip(Box::new(GossipPayload {
            message: GossipMessage {
                source,
                data,
                sequence_number: None,
                topic: TopicHash::from_raw(topic),
            },
            relayer: None,
            author: None,
            validation,
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
        let vote = vote_gossip(EPOCH, Some(PeerId::random()));

        (0..4_000).for_each(|_| {
            assert_eq!(outcome(&channel, certificate()), Some(TrySendOutcome::Unsubscribed));
        });
        assert_eq!(outcome(&channel, output), Some(TrySendOutcome::Unsubscribed));
        assert_eq!(outcome(&channel, vote), Some(TrySendOutcome::Queued));
    }

    /// Certificate gossip that arrives in a gap is shed. The shed event drops its validation
    /// ticket, and the ticket reports `Ignore`, so gossipsub does not forward the message from this
    /// node (#1772).
    #[test]
    fn gap_sheds_certificate_gossip_and_its_ticket_reports_ignore() {
        let channel = primary_channel();
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let ticket =
            GossipValidation::new(MessageId::from(b"certificate".to_vec()), PeerId::random(), tx);
        let data = encode(&PrimaryGossip::Certificate(Box::default()));
        let certificate = gossip_with_ticket(
            LibP2pConfig::primary_topic(0),
            Some(PeerId::random()),
            data,
            ticket,
        );

        assert_eq!(outcome(&channel, certificate), Some(TrySendOutcome::Unsubscribed));
        assert!(matches!(
            rx.try_recv(),
            Ok(NetworkCommand::ReportGossipValidation { verdict: GossipVerdict::Ignore, .. })
        ));
    }

    /// One author holds at most two copies of its vote for one epoch in a gap. Its vote for the
    /// other epoch of the window and the votes of other authors still queue.
    #[test]
    fn one_author_cannot_take_the_vote_space() {
        let channel = primary_channel();
        let member = PeerId::random();
        let vote = |epoch| vote_gossip(epoch, Some(member));

        (0..PRIMARY_GAP_MAX_COPIES_PER_VOTE).for_each(|_| {
            assert_eq!(outcome(&channel, vote(EPOCH)), Some(TrySendOutcome::Queued));
        });
        assert_eq!(outcome(&channel, vote(EPOCH)), Some(TrySendOutcome::OverBudget));
        assert_eq!(outcome(&channel, vote(EPOCH.saturating_sub(1))), Some(TrySendOutcome::Queued));
        let other = vote_gossip(EPOCH, Some(PeerId::random()));
        assert_eq!(outcome(&channel, other), Some(TrySendOutcome::Queued));
    }

    /// The gap does not hold a vote with no author.
    #[test]
    fn vote_with_no_author_is_not_held() {
        let channel = primary_channel();
        let vote = vote_gossip(EPOCH, None);
        assert_eq!(outcome(&channel, vote), Some(TrySendOutcome::Unsubscribed));
    }

    /// A peer that replays old vote frames of a member does not take the share of the vote of
    /// that member: the gap does not hold a vote for an epoch before the window, and the vote of
    /// the member still queues.
    #[test]
    fn replayed_old_votes_do_not_take_the_member_share() {
        let channel = primary_channel();
        let member = PeerId::random();

        (0..EPOCH.saturating_sub(1)).for_each(|old| {
            let replay = vote_gossip(old, Some(member));
            assert_eq!(outcome(&channel, replay), Some(TrySendOutcome::Unsubscribed));
        });
        let vote = vote_gossip(EPOCH, Some(member));
        assert_eq!(outcome(&channel, vote), Some(TrySendOutcome::Queued));
    }

    /// The gap does not hold a vote for an epoch after the window.
    #[test]
    fn vote_after_the_window_is_not_held() {
        let channel = primary_channel();
        let vote = vote_gossip(EPOCH.saturating_add(1), Some(PeerId::random()));
        assert_eq!(outcome(&channel, vote), Some(TrySendOutcome::Unsubscribed));
    }

    /// The gap does not hold a frame on the epoch-vote topic that does not decode to a vote.
    #[test]
    fn frame_that_is_not_a_vote_is_not_held() {
        let channel = primary_channel();
        let topic = LibP2pConfig::epoch_vote_topic(0);
        let frame = gossip_with(topic, Some(PeerId::random()), vec![9; 100]);
        assert_eq!(outcome(&channel, frame), Some(TrySendOutcome::Unsubscribed));
    }

    /// Before the first event loop the gap has no epoch, so it holds no vote.
    #[test]
    fn no_vote_is_held_before_the_first_event_loop() {
        let channel = QueChannel::new_always_subscribed_held(primary_event_gap_policy());
        let vote = vote_gossip(EPOCH, Some(PeerId::random()));
        assert_eq!(outcome(&channel, vote), Some(TrySendOutcome::Unsubscribed));
    }

    /// A vote held in a gap stays queued when the next event loop subscribes, although the next
    /// epoch moves the window past the epoch of the vote.
    #[test]
    fn held_votes_stay_queued_for_the_next_event_loop() {
        let channel = primary_channel();
        let vote = vote_gossip(EPOCH.saturating_sub(1), Some(PeerId::random()));
        assert_eq!(outcome(&channel, vote), Some(TrySendOutcome::Queued));
        let mut rx = channel.subscribe_in_epoch(Some(EPOCH.saturating_add(1)));
        assert!(rx.try_recv().is_ok(), "the held vote must stay queued");
    }

    /// The gap item cap of the primary network event queue is at most the channel capacity, so
    /// the held votes alone never fill the queue.
    #[test]
    fn primary_gap_item_cap_fits_the_channel() {
        assert_eq!(primary_event_gap_policy().max_items(), PRIMARY_GAP_MAX_ITEMS);
        const { assert!(PRIMARY_GAP_MAX_ITEMS <= CHANNEL_CAPACITY) };
    }
}
