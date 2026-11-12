//! The gap policy of the primary network event queue.
//!
//! The primary network event queue lives for the whole process, but its consumer, the epoch's
//! event loop, does not. A live event loop drains the queue as events arrive, so the queue can
//! fill only in the gap with no event loop: between two epochs, and at start before the first
//! epoch. This policy decides what the queue holds in that gap: the epoch votes, which
//! epoch-record voting needs, and the newest consensus result of each author, from which a node
//! outside active consensus learns the consensus head.

use crate::{
    consensus_bus::{GapAuthor, GapPolicy, GapWeight},
    network::{message::PrimaryGossip, Req, Res},
};
use tn_config::{CONSENSUS_OUTPUT_TOPIC_PREFIX, EPOCH_VOTE_TOPIC_PREFIX, MAX_GOSSIP_MESSAGE_SIZE};
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

/// The committees whose members can publish on the consensus-output topic: the previous, the
/// current and the next committee.
const PRIMARY_GAP_OUTPUT_COMMITTEES: usize = 3;

/// The most consensus results that the primary network event queue keeps in one gap: one for each
/// member of the three committees, so 300.
///
/// The results are kept apart from the votes, under their own caps. With the vote cap the sum is
/// 1,500, below [`tn_types::CHANNEL_CAPACITY`], so the held votes and results alone never fill
/// the queue. The votes that the last event loop left unread go back in the queue too, outside the
/// gap caps, so in an extreme case the queue is full when the kept results are queued. The results
/// are queued without waiting: one that does not fit is dropped and counted as discarded in the gap
/// log.
const PRIMARY_GAP_MAX_OUTPUTS: usize =
    PRIMARY_GAP_OUTPUT_COMMITTEES.saturating_mul(PRIMARY_GAP_COMMITTEE_SIZE);

/// The most consensus-result bytes that the primary network event queue keeps in one gap: the
/// result cap at [`MAX_GOSSIP_MESSAGE_SIZE`] each, so 3,600,000 bytes (less than 3.5 MiB).
const PRIMARY_GAP_MAX_OUTPUT_BYTES: usize =
    PRIMARY_GAP_MAX_OUTPUTS.saturating_mul(MAX_GOSSIP_MESSAGE_SIZE);

/// The gap policy of the primary network event queue.
///
/// In a gap the queue holds the gossip on the epoch-vote topic, at its data length, because the
/// epoch votes arrive as gossip between two epochs and epoch-record voting depends on them. The
/// queue holds only the votes for the epoch of the last event loop and the epoch before it, at
/// most [`PRIMARY_GAP_MAX_COPIES_PER_VOTE`] copies for each author and epoch. Thus certificate
/// gossip, consensus-output gossip, replayed old votes and the votes of one member cannot take the
/// space of the other votes.
///
/// The queue also keeps the newest consensus result of each author, apart from the votes and
/// under its own caps, so the votes and the results cannot take each other's space (see
/// [`consensus_output_weight`]). A node outside active consensus learns the consensus head of the
/// current epoch only from this gossip: the handler publishes the number and hash once enough
/// authors agree, and state sync then fetches that output. A node that drops the last results in a
/// gap cannot get them again once the committee halts, because nothing new is gossiped. It then
/// never learns the head and never rejoins the halted committee on a stalled head. The next event
/// loop gets the held votes first, then the kept results by consensus number. The order does not
/// matter to the handler: it processes each gossip in its own task and never moves the published
/// head back.
///
/// The policy drops every other event. The certificate fetcher of the new epoch gets the dropped
/// certificates again when a later certificate names them as parents. A dropped request drops its
/// response channel, and the peer retries. The certifier retries a vote request until a new
/// proposal supersedes it, and peers retry epoch-record and stream requests.
pub(crate) fn primary_event_gap_policy() -> GapPolicy<NetworkEvent<Req, Res>> {
    GapPolicy::new(
        "primary_network_events",
        primary_event_hold,
        PRIMARY_GAP_MAX_ITEMS,
        PRIMARY_GAP_MAX_BYTES,
        PRIMARY_GAP_MAX_COPIES_PER_VOTE,
    )
    .keep_newest(PRIMARY_GAP_MAX_OUTPUTS, PRIMARY_GAP_MAX_OUTPUT_BYTES)
}

/// The weight of a primary network event to hold in the gap after the event loop of `epoch`, or
/// `None` for an event to drop.
fn primary_event_hold(event: &NetworkEvent<Req, Res>, epoch: Option<Epoch>) -> Option<GapWeight> {
    match event {
        NetworkEvent::Gossip(gossip) => {
            epoch_vote_weight(gossip, epoch).or_else(|| consensus_output_weight(gossip))
        }
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
fn epoch_vote_weight(gossip: &GossipPayload, epoch: Option<Epoch>) -> Option<GapWeight> {
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

/// The weight of a consensus result to keep in the gap, or `None` for other gossip.
///
/// The queue keeps gossip on the consensus-output topic with an author that decodes to a consensus
/// result, at its data length, ranked by its consensus number. It keeps one result for each author,
/// the gossip source: a result with a higher number replaces the kept result of its author, and a
/// result with a lower or equal number does not. So the queue keeps the newest result, not the
/// first.
///
/// The queue keeps the results in every gap. A vote needs the epoch of the last event loop to bound
/// the records it can be for, but a result does not: the slot of its author already bounds the
/// results of one author to one. The gap that matters is the one between two event loops of a
/// running node, such as the restart of the epoch tasks after a demotion or an epoch change: the
/// node stays subscribed to the topic through it, so results keep arriving. Before the first event
/// loop nothing arrives on the topic, because the node subscribes to it only after that event loop
/// holds the queue (see `spawn_primary_network_for_epoch` in the node crate).
///
/// The hold gives an attacker nothing. Only members of the previous, current and next committee
/// can publish on this topic, and the swarm forwards only gossip that such a publisher signed, so
/// a peer holds at most its own slot. A replayed old result cannot replace a newer kept result of
/// its author, and the handler ignores one at or below the published number. A forged high number
/// replaces only the result of its own author, and the handler counts it only toward an agreement
/// of a third of the committee plus one on the same signed result and drops a number past a
/// certified epoch record, so one author alone publishes nothing. The hold replays only what a
/// live event loop would have received, and the rules of the handler do not change.
///
/// The topic prefix has no chain id, as for the votes: the swarm subscribes only to the topics of
/// this chain.
fn consensus_output_weight(gossip: &GossipPayload) -> Option<GapWeight> {
    let message = &gossip.message;
    let source = message
        .source
        .filter(|_| message.topic.as_str().starts_with(CONSENSUS_OUTPUT_TOPIC_PREFIX))?;
    try_decode::<PrimaryGossip>(&message.data)
        .ok()
        .and_then(|gossip| match gossip {
            PrimaryGossip::Consensus(result) => Some(result.number),
            PrimaryGossip::Certificate(_) | PrimaryGossip::EpochVote(_) => None,
        })
        .map(|number| {
            GapWeight::newest(message.data.len(), GapAuthor::new(source.to_bytes()), number)
        })
}

#[cfg(test)]
mod tests {
    use super::{
        primary_event_gap_policy, PRIMARY_GAP_MAX_COPIES_PER_VOTE, PRIMARY_GAP_MAX_ITEMS,
        PRIMARY_GAP_MAX_OUTPUTS,
    };
    use crate::{
        consensus_bus::QueChannel,
        network::{message::PrimaryGossip, Req, Res},
    };
    use std::collections::HashMap;
    use tn_config::LibP2pConfig;
    use tn_network_libp2p::{
        types::{GossipPayload, NetworkEvent},
        GossipMessage, PeerId, TopicHash,
    };
    use tn_types::{
        encode, try_decode, ConsensusResult, Epoch, EpochVote, TnReceiver as _, TnSender as _,
        TrySendOutcome, CHANNEL_CAPACITY,
    };

    /// The epoch of the last event loop in the tests.
    const EPOCH: Epoch = 10;

    /// A consensus number of a result in the tests.
    const NUMBER: u64 = 41;

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

    /// A consensus-output gossip event for the consensus `number` from `source`.
    fn output_gossip(number: u64, source: Option<PeerId>) -> NetworkEvent<Req, Res> {
        gossip_with(LibP2pConfig::consensus_output_topic(0), source, output_data(number))
    }

    /// The gossip data of a consensus result for the consensus `number`.
    fn output_data(number: u64) -> Vec<u8> {
        let result = ConsensusResult { number, ..ConsensusResult::default() };
        encode(&PrimaryGossip::Consensus(Box::new(result)))
    }

    /// What the next event loop of `channel`, for the epoch after [`EPOCH`], receives, in order:
    /// the source of each gossip, and the consensus number of a result or `None` for a vote.
    fn delivered(
        channel: &QueChannel<NetworkEvent<Req, Res>>,
    ) -> Vec<(Option<PeerId>, Option<u64>)> {
        let mut rx = channel.subscribe_in_epoch(Some(EPOCH.saturating_add(1)));
        std::iter::from_fn(|| rx.try_recv().ok())
            .map(|event| {
                let NetworkEvent::Gossip(gossip) = event else {
                    panic!("the gap holds only gossip")
                };
                let message = gossip.message;
                let number = match try_decode(&message.data).expect("held gossip decodes") {
                    PrimaryGossip::EpochVote(_) => None,
                    PrimaryGossip::Consensus(result) => Some(result.number),
                    PrimaryGossip::Certificate(_) => panic!("the gap holds no certificate"),
                };
                (message.source, number)
            })
            .collect()
    }

    /// What the next event loop of `channel`, for the epoch after [`EPOCH`], receives: the
    /// number of epoch votes, and the consensus numbers of the results of each source.
    fn received(
        channel: &QueChannel<NetworkEvent<Req, Res>>,
    ) -> (usize, HashMap<Option<PeerId>, Vec<u64>>) {
        let mut votes = 0_usize;
        let mut outputs = HashMap::<_, Vec<_>>::new();
        delivered(channel).into_iter().for_each(|(source, number)| match number {
            Some(number) => outputs.entry(source).or_default().push(number),
            None => votes = votes.saturating_add(1),
        });
        (votes, outputs)
    }

    /// A gossip event on `topic` from `source` with `data`.
    fn gossip_with(topic: String, source: Option<PeerId>, data: Vec<u8>) -> NetworkEvent<Req, Res> {
        NetworkEvent::Gossip(Box::new(GossipPayload {
            message: GossipMessage {
                source,
                data,
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
    /// take the space of the epoch votes. The gap drops certificate gossip and keeps the consensus
    /// result apart from the votes.
    #[test]
    fn certificate_gossip_does_not_crowd_out_epoch_votes() {
        let channel = primary_channel();
        let certificate = || gossip(LibP2pConfig::primary_topic(0), Some(PeerId::random()));
        let output = output_gossip(NUMBER, Some(PeerId::random()));
        let vote = vote_gossip(EPOCH, Some(PeerId::random()));

        (0..4_000).for_each(|_| {
            assert_eq!(outcome(&channel, certificate()), Some(TrySendOutcome::Unsubscribed));
        });
        assert_eq!(outcome(&channel, output), Some(TrySendOutcome::Queued));
        assert_eq!(outcome(&channel, vote), Some(TrySendOutcome::Queued));
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

    /// The stalled-head rejoin needs the last consensus results that reach a node between two
    /// event loops. In a gap with thousands of certificate gossips, results for two consensus
    /// numbers from three authors and the epoch votes, the next event loop gets every vote and,
    /// for each author, only its result for the higher number.
    #[test]
    fn newest_consensus_output_of_each_author_is_held_with_the_votes() {
        let channel = primary_channel();
        let authors: Vec<_> = (0..3).map(|_| Some(PeerId::random())).collect();
        let voters: Vec<_> = (0..3).map(|_| Some(PeerId::random())).collect();
        let certificate = || gossip(LibP2pConfig::primary_topic(0), Some(PeerId::random()));
        let queued = Some(TrySendOutcome::Queued);

        [NUMBER, NUMBER.saturating_add(1)].into_iter().for_each(|number| {
            (0..2_000).for_each(|_| {
                assert_eq!(outcome(&channel, certificate()), Some(TrySendOutcome::Unsubscribed));
            });
            authors.iter().zip(&voters).for_each(|(author, voter)| {
                assert_eq!(outcome(&channel, output_gossip(number, *author)), queued);
                assert_eq!(outcome(&channel, vote_gossip(EPOCH, *voter)), queued);
            });
        });

        let (votes, outputs) = received(&channel);
        assert_eq!(votes, authors.len().saturating_mul(2), "every vote reaches the event loop");
        let newest = authors.iter().map(|author| (*author, vec![NUMBER.saturating_add(1)]));
        assert_eq!(
            outputs,
            newest.collect::<HashMap<_, _>>(),
            "one result for each author, the newest"
        );
    }

    /// A result with a lower or an equal consensus number does not replace the kept result of
    /// its author. It is superseded, not over budget, so it reports unsubscribed and the swarm
    /// does not count it as a queue budget shed.
    #[test]
    fn older_consensus_output_does_not_replace_the_newer() {
        let channel = primary_channel();
        let author = Some(PeerId::random());
        let newer = NUMBER.saturating_add(1);
        let unsubscribed = Some(TrySendOutcome::Unsubscribed);

        assert_eq!(outcome(&channel, output_gossip(newer, author)), Some(TrySendOutcome::Queued));
        assert_eq!(outcome(&channel, output_gossip(NUMBER, author)), unsubscribed);
        assert_eq!(outcome(&channel, output_gossip(newer, author)), unsubscribed);

        let (_, outputs) = received(&channel);
        assert_eq!(outputs, HashMap::from([(author, vec![newer])]));
    }

    /// The next event loop gets the held votes first, in the order they arrived, then the kept
    /// results by consensus number, although the results arrived first and the higher number
    /// before the lower.
    #[test]
    fn next_event_loop_gets_the_votes_then_the_results_by_number() {
        let channel = primary_channel();
        let [high, voter, low, other_voter] = [(); 4].map(|()| Some(PeerId::random()));
        let higher = NUMBER.saturating_add(2);
        let queued = Some(TrySendOutcome::Queued);

        assert_eq!(outcome(&channel, output_gossip(higher, high)), queued);
        assert_eq!(outcome(&channel, vote_gossip(EPOCH, voter)), queued);
        assert_eq!(outcome(&channel, output_gossip(NUMBER, low)), queued);
        assert_eq!(outcome(&channel, vote_gossip(EPOCH, other_voter)), queued);

        assert_eq!(
            delivered(&channel),
            [(voter, None), (other_voter, None), (low, Some(NUMBER)), (high, Some(higher))]
        );
    }

    /// More authors than the result cap keep at most the cap of results, and neither the votes
    /// nor the results take the space of the other: the votes before and after the results all
    /// queue up to the vote cap.
    #[test]
    fn consensus_outputs_stay_within_their_cap_and_keep_every_vote() {
        let channel = primary_channel();
        let vote = || vote_gossip(EPOCH, Some(PeerId::random()));
        let queued = Some(TrySendOutcome::Queued);
        let half = PRIMARY_GAP_MAX_ITEMS / 2;

        (0..half).for_each(|_| assert_eq!(outcome(&channel, vote()), queued));
        (0..PRIMARY_GAP_MAX_OUTPUTS).for_each(|_| {
            assert_eq!(outcome(&channel, output_gossip(NUMBER, Some(PeerId::random()))), queued);
        });
        let past_cap = output_gossip(NUMBER, Some(PeerId::random()));
        assert_eq!(outcome(&channel, past_cap), Some(TrySendOutcome::OverBudget));
        (half..PRIMARY_GAP_MAX_ITEMS).for_each(|_| assert_eq!(outcome(&channel, vote()), queued));

        let (votes, outputs) = received(&channel);
        assert_eq!(votes, PRIMARY_GAP_MAX_ITEMS);
        assert_eq!(outputs.len(), PRIMARY_GAP_MAX_OUTPUTS);
        assert!(outputs.values().all(|numbers| numbers == &[NUMBER]));
    }

    /// Before the first event loop the gap has no epoch, but it keeps the consensus results: a
    /// result needs no epoch to be bounded. In a node nothing arrives on the topic this early.
    #[test]
    fn consensus_output_is_held_before_the_first_event_loop() {
        let channel = QueChannel::new_always_subscribed_held(primary_event_gap_policy());
        let author = Some(PeerId::random());
        assert_eq!(outcome(&channel, output_gossip(NUMBER, author)), Some(TrySendOutcome::Queued));

        let (_, outputs) = received(&channel);
        assert_eq!(outputs, HashMap::from([(author, vec![NUMBER])]));
    }

    /// The gap keeps only a consensus result on the consensus-output topic with an author: not a
    /// frame there that does not decode to a result, not a result with no author and not a result
    /// on another topic.
    #[test]
    fn only_an_authored_consensus_result_on_its_topic_is_held() {
        let channel = primary_channel();
        let topic = LibP2pConfig::consensus_output_topic(0);
        let unsubscribed = Some(TrySendOutcome::Unsubscribed);

        let frame = gossip_with(topic, Some(PeerId::random()), vec![9; 100]);
        assert_eq!(outcome(&channel, frame), unsubscribed);
        assert_eq!(outcome(&channel, output_gossip(NUMBER, None)), unsubscribed);
        let off_topic = gossip_with(
            LibP2pConfig::primary_topic(0),
            Some(PeerId::random()),
            output_data(NUMBER),
        );
        assert_eq!(outcome(&channel, off_topic), unsubscribed);
        let output = output_gossip(NUMBER, Some(PeerId::random()));
        assert_eq!(outcome(&channel, output), Some(TrySendOutcome::Queued));
    }

    /// The held votes and the kept results together fit the channel, so they alone never fill
    /// the queue.
    #[test]
    fn held_votes_and_outputs_fit_the_channel() {
        const { assert!(PRIMARY_GAP_MAX_ITEMS + PRIMARY_GAP_MAX_OUTPUTS <= CHANNEL_CAPACITY) };
    }
}
