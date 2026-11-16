//! Aggregate certificates for the round.

use crate::{error::CertManagerResult, ConsensusBus};
use std::collections::{BTreeMap, HashSet};
use tn_types::{AuthorityIdentifier, Certificate, Committee, Round, TnSender as _, VotingPower};
use tokio::time::Instant;
use tracing::{info, trace};

/// Manage certificates as they aggregate through rounds.
#[derive(Debug, Clone)]
pub(crate) struct CertificatesAggregatorManager {
    /// Collection of [CertificatesAggregator]s.
    aggregators: BTreeMap<Round, Box<CertificatesAggregator>>,
    /// Consensus bus to forward parents for a round to the proposer.
    consensus_bus: ConsensusBus,
}

impl CertificatesAggregatorManager {
    /// Create a new instance of self with allocation for the max gc-depth.
    pub(crate) fn new(consensus_bus: ConsensusBus) -> Self {
        Self { aggregators: BTreeMap::new(), consensus_bus }
    }

    /// Append a certificate by round and alert proposer if quorum is reached (2f+1).
    ///
    /// Once the round has a quorum, every later certificate of the round is sent to the proposer
    /// as well, on its own. Each certificate that enters its round's aggregator is logged once as
    /// `certificate accepted` (target `consensus::metrics`), before its parent set is sent: the
    /// certificates of the round this node then holds, and whether and how long ago this node
    /// first sent a parent set of the round. A second certificate of an author already seen in
    /// the round enters nothing and is not logged.
    pub(crate) async fn append_certificate(
        &mut self,
        certificate: Certificate,
        committee: &Committee,
        source: CertificateSource,
    ) -> CertManagerResult<()> {
        let round = certificate.round();
        let epoch = certificate.epoch();
        let origin = certificate.origin().clone();

        // append certificate
        let appended = self
            .aggregators
            .entry(round)
            .or_insert_with(|| Box::new(CertificatesAggregator::new()))
            .append(certificate, committee);
        let Some(appended) = appended else {
            return Ok(());
        };

        let sends_parents = appended.parents.is_some();
        info!(
            target: "consensus::metrics",
            epoch,
            round,
            origin = %origin,
            round_certs = appended.round_certs,
            parents_sent = appended.parents_sent,
            late_ms = appended.late_ms,
            source = %source.as_str(),
            parents_resent = sends_parents && appended.parents_sent,
            quorum_reached = sends_parents && !appended.parents_sent,
            "certificate accepted",
        );

        // forward to proposer if enough parents to advance the round (2f+1)
        if let Some(parents) = appended.parents {
            self.consensus_bus.parents().send((parents, round)).await?;
        }

        Ok(())
    }

    /// Process the next gc round and remove old parents that can never be accepted in the DAG.
    pub(crate) fn garbage_collect(&mut self, gc_round: &Round) {
        self.aggregators.retain(|k, _| k > gc_round);
    }
}

/// Where a certificate handed to [`CertificatesAggregatorManager::append_certificate`] came from,
/// as far as its caller knows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CertificateSource {
    /// This node's own certificate (its origin is this node), accepted after verification.
    Own,
    /// Another authority's certificate, accepted after verification. The caller cannot tell a
    /// certificate received in a broadcast from one fetched from a peer.
    Peer,
    /// A certificate of the last two rounds, reloaded from this node's store at startup.
    Recovered,
}

impl CertificateSource {
    /// The value of the `source` field of the `certificate accepted` line.
    fn as_str(self) -> &'static str {
        match self {
            Self::Own => "own",
            Self::Peer => "peer",
            Self::Recovered => "recovered",
        }
    }
}

/// Aggregate certificates until quorum is reached
#[derive(Debug, Clone)]
struct CertificatesAggregator {
    /// The accumulated amount of voting power in favor of a proposed header.
    ///
    /// This amount is used to verify enough voting power to reach quorum within the committee.
    weight: VotingPower,
    /// The certificates aggregated for this round.
    certificates: Vec<Certificate>,
    /// The collection of authority ids that have already voted.
    authorities_seen: HashSet<AuthorityIdentifier>,
    /// When the first parent set of the round was handed back for the proposer, if it was.
    first_sent: Option<Instant>,
}

impl CertificatesAggregator {
    /// Create a new instance of `Self`.
    fn new() -> Self {
        Self {
            weight: 0,
            certificates: Vec::new(),
            authorities_seen: HashSet::new(),
            first_sent: None,
        }
    }

    /// Append the certificate to the collection.
    ///
    /// This method protects against equivocation by keeping track of peers that have already issued
    /// certificates: a second certificate of an author returns `None`.
    fn append(&mut self, certificate: Certificate, committee: &Committee) -> Option<Appended> {
        let origin = certificate.origin().clone();

        // ensure authority hasn't issued certificate already
        if !self.authorities_seen.insert(origin.clone()) {
            return None;
        }

        // the send state before this certificate, for the line that logs it
        let parents_sent = self.first_sent.is_some();
        let late_ms = self
            .first_sent
            .map_or(0, |sent| u64::try_from(sent.elapsed().as_millis()).unwrap_or(u64::MAX));

        // accumulate certificates and voting power
        self.certificates.push(certificate);
        self.weight += committee.voting_power_by_id(&origin);

        // check for quorum
        let mut parents = None;
        if self.weight >= committee.quorum_threshold() {
            trace!(target: "primary::certificate_aggregator", "quorum reached");
            // NOTE: do not reset the weight here
            //
            // this method could be called again if the proposer doesn't
            // advance the round
            self.first_sent.get_or_insert_with(Instant::now);
            parents = Some(self.certificates.drain(..).collect());
        }

        Some(Appended { parents, round_certs: self.authorities_seen.len(), parents_sent, late_ms })
    }
}

/// What appending one certificate did to its round's aggregator.
#[derive(Debug)]
struct Appended {
    /// The parents to send to the proposer: the round's certificates since the last send, once
    /// the round has a quorum.
    parents: Option<Vec<Certificate>>,
    /// The certificates of the round this node holds, this one included.
    round_certs: usize,
    /// Whether a parent set of the round was handed back before this certificate.
    parents_sent: bool,
    /// Milliseconds from the first parent set of the round to this certificate, or 0.
    late_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::{CertificateSource, CertificatesAggregatorManager};
    use crate::{log_capture::LogCapture, ConsensusBus};
    use std::{num::NonZeroUsize, time::Duration};
    use tn_storage::mem_db::MemDatabase;
    use tn_test_utils_committee::CommitteeFixture;
    use tn_types::{AuthorityIdentifier, Certificate, TnReceiver as _};

    /// Ten certificates of one round in a fixed order at a ten-member committee (quorum 7): each
    /// logs `certificate accepted` once with the count it makes, the seventh sends the first parent
    /// set, each later one sends itself again, timed from the first send, and a second certificate
    /// of an author already seen logs and sends nothing.
    ///
    /// Catches: a count that misses the certificates drained by the first send, a send flagged on
    /// the wrong line, a `late_ms` not timed from the first send, and a duplicate that logs.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn certificate_accepted_lines_mark_the_first_parent_send() {
        let capture = LogCapture::new("consensus::metrics");
        let _capture_guard = tracing::subscriber::set_default(capture.clone());
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .randomize_ports(true)
            .committee_size(NonZeroUsize::new(10).unwrap())
            .build();
        let committee = fixture.committee();
        let certificates: Vec<Certificate> =
            fixture.headers().iter().map(|header| fixture.certificate(header)).collect();
        assert_eq!(certificates.len(), 10, "one certificate per member");
        assert_eq!(committee.quorum_threshold(), 7, "ten members of equal stake");
        let round = certificates[0].round();

        let bus = ConsensusBus::new();
        let mut rx_parents = bus.subscribe_parents();
        let mut manager = CertificatesAggregatorManager::new(bus.clone());
        for (index, certificate) in certificates.iter().enumerate() {
            if index >= 7 {
                tokio::time::advance(Duration::from_millis(25)).await;
            }
            manager
                .append_certificate(certificate.clone(), &committee, CertificateSource::Peer)
                .await
                .unwrap();
        }
        manager
            .append_certificate(certificates[3].clone(), &committee, CertificateSource::Own)
            .await
            .unwrap();

        let lines = capture.events("certificate accepted");
        assert_eq!(lines.len(), 10, "one line per certificate that enters the aggregator");
        for (index, line) in lines.iter().enumerate() {
            let sent_before = index >= 7;
            let late_ms = if sent_before { 25 * (index as u64 - 6) } else { 0 };
            let origin = certificates[index].origin().to_string();
            assert_eq!(line.u64_field("epoch"), Some(u64::from(committee.epoch())), "{index}");
            assert_eq!(line.u64_field("round"), Some(u64::from(round)), "{index}");
            assert_eq!(line.field("origin"), Some(origin.as_str()), "{index}");
            assert_eq!(line.u64_field("round_certs"), Some(index as u64 + 1), "{index}");
            assert_eq!(line.field("parents_sent"), Some(bool_str(sent_before)), "{index}");
            assert_eq!(line.u64_field("late_ms"), Some(late_ms), "{index}");
            assert_eq!(line.field("source"), Some("peer"), "{index}");
            assert_eq!(line.field("parents_resent"), Some(bool_str(sent_before)), "{index}");
            assert_eq!(line.field("quorum_reached"), Some(bool_str(index == 6)), "{index}");
        }

        // the quorum's seven certificates go in one set, each later certificate in its own
        let origins = |set: &[Certificate]| -> Vec<AuthorityIdentifier> {
            set.iter().map(|cert| cert.origin().clone()).collect()
        };
        let mut expected = vec![origins(&certificates[..7])];
        expected.extend(certificates[7..].iter().map(|cert| vec![cert.origin().clone()]));
        for (send, want) in expected.iter().enumerate() {
            let (parents, parents_round) = rx_parents.recv().await.unwrap();
            assert_eq!(parents_round, round, "send {send}");
            assert_eq!(&origins(&parents), want, "send {send}");
        }
    }

    /// The rendering of a bool field in a captured line.
    fn bool_str(value: bool) -> &'static str {
        if value {
            "true"
        } else {
            "false"
        }
    }
}
