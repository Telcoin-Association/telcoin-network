//! Synchronize state between primaries.
//!
//! This module primarily deals with Certificate synchronization.

// - primary receives gossip for cert digest
// - request state_sync to retrieve certificate
//  - verify cert
//  - store cert
//  - share with others
//      - gossip message "available" ?

use crate::{
    error::{PrimaryNetworkError, PrimaryNetworkResult},
    network::MissingCertificatesRequest,
};
use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap},
};
use tn_config::ConsensusConfig;
use tn_storage::{traits::Database, CertificateStore};
use tn_types::{AuthorityIdentifier, Certificate, Round};
use tokio::time::Instant;
use tracing::{debug, warn};

/// Time-bounded iterator to retrieve certificates from the database.
pub(crate) struct CertificateCollector<DB> {
    /// Priority queue for tracking the next rounds to fetch
    fetch_queue: BinaryHeap<Reverse<(Round, AuthorityIdentifier)>>,
    /// Rounds to skip per authority
    skip_rounds: BTreeMap<AuthorityIdentifier, BTreeSet<Round>>,
    /// Configuration for syncing behavior and access to the database.
    config: ConsensusConfig<DB>,
    /// The start time for processing the missing certificates request.
    start_time: Instant,
    /// The max items allowed from the request.
    max_items: usize,
    /// The number of items collected.
    items_returned: usize,
}

impl<DB> CertificateCollector<DB>
where
    DB: Database,
{
    /// Create a new certificate collector with the given parameters
    pub(crate) fn new(
        request: MissingCertificatesRequest,
        config: ConsensusConfig<DB>,
    ) -> PrimaryNetworkResult<Self> {
        let start_time = Instant::now();
        let max_items = request.max_items;
        let (lower_bound, skip_rounds) = request.get_bounds()?;

        // initialize the fetch queue with the first round for each authority
        let mut fetch_queue = BinaryHeap::new();
        for (origin, rounds) in &skip_rounds {
            // validate skip rounds count
            if rounds.len()
                > config.network_config().sync_config().max_skip_rounds_for_missing_certs
            {
                warn!(target: "cert_collector", "{} has sent {} rounds to skip", origin, rounds.len());

                return Err(PrimaryNetworkError::InvalidRequest(
                    "Request for rounds out of bounds".into(),
                ));
            }

            if let Some(next_round) = Self::find_next_round(
                &config.node_storage().certificate_store,
                *origin,
                lower_bound,
                &rounds,
            )? {
                debug!(target: "cert_collector", ?next_round, ?origin, "found next round!");
                fetch_queue.push(Reverse((next_round, *origin)));
            }
        }

        debug!(
            target: "cert_collector",
            "Initialized origins and rounds to fetch, elapsed = {}ms",
            start_time.elapsed().as_millis(),
        );

        Ok(Self { fetch_queue, skip_rounds, config, start_time, max_items, items_returned: 0 })
    }

    /// Reference to the collector's start time.
    pub fn start_time(&self) -> &Instant {
        &self.start_time
    }

    /// Find the next available round for an authority that shouldn't be skipped
    fn find_next_round(
        store: &CertificateStore<DB>,
        origin: AuthorityIdentifier,
        current_round: Round,
        skip_rounds: &BTreeSet<Round>,
    ) -> PrimaryNetworkResult<Option<Round>> {
        let mut current_round = current_round;

        while let Some(round) = store.next_round_number(origin, current_round)? {
            if !skip_rounds.contains(&round) {
                return Ok(Some(round));
            }
            current_round = round;
        }

        Ok(None)
    }

    /// Try to fetch the next available certificate
    pub(crate) fn next_certificate(&mut self) -> PrimaryNetworkResult<Option<Certificate>> {
        while let Some(Reverse((round, origin))) = self.fetch_queue.pop() {
            match self.config.node_storage().certificate_store.read_by_index(origin, round)? {
                Some(cert) => {
                    // Queue up the next round for this authority if available
                    if let Some(next_round) = Self::find_next_round(
                        &self.config.node_storage().certificate_store,
                        origin,
                        round,
                        self.skip_rounds.get(&origin).ok_or(PrimaryNetworkError::Internal(
                            "failed to retrieve authority from skipped rounds".to_string(),
                        ))?,
                    )? {
                        self.fetch_queue.push(Reverse((next_round, origin)));
                    }
                    return Ok(Some(cert));
                }
                None => continue,
            }
        }
        Ok(None)
    }

    /// Return a bool representing the max limits for this function.
    ///
    /// If the max limit is reached, the stream should end:
    /// - items returned reaches the max
    /// - time limit is reached
    fn max_limits_reached(&self) -> bool {
        self.items_returned >= self.max_items
            || self.start_time.elapsed()
                >= self.config.network_config().sync_config().max_cert_collection_duration
    }
}

impl<DB> Iterator for CertificateCollector<DB>
where
    DB: Database,
{
    type Item = PrimaryNetworkResult<Certificate>;

    fn next(&mut self) -> Option<Self::Item> {
        // check if any limits have been reached
        if self.max_limits_reached() {
            debug!(target: "cert_collector", "timeout / max items hit! returning None");
            return None;
        }

        // try to fetch the next certificate
        match self.next_certificate() {
            Ok(Some(cert)) => {
                debug!(target: "cert_collector", ?cert, "next cert Ok(Some)");
                self.items_returned += 1;
                Some(Ok(cert))
            }
            Ok(None) => {
                debug!(target: "cert_collector", "next cert Ok(None)");
                None
            }
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{network::MissingCertificatesRequest, state_sync::CertificateCollector};
    use fastcrypto::hash::Hash;
    use std::{collections::BTreeSet, num::NonZeroUsize};
    use tn_storage::mem_db::MemDatabase;
    use tn_test_utils::CommitteeFixture;
    use tn_types::{AuthorityIdentifier, Certificate, SignatureVerificationState};

    #[test]
    fn test_certificate_iterator() {
        // authority fixtures
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .randomize_ports(true)
            .committee_size(NonZeroUsize::new(4).unwrap())
            .build();
        let primary = fixture.authorities().next().expect("primary in committee fixture");
        let consensus_config = primary.consensus_config();
        let certificate_store = primary.consensus_config().node_storage().certificate_store.clone();

        // setup dummy data
        let mut current_round: Vec<_> = Certificate::genesis(&fixture.committee())
            .into_iter()
            .map(|cert| cert.header().clone())
            .collect();
        let mut headers = vec![];
        let total_rounds = 4;
        for i in 0..total_rounds {
            let parents: BTreeSet<_> = current_round
                .into_iter()
                .map(|header| fixture.certificate(&header).digest())
                .collect();
            (_, current_round) = fixture.headers_round(i, &parents);
            headers.extend(current_round.clone());
        }
        let total_authorities = fixture.authorities().count();
        let total_certificates = total_authorities * total_rounds as usize;
        // Create certificates test data.
        let mut certificates = vec![];
        for header in headers.into_iter() {
            certificates.push(fixture.certificate(&header));
        }
        assert_eq!(certificates.len(), total_certificates);
        assert_eq!(16, total_certificates);

        // Populate certificate store such that each authority has the following rounds:
        // Authority 0: 1
        // Authority 1: 1 2
        // Authority 2: 1 2 3
        // Authority 3: 1 2 3 4
        // This is unrealistic because in practice a certificate can only be stored with 2f+1 parents
        // already in store. But this does not matter for testing here.
        let mut authorities = Vec::<AuthorityIdentifier>::new();
        for i in 0..total_authorities {
            authorities.push(certificates[i].header().author());
            for j in 0..=i {
                let mut cert = certificates[i + j * total_authorities].clone();
                assert_eq!(&cert.header().author(), authorities.last().unwrap());
                if i == 3 && j == 3 {
                    // Simulating only 1 directly verified certificate (Auth 3 Round 4) being stored.
                    cert.set_signature_verification_state(
                        SignatureVerificationState::VerifiedDirectly(
                            cert.aggregated_signature().expect("Invalid Signature").clone(),
                        ),
                    );
                } else {
                    // Simulating some indirectly verified certificates being stored.
                    cert.set_signature_verification_state(
                        SignatureVerificationState::VerifiedIndirectly(
                            cert.aggregated_signature().expect("Invalid Signature").clone(),
                        ),
                    );
                }
                certificate_store.write(cert).expect("Writing certificate to store failed");
            }
        }

        // Each test case contains (lower bound round, skip rounds, max items, expected output).
        let test_cases = vec![
            (0, vec![vec![], vec![], vec![], vec![]], 20, vec![1_u32, 1, 1, 1, 2, 2, 2, 3, 3, 4]),
            (0, vec![vec![1u32], vec![1], vec![], vec![]], 20, vec![1, 1, 2, 2, 2, 3, 3, 4]),
            (0, vec![vec![], vec![], vec![1], vec![1]], 20, vec![1, 1, 2, 2, 2, 3, 3, 4]),
            (1, vec![vec![], vec![], vec![2], vec![2]], 4, vec![2, 3, 3, 4]),
            (1, vec![vec![], vec![], vec![2], vec![2]], 2, vec![2, 3]),
            (0, vec![vec![1], vec![1], vec![1, 2, 3], vec![1, 2, 3]], 2, vec![2, 4]),
            (2, vec![vec![], vec![], vec![], vec![]], 3, vec![3, 3, 4]),
            (2, vec![vec![], vec![], vec![], vec![]], 2, vec![3, 3]),
            // Check that round 2 and 4 are fetched for the last authority, skipping round 3.
            (1, vec![vec![], vec![], vec![3], vec![3]], 5, vec![2, 2, 2, 4]),
        ];

        for (lower_bound_round, skip_rounds_vec, max_items, expected_rounds) in test_cases {
            let req = MissingCertificatesRequest::default()
                .set_bounds(
                    lower_bound_round,
                    authorities
                        .clone()
                        .into_iter()
                        .zip(skip_rounds_vec.into_iter().map(|rounds| rounds.into_iter().collect()))
                        .collect(),
                )
                .expect("bounds within range")
                .set_max_items(max_items);

            // collect from database
            let mut missing = Vec::with_capacity(req.max_items);
            let mut collector = CertificateCollector::new(req, consensus_config.clone())
                .expect("certificate collector process valid request");

            // Collect certificates from iterator
            while let Some(certs) = collector.next() {
                missing.push(certs.expect("cert recovered correctly"));
            }

            assert_eq!(
                missing.iter().map(|cert| cert.round()).collect::<Vec<u32>>(),
                expected_rounds
            );
        }
    }
}
