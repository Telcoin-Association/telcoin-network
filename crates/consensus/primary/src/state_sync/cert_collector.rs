//! Collect certificates from storage for peers who are missing them.
//!
//! This module is used when retrieving certificates from local storage for peers.

use crate::{
    error::{PrimaryNetworkError, PrimaryNetworkResult},
    network::MissingCertificatesRequest,
};
use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap},
};
use tn_config::ConsensusConfig;
use tn_storage::CertificateStore;
use tn_types::{AuthorityIdentifier, Certificate, Database, Round};
use tokio::time::Instant;
use tracing::{debug, warn};

#[cfg(test)]
#[path = "../tests/cert_collector_tests.rs"]
mod cert_collector_tests;

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
                warn!(target: "cert-collector", "{} has sent {} rounds to skip", origin, rounds.len());

                return Err(PrimaryNetworkError::InvalidRequest(
                    "Request for rounds out of bounds".into(),
                ));
            }

            if let Some(next_round) =
                Self::find_next_round(config.node_storage(), origin, lower_bound, rounds)?
            {
                debug!(target: "cert-collector", ?next_round, ?origin, "found next round!");
                fetch_queue.push(Reverse((next_round, origin.clone())));
            }
        }

        debug!(
            target: "cert-collector",
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
        store: &DB,
        origin: &AuthorityIdentifier,
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
            match self.config.node_storage().read_by_index(&origin, round)? {
                Some(cert) => {
                    // Queue up the next round for this authority if available
                    if let Some(next_round) = Self::find_next_round(
                        self.config.node_storage(),
                        &origin,
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
                >= self
                    .config
                    .network_config()
                    .sync_config()
                    .max_db_read_time_for_fetching_certificates
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
            debug!(target: "cert-collector", "timeout / max items hit! returning None");
            return None;
        }

        // try to fetch the next certificate
        match self.next_certificate() {
            Ok(Some(cert)) => {
                debug!(target: "cert-collector", ?cert, "next cert Ok(Some)");
                self.items_returned += 1;
                Some(Ok(cert))
            }
            Ok(None) => {
                debug!(target: "cert-collector", "next cert Ok(None)");
                None
            }
            Err(e) => Some(Err(e)),
        }
    }
}
