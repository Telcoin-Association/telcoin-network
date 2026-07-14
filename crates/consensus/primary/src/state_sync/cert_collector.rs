//! Collect certificates from storage for peers who are missing them.
//!
//! Used by the typed sync `MissingCertificates` responder to read the certificates a
//! requesting peer is missing from local storage.

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

/// Time-bounded iterator that reads the certificates a peer is missing from storage.
///
/// The reply is streamed frame by frame by the sync `MissingCertificates` responder and
/// ended explicitly, so there is no aggregate response-size cap (the request-response
/// codec's fixed message limit no longer applies after the item-7 cutover). The iterator
/// is bounded only by the per-request processing-time limit
/// (`max_db_read_time_for_fetching_certificates`) and the rounds present in storage; the
/// caller frames and byte-caps the wire response.
pub(crate) struct CertificateCollector<DB> {
    /// Priority queue for tracking the next rounds to fetch
    fetch_queue: BinaryHeap<Reverse<(Round, AuthorityIdentifier)>>,
    /// Rounds to skip per authority
    skip_rounds: BTreeMap<AuthorityIdentifier, BTreeSet<Round>>,
    /// Configuration for syncing behavior and access to the database.
    config: ConsensusConfig<DB>,
    /// The start time for processing the missing certificates request.
    start_time: Instant,
}

impl<DB> CertificateCollector<DB>
where
    DB: Database,
{
    /// Create a streaming certificate collector for `exclusive_lower_bound` /
    /// `skip_rounds`.
    ///
    /// The sync `MissingCertificates` path (item 7 of #739) streams the reply frame by
    /// frame and ends it explicitly, so there is no aggregate response-size cap. The
    /// collector is bounded only by the per-request processing-time limit
    /// (`max_db_read_time_for_fetching_certificates`) and the rounds present in storage.
    /// `skip_rounds` is validated against `max_skip_rounds_for_missing_certs`.
    pub(crate) fn new(
        exclusive_lower_bound: Round,
        skip_rounds: Vec<(AuthorityIdentifier, Vec<u8>)>,
        config: ConsensusConfig<DB>,
    ) -> PrimaryNetworkResult<Self> {
        let start_time = Instant::now();
        let request =
            MissingCertificatesRequest { exclusive_lower_bound, skip_rounds, max_response_size: 0 };
        // Bound the request *before* anything is materialized (see
        // `MissingCertificatesRequest::get_bounds`): the committee size caps how many
        // per-authority entries a peer can name, and the per-authority container/cardinality gates
        // keep a compressed bitmap under the RPC size cap from decoding to billions of rounds and
        // OOM-ing the node. The per-authority loop below re-checks the cardinality limit as a
        // defense-in-depth secondary guard.
        let max_skip_rounds =
            config.network_config().sync_config().max_skip_rounds_for_missing_certs;
        let max_authorities = config.committee().size();
        let (lower_bound, skip_rounds) = request.get_bounds(max_skip_rounds, max_authorities)?;

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

        Ok(Self { fetch_queue, skip_rounds, config, start_time })
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
    /// The stream ends once the per-request processing-time limit has expired.
    fn time_limit_reached(&self) -> bool {
        let reached = self.start_time.elapsed()
            >= self
                .config
                .network_config()
                .sync_config()
                .max_db_read_time_for_fetching_certificates;
        debug!(target: "cert-collector", ?reached, "time limit reached?");
        reached
    }
}

impl<DB> Iterator for CertificateCollector<DB>
where
    DB: Database,
{
    type Item = PrimaryNetworkResult<Certificate>;

    fn next(&mut self) -> Option<Self::Item> {
        // stop once the per-request processing-time limit is reached
        if self.time_limit_reached() {
            debug!(target: "cert-collector", "time limit reached; ending certificate stream");
            return None;
        }

        // Result<Option<cert>> -> Option<Result<cert>>: a fetch error becomes one error item
        self.next_certificate().transpose()
    }
}
