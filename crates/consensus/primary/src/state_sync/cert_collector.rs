//! Collect certificates from storage for peers who are missing them.
//!
//! This module is used when retrieving certificates from local storage for peers.

use crate::{
    error::{PrimaryNetworkError, PrimaryNetworkResult},
    network::{MissingCertificatesRequest, PrimaryRequest},
};
use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap},
    sync::LazyLock,
};
use tn_config::ConsensusConfig;
use tn_storage::CertificateStore;
use tn_types::{AuthorityIdentifier, Certificate, Database, Round};
use tokio::time::Instant;
use tracing::{debug, warn};

/// The minimal length of a single, encoded, default [Certificate] used to set a local min for
/// message validation.
static LOCAL_MIN_REQUEST_SIZE: LazyLock<usize> =
    LazyLock::new(|| tn_types::encode(&Certificate::default()).len());
/// The minimal wrapper overhead using a default, empty message.
static MESSAGE_OVERHEAD: LazyLock<usize> = LazyLock::new(|| {
    tn_types::encode(&PrimaryRequest::MissingCertificates {
        inner: MissingCertificatesRequest::default(),
    })
    .len()
});

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
    /// The maximum uncompressed message size in bytes (with safety margin).
    max_message_size: usize,
    /// The current collection of uncompressed, encoded certificates (in bytes) used to ensure
    /// appropriate message size.
    accumulated_size: usize,
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

        // assume reasonable min is 1 encoded certificate
        // NOTE: caller needs to account for cert + msg overhead
        if request.max_response_size < *LOCAL_MIN_REQUEST_SIZE {
            warn!(target: "cert-collector", "missing cert request max size too small: {}", request.max_response_size);
            return Err(PrimaryNetworkError::InvalidRequest("Request size too small".into()));
        }

        // use the min value between this node's max rpc message size and the requestor's reported
        // max message size
        //
        // NOTE: assume safe overhead is accounted for because the codec will also compress messages
        // unit tests show 318b uncompressed (response with 2 certs) -> 61b compressed
        let local_max =
            config.network_config().libp2p_config().max_rpc_message_size - *MESSAGE_OVERHEAD;
        let max_message_size = request.max_response_size.min(local_max);
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

        Ok(Self {
            fetch_queue,
            skip_rounds,
            config,
            start_time,
            max_message_size,
            accumulated_size: 0,
        })
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
    /// The stream should end:
    /// - if there is less than the min request size left before reaching the max message size
    /// - the time limit has expired
    fn max_limits_reached(&self) -> bool {
        let size = self.max_message_size - self.accumulated_size <= *LOCAL_MIN_REQUEST_SIZE;
        let time = self.start_time.elapsed()
            >= self
                .config
                .network_config()
                .sync_config()
                .max_db_read_time_for_fetching_certificates;
        debug!(target: "cert-collector", ?size, ?time, "limit reached?");
        size || time
    }

    /// Check if adding an encoded certificate would exceed size limits.
    fn would_exceed_size_limit(&self, cert_bytes: usize) -> bool {
        self.accumulated_size + cert_bytes > self.max_message_size
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
            debug!(target: "cert-collector", "max limit reached! returning None");
            return None;
        }

        // try to fetch the next certificate
        match self.next_certificate() {
            Ok(Some(cert)) => {
                debug!(target: "cert-collector", ?cert, "next cert Ok(Some)");
                // encode the certificate and check size limits
                let bytes = tn_types::encode(&cert);

                // check accumulated total to ensure this cert doesn't exceed msg size limit
                if self.would_exceed_size_limit(bytes.len()) {
                    debug!(
                        target: "cert-collector",
                        "Next certificate would exceed size limit. Current size: {} bytes",
                        self.accumulated_size
                    );

                    // put the certificate back for future requests
                    return None;
                }

                self.accumulated_size += bytes.len();
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
