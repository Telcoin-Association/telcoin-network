//! Tests for the garbage collector's commit watchdog.

use super::GarbageCollector;
use crate::{
    certificate_fetcher::CertificateFetcherCommand, error::GarbageCollectorError,
    log_capture::LogCapture, ConsensusBus,
};
use assert_matches::assert_matches;
use tn_config::ConsensusConfig;
use tn_storage::mem_db::MemDatabase;
use tn_test_utils_committee::CommitteeFixture;
use tn_types::{TimestampSec, TnReceiver as _};
use tokio::time::Instant;
use tracing::Level;

/// The watchdog's line when the timeout passes without a commit, once genesis has passed or
/// outside epoch 0.
const NO_COMMIT: &str = "no consensus commit happened for 30s, triggering certificate fetching.";
/// The same line while the node waits for genesis.
const NO_COMMIT_BEFORE_GENESIS: &str =
    "no consensus commit happened for 30s before genesis, triggering certificate fetching.";

/// A fixture authority's config rebuilt with its node config's genesis timestamp set to
/// `genesis_timestamp` (unix seconds): the fixture stamps genesis at the moment it is built.
fn config_with_genesis_time(
    fixture: &CommitteeFixture<MemDatabase>,
    genesis_timestamp: TimestampSec,
) -> ConsensusConfig<MemDatabase> {
    let fixture_config = fixture.authorities().next().expect("one authority").consensus_config();
    let mut node_config = fixture_config.config().clone();
    node_config.genesis.timestamp = genesis_timestamp;
    ConsensusConfig::new_with_committee_for_test(
        node_config,
        fixture_config.node_storage().clone(),
        fixture_config.key_config().clone(),
        fixture_config.committee().clone(),
        fixture_config.network_config().clone(),
    )
    .expect("fixture config")
}

/// Nothing commits before genesis, so a wait of days would trip the watchdog at every timeout: the
/// line is logged at debug while an epoch-0 node waits for genesis, and as a warning at the same
/// cadence once genesis has passed or in a later epoch, whatever its genesis time. Every timeout
/// kicks the certificate fetcher either way.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_no_commit_watchdog_warns_only_outside_the_genesis_wait() {
    let now = tn_types::now();
    for (epoch, genesis, waiting) in
        [(0, now + 86_400, true), (0, now.saturating_sub(1), false), (1, now + 86_400, false)]
    {
        let fixture = CommitteeFixture::builder(MemDatabase::default).epoch(epoch).build();
        let capture = LogCapture::new("primary::gc");
        let _capture_guard = tracing::subscriber::set_default(capture.clone());
        let config = config_with_genesis_time(&fixture, genesis);
        let timeout = config.network_config().sync_config().max_consenus_round_timeout;
        assert_eq!(timeout.as_secs(), 30, "the test assumes the default watchdog timeout");
        let consensus_bus = ConsensusBus::new();
        let mut fetcher = consensus_bus.subscribe_certificate_fetcher();
        let mut gc = GarbageCollector::new(config, consensus_bus);

        let start = Instant::now();
        let mut lines = Vec::new();
        for _ in 0..3 {
            assert_matches!(gc.ready().await, Err(GarbageCollectorError::Timeout));
            assert_matches!(fetcher.try_recv(), Ok(CertificateFetcherCommand::Kick));
            let level = |message: &str| capture.events(message).last().map(|event| event.level);
            lines.push((
                start.elapsed().as_secs(),
                level(NO_COMMIT),
                level(NO_COMMIT_BEFORE_GENESIS),
            ));
        }

        let expected: Vec<_> = [30, 60, 90]
            .into_iter()
            .map(|secs| {
                if waiting {
                    (secs, None, Some(Level::DEBUG))
                } else {
                    (secs, Some(Level::WARN), None)
                }
            })
            .collect();
        assert_eq!(
            lines, expected,
            "epoch {epoch}, genesis {genesis}: (secs, warning, debug line)"
        );
        assert_eq!(
            capture.events(NO_COMMIT).len() + capture.events(NO_COMMIT_BEFORE_GENESIS).len(),
            3,
            "one line per timeout"
        );
    }
}
