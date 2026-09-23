//! Configuration for consensus network (primary and worker).
use crate::{
    Config, ConfigFmt, ConfigTrait as _, KeyConfig, NetworkConfig, Parameters, SyncConfig,
    TelcoinDirs,
};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tn_network_types::local::LocalNetwork;
use tn_types::{
    forks::subsecond_timestamp_active, Authority, AuthorityIdentifier, BlsPublicKey, Certificate,
    Committee, Database, Epoch, EpochDigest, Hash as _, HeaderDigest, Multiaddr, NetworkPublicKey,
    ShutdownNotifier, TimestampSec, WorkerId,
};
use tracing::{info, warn};

#[derive(Debug)]
struct ConsensusConfigInner<DB> {
    config: Config,
    committee: Committee,
    /// Contains the keys for the next epoch.
    next_committee_keys: Vec<BlsPublicKey>,
    node_storage: DB,
    key_config: KeyConfig,
    authority: Option<Authority>,
    /// One [`LocalNetwork`] per worker id, keyed by the committee's worker ids.
    ///
    /// Each worker communicates with the primary over its own instance: this is the seam for
    /// future process separation. Sized from [`Committee::number_of_workers`], the chain-derived
    /// count, so the key set cannot drift from the committee the config carries.
    local_networks: BTreeMap<WorkerId, LocalNetwork>,
    network_config: NetworkConfig,
    genesis: HashMap<HeaderDigest, Certificate>,
    /// Digest of the previous epoch's `EpochRecord` ([`EpochDigest::default`] for epoch 0).
    ///
    /// Single source of truth for the canonical epoch-close seed message: proposers sign it and
    /// voters verify header seed signatures against it, so both sides must read the same value.
    prior_epoch_record: EpochDigest,
    /// Timestamp, in seconds, of the previous epoch's closing EVM block (`None` for epoch 0).
    ///
    /// See [`ConsensusConfig::prior_epoch_close`].
    prior_epoch_close: Option<TimestampSec>,
}

/// The configuration for consensus.
///
/// This structure holds all necessary configuration data for both primary and worker
/// consensus components. It manages committee membership, cryptographic keys, network
/// topology, and genesis state required for consensus participation.
///
/// The configuration is designed to be shared across consensus components and provides
/// both authority-specific and network-wide configuration access.
#[derive(Debug, Clone)]
pub struct ConsensusConfig<DB> {
    inner: Arc<ConsensusConfigInner<DB>>,
    shutdown: ShutdownNotifier,
}

impl<DB> ConsensusConfig<DB>
where
    DB: Database,
{
    /// Creates a new consensus configuration by loading committee and worker cache from disk.
    ///
    /// This is the primary constructor that loads configuration from the filesystem,
    /// including committee membership and worker topology from YAML files.
    #[allow(clippy::too_many_arguments)]
    pub fn new<TND: TelcoinDirs + 'static>(
        config: Config,
        tn_datadir: &TND,
        node_storage: DB,
        key_config: KeyConfig,
        network_config: NetworkConfig,
        next_committee_keys: Vec<BlsPublicKey>,
        prior_epoch_record: EpochDigest,
        prior_epoch_close: Option<TimestampSec>,
    ) -> eyre::Result<Self> {
        // Production entry point: enforce the operational floors that the shared, test-facing
        // `new_with_committee` deliberately skips so DAG test fixtures may use small `gc_depth`
        // values. The protocol ceilings are still validated for every constructor inside
        // `new_with_committee`.
        config.parameters.validate_operational_floors()?;

        // load committee from file
        let committee: Committee =
            Config::load_from_path_or_default(tn_datadir.committee_path(), ConfigFmt::YAML)?;
        info!(target: "telcoin", "committee loaded");
        validate_epoch_timing(&config.parameters, network_config.sync_config(), committee.epoch())?;
        Self::new_with_committee(
            config,
            node_storage,
            key_config,
            committee,
            network_config,
            next_committee_keys,
            prior_epoch_record,
            prior_epoch_close,
        )
    }

    /// Creates a new configuration with a pre-loaded committee for testing purposes.
    ///
    /// **WARNING: This method is exposed publicly for testing ONLY.**
    /// Production code should use `new()` or `new_for_epoch()` to ensure proper configuration
    /// loading. The config carries no [`Self::prior_epoch_close`].
    pub fn new_with_committee_for_test(
        config: Config,
        node_storage: DB,
        key_config: KeyConfig,
        committee: Committee,
        network_config: NetworkConfig,
    ) -> eyre::Result<Self> {
        Self::new_with_committee(
            config,
            node_storage,
            key_config,
            committee,
            network_config,
            vec![],
            EpochDigest::default(),
            None,
        )
    }

    /// Creates a test configuration with a pre-loaded committee and an explicit
    /// `prior_epoch_record`.
    ///
    /// **WARNING: This method is exposed publicly for testing ONLY.**
    /// Mirrors [`Self::new_with_committee_for_test`] but seeds a specific `prior_epoch_record`
    /// instead of [`EpochDigest::default`], so tests can exercise the epoch-close seed path with a
    /// non-default cross-epoch anchor (the value a proposer signs and a voter verifies against).
    /// The config carries no [`Self::prior_epoch_close`].
    pub fn new_with_committee_and_prior_epoch_record_for_test(
        config: Config,
        node_storage: DB,
        key_config: KeyConfig,
        committee: Committee,
        network_config: NetworkConfig,
        prior_epoch_record: EpochDigest,
    ) -> eyre::Result<Self> {
        Self::new_with_committee(
            config,
            node_storage,
            key_config,
            committee,
            network_config,
            vec![],
            prior_epoch_record,
            None,
        )
    }

    /// Creates configuration for the next consensus epoch.
    ///
    /// This constructor is used during epoch transitions to initialize configuration
    /// with updated committee membership and worker topology for the new epoch.
    ///
    /// `prior_epoch_close` is the timestamp, in seconds, of the previous epoch's closing EVM block,
    /// or `None` for epoch 0 (see [`Self::prior_epoch_close`]).
    ///
    /// Fails when the parameters violate their operational floors, or when `vote_timeout` is
    /// shorter than `max_header_delay` plus the network config's
    /// `max_header_time_drift_tolerance`.
    #[allow(clippy::too_many_arguments)]
    pub fn new_for_epoch(
        config: Config,
        node_storage: DB,
        key_config: KeyConfig,
        committee: Committee,
        network_config: NetworkConfig,
        next_committee_keys: Vec<BlsPublicKey>,
        prior_epoch_record: EpochDigest,
        prior_epoch_close: Option<TimestampSec>,
    ) -> eyre::Result<Self> {
        // Production entry point: enforce the operational floors (see
        // [`Parameters::validate_operational_floors`]); the shared test-facing constructor skips
        // them so DAG fixtures may use small `gc_depth` values.
        config.parameters.validate_operational_floors()?;
        validate_epoch_timing(&config.parameters, network_config.sync_config(), committee.epoch())?;

        Self::new_with_committee(
            config,
            node_storage,
            key_config,
            committee,
            network_config,
            next_committee_keys,
            prior_epoch_record,
            prior_epoch_close,
        )
    }

    /// Internal constructor that initializes consensus configuration with provided committee.
    ///
    /// This method performs the core initialization logic including:
    /// - Setting up local network identity
    /// - Resolving authority status within the committee
    /// - Creating genesis certificates
    /// - Initializing shutdown notification system
    #[allow(clippy::too_many_arguments)]
    fn new_with_committee(
        config: Config,
        node_storage: DB,
        key_config: KeyConfig,
        committee: Committee,
        network_config: NetworkConfig,
        next_committee_keys: Vec<BlsPublicKey>,
        prior_epoch_record: EpochDigest,
        prior_epoch_close: Option<TimestampSec>,
    ) -> eyre::Result<Self> {
        // Reject a configuration whose consensus parameters exceed the protocol ceilings the
        // consensus-pack reader relies on, so a node can never commit an output it cannot later
        // reconstruct.
        config.parameters.validate()?;

        // Reject a peer-score configuration whose bounds would panic `Score::add`'s `f64::clamp`
        // (a `min_score > max_score` or `NaN` bound) or silently disable reputation enforcement,
        // on the same startup footing as the consensus parameters above, long before the first
        // peer penalty routes the config through the running swarm.
        network_config.peer_config().score_config.validate()?;

        // Reject kad cadences that would panic the network task or let stored records expire
        // before replication or publication can refresh them.
        network_config.libp2p_config().validate()?;

        let local_networks = committee
            .worker_ids()
            .map(|worker_id| (worker_id, LocalNetwork::new(key_config.primary_public_key())))
            .collect();

        let primary_public_key = key_config.primary_public_key();
        let authority = committee.authority_by_key(&primary_public_key);

        let shutdown = ShutdownNotifier::new();
        let genesis = Certificate::genesis(&committee)
            .into_iter()
            .map(|cert| (cert.digest(), cert))
            .collect();

        Ok(Self {
            inner: Arc::new(ConsensusConfigInner {
                config,
                committee,
                next_committee_keys,
                node_storage,
                key_config,
                authority,
                local_networks,
                network_config,
                genesis,
                prior_epoch_record,
                prior_epoch_close,
            }),
            shutdown,
        })
    }

    /// Returns a reference to the shutdown notifier.
    ///
    /// The shutdown notifier can be used to either subscribe to shutdown events
    /// or trigger shutdown across consensus components.
    pub fn shutdown(&self) -> &ShutdownNotifier {
        &self.shutdown
    }

    /// Returns a reference to the inner config parameters.
    pub fn config(&self) -> &Config {
        &self.inner.config
    }

    /// Returns a reference to the genesis certificate collection.
    ///
    /// Genesis certificates establish the initial state and authority set
    /// for the consensus protocol to produce the first primary `Header`.
    pub fn genesis(&self) -> &HashMap<HeaderDigest, Certificate> {
        &self.inner.genesis
    }

    /// Returns a reference to the current committee membership.
    ///
    /// The committee defines the set of authorities participating in consensus
    /// for the current epoch.
    pub fn committee(&self) -> &Committee {
        &self.inner.committee
    }

    /// Returns the keys for the next committee.
    pub fn next_committee_keys(&self) -> &[BlsPublicKey] {
        &self.inner.next_committee_keys
    }

    /// Returns the digest of the previous epoch's `EpochRecord`.
    ///
    /// [`EpochDigest::default`] for epoch 0, which has no prior record. This anchors the
    /// canonical epoch-close seed message: proposers sign over it and voters verify header
    /// seed signatures against it.
    pub fn prior_epoch_record(&self) -> EpochDigest {
        self.inner.prior_epoch_record
    }

    /// Returns the timestamp, in seconds, of the previous epoch's closing EVM block.
    ///
    /// `None` for epoch 0, which follows genesis rather than a closed epoch. In epochs with
    /// sub-second timestamps active, consensus floors the epoch's first commit timestamp on this
    /// value so EVM time does not run backwards across the epoch seam.
    pub fn prior_epoch_close(&self) -> Option<TimestampSec> {
        self.inner.prior_epoch_close
    }

    /// Returns a reference to the node's persistent storage database for the current epoch.
    pub fn node_storage(&self) -> &DB {
        &self.inner.node_storage
    }

    /// Returns a reference to the cryptographic key configuration.
    ///
    /// Contains both primary and worker cryptographic keys used for
    /// consensus participation and network communication.
    pub fn key_config(&self) -> &KeyConfig {
        &self.inner.key_config
    }

    /// Returns the authority information for this node. Optional if it is a committee member or
    /// not.
    pub fn authority(&self) -> &Option<Authority> {
        &self.inner.authority
    }

    /// Returns the authority identifier for this node, if it is a committee member.
    pub fn authority_id(&self) -> Option<AuthorityIdentifier> {
        self.inner.authority.as_ref().map(|a| a.id())
    }

    /// Returns a reference to the consensus protocol parameters.
    ///
    /// Parameters include timing constraints, batch sizes, and other
    /// protocol-specific configuration values.
    pub fn parameters(&self) -> &Parameters {
        &self.inner.config.parameters
    }

    /// Returns a reference to the given worker's local network.
    ///
    /// Contains network identity and local networking setup information.
    /// This is how the Primary and worker `worker_id` communicate.
    ///
    /// Total over any input: `None` for an id outside the committee's worker set. Callers must
    /// surface the miss (this accessor is reached with peer-supplied ids), never fall back to
    /// another worker's instance.
    pub fn local_network(&self, worker_id: WorkerId) -> Option<&LocalNetwork> {
        self.inner.local_networks.get(&worker_id)
    }

    /// Returns every worker's local network with its id, in ascending worker-id order.
    ///
    /// The primary registers its worker-to-primary handler on each instance through this.
    pub fn local_networks(&self) -> impl Iterator<Item = (WorkerId, &LocalNetwork)> {
        self.inner
            .local_networks
            .iter()
            .map(|(worker_id, local_network)| (*worker_id, local_network))
    }

    /// Returns a reference to the network configuration.
    ///
    /// Contains p2p settings and connectivity parameters for libp2p.
    pub fn network_config(&self) -> &NetworkConfig {
        &self.inner.network_config
    }

    /// The chain id used to namespace this node's wire protocols and gossip topics.
    ///
    /// Reads the value stamped onto the network config from genesis at node startup,
    /// so the gossip-validation side (here) and the publish/subscribe side share one
    /// source. See [`NetworkConfig::set_chain_id`].
    pub fn chain_id(&self) -> u64 {
        self.inner.network_config.chain_id()
    }

    /// The current epoch for [Committee].
    pub fn epoch(&self) -> Epoch {
        self.inner.committee.epoch()
    }

    /// Committee network peer ids.
    pub fn committee_pub_keys(&self) -> HashSet<BlsPublicKey> {
        self.inner.committee.authorities().iter().map(|a| a.protocol_key()).copied().collect()
    }

    /// Retrieve the primaries network address.
    pub fn primary_address(&self) -> Multiaddr {
        self.inner.config.node_info.p2p_info.primary.network_address.clone()
    }

    /// Retrieve the primaries network address.
    pub fn primary_networkkey(&self) -> NetworkPublicKey {
        self.inner.config.node_info.p2p_info.primary.network_key.clone()
    }

    /// Bool indicating if an authority identifier is in the current committee.
    pub fn in_committee(&self, id: &AuthorityIdentifier) -> bool {
        self.inner.committee.is_authority(id)
    }

    /// Retrieve the network address of worker `worker_id`, if this node runs that worker.
    pub fn worker_address(&self, worker_id: WorkerId) -> Option<Multiaddr> {
        self.inner.config.node_info.worker_network_address(worker_id).cloned()
    }
}

/// The libp2p request-response timeout every vote request runs under.
///
/// `ConsensusNetwork` builds its request-response behaviours from
/// `request_response::Config::default()` (`crates/network-libp2p/src/consensus.rs`), whose request
/// timeout is ten seconds in libp2p-request-response 0.30; no config knob reaches it. The timeout
/// covers the whole inbound exchange, from reading the request to sending the response, so a
/// voter still evaluating a header when it fires has its vote cancelled. Keep this in step with
/// that call site if it ever sets its own timeout.
const LIBP2P_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Validate the consensus timing knobs that span [`Parameters`] and [`SyncConfig`] for the epoch
/// about to run.
///
/// Rejects a `vote_timeout` outside the window [`validate_vote_timeout`] describes.
///
/// Warns, without failing, when `max_header_delay` is below one second while sub-second
/// timestamps are inactive for `epoch`: header timestamps and the voter's drift wait stay whole
/// seconds for that epoch, so a sub-second cadence can stall at second boundaries.
fn validate_epoch_timing(
    parameters: &Parameters,
    sync_config: &SyncConfig,
    epoch: Epoch,
) -> eyre::Result<()> {
    let subsecond_active = subsecond_timestamp_active(epoch);
    validate_vote_timeout(parameters, sync_config, subsecond_active)?;

    if parameters.max_header_delay < Duration::from_secs(1) && !subsecond_active {
        warn!(
            target: "tn::config",
            epoch,
            max_header_delay = ?parameters.max_header_delay,
            "max_header_delay is below 1s but sub-second timestamps are inactive for this epoch; \
             header timestamps stay whole seconds, so rounds can stall at second boundaries"
        );
    }

    Ok(())
}

/// Check `vote_timeout` against the longest an honest vote evaluation can take and the transport
/// timeout it runs under.
///
/// The lower bound is `max_header_delay` plus the longest the voter may hold its vote while it
/// waits out a future-dated header's lead, so a vote request stays open for a full header cadence
/// plus that wait. With sub-second timestamps active (`subsecond_active`) the wait is at most
/// `max_header_time_drift_tolerance`. Without them the voter compares whole seconds against the
/// tolerance rounded up to whole seconds, so it can admit, and then wait out, a lead of that
/// rounded-up length: 1 s at the 250 ms default.
///
/// The upper bound is [`LIBP2P_REQUEST_TIMEOUT`], exclusive: at or above it the transport cancels
/// a slow vote before `vote_timeout` can fire. The transport also counts the time to read and
/// dispatch the request, so a value just below the bound leaves little margin.
fn validate_vote_timeout(
    parameters: &Parameters,
    sync_config: &SyncConfig,
    subsecond_active: bool,
) -> eyre::Result<()> {
    let tolerance = sync_config.max_header_time_drift_tolerance;
    let drift_wait = if subsecond_active {
        tolerance
    } else {
        Duration::from_secs(
            tolerance.as_secs().saturating_add(u64::from(tolerance.subsec_nanos() != 0)),
        )
    };
    // saturating: both terms come from operator config files, so an overflowing sum must not
    // panic node startup
    let vote_window = parameters.max_header_delay.saturating_add(drift_wait);
    eyre::ensure!(
        parameters.vote_timeout >= vote_window,
        "vote_timeout {:?} is shorter than max_header_delay {:?} + the voter's longest drift wait \
         {:?} (max_header_time_drift_tolerance {:?}{}); raise vote_timeout to at least {:?}",
        parameters.vote_timeout,
        parameters.max_header_delay,
        drift_wait,
        tolerance,
        if subsecond_active {
            ""
        } else {
            ", rounded up to whole seconds while sub-second timestamps are inactive"
        },
        vote_window,
    );
    eyre::ensure!(
        parameters.vote_timeout < LIBP2P_REQUEST_TIMEOUT,
        "vote_timeout {:?} must stay below the libp2p request timeout {:?}, which otherwise \
         cancels a slow vote before vote_timeout fires; lower vote_timeout",
        parameters.vote_timeout,
        LIBP2P_REQUEST_TIMEOUT,
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        validate_epoch_timing, validate_vote_timeout, ConsensusConfig, LIBP2P_REQUEST_TIMEOUT,
    };
    use crate::{Config, KeyConfig, NetworkConfig, Parameters, SyncConfig};
    use rand::{rngs::StdRng, SeedableRng as _};
    use std::time::Duration;
    use tn_types::{
        Address, BlsKeypair, Committee, CommitteeBuilder, DBIter, Database, DbTx, DbTxMut, Epoch,
        EpochDigest, Table, TimestampSec, MAINNET_PARAMETERS, TESTNET_PARAMETERS,
    };

    /// Storage stand-in: building a [`ConsensusConfig`] only stores the handle, so none of these
    /// methods is ever reached.
    #[derive(Clone, Debug)]
    struct NoStorage;

    /// Transaction stand-in for [`NoStorage`].
    #[derive(Debug)]
    struct NoTx;

    impl DbTx for NoTx {
        fn get<T: Table>(&self, _key: &T::Key) -> eyre::Result<Option<T::Value>> {
            unreachable!()
        }
    }

    impl DbTxMut for NoTx {
        fn insert<T: Table>(&mut self, _key: &T::Key, _value: &T::Value) -> eyre::Result<()> {
            unreachable!()
        }

        fn remove<T: Table>(&mut self, _key: &T::Key) -> eyre::Result<()> {
            unreachable!()
        }

        fn clear_table<T: Table>(&mut self) -> eyre::Result<()> {
            unreachable!()
        }

        fn commit(self) -> eyre::Result<()> {
            unreachable!()
        }
    }

    impl Database for NoStorage {
        type TX<'txn> = NoTx;
        type TXMut<'txn> = NoTx;

        fn open_table<T: Table>(&self) -> eyre::Result<()> {
            unreachable!()
        }

        fn read_txn(&self) -> eyre::Result<Self::TX<'_>> {
            unreachable!()
        }

        fn write_txn(&self) -> eyre::Result<Self::TXMut<'_>> {
            unreachable!()
        }

        fn contains_key<T: Table>(&self, _key: &T::Key) -> eyre::Result<bool> {
            unreachable!()
        }

        fn get<T: Table>(&self, _key: &T::Key) -> eyre::Result<Option<T::Value>> {
            unreachable!()
        }

        fn insert<T: Table>(&self, _key: &T::Key, _value: &T::Value) -> eyre::Result<()> {
            unreachable!()
        }

        fn remove<T: Table>(&self, _key: &T::Key) -> eyre::Result<()> {
            unreachable!()
        }

        fn clear_table<T: Table>(&self) -> eyre::Result<()> {
            unreachable!()
        }

        fn is_empty<T: Table>(&self) -> bool {
            unreachable!()
        }

        fn iter<T: Table>(&self) -> DBIter<'_, T> {
            unreachable!()
        }

        fn skip_to<T: Table>(&self, _key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
            unreachable!()
        }

        fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
            unreachable!()
        }

        fn record_prior_to<T: Table>(&self, _key: &T::Key) -> Option<(T::Key, T::Value)> {
            unreachable!()
        }

        fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
            unreachable!()
        }
    }

    /// A two-member committee for `epoch` (the minimum a [`Committee`] accepts) and the key config
    /// of its first member.
    fn committee_and_keys(epoch: Epoch) -> (Committee, KeyConfig) {
        let mut rng = StdRng::from_seed([7; 32]);
        let own = BlsKeypair::generate(&mut rng);
        let peer = BlsKeypair::generate(&mut rng);
        let mut builder = CommitteeBuilder::new(epoch);
        builder.add_authority(*own.public(), Address::ZERO);
        builder.add_authority(*peer.public(), Address::ZERO);
        (builder.build(), KeyConfig::new_with_testing_key(own))
    }

    /// Builds a config for `epoch` through the production [`ConsensusConfig::new_for_epoch`].
    fn config_for_epoch(
        epoch: Epoch,
        prior_epoch_close: Option<TimestampSec>,
    ) -> ConsensusConfig<NoStorage> {
        let (committee, key_config) = committee_and_keys(epoch);
        ConsensusConfig::new_for_epoch(
            Config::default_for_test(),
            NoStorage,
            key_config,
            committee,
            NetworkConfig::default(),
            vec![],
            EpochDigest::default(),
            prior_epoch_close,
        )
        .expect("default test config passes new_for_epoch validation")
    }

    #[test]
    fn default_timing_passes() {
        validate_epoch_timing(&Parameters::default(), &SyncConfig::default(), 0)
            .expect("default parameters and sync config must pass the vote_timeout check");
    }

    /// A sync config whose drift tolerance is `millis`.
    fn tolerance_ms(millis: u64) -> SyncConfig {
        SyncConfig {
            max_header_time_drift_tolerance: Duration::from_millis(millis),
            ..Default::default()
        }
    }

    /// Parameters with a 2 s `max_header_delay` and the given `vote_timeout`.
    fn two_second_rounds(vote_timeout: Duration) -> Parameters {
        Parameters { max_header_delay: Duration::from_secs(2), vote_timeout, ..Default::default() }
    }

    #[test]
    fn vote_timeout_below_header_window_is_rejected() {
        let params = two_second_rounds(Duration::from_millis(2_249));
        let err = validate_vote_timeout(&params, &tolerance_ms(250), true)
            .expect_err("vote_timeout below max_header_delay + tolerance must be rejected");
        assert!(
            err.to_string().contains("vote_timeout"),
            "the error must name vote_timeout as the knob to raise: {err}"
        );
    }

    #[test]
    fn vote_timeout_equal_to_header_window_is_accepted() {
        let params = two_second_rounds(Duration::from_millis(2_250));
        validate_vote_timeout(&params, &tolerance_ms(250), true)
            .expect("vote_timeout exactly max_header_delay + tolerance must be accepted");
    }

    /// Without sub-second timestamps the voter admits a lead of the tolerance rounded up to whole
    /// seconds and waits all of it out, so the window budgets 1 s for the 250 ms default: the
    /// `vote_timeout` that suffices with sub-second timestamps is rejected, and the bound sits at
    /// exactly `max_header_delay + 1 s`. A whole-second tolerance is budgeted as is.
    #[test]
    fn inactive_subsecond_budgets_the_tolerance_rounded_up_to_whole_seconds() {
        let sub_second_window = two_second_rounds(Duration::from_millis(2_250));
        let err = validate_vote_timeout(&sub_second_window, &tolerance_ms(250), false)
            .expect_err("a 250 ms budget undercounts the whole-second drift wait");
        assert!(err.to_string().contains("rounded up to whole seconds"), "{err}");

        let err = validate_vote_timeout(
            &two_second_rounds(Duration::from_millis(2_999)),
            &tolerance_ms(250),
            false,
        )
        .expect_err("one millisecond short of max_header_delay + 1 s must be rejected");
        assert!(err.to_string().contains("at least 3s"), "{err}");
        validate_vote_timeout(
            &two_second_rounds(Duration::from_secs(3)),
            &tolerance_ms(250),
            false,
        )
        .expect("max_header_delay + 1 s must be accepted");
        validate_vote_timeout(
            &two_second_rounds(Duration::from_secs(3)),
            &tolerance_ms(1_000),
            false,
        )
        .expect("a whole-second tolerance is not rounded further");
    }

    /// A `vote_timeout` at or above the libp2p request timeout is rejected whether or not
    /// sub-second timestamps are active, and anything below it that covers the header window is
    /// accepted.
    #[test]
    fn vote_timeout_must_stay_below_the_libp2p_request_timeout() {
        for subsecond_active in [true, false] {
            for vote_timeout in [LIBP2P_REQUEST_TIMEOUT, LIBP2P_REQUEST_TIMEOUT * 2] {
                let err = validate_vote_timeout(
                    &two_second_rounds(vote_timeout),
                    &tolerance_ms(250),
                    subsecond_active,
                )
                .expect_err("the transport would cancel the vote first");
                assert!(
                    err.to_string().contains("libp2p request timeout"),
                    "subsecond_active {subsecond_active}, {vote_timeout:?}: {err}"
                );
            }
            let just_below = LIBP2P_REQUEST_TIMEOUT - Duration::from_millis(1);
            validate_vote_timeout(
                &two_second_rounds(just_below),
                &tolerance_ms(250),
                subsecond_active,
            )
            .expect("just below the transport timeout must be accepted");
        }
    }

    /// An overflowing `max_header_delay + tolerance` must reject rather than panic, including
    /// when the tolerance is first rounded up to whole seconds.
    #[test]
    fn overflowing_header_window_is_rejected_without_panic() {
        let sync_config =
            SyncConfig { max_header_time_drift_tolerance: Duration::MAX, ..Default::default() };
        let params = Parameters { max_header_delay: Duration::from_secs(1), ..Default::default() };
        assert!(validate_epoch_timing(&params, &sync_config, 0).is_err());
        for subsecond_active in [true, false] {
            assert!(validate_vote_timeout(&params, &sync_config, subsecond_active).is_err());
        }
    }

    /// Both shipped `parameters.yaml` presets, loaded through the node's deserializer, must pass
    /// with the default network sync config.
    #[test]
    fn shipped_chain_presets_pass_the_vote_timeout_check() {
        for (name, yaml) in [("mainnet", MAINNET_PARAMETERS), ("adiri", TESTNET_PARAMETERS)] {
            let params: Parameters = serde_yaml::from_str(yaml)
                .unwrap_or_else(|e| panic!("{name} parameters.yaml must parse: {e}"));
            validate_epoch_timing(&params, &SyncConfig::default(), 0)
                .unwrap_or_else(|e| panic!("{name} preset must pass the vote_timeout check: {e}"));
        }
    }

    #[test]
    fn new_for_epoch_without_prior_close_reports_none() {
        assert_eq!(config_for_epoch(0, None).prior_epoch_close(), None);
    }

    #[test]
    fn new_for_epoch_threads_prior_close() {
        let close: TimestampSec = 1_700_000_000;
        assert_eq!(config_for_epoch(1, Some(close)).prior_epoch_close(), Some(close));
    }

    /// Test-facing constructors never seed a floor; tests opt in through the test setter.
    #[test]
    fn test_constructors_carry_no_prior_close() {
        let (committee, key_config) = committee_and_keys(1);
        let config = ConsensusConfig::new_with_committee_for_test(
            Config::default_for_test(),
            NoStorage,
            key_config.clone(),
            committee.clone(),
            NetworkConfig::default(),
        )
        .expect("test config");
        assert_eq!(config.prior_epoch_close(), None);

        let config = ConsensusConfig::new_with_committee_and_prior_epoch_record_for_test(
            Config::default_for_test(),
            NoStorage,
            key_config,
            committee,
            NetworkConfig::default(),
            EpochDigest::default(),
        )
        .expect("test config");
        assert_eq!(config.prior_epoch_close(), None);
    }
}
