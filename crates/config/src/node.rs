//! Configurations for the Telcoin Network.

use crate::{ConfigFmt, ConfigTrait, NodeInfo, TelcoinDirs, GOVERNANCE_SAFE_ADDRESS};
use reth_chainspec::ChainSpec;
use serde::{Deserialize, Serialize};
use std::{fs::File, io::Write, time::Duration};
use tn_types::{
    get_available_udp_port, test_genesis, Address, BlsPublicKey, BlsSignature, Genesis,
    NetworkPublicKey, WorkerId, MAINNET_COMMITTEE, MAINNET_GENESIS, MAINNET_PARAMETERS,
    TESTNET_COMMITTEE, TESTNET_GENESIS, TESTNET_PARAMETERS,
};
use tracing::info;

/// The filename to use when reading/writing the validator's BlsKey.
pub const BLS_KEYFILE: &str = "bls.key";
/// The filename to use when reading/writing a wrapped (encypted) validator BlsKey.
pub const BLS_WRAPPED_KEYFILE: &str = "bls.kw";
/// The filename to use when reading/writing the primary's network keys seed.
pub const PRIMARY_NETWORK_SEED_FILE: &str = "primary.seed";
/// The filename to use when reading/writing the network key seed used by all workers.
pub const WORKER_NETWORK_SEED_FILE: &str = "worker.seed";

/// Configuration for the Telcoin Network node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// [NodeInfo] for the node
    pub node_info: NodeInfo,

    /// Parameters for the network.
    pub parameters: Parameters,

    /// The [Genesis] for the node.
    pub genesis: Genesis,

    /// Spawn ExEx tasks (and the ExEx manager) as critical tasks.
    ///
    /// Default `false`: ExExes run isolated and non-critical, so a stuck,
    /// panicking, or finished ExEx can never shut the node down. Set `true` when
    /// an ExEx is load-bearing for this deployment and its failure (or clean
    /// exit) *should* take the node down — e.g. a bridge that must not silently
    /// stop following the chain.
    #[serde(default)]
    pub exex_critical: bool,

    /// Reference to the apps version string.
    #[serde(skip)]
    pub version: &'static str,
}

impl ConfigTrait for Config {}

impl Config {
    /// Create a Config for testing.
    pub fn default_for_test() -> Self {
        Self::default_for_test_with_genesis(test_genesis())
    }

    /// Create a Config for testing.
    pub fn default_for_test_with_genesis(genesis: Genesis) -> Self {
        Self {
            // defaults
            node_info: Default::default(),
            parameters: Default::default(),
            genesis,
            exex_critical: false,
            version: "UNKNOWN",
        }
    }

    /// Load a config from it's component parts.
    /// Fallback to defaults if files are missing.
    pub fn load_or_default<P: TelcoinDirs>(
        tn_datadir: &P,
        version: &'static str,
    ) -> eyre::Result<Self> {
        let node_info: NodeInfo =
            Config::load_from_path_or_default(tn_datadir.node_info_path(), ConfigFmt::YAML)?;
        let parameters: Parameters = Config::load_from_path_or_default(
            tn_datadir.node_config_parameters_path(),
            ConfigFmt::YAML,
        )?;
        let genesis: Genesis =
            Config::load_from_path_or_default(tn_datadir.genesis_file_path(), ConfigFmt::YAML)?;

        Ok(Config { node_info, parameters, genesis, exex_critical: false, version })
    }

    /// Load a config from it's component parts.
    pub fn load<P: TelcoinDirs>(tn_datadir: &P, version: &'static str) -> eyre::Result<Self> {
        let validator_info: NodeInfo =
            Config::load_from_path(tn_datadir.node_info_path(), ConfigFmt::YAML)?;
        let parameters: Parameters =
            Config::load_from_path(tn_datadir.node_config_parameters_path(), ConfigFmt::YAML)?;
        let genesis: Genesis =
            Config::load_from_path(tn_datadir.genesis_file_path(), ConfigFmt::YAML)?;

        Ok(Config { node_info: validator_info, parameters, genesis, exex_critical: false, version })
    }

    /// Load a config from it's component parts.
    pub fn load_adiri<P: TelcoinDirs>(tn_datadir: &P, version: &'static str) -> eyre::Result<Self> {
        let validator_info: NodeInfo =
            Config::load_from_path(tn_datadir.node_info_path(), ConfigFmt::YAML)?;
        let parameters: Parameters =
            serde_yaml::from_str(TESTNET_PARAMETERS).expect("bad adiri parameters yaml data");
        let genesis: Genesis =
            serde_yaml::from_str(TESTNET_GENESIS).expect("bad adiri genesis yaml data");
        // If the default committee file does not exist then save it.
        let committee_path = tn_datadir.committee_path();
        if !committee_path.exists() {
            std::fs::create_dir_all(tn_datadir.genesis_path())?;
            File::create_new(committee_path)?.write_all(TESTNET_COMMITTEE.as_bytes())?
        }

        Ok(Config { node_info: validator_info, parameters, genesis, exex_critical: false, version })
    }

    /// Load a config from it's component parts.
    pub fn load_mainnet<P: TelcoinDirs>(
        tn_datadir: &P,
        version: &'static str,
    ) -> eyre::Result<Self> {
        let validator_info: NodeInfo =
            Config::load_from_path(tn_datadir.node_info_path(), ConfigFmt::YAML)?;
        let parameters: Parameters =
            serde_yaml::from_str(MAINNET_PARAMETERS).expect("bad adiri parameters yaml data");
        let genesis: Genesis =
            serde_yaml::from_str(MAINNET_GENESIS).expect("bad adiri genesis yaml data");
        // If the default committee file does not exist then save it.
        let committee_path = tn_datadir.committee_path();
        if !committee_path.exists() {
            std::fs::create_dir_all(tn_datadir.genesis_path())?;
            File::create_new(committee_path)?.write_all(MAINNET_COMMITTEE.as_bytes())?
        }

        Ok(Config { node_info: validator_info, parameters, genesis, exex_critical: false, version })
    }

    /// Update the authority protocol key.
    pub fn update_protocol_key(&mut self, value: BlsPublicKey) -> eyre::Result<()> {
        self.node_info.bls_public_key = value;
        Ok(())
    }

    /// Update the authority execution address.
    pub fn update_proof_of_possession(&mut self, value: BlsSignature) -> eyre::Result<()> {
        self.node_info.proof_of_possession = value;
        Ok(())
    }

    /// Update the authority network key.
    pub fn update_primary_network_key(&mut self, value: NetworkPublicKey) -> eyre::Result<()> {
        self.node_info.p2p_info.primary.network_key = value;
        Ok(())
    }

    /// Update the network key of worker `worker_id`.
    ///
    /// Errors if this node runs no worker with that id.
    pub fn update_worker_network_key(
        &mut self,
        worker_id: WorkerId,
        value: NetworkPublicKey,
    ) -> eyre::Result<()> {
        self.node_info
            .p2p_info
            .worker_mut(worker_id)
            .map(|worker| worker.network_key = value)
            .ok_or_else(|| eyre::eyre!("no worker {worker_id} in node info"))
    }

    /// Update the authority execution address.
    pub fn update_execution_address(&mut self, value: Address) -> eyre::Result<()> {
        self.node_info.execution_address = value;
        Ok(())
    }

    /// Update genesis.
    pub fn with_genesis(mut self, genesis: Genesis) -> Self {
        self.genesis = genesis;
        self
    }

    /// Return a reference to the
    pub fn genesis(&self) -> &Genesis {
        &self.genesis
    }

    /// Return the ChainSpec for the configured Genesis
    pub fn chain_spec(&self) -> ChainSpec {
        self.genesis.clone().into()
    }

    /// Return a reference to the exeuction address for suggested fee recipient.
    pub fn execution_address(&self) -> &Address {
        &self.node_info.execution_address
    }

    /// Return a reference to the primary's public BLS key.
    pub fn primary_bls_key(&self) -> &BlsPublicKey {
        self.node_info.public_key()
    }
}

/// Holds all the node properties.
///
/// An example is provided to
/// showcase the usage and deserialization from a json file.
/// To define a Duration on the property file can use either
/// milliseconds or seconds (e.x 5s, 10ms , 2000ms).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Parameters {
    /// When the primary has `header_num_of_batches_threshold` num of batch digests available,
    /// then it can propose a new header.
    #[serde(default = "Parameters::default_header_num_of_batches_threshold")]
    pub header_num_of_batches_threshold: usize,

    /// The maximum number of batch digests included in a header.
    #[serde(default = "Parameters::default_max_header_num_of_batches")]
    pub max_header_num_of_batches: usize,

    /// The maximum delay that the primary should wait between generating two headers, even if
    /// other conditions are not satisfied besides having enough parent stakes.
    #[serde(with = "humantime_serde", default = "Parameters::default_max_header_delay")]
    pub max_header_delay: Duration,
    /// When the delay from last header reaches `min_header_delay`, a new header can be proposed
    /// even if batches have not reached `header_num_of_batches_threshold`.
    #[serde(with = "humantime_serde", default = "Parameters::default_min_header_delay")]
    pub min_header_delay: Duration,
    /// How long a voter may spend evaluating one vote request, and how far past the drift
    /// tolerance a future-dated header can be and still get a retryable answer.
    ///
    /// Both uses are on the voter side. Each vote request is evaluated under this time limit; a
    /// request that runs out of time gets a retryable response instead of a vote. A header ahead
    /// of the voter's clock by more than `max_header_time_drift_tolerance` but by no more than
    /// that tolerance plus this timeout also gets a retryable response rather than a rejection.
    /// The requesting proposer retries on either response, so a slow voter or a small clock skew
    /// delays that vote rather than ending the request.
    ///
    /// Kept separate from `max_header_delay` because a voter may hold its vote while it waits out
    /// a future-dated header's lead, up to `max_header_time_drift_tolerance`, before it answers.
    /// A limit tied to the header cadence can expire while an honest voter is still waiting, and
    /// the shorter the cadence the more often that happens.
    ///
    /// Must be at least `max_header_delay + max_header_time_drift_tolerance` (the tolerance lives
    /// in the network config's `sync_config`; while the sub-second gate is dormant for the epoch
    /// it is rounded up to whole seconds, matching the voter), above the voter's 1 s execution
    /// wait (`VOTE_EXECUTION_WAIT`) and below the 10 s libp2p request timeout.
    /// `ConsensusConfig::validate_epoch_timing` rejects other values, and the node runs
    /// it on entering any epoch it can still vote in, so it refuses to start such an epoch with
    /// one. An epoch its committee has already closed is replayed without the check. Defaults to
    /// 5 s.
    #[serde(with = "humantime_serde", default = "Parameters::default_vote_timeout")]
    pub vote_timeout: Duration,

    /// The depth of the garbage collection (Denominated in number of rounds).
    #[serde(default = "Parameters::default_gc_depth")]
    pub gc_depth: u32,
    /// The delay after which the synchronizer retries to send sync requests. Denominated in ms.
    #[serde(with = "humantime_serde", default = "Parameters::default_sync_retry_delay")]
    pub sync_retry_delay: Duration,
    /// Determine with how many nodes to sync when re-trying to send sync-request. These nodes
    /// are picked at random from the committee.
    #[serde(default = "Parameters::default_sync_retry_nodes")]
    pub sync_retry_nodes: usize,
    /// The delay after which the workers seal a batch of transactions, even if `max_batch_size`
    /// is not reached.
    #[serde(with = "humantime_serde", default = "Parameters::default_max_batch_delay")]
    pub max_batch_delay: Duration,
    /// The maximum number of concurrent requests for messages accepted from an un-trusted entity
    #[serde(default = "Parameters::default_max_concurrent_requests")]
    pub max_concurrent_requests: usize,
    /// Worker timeout when request vote from peers.
    #[serde(default = "Parameters::default_batch_vote_timeout")]
    pub batch_vote_timeout: Duration,
    /// The address that receives the base-fee portion of every transaction's gas payment.
    ///
    /// CONSENSUS-CRITICAL. The EVM credits this account on every transaction, so its balance
    /// enters the state root. All nodes on a network must hold the same value. If two nodes
    /// disagree, they still agree on the batch, the certificate, and the commit order, but they
    /// build different state roots from the same committed output and the network splits. See the
    /// `BASEFEE_ADDRESS` documentation in `tn-reth` for the execution-side detail.
    ///
    /// The key is required: a parameters file that omits it fails to deserialize with an error
    /// that names the field, wherever that file is parsed. (The one adjacent gap is a missing
    /// FILE under [`Config::load_or_default`], which substitutes the complete
    /// [`Parameters::default`] and writes it back to disk; its only caller today is a test
    /// helper, and the production node start loads through `Config::load`, which errors on a
    /// missing file.) This field deliberately declares no serde default. A default here would fill
    /// a fallback on a missing key in silence, which is the exact failure mode of issue #1113
    /// moved one layer up. Test fixtures obtain an address through [`Parameters::default`],
    /// which supplies `GOVERNANCE_SAFE_ADDRESS`. The chain presets and the genesis ceremony
    /// both write the key, so `--chain mainnet`, `--chain adiri`, and ceremony-founded
    /// networks all parse.
    pub basefee_address: Address,
    /// The default duration between parallel/fallback fetch requests to peers for missing
    /// certificates.
    #[serde(default = "Parameters::default_parallel_fetch_request_delay_interval")]
    pub parallel_fetch_request_delay_interval: Duration,

    /// Allow observer transaction forwarding to dial advertised JSON-RPC endpoints whose host is
    /// not a public internet address.
    ///
    /// An observer forwards each transaction it seals to the endpoint the owning validator
    /// advertised on its node record, so the dial target is chosen by a committee member rather
    /// than by this node. Default `false`: an endpoint on a loopback, private, link-local,
    /// unique-local, shared-address-space or unspecified address is refused, so a committee member
    /// cannot aim this node's outbound HTTP at hosts inside its own perimeter (issue #1092).
    ///
    /// Set `true` only where every committee member is under the same operator as this node --
    /// single-host and docker-compose deployments, where validators legitimately advertise
    /// `127.0.0.1` -- since it restores dialing of arbitrary internal addresses.
    #[serde(default = "Parameters::default_allow_private_forward_targets")]
    pub allow_private_forward_targets: bool,

    /// Propose headers that name the execution block a quorum of parents has reached, not this
    /// node's own newest block.
    ///
    /// When `true`, the proposer ranks its own tip and each same-epoch parent's latest execution
    /// block by voting weight and names the highest block that holders of 2f+1 weight have
    /// reached, capped at its own tip and never older than a fixed number of entries back in its
    /// recent-blocks ring. Voters then rarely wait for, or refuse, a block they have not executed.
    /// [`Self::quorum_exec_block_starved_rounds`] picks when the rule applies. The default `false`
    /// keeps the previous behaviour: every header names this node's own newest executed block.
    /// Issue #1804.
    #[serde(default, skip_serializing_if = "is_default")]
    pub quorum_exec_block: bool,

    /// When the proposer applies the [`Self::quorum_exec_block`] rule. Only matters while that
    /// switch is `true`.
    ///
    /// The default `0` applies the rule to every header, as first designed. A value `K` above
    /// zero names this node's own execution tip by default and applies the rule only for `K`
    /// rounds after one of this node's headers was cancelled uncertified, doubling `K` while that
    /// keeps happening. With the rule on every header, the blocks parents name are earlier picks
    /// of the same rule rather than tips, so every proposer settles at the ring floor; `K` keeps
    /// tips in the parents. Issue #1804.
    #[serde(default, skip_serializing_if = "is_default")]
    pub quorum_exec_block_starved_rounds: u32,

    /// Keep vote requests queued behind the voter's per-author lock from each waiting out the
    /// execution wait in turn.
    ///
    /// When `true`, the voter refuses at once, with a recoverable answer, a request older than the
    /// newest it has received from the same author, and does its bounded wait for execution
    /// before it takes the per-author lock; under the lock it only checks execution, without
    /// waiting again. The default `false` keeps the previous behaviour: each request takes the
    /// lock in turn and waits for execution while holding it. Issue #1804.
    #[serde(default, skip_serializing_if = "is_default")]
    pub vote_queue_fix: bool,

    /// Advise the kernel that the execution database's memory map is read at random.
    ///
    /// When `true`, the node applies `madvise(MADV_RANDOM)` to every mapping of `mdbx.dat` after
    /// it opens the database, and again if the map's upper limit changes (growth inside the limit
    /// adds no mapping, so the first call covers it), so page faults stop pulling read-ahead into
    /// the page cache. The default `false` keeps the previous behaviour: only libmdbx's own advice
    /// applies, which covers the pages in use at open and at each growth step, not the whole
    /// map. Issue #1802.
    #[serde(default, skip_serializing_if = "is_default")]
    pub mdbx_random_advice: bool,

    /// Keep a sealed batch's transactions in the pool, marked, until they execute.
    ///
    /// When `true`, a batch that reaches the worker quorum marks its transactions as sealed
    /// instead of removing them from the pool and advancing their senders' nonces. The batch
    /// builder skips marked transactions, and the marks clear once the transactions execute or
    /// their batch is orphaned at an epoch close, so the pool and the RPC still see a transaction
    /// while its batch waits. The default `false` keeps the previous behaviour: the transactions
    /// leave the pool as soon as their batch is sealed. Issue #1807.
    ///
    /// Because in-flight transactions then stay in the pending sub-pool, a node with the switch on
    /// builds its worker pools with twice the configured pending limits, count and bytes (reth's
    /// defaults, 10,000 transactions and 20 MiB, unless `--txpool.pending-max-count` and
    /// `--txpool.pending-max-size` set them): the configured share stays for unsealed
    /// transactions and the in-flight ones get as much again. At the defaults the pending
    /// sub-pool can then hold 20 MiB more transactions than with the switch off, plus reth's
    /// bookkeeping for 10,000 more entries. With the switch off every pool limit is the configured
    /// one.
    #[serde(default, skip_serializing_if = "is_default")]
    pub sealed_tx_keep: bool,

    /// Drop a batch digest this node already holds instead of handling it again.
    ///
    /// When `true`, the worker skips a sealed batch whose digest the primary acked for this
    /// worker's latest seal of it and whose own-batches cache row is still present: it is neither
    /// sent for a vote nor reported again. A cache row alone does not count (it survives a failed
    /// report and a restart). The proposer still acks every report but drops a digest it already
    /// holds: pending, in the header being built, or in an own header that is proposed and
    /// neither committed nor re-queued. The default `false` keeps the previous behaviour: a
    /// repeated digest is reported again and can be proposed twice. Issue #1807.
    #[serde(default, skip_serializing_if = "is_default")]
    pub digest_dedupe: bool,

    /// Threads in a dedicated pool for recovering transaction signers.
    ///
    /// A value above zero runs signer recovery for queued outputs on its own pool of that many
    /// threads, so it no longer competes with the executing output's work on the global rayon
    /// pool. The default `0` keeps the previous behaviour: recovery runs on the global pool.
    /// Issue #1805.
    #[serde(default, skip_serializing_if = "is_default")]
    pub recovery_threads: u16,

    /// Keep the epoch's task manager tracking and reaping tasks while the node handles the epoch
    /// boundary.
    ///
    /// When `true`, the boundary handler awaits closing the epoch, writing its record and
    /// exporting it while the epoch's task manager keeps taking in newly spawned tasks and
    /// reaping finished ones. The default `false` keeps the previous behaviour: the task manager
    /// does neither until the boundary handler returns, and tasks spawned meanwhile can fail to
    /// be tracked. Issue #1806.
    #[serde(default, skip_serializing_if = "is_default")]
    pub epoch_close_task_drain: bool,

    /// Compare an epoch record that is already stored with this node's own closing block.
    ///
    /// When `true`, a node that reaches an epoch's close and finds the epoch's record already
    /// stored (the members that closed first wrote it) halts if the record's `final_state` is not
    /// this node's closing block, rather than enter the next epoch on a different state. A record
    /// this node builds itself pins its own closing block and is not checked. The check runs once,
    /// at the close: a node restarted after the halt does not repeat it. The default `false` keeps
    /// the previous behaviour: nothing at the boundary compares the stored record with this node's
    /// execution. Issue #1806.
    #[serde(default, skip_serializing_if = "is_default")]
    pub epoch_record_state_check: bool,

    /// How long the proposer waits for late parents before it proposes a header.
    ///
    /// When the proposer is ready to propose, it waits up to this long for the certificates of
    /// the committee members that were live in the round its last header built on and are
    /// missing from the new parents. Live means their certificate of that round reached this
    /// node, in time for that header or later, so an author whose certificate missed one header
    /// comes back once it arrives. The proposer never waits for its own certificate. A header
    /// forced by `max_header_delay` never waits. Neither does the header of the node that leads
    /// the next round: a quorum of next-round certificates arriving during its wait would move it
    /// past the round it leads, and that round would have no leader certificate to commit.
    ///
    /// The cost is up to this value per round on each node that waits, also under load: the
    /// header is then ready on the quorum message, so the wait arms on almost every round. One
    /// author whose certificate lands just inside the wait costs about this value every round.
    /// Keep it at or below the largest one-way delay between members. A value above
    /// `max_header_delay` is capped there, with a warning when each epoch's proposer starts.
    ///
    /// Written like the other durations (`150ms`). The default `0ms` keeps the previous
    /// behaviour: propose as soon as ready. Issue #1803.
    #[serde(with = "humantime_serde", default, skip_serializing_if = "is_default")]
    pub parent_grace: Duration,

    /// Refuse RPC transaction submissions while execution is this many consensus outputs behind.
    ///
    /// When above zero, the RPC submission guard answers "txpool is full" once execution trails
    /// the outputs forwarded to it by this many ([`tn_types::ExecutionBacklog::outputs_behind`]),
    /// and admits again once the backlog is below one less than this, or at zero when this is 1
    /// ([`tn_types::AdmissionGate`]). Only an active validator's own RPC is gated: an observer's
    /// submissions, the `--forward-txs` path and re-pooled transactions are not. The backlog
    /// cannot exceed what the channel to the engine and the engine's queue hold (64 + 8 + the
    /// output being executed), so a bound above that never closes the gate. The default `0`
    /// keeps the previous behaviour: no bound.
    #[serde(default, skip_serializing_if = "is_default")]
    pub rpc_admission_backlog_outputs: usize,

    /// Persist one consensus output's blocks while the next output executes.
    ///
    /// When `true`, the engine hands output N's persist to a background task and starts N+1 on
    /// N's in-memory state. Persists stay in order, consensus sees a block only once it is
    /// durable, and an output that closes an epoch is never overlapped with the next. The default
    /// `false` keeps the previous behaviour: each output executes, persists and commits before
    /// the next one starts.
    #[serde(default, skip_serializing_if = "is_default")]
    pub pipelined_persist: bool,
}

impl Parameters {
    fn default_header_num_of_batches_threshold() -> usize {
        5
    }

    fn default_max_header_num_of_batches() -> usize {
        tn_types::MAX_HEADER_NUM_OF_BATCHES
    }

    fn default_max_header_delay() -> Duration {
        Duration::from_millis(2500)
    }

    fn default_min_header_delay() -> Duration {
        Duration::from_millis(1000)
    }

    fn default_vote_timeout() -> Duration {
        Duration::from_secs(5)
    }

    /// The default gc depth for consensus.
    pub fn default_gc_depth() -> u32 {
        tn_types::MAX_GC_DEPTH
    }

    fn default_sync_retry_delay() -> Duration {
        Duration::from_millis(5_000)
    }

    fn default_sync_retry_nodes() -> usize {
        3
    }

    fn default_max_batch_delay() -> Duration {
        Duration::from_secs(1)
    }

    fn default_max_concurrent_requests() -> usize {
        500_000
    }

    fn default_batch_vote_timeout() -> Duration {
        Duration::from_secs(10)
    }

    fn default_parallel_fetch_request_delay_interval() -> Duration {
        Duration::from_secs(5)
    }

    /// Refuse non-public forwarding targets unless the operator opts in.
    fn default_allow_private_forward_targets() -> bool {
        false
    }
}

/// Admin server settings.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct NetworkAdminServerParameters {
    /// Primary network admin server port number
    pub primary_network_admin_server_port: u16,
    /// Worker network admin server base port number
    pub worker_network_admin_server_base_port: u16,
}

impl Default for NetworkAdminServerParameters {
    fn default() -> Self {
        let host = "127.0.0.1";
        Self {
            primary_network_admin_server_port: get_available_udp_port(host)
                .expect("udp port is available for primary"),
            worker_network_admin_server_base_port: get_available_udp_port(host)
                .expect("udp port is available for worker admin server"),
        }
    }
}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            header_num_of_batches_threshold: Parameters::default_header_num_of_batches_threshold(),
            max_header_num_of_batches: Parameters::default_max_header_num_of_batches(),
            max_header_delay: Parameters::default_max_header_delay(),
            min_header_delay: Parameters::default_min_header_delay(),
            vote_timeout: Parameters::default_vote_timeout(),
            gc_depth: Parameters::default_gc_depth(),
            sync_retry_delay: Parameters::default_sync_retry_delay(),
            sync_retry_nodes: Parameters::default_sync_retry_nodes(),
            max_batch_delay: Parameters::default_max_batch_delay(),
            max_concurrent_requests: Parameters::default_max_concurrent_requests(),
            batch_vote_timeout: Parameters::default_batch_vote_timeout(),
            // The test-fixture path. Production reads a parameters file, and a file without the
            // key fails to deserialize, so this value never reaches a production node. The
            // governance safe is the address an unset `Option` historically resolved to inside
            // `set_basefee_address`, so fixtures keep their old effective value.
            basefee_address: GOVERNANCE_SAFE_ADDRESS,
            parallel_fetch_request_delay_interval:
                Parameters::default_parallel_fetch_request_delay_interval(),
            allow_private_forward_targets: Parameters::default_allow_private_forward_targets(),
            quorum_exec_block: false,
            quorum_exec_block_starved_rounds: 0,
            vote_queue_fix: false,
            mdbx_random_advice: false,
            sealed_tx_keep: false,
            digest_dedupe: false,
            recovery_threads: 0,
            epoch_close_task_drain: false,
            epoch_record_state_check: false,
            parent_grace: Duration::ZERO,
            rpc_admission_backlog_outputs: 0,
            pipelined_persist: false,
        }
    }
}

impl Parameters {
    /// Validate the protocol **ceilings** every node (production or test fixture) must honor.
    ///
    /// `gc_depth` and `max_header_num_of_batches` together bound how many unique batches a single
    /// committed [`ConsensusOutput`](tn_types::ConsensusOutput) can reference. The consensus-pack
    /// reader derives its reconstruction bound from [`tn_types::MAX_GC_DEPTH`] and
    /// [`tn_types::MAX_HEADER_NUM_OF_BATCHES`], so a node configured above those ceilings could
    /// commit an output that no node (including itself, on restart) can later reconstruct.
    /// Rejecting such a configuration keeps the producing and reconstructing halves in
    /// agreement.
    ///
    /// These are safe to enforce everywhere, so this runs for every constructor. The lower
    /// **operational floors** are enforced separately by
    /// [`Parameters::validate_operational_floors`] at the production entry points only, so DAG
    /// test fixtures may still use small `gc_depth` values.
    pub fn validate(&self) -> eyre::Result<()> {
        eyre::ensure!(
            self.gc_depth <= tn_types::MAX_GC_DEPTH,
            "gc_depth {} exceeds the protocol maximum {}",
            self.gc_depth,
            tn_types::MAX_GC_DEPTH,
        );
        eyre::ensure!(
            self.max_header_num_of_batches <= tn_types::MAX_HEADER_NUM_OF_BATCHES,
            "max_header_num_of_batches {} exceeds the protocol maximum {}",
            self.max_header_num_of_batches,
            tn_types::MAX_HEADER_NUM_OF_BATCHES,
        );
        Ok(())
    }

    /// Validate the operational **floors** and cross-field coupling: the values below which a node
    /// degrades silently instead of erroring.
    ///
    /// Unlike the ceilings in [`Parameters::validate`], these are enforced only at the production
    /// startup entry points (`ConsensusConfig::new` / `new_for_epoch`), never in the shared
    /// test-fixture constructor, so DAG tests that deliberately drive a small `gc_depth` keep
    /// working (see issue #954, "retain test-only overrides for DAG tests requiring small values").
    ///
    /// - `gc_depth` must exceed [`tn_types::GC_ACTIVITY_BUFFER`]. The consensus network handler
    ///   computes its activity window as `gc_depth - GC_ACTIVITY_BUFFER`; a `gc_depth` at or below
    ///   the buffer collapses that window to zero and wedges the node inactive during normal
    ///   operation. Coupling the floor to the same constant the handler subtracts keeps the two
    ///   from drifting apart.
    /// - `gc_depth + gc_activity_window(gc_depth) + RETENTION_SLACK` must not exceed
    ///   [`tn_types::ROUNDS_TO_KEEP`]. Peers prune certificates below their newest written round
    ///   minus `ROUNDS_TO_KEEP`, and a validator that restarts consensus up to the activity window
    ///   behind (a rejoin, a restart) rebuilds its DAG from the `gc_depth` rounds below that point,
    ///   which it can fetch only while some peer still holds them. With `ROUNDS_TO_KEEP` 128 any
    ///   `gc_depth` above 64 is refused here (100 <= 128 at `gc_depth` 50); the protocol ceiling
    ///   [`tn_types::MAX_GC_DEPTH`] (50) refuses those first today, so this check binds once that
    ///   ceiling is raised.
    /// - `max_header_num_of_batches` must be at least one, otherwise the proposer caps every header
    ///   at zero batch digests: it drains no transactions while its digest queue grows without
    ///   bound.
    /// - `header_num_of_batches_threshold` must be at least one and must not exceed
    ///   `max_header_num_of_batches`. The proposer seals a header once it holds `threshold` digests
    ///   but includes at most `max_header_num_of_batches` of them, so a zero threshold seals empty
    ///   headers on the fast path and a threshold above the max makes the two conditions mutually
    ///   inconsistent.
    /// - `min_header_delay` must not exceed `max_header_delay`. The proposer treats the minimum as
    ///   an early-proposal point inside the maximum's window; with the two inverted the maximum
    ///   expires first and the configured minimum never takes effect.
    ///
    /// `basefee_address` needs no floor here: the field is a required key with no serde default,
    /// so a `parameters.yaml` that omits it already fails to deserialize, before any constructor
    /// runs, with an error that names the field.
    pub fn validate_operational_floors(&self) -> eyre::Result<()> {
        eyre::ensure!(
            self.gc_depth > tn_types::GC_ACTIVITY_BUFFER,
            "gc_depth {} must exceed the activity buffer {} or the node cannot stay active",
            self.gc_depth,
            tn_types::GC_ACTIVITY_BUFFER,
        );
        // the window is `gc_depth - GC_ACTIVITY_BUFFER` here (the check above), so the largest
        // gc_depth that fits is `(ROUNDS_TO_KEEP + GC_ACTIVITY_BUFFER - RETENTION_SLACK) / 2`
        let window = tn_types::gc_activity_window(self.gc_depth);
        let retention =
            self.gc_depth.saturating_add(window).saturating_add(tn_types::RETENTION_SLACK);
        eyre::ensure!(
            retention <= tn_types::ROUNDS_TO_KEEP,
            "gc_depth {} + activity window {} + retention slack {} = {} exceeds ROUNDS_TO_KEEP {}: \
             peers prune the certificate rounds a rejoining validator rebuilds its DAG from, so a \
             gc_depth above {} refuses to start",
            self.gc_depth,
            window,
            tn_types::RETENTION_SLACK,
            retention,
            tn_types::ROUNDS_TO_KEEP,
            (tn_types::ROUNDS_TO_KEEP + tn_types::GC_ACTIVITY_BUFFER - tn_types::RETENTION_SLACK) / 2,
        );
        eyre::ensure!(
            self.max_header_num_of_batches >= 1,
            "max_header_num_of_batches must be at least 1, got {}",
            self.max_header_num_of_batches,
        );
        eyre::ensure!(
            self.header_num_of_batches_threshold >= 1,
            "header_num_of_batches_threshold must be at least 1, got {}",
            self.header_num_of_batches_threshold,
        );
        eyre::ensure!(
            self.header_num_of_batches_threshold <= self.max_header_num_of_batches,
            "header_num_of_batches_threshold {} must not exceed max_header_num_of_batches {}",
            self.header_num_of_batches_threshold,
            self.max_header_num_of_batches,
        );
        eyre::ensure!(
            self.min_header_delay <= self.max_header_delay,
            "min_header_delay {:?} must not exceed max_header_delay {:?}",
            self.min_header_delay,
            self.max_header_delay,
        );
        Ok(())
    }

    /// Tracing::info! for [Self].
    pub fn tracing(&self) {
        info!("Header number of batches threshold set to {}", self.header_num_of_batches_threshold);
        info!("Header max number of batches set to {}", self.max_header_num_of_batches);
        info!("Max header delay set to {} ms", self.max_header_delay.as_millis());
        info!("Min header delay set to {} ms", self.min_header_delay.as_millis());
        info!("Vote timeout set to {} ms", self.vote_timeout.as_millis());
        info!("Garbage collection depth set to {} rounds", self.gc_depth);
        info!("Sync retry delay set to {} ms", self.sync_retry_delay.as_millis());
        info!("Sync retry nodes set to {} nodes", self.sync_retry_nodes);
        info!("Max batch delay set to {} ms", self.max_batch_delay.as_millis());
        info!("Max concurrent requests set to {}", self.max_concurrent_requests);
        // Consensus-critical, and the only parameter here whose value enters the state root. Two
        // operators debugging a state-root split have no other way to compare this value without
        // reading each other's files.
        info!(
            "Basefee address set to {} (consensus-critical: all nodes must agree)",
            self.basefee_address
        );
    }

    /// The runtime switches in these parameters, each with its `parameters.yaml` key, in a fixed
    /// order.
    ///
    /// A switch turns one behaviour change on or off per benchmark arm, and its default is the
    /// behaviour from before the switch existed. A mistyped key is ignored without an error, like
    /// any unknown key, so the node logs this list once at start and exports it as a gauge: that
    /// shows which arm a node actually runs. A switch at its default is left out when the
    /// parameters are written, so a key absent from a node's file means the default.
    ///
    /// Every switch field must be listed here. A test fails while one is missing.
    pub fn switches(&self) -> Vec<(&'static str, SwitchValue)> {
        vec![
            ("quorum_exec_block", SwitchValue::Bool(self.quorum_exec_block)),
            (
                "quorum_exec_block_starved_rounds",
                SwitchValue::Count(u64::from(self.quorum_exec_block_starved_rounds)),
            ),
            ("vote_queue_fix", SwitchValue::Bool(self.vote_queue_fix)),
            ("mdbx_random_advice", SwitchValue::Bool(self.mdbx_random_advice)),
            ("sealed_tx_keep", SwitchValue::Bool(self.sealed_tx_keep)),
            ("digest_dedupe", SwitchValue::Bool(self.digest_dedupe)),
            ("recovery_threads", SwitchValue::Count(u64::from(self.recovery_threads))),
            ("epoch_close_task_drain", SwitchValue::Bool(self.epoch_close_task_drain)),
            ("epoch_record_state_check", SwitchValue::Bool(self.epoch_record_state_check)),
            ("parent_grace", SwitchValue::Duration(self.parent_grace)),
            (
                "rpc_admission_backlog_outputs",
                SwitchValue::Count(
                    u64::try_from(self.rpc_admission_backlog_outputs).unwrap_or(u64::MAX),
                ),
            ),
            ("pipelined_persist", SwitchValue::Bool(self.pipelined_persist)),
        ]
    }
}

/// The value of one runtime switch, as listed by [`Parameters::switches`].
///
/// [`Display`](std::fmt::Display) renders the value the way `parameters.yaml` holds it (`true`,
/// `3`, `150ms`), so a logged value can be written back into the file unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SwitchValue {
    /// An on/off switch.
    Bool(bool),
    /// A count, such as a number of threads or outputs.
    Count(u64),
    /// A duration, written in `humantime` form.
    Duration(Duration),
}

impl SwitchValue {
    /// The value as a gauge sample: 0 or 1 for a boolean, the count itself, and a duration in
    /// milliseconds.
    pub fn gauge(&self) -> f64 {
        match self {
            Self::Bool(on) => f64::from(u8::from(*on)),
            Self::Count(count) => *count as f64,
            Self::Duration(duration) => duration.as_secs_f64() * 1_000.0,
        }
    }
}

impl std::fmt::Display for SwitchValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bool(on) => write!(f, "{on}"),
            Self::Count(count) => write!(f, "{count}"),
            // the rendering `humantime_serde` writes for a duration field
            Self::Duration(duration) => {
                write!(f, "{}", humantime_serde::re::humantime::format_duration(*duration))
            }
        }
    }
}

/// Whether `value` is its type's default. A runtime switch at its default is left out when
/// [`Parameters`] are written, so a written `parameters.yaml` matches one from before the switch
/// existed.
fn is_default<T: Default + PartialEq>(value: &T) -> bool {
    *value == T::default()
}

#[cfg(test)]
mod test {
    use super::Parameters;
    use crate::{
        Config, ConfigFmt, ConfigTrait as _, CONSENSUS_REGISTRY_JSON, GOVERNANCE_SAFE_ADDRESS,
    };
    use std::time::Duration;
    use tn_types::{
        address, Address, Bytes, Committee, Genesis, MAINNET_COMMITTEE, MAINNET_GENESIS,
        MAINNET_PARAMETERS, TESTNET_PARAMETERS,
    };

    /// The fixed mainnet deployment address of the `ConsensusRegistry` system contract.
    ///
    /// Mirrors `tn_reth::system_calls::CONSENSUS_REGISTRY_ADDRESS`; tn-config sits below
    /// tn-reth in the dependency graph, so the address is restated here for the compiled-in
    /// mainnet genesis gate.
    const CONSENSUS_REGISTRY_ADDRESS: Address =
        address!("07E17e17E17e17E17e17E17E17E17e17e17E17e1");

    /// How to regenerate the committed mainnet chain-config placeholders when a gate below
    /// fails (issue #1063).
    const REGENERATION_LEVER: &str =
        "regenerate chain-configs/mainnet with `cargo test -p telcoin-network-cli \
         regenerate_mainnet_chain_configs -- --ignored`";

    /// The embedded mainnet chain-config files must load through the same deserialization
    /// paths `--chain mainnet` uses.
    ///
    /// `Config::load_mainnet` parses `MAINNET_GENESIS`/`MAINNET_PARAMETERS` with
    /// `serde_yaml::from_str` and materializes `MAINNET_COMMITTEE` to
    /// `<datadir>/genesis/committee.yaml`, which the epoch manager later reads back via
    /// `Config::load_from_path::<Committee>` (crates/node/src/manager/node.rs). This gate
    /// drives all three files through exactly those paths, so a schema-stale committed file
    /// fails here instead of at node startup.
    #[test]
    fn mainnet_chain_configs_load_via_the_node_paths() {
        let _genesis: Genesis = serde_yaml::from_str(MAINNET_GENESIS)
            .expect("embedded mainnet genesis.yaml must deserialize like Config::load_mainnet");
        let _parameters: Parameters = serde_yaml::from_str(MAINNET_PARAMETERS)
            .expect("embedded mainnet parameters.yaml must deserialize like Config::load_mainnet");

        // mirror the node's real committee read: file on disk -> Config::load_from_path
        let dir = tempfile::tempdir().expect("tempdir for committee round-trip");
        let committee_path = dir.path().join("committee.yaml");
        std::fs::write(&committee_path, MAINNET_COMMITTEE)
            .expect("write embedded mainnet committee.yaml to disk");
        let committee: Committee = Config::load_from_path(&committee_path, ConfigFmt::YAML)
            .unwrap_or_else(|e| {
                panic!(
                    "embedded mainnet committee.yaml must deserialize as tn_types::Committee \
                     via Config::load_from_path (the node's read path); schema-stale file \
                     (issue #1063) - {REGENERATION_LEVER}: {e}"
                )
            });
        assert_eq!(
            committee.size(),
            4,
            "mainnet placeholder committee must seat the 4 placeholder validators"
        );
    }

    /// The mainnet genesis `ConsensusRegistry` account must carry the CURRENT tn-contracts
    /// runtime bytecode.
    ///
    /// The genesis ceremony (`RethEnv::create_consensus_registry_genesis_accounts`) splices
    /// the tmp-chain DEPLOYED registry code into the registry account: the artifact's
    /// `deployedBytecode.object` with the constructor-patched immutables at the artifact's
    /// `immutableReferences` sites (issue #1278). The node's system calls unconditionally
    /// speak that artifact's ABI. A stale committed code blob means a mainnet node can
    /// neither start epoch 0 (`getCommitteeBlsPubkeys`) nor close an epoch
    /// (`getValidatorsInfo`/`getNextCommitteeSize`) - issue #1063.
    #[test]
    fn mainnet_genesis_registry_code_matches_current_artifact() {
        let genesis: Genesis = serde_yaml::from_str(MAINNET_GENESIS)
            .expect("embedded mainnet genesis.yaml must deserialize");

        // same extraction the ceremony performs (deployedBytecode.object, hex-decoded)
        let artifact: serde_json::Value = serde_json::from_str(CONSENSUS_REGISTRY_JSON)
            .expect("embedded ConsensusRegistry artifact json must parse");
        let expected: Bytes = artifact
            .pointer("/deployedBytecode/object")
            .and_then(serde_json::Value::as_str)
            .expect("ConsensusRegistry artifact must contain deployedBytecode.object")
            .parse()
            .expect("deployedBytecode.object must be valid hex");

        let actual = genesis
            .alloc
            .get(&CONSENSUS_REGISTRY_ADDRESS)
            .and_then(|account| account.code.clone())
            .expect("mainnet genesis must allocate code for the ConsensusRegistry account");

        // The genesis ceremony splices the DEPLOYED tmp-chain code (issue #1278): identical to
        // the artifact's compile-time code except at the immutable sites, where forge ships
        // zeros and the constructor patched real values. Compare accordingly: byte-equal
        // outside `deployedBytecode.immutableReferences`, non-zero inside.
        let sites: Vec<(usize, usize)> = artifact
            .pointer("/deployedBytecode/immutableReferences")
            .and_then(serde_json::Value::as_object)
            .map(|refs| {
                refs.values()
                    .filter_map(serde_json::Value::as_array)
                    .flatten()
                    .filter_map(|site| {
                        site.get("start")
                            .and_then(serde_json::Value::as_u64)
                            .zip(site.get("length").and_then(serde_json::Value::as_u64))
                            .map(|(start, length)| (start as usize, length as usize))
                    })
                    .collect()
            })
            .unwrap_or_default();
        // Fail loud on artifact schema drift: with zero parsed sites the comparison below
        // degrades to byte-equality, which a still-zeroed genesis satisfies (issue #1278).
        assert!(
            !sites.is_empty(),
            "registry artifact must list immutable sites (the Solady EIP712 cache); a missing \
             or renamed immutableReferences key would make this gate vacuous"
        );
        let inside_site =
            |i: usize| sites.iter().any(|(start, length)| i >= *start && i < start + length);
        let matches_outside_immutables = actual.len() == expected.len()
            && actual
                .iter()
                .zip(expected.iter())
                .enumerate()
                .all(|(i, (spliced, compiled))| inside_site(i) || spliced == compiled);
        assert!(
            matches_outside_immutables,
            "chain-configs/mainnet/genesis.yaml seeds the ConsensusRegistry with {} bytes of \
             runtime code but the current tn-contracts artifact deploys {} bytes; the node's \
             system calls speak the current ABI, so a mainnet node cannot start or close an \
             epoch (issue #1063) - {REGENERATION_LEVER}",
            actual.len(),
            expected.len(),
        );
        assert!(
            sites.iter().all(|(start, length)| {
                actual
                    .get(*start..start + length)
                    .is_some_and(|segment| segment.iter().any(|byte| *byte != 0))
            }),
            "chain-configs/mainnet/genesis.yaml ships an ALL-ZERO EIP712 immutable in the \
             ConsensusRegistry code: the ceremony spliced compile-time bytecode instead of the \
             deployed tmp-chain code (issue #1278) - {REGENERATION_LEVER}"
        );
    }

    #[test]
    fn default_parameters_are_within_protocol_ceilings() {
        Parameters::default().validate().expect("default parameters must be within ceilings");
    }

    #[test]
    fn default_parameters_pass_operational_floors() {
        Parameters::default()
            .validate_operational_floors()
            .expect("default parameters must satisfy the operational floors");
    }

    #[test]
    fn parameters_reject_gc_depth_over_ceiling() {
        let params = Parameters { gc_depth: tn_types::MAX_GC_DEPTH + 1, ..Default::default() };
        assert!(params.validate().is_err(), "gc_depth over the protocol ceiling must be rejected");
    }

    #[test]
    fn parameters_reject_max_header_num_of_batches_over_ceiling() {
        let params = Parameters {
            max_header_num_of_batches: tn_types::MAX_HEADER_NUM_OF_BATCHES + 1,
            ..Default::default()
        };
        assert!(
            params.validate().is_err(),
            "max_header_num_of_batches over the protocol ceiling must be rejected"
        );
    }

    #[test]
    fn parameters_reject_gc_depth_at_or_below_activity_buffer() {
        let at_buffer = Parameters { gc_depth: tn_types::GC_ACTIVITY_BUFFER, ..Default::default() };
        assert!(
            at_buffer.validate_operational_floors().is_err(),
            "gc_depth equal to the activity buffer collapses the activity window and must be rejected"
        );
        let below_buffer =
            Parameters { gc_depth: tn_types::GC_ACTIVITY_BUFFER - 1, ..Default::default() };
        assert!(
            below_buffer.validate_operational_floors().is_err(),
            "gc_depth below the activity buffer must be rejected"
        );
    }

    #[test]
    fn parameters_accept_gc_depth_one_above_activity_buffer() {
        let params =
            Parameters { gc_depth: tn_types::GC_ACTIVITY_BUFFER + 1, ..Default::default() };
        assert!(
            params.validate_operational_floors().is_ok(),
            "the smallest gc_depth leaving a positive activity window must be accepted"
        );
    }

    #[test]
    fn parameters_reject_zero_max_header_num_of_batches() {
        let params = Parameters {
            max_header_num_of_batches: 0,
            header_num_of_batches_threshold: 0,
            ..Default::default()
        };
        assert!(
            params.validate_operational_floors().is_err(),
            "a zero max_header_num_of_batches drains no digests and must be rejected"
        );
    }

    #[test]
    fn parameters_reject_zero_header_num_of_batches_threshold() {
        let params = Parameters { header_num_of_batches_threshold: 0, ..Default::default() };
        assert!(
            params.validate_operational_floors().is_err(),
            "a zero header_num_of_batches_threshold seals empty headers and must be rejected"
        );
    }

    #[test]
    fn parameters_reject_threshold_above_max_header_num_of_batches() {
        let params = Parameters {
            max_header_num_of_batches: 2,
            header_num_of_batches_threshold: 3,
            ..Default::default()
        };
        assert!(
            params.validate_operational_floors().is_err(),
            "a threshold above max_header_num_of_batches is mutually inconsistent and must be rejected"
        );
    }

    #[test]
    fn parameters_accept_minimal_valid_batch_bounds() {
        let params = Parameters {
            max_header_num_of_batches: 1,
            header_num_of_batches_threshold: 1,
            ..Default::default()
        };
        assert!(
            params.validate_operational_floors().is_ok(),
            "threshold == max == 1 is the minimal consistent batch configuration and must be accepted"
        );
    }

    #[test]
    fn parameters_reject_min_header_delay_above_max_header_delay() {
        let params = Parameters {
            min_header_delay: Duration::from_millis(1_001),
            max_header_delay: Duration::from_millis(1_000),
            ..Default::default()
        };
        let err = params
            .validate_operational_floors()
            .expect_err("a min_header_delay above max_header_delay must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("min_header_delay") && msg.contains("max_header_delay"),
            "the error must name both fields: {msg}"
        );
    }

    #[test]
    fn parameters_accept_min_header_delay_equal_to_max_header_delay() {
        let params = Parameters {
            min_header_delay: Duration::from_secs(1),
            max_header_delay: Duration::from_secs(1),
            ..Default::default()
        };
        assert!(
            params.validate_operational_floors().is_ok(),
            "min_header_delay == max_header_delay (the mainnet preset) must be accepted"
        );
    }

    #[test]
    fn default_vote_timeout_is_five_seconds() {
        assert_eq!(Parameters::default().vote_timeout, Duration::from_secs(5));
    }

    #[test]
    fn vote_timeout_parses_from_humantime_yaml() {
        let yaml = format!("basefee_address: \"{GOVERNANCE_SAFE_ADDRESS}\"\nvote_timeout: \"7s\"");
        let params: Parameters =
            serde_yaml::from_str(&yaml).expect("a humantime vote_timeout must parse");
        assert_eq!(params.vote_timeout, Duration::from_secs(7));
    }

    /// Parameters files written before `vote_timeout` existed omit the key and must still load.
    #[test]
    fn absent_vote_timeout_key_defaults_to_five_seconds() {
        let yaml = format!("basefee_address: \"{GOVERNANCE_SAFE_ADDRESS}\"");
        let params: Parameters =
            serde_yaml::from_str(&yaml).expect("a parameters file without vote_timeout must parse");
        assert_eq!(params.vote_timeout, Duration::from_secs(5));
    }

    /// A parameters file that omits `basefee_address` must fail to deserialize.
    ///
    /// Every other key in the file carries a serde default, so this partial file isolates the one
    /// required key. Without this pin, a `#[serde(default)]` reintroduced on the field would fill
    /// a fallback on a missing key in silence, which is the failure mode of issue #1113.
    #[test]
    fn absent_basefee_address_key_fails_to_deserialize() {
        let err = serde_yaml::from_str::<Parameters>("gc_depth: 50")
            .expect_err("a parameters file without basefee_address must not parse");
        assert!(
            err.to_string().contains("basefee_address"),
            "the error must name the field an operator has to fix: {err}"
        );
    }

    /// A parameters file with an explicit `basefee_address: null` must also fail to deserialize.
    ///
    /// This is the upgrade shape, not a hypothetical: before this change `Parameters::default()`
    /// held `None`, and `load_from_path_or_default` writes the default back to disk when the file
    /// is absent, so a datadir created under the old code can hold `basefee_address: null`. That
    /// file takes serde's type-error path, not the missing-field path the previous test pins, so
    /// it needs its own pin.
    #[test]
    fn null_basefee_address_key_fails_to_deserialize() {
        let err = serde_yaml::from_str::<Parameters>("gc_depth: 50\nbasefee_address: null")
            .expect_err("a parameters file with a null basefee_address must not parse");
        assert!(
            err.to_string().contains("basefee_address"),
            "the error must name the field an operator has to fix: {err}"
        );
    }

    /// Both shipped presets must keep an explicit, correct address.
    ///
    /// The addresses are pinned by value. They are consensus-critical, so a change to either is a
    /// hard fork for that chain and updating this test is the intended cost of making one.
    #[test]
    fn shipped_chain_presets_pin_their_basefee_address() {
        let mainnet: Parameters =
            serde_yaml::from_str(MAINNET_PARAMETERS).expect("mainnet parameters parse");
        let adiri: Parameters =
            serde_yaml::from_str(TESTNET_PARAMETERS).expect("adiri parameters parse");
        assert_eq!(
            mainnet.basefee_address,
            address!("0x9999999999999999999999999999999999999999"),
            "mainnet's committed basefee address changed"
        );
        assert_eq!(
            adiri.basefee_address, GOVERNANCE_SAFE_ADDRESS,
            "adiri's committed basefee address changed"
        );
        mainnet
            .validate_operational_floors()
            .expect("the mainnet preset must satisfy the operational floors");
        adiri
            .validate_operational_floors()
            .expect("the adiri preset must satisfy the operational floors");
    }

    /// The certificate-retention floor accepts the devnet `gc_depth` (50 + 40 + 10 = 100 <= 128)
    /// and the largest that fits (64 + 54 + 10 = 128).
    #[test]
    fn parameters_accept_gc_depth_within_certificate_retention() {
        for gc_depth in [50, 64] {
            let params = Parameters { gc_depth, ..Default::default() };
            assert!(
                params.validate_operational_floors().is_ok(),
                "gc_depth {gc_depth} fits ROUNDS_TO_KEEP and must be accepted"
            );
        }
    }

    /// The certificate-retention floor refuses a `gc_depth` whose rebuild window peers may have
    /// pruned, with a message naming the numbers and the rule.
    #[test]
    fn parameters_reject_gc_depth_beyond_certificate_retention() {
        let params = Parameters { gc_depth: 65, ..Default::default() };
        assert!(
            params.validate_operational_floors().is_err(),
            "gc_depth 65 needs 130 rounds of retention and must be rejected"
        );
        let params = Parameters { gc_depth: 79, ..Default::default() };
        let err = params
            .validate_operational_floors()
            .expect_err("gc_depth 79 needs 158 rounds of retention and must be rejected");
        assert_eq!(
            err.to_string(),
            "gc_depth 79 + activity window 69 + retention slack 10 = 158 exceeds ROUNDS_TO_KEEP \
             128: peers prune the certificate rounds a rejoining validator rebuilds its DAG from, \
             so a gc_depth above 64 refuses to start"
        );
    }

    // ---- bench6 bench6-switches ----

    /// The parameters keys that are not runtime switches.
    ///
    /// A new `Parameters` field is either a switch, listed by [`Parameters::switches`], or a
    /// plain parameter listed here. [`switches_list_every_switch_field`] fails until it is one or
    /// the other.
    const NON_SWITCH_KEYS: &[&str] = &[
        "header_num_of_batches_threshold",
        "max_header_num_of_batches",
        "max_header_delay",
        "min_header_delay",
        "vote_timeout",
        "gc_depth",
        "sync_retry_delay",
        "sync_retry_nodes",
        "max_batch_delay",
        "max_concurrent_requests",
        "batch_vote_timeout",
        "basefee_address",
        "parallel_fetch_request_delay_interval",
        "allow_private_forward_targets",
    ];

    /// Parses a parameters file that holds the one required key plus the `extra` lines.
    fn parse_with(extra: &str) -> Parameters {
        let yaml = format!("basefee_address: \"{GOVERNANCE_SAFE_ADDRESS}\"\n{extra}");
        serde_yaml::from_str(&yaml).unwrap_or_else(|e| panic!("{extra:?} must parse: {e}"))
    }

    /// Renders a scalar YAML value the way it is written in `parameters.yaml`.
    fn yaml_scalar(value: &serde_yaml::Value) -> String {
        match value {
            serde_yaml::Value::Bool(on) => on.to_string(),
            serde_yaml::Value::Number(number) => number.to_string(),
            serde_yaml::Value::String(text) => text.clone(),
            other => panic!("a switch must serialize as a yaml scalar, got {other:?}"),
        }
    }

    /// Parameters with every switch set away from its default: booleans on, counts 3, durations
    /// 150 ms.
    fn every_switch_flipped() -> Parameters {
        let mut yaml = serde_yaml::to_value(Parameters::default()).expect("parameters serialize");
        let mapping = yaml.as_mapping_mut().expect("parameters serialize as a mapping");
        for (name, value) in Parameters::default().switches() {
            let flipped = match value {
                super::SwitchValue::Bool(_) => serde_yaml::Value::Bool(true),
                super::SwitchValue::Count(_) => serde_yaml::Value::Number(3.into()),
                super::SwitchValue::Duration(_) => serde_yaml::Value::String("150ms".into()),
            };
            mapping.insert(name.into(), flipped);
        }
        serde_yaml::from_value(yaml).expect("flipped switches parse")
    }

    /// A deserializer that records the field names a struct's `Deserialize` impl asks for, then
    /// stops. It sees every field, including those a serializer leaves out.
    struct FieldNames<'a>(&'a mut Vec<&'static str>);

    impl<'de> serde::Deserializer<'de> for FieldNames<'_> {
        type Error = serde::de::value::Error;

        fn deserialize_any<V: serde::de::Visitor<'de>>(
            self,
            _: V,
        ) -> Result<V::Value, Self::Error> {
            Err(serde::de::Error::custom("only a struct's field names are collected"))
        }

        fn deserialize_struct<V: serde::de::Visitor<'de>>(
            self,
            _: &'static str,
            fields: &'static [&'static str],
            _: V,
        ) -> Result<V::Value, Self::Error> {
            self.0.extend_from_slice(fields);
            Err(serde::de::Error::custom("field names collected"))
        }

        serde::forward_to_deserialize_any! {
            bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf
            option unit unit_struct newtype_struct seq tuple tuple_struct map enum identifier
            ignored_any
        }
    }

    /// The switch list names every field that is not a plain parameter, once, by its YAML key,
    /// and a default `Parameters` writes only the plain parameters.
    #[test]
    fn switches_list_every_switch_field() {
        let mut names = Vec::new();
        <Parameters as serde::Deserialize>::deserialize(FieldNames(&mut names))
            .expect_err("the probe stops once it has the field names");
        assert!(names.len() > NON_SWITCH_KEYS.len(), "field names must be collected: {names:?}");
        let fields: std::collections::BTreeSet<&str> =
            names.into_iter().filter(|name| !NON_SWITCH_KEYS.contains(name)).collect();
        let switches = Parameters::default().switches();
        let listed: std::collections::BTreeSet<&str> =
            switches.iter().map(|(name, _)| *name).collect();
        assert_eq!(listed.len(), switches.len(), "a switch is listed twice: {switches:?}");
        assert_eq!(
            listed, fields,
            "every Parameters field must be listed by Parameters::switches or in NON_SWITCH_KEYS"
        );

        // the genesis ceremony writes `Parameters::default()`; it must match the pre-switch file
        let yaml = serde_yaml::to_value(Parameters::default()).expect("parameters serialize");
        let written: std::collections::BTreeSet<String> = yaml
            .as_mapping()
            .expect("parameters serialize as a mapping")
            .iter()
            .map(|(key, _)| key.as_str().expect("parameter keys are strings").to_string())
            .collect();
        let plain: std::collections::BTreeSet<String> =
            NON_SWITCH_KEYS.iter().map(|key| key.to_string()).collect();
        assert_eq!(written, plain, "a default switch must not be written");
    }

    /// Each listed value is the value of its own field. Away from its default it renders as the
    /// YAML serializer writes it; at its default it is not written, and its rendering parses back
    /// to the default.
    #[test]
    fn switch_values_render_as_their_yaml() {
        let flipped = every_switch_flipped();
        let yaml = serde_yaml::to_value(&flipped).expect("parameters serialize");
        for (name, value) in flipped.switches() {
            assert_eq!(value.to_string(), yaml_scalar(&yaml[name]), "switch {name}");
        }
        let yaml = serde_yaml::to_value(Parameters::default()).expect("parameters serialize");
        for (name, value) in Parameters::default().switches() {
            assert_eq!(yaml.get(name), None, "default switch {name} is written");
            assert_eq!(parse_with(&format!("{name}: {value}")), parse_with(""), "switch {name}");
        }
        let defaults = Parameters::default().switches();
        for ((name, default), (_, flipped)) in
            defaults.iter().zip(every_switch_flipped().switches())
        {
            assert_ne!(*default, flipped, "switch {name} must read its own field");
        }
    }

    /// Display gives the YAML form and the gauge a number for every kind of switch value.
    #[test]
    fn switch_value_display_and_gauge() {
        #[derive(serde::Serialize)]
        struct Humantime(#[serde(with = "humantime_serde")] Duration);
        let cases = [
            (super::SwitchValue::Bool(false), "false", 0.0),
            (super::SwitchValue::Bool(true), "true", 1.0),
            (super::SwitchValue::Count(0), "0", 0.0),
            (super::SwitchValue::Count(3), "3", 3.0),
            (super::SwitchValue::Duration(Duration::ZERO), "0s", 0.0),
            (super::SwitchValue::Duration(Duration::from_millis(150)), "150ms", 150.0),
            (super::SwitchValue::Duration(Duration::from_millis(1_500)), "1s 500ms", 1_500.0),
        ];
        for (value, yaml, gauge) in cases {
            assert_eq!(value.to_string(), yaml, "{value:?}");
            assert_eq!(value.gauge(), gauge, "{value:?}");
            if let super::SwitchValue::Duration(duration) = value {
                let written =
                    serde_yaml::to_value(Humantime(duration)).expect("duration serializes");
                assert_eq!(yaml_scalar(&written), yaml, "humantime_serde writes {duration:?}");
            }
        }
    }

    /// A file without any switch key, such as a bench-5 `parameters.yaml`, loads every switch at
    /// its default, and the struct's `Default` agrees with the serde defaults.
    #[test]
    fn absent_switch_keys_load_the_defaults() {
        assert_eq!(parse_with("").switches(), Parameters::default().switches());
        assert_eq!(parse_with("gc_depth: 40").switches(), Parameters::default().switches());
    }

    /// Re-serializing a file that predates the switches keeps every key it held at the same
    /// value, adds no switch key, and the result parses back to the same parameters.
    ///
    /// Strings compare without case: an address is written back in lowercase hex whatever its
    /// checksum case in the file, which is the same address.
    #[test]
    fn reserializing_keeps_the_existing_keys() {
        let lowercase = |value: &serde_yaml::Value| match value {
            serde_yaml::Value::String(text) => serde_yaml::Value::String(text.to_lowercase()),
            other => other.clone(),
        };
        let bench5 = format!(
            "basefee_address: \"{GOVERNANCE_SAFE_ADDRESS}\"\nmax_header_delay: 300ms\n\
             min_header_delay: 250ms\nvote_timeout: 5s\ngc_depth: 50\n\
             header_num_of_batches_threshold: 1\nmax_header_num_of_batches: 10\n\
             max_batch_delay: 200ms\nsync_retry_delay: 5s\n"
        );
        for text in [bench5.as_str(), MAINNET_PARAMETERS, TESTNET_PARAMETERS] {
            let input: serde_yaml::Value = serde_yaml::from_str(text).expect("yaml parses");
            let params: Parameters = serde_yaml::from_str(text).expect("parameters parse");
            let output = serde_yaml::to_value(&params).expect("parameters serialize");
            for (key, value) in input.as_mapping().expect("a parameters file is a mapping") {
                assert_eq!(
                    output.get(key).map(lowercase),
                    Some(lowercase(value)),
                    "key {key:?} changed on re-serialization"
                );
            }
            for (name, _) in params.switches() {
                assert_eq!(output.get(name), None, "re-serialization added switch {name}");
            }
            let reparsed: Parameters = serde_yaml::from_value(output).expect("output parses");
            assert_eq!(reparsed, params);
        }
    }

    /// An unknown key is ignored, so a mistyped switch leaves the node at its defaults. This pins
    /// today's behaviour: the `node switches` log line and gauge exist because of it.
    #[test]
    fn unknown_keys_are_ignored() {
        assert_eq!(parse_with("no_such_switch: true\nanother_unknown: 3ms"), parse_with(""));
    }

    /// The `--chain adiri` and `--chain mainnet` presets hold no switch, so both run every switch
    /// at its default.
    #[test]
    fn chain_presets_leave_every_switch_at_its_default() {
        let defaults = Parameters::default().switches();
        for (chain, text) in [("mainnet", MAINNET_PARAMETERS), ("adiri", TESTNET_PARAMETERS)] {
            let preset: Parameters = serde_yaml::from_str(text).expect("preset parses");
            assert_eq!(preset.switches(), defaults, "{chain} preset");
            for (name, _) in &defaults {
                assert!(!text.contains(name), "{chain} preset sets switch {name}");
            }
        }
    }

    // ---- bench6 quorum-exec-block ----

    /// The value [`Parameters::switches`] lists for `name` in a file holding the `extra` lines.
    /// The per-switch tests below share it.
    fn listed(extra: &str, name: &str) -> String {
        parse_with(extra)
            .switches()
            .into_iter()
            .find(|(key, _)| *key == name)
            .map(|(_, value)| value.to_string())
            .unwrap_or_else(|| panic!("switch {name} is not listed"))
    }

    /// Both quorum-exec-block switches stay at the bench-5 behaviour unless the file sets them,
    /// parse their arm values, and are listed next to each other.
    #[test]
    fn quorum_exec_block_switches_parse() {
        let params = parse_with("");
        assert!(!params.quorum_exec_block);
        assert_eq!(params.quorum_exec_block_starved_rounds, 0);
        let params = parse_with("quorum_exec_block: true\nquorum_exec_block_starved_rounds: 4");
        assert!(params.quorum_exec_block);
        assert_eq!(params.quorum_exec_block_starved_rounds, 4);
        assert_eq!(listed("quorum_exec_block: true", "quorum_exec_block"), "true");
        assert_eq!(
            listed("quorum_exec_block_starved_rounds: 4", "quorum_exec_block_starved_rounds"),
            "4"
        );
        let names: Vec<_> = params.switches().into_iter().map(|(name, _)| name).collect();
        let at = names.iter().position(|name| *name == "quorum_exec_block").expect("listed");
        assert_eq!(names.get(at + 1), Some(&"quorum_exec_block_starved_rounds"));
    }

    // ---- bench6 voter-lock ----

    /// `vote_queue_fix` stays at the bench-5 behaviour unless the file turns it on.
    #[test]
    fn vote_queue_fix_parses() {
        assert!(!parse_with("").vote_queue_fix);
        assert!(parse_with("vote_queue_fix: true").vote_queue_fix);
        assert_eq!(listed("vote_queue_fix: true", "vote_queue_fix"), "true");
    }

    // ---- bench6 mdbx-advice ----

    /// `mdbx_random_advice` stays at the bench-5 behaviour unless the file turns it on.
    #[test]
    fn mdbx_random_advice_parses() {
        assert!(!parse_with("").mdbx_random_advice);
        assert!(parse_with("mdbx_random_advice: true").mdbx_random_advice);
        assert_eq!(listed("mdbx_random_advice: true", "mdbx_random_advice"), "true");
    }

    // ---- bench6 sealed-keep ----

    /// `sealed_tx_keep` stays at the bench-5 behaviour unless the file turns it on.
    #[test]
    fn sealed_tx_keep_parses() {
        assert!(!parse_with("").sealed_tx_keep);
        assert!(parse_with("sealed_tx_keep: true").sealed_tx_keep);
        assert_eq!(listed("sealed_tx_keep: true", "sealed_tx_keep"), "true");
    }

    // ---- bench6 digest-dedupe ----

    /// `digest_dedupe` stays at the bench-5 behaviour unless the file turns it on.
    #[test]
    fn digest_dedupe_parses() {
        assert!(!parse_with("").digest_dedupe);
        assert!(parse_with("digest_dedupe: true").digest_dedupe);
        assert_eq!(listed("digest_dedupe: true", "digest_dedupe"), "true");
    }

    // ---- bench6 recovery-pool ----

    /// `recovery_threads` stays on the global pool (0) unless the file sets a thread count.
    #[test]
    fn recovery_threads_parses() {
        assert_eq!(parse_with("").recovery_threads, 0);
        assert_eq!(parse_with("recovery_threads: 3").recovery_threads, 3);
        assert_eq!(listed("recovery_threads: 3", "recovery_threads"), "3");
    }

    // ---- bench6 tracker-drain ----

    /// Both tracker-drain switches stay at the bench-5 behaviour unless the file turns them on.
    #[test]
    fn tracker_drain_switches_parse() {
        let params = parse_with("");
        assert!(!params.epoch_close_task_drain);
        assert!(!params.epoch_record_state_check);
        let params = parse_with("epoch_close_task_drain: true\nepoch_record_state_check: true");
        assert!(params.epoch_close_task_drain);
        assert!(params.epoch_record_state_check);
        assert_eq!(listed("epoch_close_task_drain: true", "epoch_close_task_drain"), "true");
        assert_eq!(listed("epoch_record_state_check: true", "epoch_record_state_check"), "true");
    }

    // ---- bench6 parent-grace ----

    /// `parent_grace` stays at zero unless the file sets it, and parses both arm values written
    /// bare or quoted.
    #[test]
    fn parent_grace_parses() {
        assert_eq!(parse_with("").parent_grace, Duration::ZERO);
        assert_eq!(parse_with("parent_grace: 0ms").parent_grace, Duration::ZERO);
        assert_eq!(parse_with("parent_grace: 150ms").parent_grace, Duration::from_millis(150));
        assert_eq!(parse_with("parent_grace: \"100ms\"").parent_grace, Duration::from_millis(100));
        assert_eq!(listed("parent_grace: 150ms", "parent_grace"), "150ms");
        assert_eq!(listed("", "parent_grace"), "0s");
    }

    // ---- bench6 inflight-bound ----

    /// `rpc_admission_backlog_outputs` stays off (0) unless the file sets a bound.
    #[test]
    fn rpc_admission_backlog_outputs_parses() {
        assert_eq!(parse_with("").rpc_admission_backlog_outputs, 0);
        assert_eq!(parse_with("rpc_admission_backlog_outputs: 2").rpc_admission_backlog_outputs, 2);
        assert_eq!(
            listed("rpc_admission_backlog_outputs: 2", "rpc_admission_backlog_outputs"),
            "2"
        );
    }

    // ---- bench6 pipelined-persist ----

    /// `pipelined_persist` stays at the bench-5 behaviour unless the file turns it on.
    #[test]
    fn pipelined_persist_parses() {
        assert!(!parse_with("").pipelined_persist);
        assert!(parse_with("pipelined_persist: true").pipelined_persist);
        assert_eq!(listed("pipelined_persist: true", "pipelined_persist"), "true");
    }
}
