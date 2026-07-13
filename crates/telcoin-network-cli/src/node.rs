//! Main node command
//!
//! Starts the client
use crate::{version::SHORT_VERSION, NoArgs};
use clap::{value_parser, Args, Parser};
use core::fmt;
use eyre::WrapErr as _;
use fdlimit::raise_fd_limit;
use rayon::ThreadPoolBuilder;
use std::{
    future::Future, net::SocketAddr, path::PathBuf, sync::Arc, thread::available_parallelism,
};
use tn_config::{Config, KeyConfig, RetryConfig, TelcoinDirs};
use tn_node::engine::TnBuilder;
use tn_reth::{parse_socket_address, RethCommand, RethConfig};
use tn_snapshot::{
    restore::{restore_from_snapshot, RestoreReceipt},
    SnapshotError, SnapshotResult, UploadConfig,
};
use tn_types::Epoch;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

/// Avaliable "named" chains.
/// These will have embedded config files and can be joined after gereating keys.
#[derive(Debug, Copy, Clone, clap::ValueEnum)]
pub enum NamedChain {
    /// Adiri- alias for TestNet
    Adiri,
    /// TestNet or Adiri
    TestNet,
    /// MainNet
    MainNet,
}

/// Start the node
#[derive(Debug, Parser)]
pub struct NodeCommand<Ext: clap::Args + fmt::Debug = NoArgs> {
    /// Join a named telcoin network (for instance test or main net).
    #[arg(long, value_name = "NAMED_TN_NETWORK", verbatim_doc_comment)]
    pub chain: Option<NamedChain>,

    /// Enable Prometheus consensus metrics.
    ///
    /// The metrics will be served at the given interface and port.
    #[arg(long, value_name = "SOCKET", value_parser = parse_socket_address, help_heading = "Consensus Metrics")]
    pub metrics: Option<SocketAddr>,

    /// Add a new instance of a node.
    ///
    /// Configures the ports of the node to avoid conflicts with the defaults.
    /// This is useful for running multiple nodes on the same machine.
    ///
    /// Max number of instances is 200. It is chosen in a way so that it's not possible to have
    /// port numbers that conflict with each other.
    ///
    /// Changes to the following port numbers:
    /// - `HTTP_RPC_PORT`: default - `instance` + 1
    /// - `WS_RPC_PORT`: default + `instance` * 2 - 2
    /// - `IPC_PATH`: default + `-instance`
    #[arg(long, value_name = "INSTANCE", global = true,  value_parser = value_parser!(u16).range(..=200))]
    pub instance: Option<u16>,

    /// Is this an observer node?  True if set, an observer will never be in the committee
    /// but will follow consensus and provide node RPC access.
    #[arg(long, value_name = "OBSERVER", global = true, default_value_t = false)]
    pub observer: bool,

    /// Sets all ports to unused, allowing the OS to choose random unused ports when sockets are
    /// bound.
    ///
    /// Mutually exclusive with `--instance`.
    #[arg(long, conflicts_with = "instance", global = true)]
    pub with_unused_ports: bool,

    /// Additional reth arguments
    #[clap(flatten)]
    pub reth: RethCommand,

    /// TCP health check endpoint port.
    ///
    /// When a port is specified, the node will spawn a TCP health check service
    /// on that port. The health check endpoint is useful for load balancers and
    /// monitoring systems to verify that the node process is running.
    ///
    /// If not specified, the health check service will not be started.
    ///
    /// WARNING: ensure the health endpoint is behind a firewall.
    /// Each connection is handled synchronously in the main accept loop.
    /// No connection limits or rate limiting are implemented.
    /// Connections are immediately closed after sending response.
    #[arg(long, value_name = "HEALTHCHECK_TCP_PORT", global = true, env = "HEALTHCHECK_TCP_PORT")]
    pub healthcheck: Option<u16>,

    /// Assign this name to node.  Currently used to name it's opentracing service.
    #[arg(long, value_name = "NODE_NAME", global = true)]
    pub node_name: Option<String>,

    /// URL of an opentracing service (like jaeger) to send tracing data to (for example http://192.168.1.2:4317).
    #[arg(long, value_name = "URL", global = true, env = "TN_TRACING_URL")]
    pub tracing_url: Option<String>,

    /// Cloud snapshot arguments (observer-only uploader, retention, and fresh-datadir restore).
    #[clap(flatten)]
    pub snapshot: NodeSnapshotArgs,

    /// Additional cli arguments
    #[clap(flatten)]
    pub ext: Ext,
}

impl<Ext: clap::Args + fmt::Debug> NodeCommand<Ext> {
    /// Execute `node` command
    pub fn execute<L>(
        mut self,
        tn_datadir: PathBuf,
        key_config: KeyConfig,
        launcher: L,
    ) -> eyre::Result<JoinHandle<eyre::Result<()>>>
    where
        L: FnOnce(TnBuilder, Ext, PathBuf, KeyConfig, &'static str) -> JoinHandle<eyre::Result<()>>,
    {
        info!(target: "cli", "telcoin-network {} starting", SHORT_VERSION);

        // Raise the fd limit of the process.
        // Does not do anything on windows.
        raise_fd_limit()?;

        // Install the global metrics recorder before any reth components are constructed
        // (in particular before `RethEnv::new_database`). Reth's derive-style metric
        // handles bind to whatever recorder is installed at construction time; anything
        // registered against the default noop recorder is silently lost. Nodes without
        // `--metrics` keep the zero-overhead noop recorder.
        if self.metrics.is_some() {
            tn_metrics::install_recorder()?;
        }

        // limit global rayon thread pool for batch validator
        //
        // ensure 2 cores are reserved unless the system only has 1 core
        let num_parallel_threads =
            available_parallelism().map_or(0, |num| num.get().saturating_sub(2).max(1));
        if let Err(err) = ThreadPoolBuilder::new()
            .num_threads(num_parallel_threads)
            .thread_name(|i| format!("tn-rayon-{i}"))
            .build_global()
        {
            error!(target: "cli", "Error: Failed to initialize global thread pool for rayon: {err}");
        }

        // overwrite all genesis if `genesis` was passed to CLI
        let tn_config = if let Some(chain) = self.chain.take() {
            info!(target: "cli", "Overwriting TN config with named chain: {chain:?}");
            match chain {
                NamedChain::Adiri | NamedChain::TestNet => {
                    Config::load_adiri(&tn_datadir, self.observer, SHORT_VERSION)?
                }
                NamedChain::MainNet => {
                    Config::load_mainnet(&tn_datadir, self.observer, SHORT_VERSION)?
                }
            }
        } else {
            Config::load(&tn_datadir, self.observer, SHORT_VERSION)?
        };
        #[cfg(not(feature = "adiri"))]
        if tn_config.genesis().config.chain_id == 2017 {
            // If we are trying to start an Adiri node without the adiri feature flag then error
            // out.
            return Err(eyre::eyre!(
                "Must compile with adiri feature flag in order to connect to adiri (testnet)!"
            ));
        }
        #[cfg(feature = "adiri")]
        if tn_config.genesis().config.chain_id != 2017 {
            // If we are trying to start an Adiri node without the adiri feature flag then error
            // out.
            return Err(eyre::eyre!(
                "Must NOT compile with adiri feature flag when connecting to non-adiri (testnet) networks!"
            ));
        }
        debug!(target: "cli", validator = ?tn_config.node_info.name, "tn datadir for node command: {tn_datadir:?}");
        info!(target: "cli", validator = ?tn_config.node_info.name, "config loaded");

        // reject the observer-only uploader on a validator before doing any further startup work.
        ensure_snapshot_upload_is_observer_only(
            self.snapshot.snapshot_upload.as_deref(),
            self.observer,
        )?;

        // get the worker's transaction address from the config
        let Self {
            chain: _,    // Used above
            observer: _, // Used above
            metrics,
            instance,
            with_unused_ports,
            reth,
            healthcheck,
            ext,
            node_name: _,
            tracing_url: _,
            snapshot:
                NodeSnapshotArgs {
                    snapshot_upload,
                    snapshot_keep_last,
                    snapshot_source,
                    snapshot_min_epoch,
                },
        } = self;

        // set up reth node config for engine components
        let node_config = RethConfig::new(
            reth,
            instance,
            &tn_datadir,
            with_unused_ports,
            Arc::new(tn_config.chain_spec()),
        );

        // auto-restore from a cloud snapshot onto a FRESH datadir before opening the reth database.
        // `RethEnv::new_database` creates the MDBX files, after which any chain data present makes
        // `restore_from_snapshot` refuse to run (ChainDataExists); running here keeps a fresh
        // datadir eligible. a datadir that already holds chain data skips the restore and boots
        // normally, while a half-restored or unverifiable datadir fails startup. `execute` runs
        // inside the node's multi-thread runtime (see cli.rs), so bridge to the async restore with
        // `block_in_place` rather than nesting a runtime.
        if let Some(source) = &snapshot_source {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(maybe_restore_from_snapshot(
                    &tn_datadir,
                    source,
                    snapshot_min_epoch,
                    &node_config,
                ))
            })?;
        }

        // create dbs to survive between sync state transitions
        let reth_db = tn_reth::RethEnv::new_database(&node_config, tn_datadir.reth_db_path())?;

        // build the observer-only uploader config; the observer/validator gate ran above.
        let snapshot_upload = snapshot_upload.map(|url| {
            let mut config = UploadConfig::new(url);
            config.keep_last = snapshot_keep_last;
            config
        });
        let builder = TnBuilder {
            node_config,
            tn_config,
            metrics,
            healthcheck,
            reth_db,
            exex_fns: vec![],
            snapshot_upload,
        };

        Ok(launcher(builder, ext, tn_datadir, key_config, SHORT_VERSION))
    }
}

/// Cloud snapshot arguments for the `node` command.
///
/// These are operational knobs (like `--metrics`), so they live on the CLI rather than in
/// `parameters.yaml`: the uploader target and the auto-restore source are per-deployment
/// operational choices, not consensus parameters. Flattened into [`NodeCommand`].
#[derive(Debug, Args)]
pub struct NodeSnapshotArgs {
    /// Object-store URL to upload epoch-boundary snapshots to (observer nodes only).
    ///
    /// When set, an observer node publishes a signed state snapshot to this bucket at every epoch
    /// boundary. A committee validator must never run the uploader, so combining this flag with a
    /// non-observer node is a hard startup error.
    ///
    /// Supported schemes: `s3://`, `gs://`, `az://`, `http(s)://`, and `file://`. Cloud backends
    /// read credentials from the standard per-scheme environment variables (for example `AWS_*`
    /// for S3, `GOOGLE_*` for GCS, and `AZURE_*` for Azure).
    #[arg(long, value_name = "URL", env = "TN_SNAPSHOT_UPLOAD")]
    pub snapshot_upload: Option<String>,

    /// Number of most-recent uploaded snapshots to retain before older ones are pruned.
    ///
    /// Only meaningful alongside `--snapshot-upload`.
    #[arg(long, value_name = "N", env = "TN_SNAPSHOT_KEEP_LAST", default_value_t = tn_snapshot::DEFAULT_KEEP_LAST)]
    pub snapshot_keep_last: u32,

    /// Object-store URL to auto-restore from when starting on a FRESH (empty) datadir.
    ///
    /// On startup, if the datadir holds no chain data, the node downloads, verifies against its
    /// local trust root, and installs the newest snapshot from this bucket before opening its
    /// databases. If the datadir already holds chain data the source is ignored and startup
    /// proceeds normally; a half-restored or unverifiable snapshot fails startup.
    ///
    /// Supported schemes: `s3://`, `gs://`, `az://`, `http(s)://`, and `file://`. Cloud backends
    /// read credentials from the standard per-scheme environment variables (for example `AWS_*`).
    #[arg(long, value_name = "URL", env = "TN_SNAPSHOT_SOURCE")]
    pub snapshot_source: Option<String>,

    /// Minimum epoch a startup auto-restore will accept from `--snapshot-source`.
    ///
    /// A malicious or misconfigured bucket cannot forge a snapshot for the wrong committee, but it
    /// CAN serve an older, still-valid one in place of the newest. Setting a floor bounds how far
    /// back an attacker-chosen start may be: a resolved epoch below this value fails startup
    /// rather than booting on stale state. Only meaningful alongside `--snapshot-source`; when
    /// unset the node accepts whatever epoch the bucket's `latest.json` advertises. Manual
    /// `snapshot restore` has no floor — pin `--epoch` instead.
    #[arg(long, value_name = "EPOCH", env = "TN_SNAPSHOT_MIN_EPOCH")]
    pub snapshot_min_epoch: Option<u32>,
}

/// Reject the observer-only `--snapshot-upload` flag on a validator node.
///
/// The uploader spends the epoch-boundary quiet window pinning and publishing state, which a
/// committee validator must never do. Returns an error when an upload target is configured on a
/// non-observer node so the misconfiguration is caught at startup rather than silently ignored.
/// The uploader's own constructor re-checks this as defense in depth.
fn ensure_snapshot_upload_is_observer_only(
    snapshot_upload: Option<&str>,
    observer: bool,
) -> eyre::Result<()> {
    if snapshot_upload.is_some() && !observer {
        return Err(eyre::eyre!(
            "--snapshot-upload is observer-only; validators never run the uploader \
             (start with --observer, or drop --snapshot-upload)"
        ));
    }
    Ok(())
}

/// Auto-restore the datadir from a cloud snapshot on a fresh start.
///
/// Runs only before the reth database is created, so the datadir is either empty (restore
/// proceeds) or already populated (restore is refused with
/// [`SnapshotError::ChainDataExists`], which is skipped here). The always-latest pointer is
/// used because the node flag does not expose an epoch pin; `min_epoch` (from
/// `--snapshot-min-epoch`) is the operator's floor against a bucket that withholds the newest
/// snapshot and serves an older, still-valid one. On success the installed epoch is logged
/// prominently: a valid trust root cannot rule out withholding, so an operator must be able to see
/// which epoch was installed.
async fn maybe_restore_from_snapshot<TND: TelcoinDirs>(
    datadir: &TND,
    source_url: &str,
    min_epoch: Option<u32>,
    reth_config: &RethConfig,
) -> eyre::Result<()> {
    // retry transient source errors (a flaky object store, a timeout) on a bounded boot budget:
    // they all occur before the restore marker is written, so re-running re-enters a clean
    // precheck. permanent failures (verification, a precondition, the min-epoch floor) stop at
    // once.
    let outcome = restore_with_retry(RetryConfig::default(), source_url, || {
        restore_from_snapshot(datadir, source_url, None, min_epoch, reth_config)
    })
    .await;
    match interpret_auto_restore(outcome, source_url)? {
        Some(epoch) => {
            info!(
                target: "tn::snapshot",
                epoch,
                source = %source_url,
                "snapshot auto-restore installed epoch {epoch}; a bucket cannot forge a snapshot \
                 but CAN withhold a newer one — confirm this is the expected epoch"
            );
        }
        None => {
            info!(
                target: "tn::snapshot",
                "chain data present; skipping snapshot auto-restore"
            );
        }
    }
    Ok(())
}

/// Run `op` under `retry`, classifying [`SnapshotError`]s as transient or permanent for backoff.
///
/// Boot auto-restore's transient failures (a flaky object store, a timeout) all originate in the
/// download-and-verify phase, strictly BEFORE the restore marker is written, so re-running the
/// whole operation re-enters a clean precheck on an untouched datadir. A transient error is retried
/// with a warning; every permanent error (verification, integrity, a precondition, an operator-set
/// floor) stops immediately. Only startup auto-restore retries — the manual `snapshot
/// restore`/`verify` paths are interactive, so an operator retries them by hand. See
/// [`RetryConfig`] for the schedule.
async fn restore_with_retry<F, Fut>(
    retry: RetryConfig,
    source_url: &str,
    mut op: F,
) -> SnapshotResult<RestoreReceipt>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = SnapshotResult<RestoreReceipt>>,
{
    retry
        .retry(|| {
            // build the operation's future eagerly, then move it into the mapping block so the
            // returned future never borrows `op` — the shape backoff's FnMut operation requires.
            let attempt = op();
            async move {
                attempt.await.map_err(|err| {
                    if err.is_transient() {
                        warn!(
                            target: "tn::snapshot",
                            error = %err,
                            source = %source_url,
                            "snapshot auto-restore hit a transient source error; retrying"
                        );
                        backoff::Error::transient(err)
                    } else {
                        backoff::Error::permanent(err)
                    }
                })
            }
        })
        .await
}

/// Interpret a snapshot auto-restore outcome as a startup decision.
///
/// Returns `Ok(Some(epoch))` when a snapshot was installed, `Ok(None)` when the datadir already
/// held chain data (so the restore was correctly refused and startup should continue), and `Err`
/// for every other failure. [`SnapshotError::ChainDataExists`] is the only non-fatal error: a
/// missing trust root, an interrupted prior restore, or a verification/integrity failure all leave
/// the datadir empty or quarantined, and booting past any of them would run on unverifiable state.
fn interpret_auto_restore(
    outcome: SnapshotResult<RestoreReceipt>,
    source_url: &str,
) -> eyre::Result<Option<Epoch>> {
    match outcome {
        Ok(receipt) => Ok(Some(receipt.epoch)),
        // the datadir already holds chain data: a normal restart, not an error.
        Err(SnapshotError::ChainDataExists) => Ok(None),
        // any other failure leaves the datadir empty or quarantined; never boot past it.
        Err(err) => {
            Err(err).wrap_err_with(|| format!("snapshot auto-restore from {source_url} failed"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    use tn_snapshot::{manifest::Counts, DEFAULT_KEEP_LAST};
    use tn_types::{BlockNumHash, ConsensusNumHash, B256};

    /// Parse a bare `node` command with `extra` args and return its parsed [`NodeSnapshotArgs`].
    fn parse_snapshot_args(extra: &[&str]) -> NodeSnapshotArgs {
        let mut argv = vec!["tn"];
        argv.extend_from_slice(extra);
        NodeCommand::<NoArgs>::try_parse_from(argv).expect("node command parsed").snapshot
    }

    /// A minimal restore receipt for the interpret and retry tests.
    fn a_receipt() -> RestoreReceipt {
        RestoreReceipt {
            epoch: 9,
            final_state: BlockNumHash::default(),
            final_consensus: ConsensusNumHash::default(),
            state_root: B256::default(),
            counts: Counts::default(),
            chain_id: 2017,
            genesis_hash: B256::default(),
            node_version: String::new(),
            source_url: String::new(),
            restored_at: 0,
        }
    }

    /// A retry config with millisecond intervals and a short budget so the retry tests finish fast.
    fn fast_retry() -> RetryConfig {
        RetryConfig {
            initial_retry_interval: Duration::from_millis(1),
            max_retry_interval: Duration::from_millis(1),
            retry_delay_multiplier: 1.0,
            retry_delay_rand_factor: 0.0,
            retrying_max_elapsed_time: Some(Duration::from_millis(50)),
        }
    }

    #[test]
    fn snapshot_args_default_to_none_with_default_keep_last() {
        let args = parse_snapshot_args(&[]);
        assert_eq!(args.snapshot_upload, None);
        assert_eq!(args.snapshot_source, None);
        assert_eq!(args.snapshot_keep_last, DEFAULT_KEEP_LAST);
        assert_eq!(args.snapshot_min_epoch, None);
    }

    #[test]
    fn snapshot_args_parse_from_flags() {
        let args = parse_snapshot_args(&[
            "--snapshot-upload",
            "s3://bucket/prefix",
            "--snapshot-keep-last",
            "7",
            "--snapshot-source",
            "file:///tmp/bucket",
            "--snapshot-min-epoch",
            "12",
        ]);
        assert_eq!(args.snapshot_upload.as_deref(), Some("s3://bucket/prefix"));
        assert_eq!(args.snapshot_keep_last, 7);
        assert_eq!(args.snapshot_source.as_deref(), Some("file:///tmp/bucket"));
        assert_eq!(args.snapshot_min_epoch, Some(12));
    }

    #[test]
    fn snapshot_args_parse_from_env() {
        // nextest runs each test in its own process, so these env mutations cannot race with the
        // other parse tests; set → parse → clear also keeps the window tight for `cargo test`.
        std::env::set_var("TN_SNAPSHOT_UPLOAD", "gs://bucket");
        std::env::set_var("TN_SNAPSHOT_KEEP_LAST", "5");
        std::env::set_var("TN_SNAPSHOT_SOURCE", "az://container");
        std::env::set_var("TN_SNAPSHOT_MIN_EPOCH", "8");
        let args = parse_snapshot_args(&[]);
        std::env::remove_var("TN_SNAPSHOT_UPLOAD");
        std::env::remove_var("TN_SNAPSHOT_KEEP_LAST");
        std::env::remove_var("TN_SNAPSHOT_SOURCE");
        std::env::remove_var("TN_SNAPSHOT_MIN_EPOCH");
        assert_eq!(args.snapshot_upload.as_deref(), Some("gs://bucket"));
        assert_eq!(args.snapshot_keep_last, 5);
        assert_eq!(args.snapshot_source.as_deref(), Some("az://container"));
        assert_eq!(args.snapshot_min_epoch, Some(8));
    }

    #[test]
    fn gating_rejects_upload_on_validator() {
        // --snapshot-upload without --observer is a validator running the uploader: reject it.
        let err = ensure_snapshot_upload_is_observer_only(Some("s3://bucket"), false)
            .expect_err("validator upload must be rejected");
        assert!(err.to_string().contains("observer-only"), "unexpected error: {err}");
    }

    #[test]
    fn gating_allows_upload_on_observer() {
        ensure_snapshot_upload_is_observer_only(Some("s3://bucket"), true)
            .expect("observer upload must be allowed");
    }

    #[test]
    fn gating_allows_no_upload_on_validator() {
        // a validator with no uploader configured is the common case.
        ensure_snapshot_upload_is_observer_only(None, false).expect("no upload must be allowed");
    }

    #[test]
    fn interpret_restore_installs_on_success() {
        let decision =
            interpret_auto_restore(Ok(a_receipt()), "file:///bucket").expect("ok outcome");
        assert_eq!(decision, Some(9));
    }

    #[test]
    fn interpret_restore_skips_when_chain_data_exists() {
        // an already-populated datadir is a normal restart: skip, do not fail startup.
        let decision =
            interpret_auto_restore(Err(SnapshotError::ChainDataExists), "file:///bucket")
                .expect("chain-data-exists must be a skip, not an error");
        assert_eq!(decision, None);
    }

    #[test]
    fn interpret_restore_fails_on_other_errors() {
        // every non-ChainDataExists error must stop startup rather than boot on unverifiable state.
        for err in [
            SnapshotError::MissingTrustRoot,
            SnapshotError::RestoreIncomplete,
            SnapshotError::Verification("bad sig".into()),
            SnapshotError::Integrity("digest mismatch".into()),
        ] {
            // expect_err panics unless the outcome is an error; the returned report is discarded.
            let _ = interpret_auto_restore(Err(err), "file:///bucket")
                .expect_err("non-skip errors must fail startup");
        }
    }

    #[tokio::test]
    async fn retry_recovers_from_transient_errors() {
        let attempts = AtomicUsize::new(0);
        let outcome = restore_with_retry(fast_retry(), "file:///bucket", || {
            let n = attempts.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 2 {
                    Err(SnapshotError::Timeout("flaky".into()))
                } else {
                    Ok(a_receipt())
                }
            }
        })
        .await;
        assert!(outcome.is_ok(), "transient errors must be retried through to success");
        assert_eq!(attempts.load(Ordering::SeqCst), 3, "two transient failures then a success");
    }

    #[tokio::test]
    async fn retry_stops_immediately_on_permanent_error() {
        let attempts = AtomicUsize::new(0);
        let outcome = restore_with_retry(fast_retry(), "file:///bucket", || {
            attempts.fetch_add(1, Ordering::SeqCst);
            async move { Err::<RestoreReceipt, _>(SnapshotError::Verification("bad sig".into())) }
        })
        .await;
        assert!(matches!(outcome, Err(SnapshotError::Verification(_))), "got {outcome:?}");
        assert_eq!(attempts.load(Ordering::SeqCst), 1, "a permanent error must not be retried");
    }

    #[tokio::test]
    async fn retry_gives_up_after_budget() {
        let attempts = AtomicUsize::new(0);
        let outcome = restore_with_retry(fast_retry(), "file:///bucket", || {
            attempts.fetch_add(1, Ordering::SeqCst);
            async move { Err::<RestoreReceipt, _>(SnapshotError::Timeout("always".into())) }
        })
        .await;
        // the transient error never clears, so retry exhausts its elapsed-time budget and returns
        // the last error rather than looping forever.
        assert!(matches!(outcome, Err(SnapshotError::Timeout(_))), "got {outcome:?}");
        assert!(
            attempts.load(Ordering::SeqCst) >= 2,
            "a transient error must be retried at least once before giving up"
        );
    }
}
