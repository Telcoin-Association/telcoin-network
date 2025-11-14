// SPDX-License-Identifier: Apache-2.0
// Library for managing all components used by a full-node in a single process.

#![allow(missing_docs)]

use engine::TnBuilder;
use manager::EpochManager;
use tn_config::{KeyConfig, TelcoinDirs};
use tn_primary::ConsensusBus;
use tn_rpc::EngineToPrimary;
use tn_storage::{ConsensusStore, EpochStore};
use tn_types::{BlockHash, ConsensusHeader, Database, Epoch, EpochCertificate, EpochRecord};
use tokio::runtime::Builder;
use tracing::{error_span, instrument, warn, Instrument as _};

pub mod engine;
mod error;
mod manager;
pub mod primary;
pub mod worker;
pub use manager::catchup_accumulator;

use opentelemetry::{global, trace::TracerProvider as _, KeyValue};
use opentelemetry_otlp::WithExportConfig as _;
use opentelemetry_sdk::{
    metrics::{MeterProviderBuilder, PeriodicReader, SdkMeterProvider},
    trace::{RandomIdGenerator, Sampler, SdkTracerProvider},
    Resource,
};
use opentelemetry_semantic_conventions::{attribute::SERVICE_VERSION, SCHEMA_URL};
use tracing_core::LevelFilter;
use tracing_opentelemetry::{MetricsLayer, OpenTelemetryLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer as _};

// Create a Resource that captures information about the entity for which telemetry is recorded.
fn resource(service: &str) -> Resource {
    Resource::builder()
        .with_service_name(service.to_string())
        .with_schema_url([KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION"))], SCHEMA_URL)
        .build()
}

// Construct MeterProvider for MetricsLayer
fn init_meter_provider(service: &str) -> SdkMeterProvider {
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_temporality(opentelemetry_sdk::metrics::Temporality::default())
        .build()
        .unwrap(); //XXXX  fail loudly or fallback to stdout logging?

    let reader =
        PeriodicReader::builder(exporter).with_interval(std::time::Duration::from_secs(30)).build();

    // For debugging in development
    let stdout_reader =
        PeriodicReader::builder(opentelemetry_stdout::MetricExporter::default()).build();

    let meter_provider = MeterProviderBuilder::default()
        .with_resource(resource(service))
        .with_reader(reader)
        .with_reader(stdout_reader)
        .build();

    global::set_meter_provider(meter_provider.clone());

    meter_provider
}

// Construct TracerProvider for OpenTelemetryLayer
fn init_tracer_provider(service: &str, url: &str) -> Option<SdkTracerProvider> {
    let exporter =
        opentelemetry_otlp::SpanExporter::builder().with_tonic().with_endpoint(url).build().ok()?;

    Some(
        SdkTracerProvider::builder()
            // Customize sampling strategy
            .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(1.0))))
            // If export trace to AWS X-Ray, you can use XrayIdGenerator
            .with_id_generator(RandomIdGenerator::default())
            .with_resource(resource(service))
            .with_batch_exporter(exporter)
            .build(),
    )
}

// Initialize tracing-subscriber and return OtelGuard for opentelemetry-related termination
// processing
fn init_tracing_subscriber(service: &str) -> OtelGuard {
    let meter_provider = init_meter_provider(service);
    let env_filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    let fmt = tracing_subscriber::fmt::layer()
        //.compact()
        .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
            // Exclude span events from the log output
            !metadata.is_span()
        }));

    let registry = tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt)
        .with(MetricsLayer::new(meter_provider.clone()));
    let mut tracer_provider = None;
    if let Ok(tracing_url) = std::env::var("TN_TRACING_URL") {
        tracer_provider = init_tracer_provider(service, &tracing_url);
        if let Some(tracer_provider) = tracer_provider.as_ref() {
            let tracer = tracer_provider.tracer("tracing-otel-subscriber");
            registry.with(OpenTelemetryLayer::new(tracer)).init();
        } else {
            registry.init();
        }
    } else {
        registry.init();
    }

    OtelGuard { tracer_provider, meter_provider }
}

struct OtelGuard {
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: SdkMeterProvider,
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        if let Some(tracer_provider) = self.tracer_provider.take() {
            if let Err(err) = tracer_provider.shutdown() {
                eprintln!("{err:?}");
            }
        }
        if let Err(err) = self.meter_provider.shutdown() {
            eprintln!("{err:?}");
        }
    }
}

/// Launch all components for the node.
///
/// Worker, Primary, and Execution.
/// This will possibly "loop" to launch multiple times in response to
/// a nodes mode changes.  This ensures a clean state and fresh tasks
/// when switching modes.
#[instrument(level = "info", skip_all)]
pub fn launch_node<P>(
    builder: TnBuilder,
    tn_datadir: P,
    passphrase: Option<String>,
) -> eyre::Result<()>
where
    P: TelcoinDirs + Clone + 'static,
{
    let runtime = Builder::new_multi_thread()
        .thread_name("telcoin-network")
        .enable_io()
        .enable_time()
        .build()?;

    // run the node
    let res = runtime.block_on(async move {
        let passphrase =
            if std::fs::exists(tn_datadir.node_keys_path().join(tn_config::BLS_WRAPPED_KEYFILE))
                .unwrap_or(false)
            {
                passphrase
            } else {
                None
            };

        // create key config for lifetime of the app
        // We DO NOT have tracing initialized at this point.
        let key_config = KeyConfig::read_config(&tn_datadir, passphrase)?;

        let _guard = init_tracing_subscriber(&format!(
            "tn-node-{}",
            key_config.primary_public_key().to_short_string()
        ));
        let span = error_span!("node-running");
        let _span_enter = span.enter();
        tracing::info!(target: "telcoin::consensus_config", "loaded validator keys at {:?}", tn_datadir.node_keys_path());
        let consensus_db = manager::open_consensus_db(&tn_datadir)?;
        // create the epoch manager
        let mut epoch_manager = EpochManager::new(builder, tn_datadir, consensus_db, key_config)?;
        epoch_manager.run().instrument(span.clone()).await
    });

    // return result after shutdown
    res
}

#[derive(Debug)]
pub struct EngineToPrimaryRpc<DB> {
    /// Container for consensus channels.
    consensus_bus: ConsensusBus,
    /// Consensus DB
    db: DB,
}

impl<DB: Database> EngineToPrimaryRpc<DB> {
    pub fn new(consensus_bus: ConsensusBus, db: DB) -> Self {
        Self { consensus_bus, db }
    }

    /// Retrieve the consensus header by number.
    fn get_epoch_by_number(&self, epoch: Epoch) -> Option<(EpochRecord, EpochCertificate)> {
        if let Some((r, Some(c))) = self.db.get_epoch_by_number(epoch) {
            Some((r, c))
        } else {
            None
        }
    }

    /// Retrieve the consensus header by hash
    fn get_epoch_by_hash(&self, hash: BlockHash) -> Option<(EpochRecord, EpochCertificate)> {
        if let Some((r, Some(c))) = self.db.get_epoch_by_hash(hash) {
            Some((r, c))
        } else {
            None
        }
    }
}

impl<DB: Database> EngineToPrimary for EngineToPrimaryRpc<DB> {
    fn get_latest_consensus_block(&self) -> ConsensusHeader {
        self.consensus_bus.last_consensus_header().borrow().clone()
    }

    fn consensus_block_by_number(&self, number: u64) -> Option<ConsensusHeader> {
        self.db.get_consensus_by_number(number)
    }

    fn consensus_block_by_hash(&self, hash: BlockHash) -> Option<ConsensusHeader> {
        self.db.get_consensus_by_hash(hash)
    }

    fn epoch(
        &self,
        epoch: Option<Epoch>,
        hash: Option<BlockHash>,
    ) -> Option<(EpochRecord, EpochCertificate)> {
        match (epoch, hash) {
            (_, Some(hash)) => self.get_epoch_by_hash(hash),
            (Some(epoch), _) => self.get_epoch_by_number(epoch),
            (None, None) => None,
        }
    }
}

#[cfg(test)]
mod clippy {
    use rand as _;
    use tn_network_types as _;
    use tn_test_utils as _;
}
