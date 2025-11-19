use opentelemetry::{global, trace::TracerProvider as _, KeyValue};
use opentelemetry_otlp::WithExportConfig as _;
use opentelemetry_sdk::{
    metrics::{MeterProviderBuilder, PeriodicReader, SdkMeterProvider},
    trace::{RandomIdGenerator, Sampler, SdkTracerProvider},
    Resource,
};
use opentelemetry_semantic_conventions::{attribute::SERVICE_VERSION, SCHEMA_URL};
use tn_reth::Layers;
use tracing::level_filters::LevelFilter;
use tracing_opentelemetry::{MetricsLayer, OpenTelemetryLayer};
use tracing_subscriber::{Layer, Registry};

// Construct MeterProvider for MetricsLayer
fn init_meter_provider(resource: Resource, url: &str) -> Option<SdkMeterProvider> {
    let exporter = match opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(url)
        .with_temporality(opentelemetry_sdk::metrics::Temporality::default())
        .build()
    {
        Ok(exporter) => exporter,
        Err(e) => {
            eprintln!("Unable to build metric exporter at {url}: {e}");
            return None;
        }
    };

    let reader =
        PeriodicReader::builder(exporter).with_interval(std::time::Duration::from_secs(30)).build();

    // For debugging in development
    let stdout_reader =
        PeriodicReader::builder(opentelemetry_stdout::MetricExporter::default()).build();

    let meter_provider = MeterProviderBuilder::default()
        .with_resource(resource)
        .with_reader(reader)
        .with_reader(stdout_reader)
        .build();

    global::set_meter_provider(meter_provider.clone());

    Some(meter_provider)
}

// Construct TracerProvider for OpenTelemetryLayer
fn init_tracer_provider(resource: Resource, url: &str) -> Option<SdkTracerProvider> {
    let exporter =
        opentelemetry_otlp::SpanExporter::builder().with_tonic().with_endpoint(url).build().ok()?;

    Some(
        SdkTracerProvider::builder()
            // Customize sampling strategy
            .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(1.0))))
            // If export trace to AWS X-Ray, you can use XrayIdGenerator
            .with_id_generator(RandomIdGenerator::default())
            .with_resource(resource)
            .with_batch_exporter(exporter)
            .build(),
    )
}

///  A boxed tracing [Layer].
type BoxedLayer<S> = Box<dyn Layer<S> + Send + Sync>;

/// Struct to hold the metrics and open telem tracing layers plus make sure they are shutdown on
/// drop.
pub(crate) struct LayersGuard {
    layers: Option<Layers>,
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
}

impl LayersGuard {
    /// Remove and return the layers, can only be called once.
    pub(crate) fn take_layers(&mut self) -> Layers {
        self.layers.take().expect("layers to exist")
    }
}

impl Drop for LayersGuard {
    fn drop(&mut self) {
        if let Some(tracer_provider) = self.tracer_provider.take() {
            if let Err(err) = tracer_provider.shutdown() {
                eprintln!("{err:?}");
            }
        }
        if let Some(meter_provider) = self.meter_provider.take() {
            if let Err(err) = meter_provider.shutdown() {
                eprintln!("{err:?}");
            }
        }
    }
}

/// Add the meter and opentracing layers for the reth tracing API.
pub(crate) fn init_opentracing_subscriber(
    service: &str,
    span_level: LevelFilter,
    meter_url: Option<String>,
    tracing_url: Option<String>,
) -> LayersGuard {
    let mut layers = Layers::new();
    if meter_url.is_none() && tracing_url.is_none() {
        return LayersGuard { layers: Some(layers), tracer_provider: None, meter_provider: None };
    }
    let env_filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(span_level.into())
        .from_env_lossy();
    let otel_filter = tracing_subscriber::filter::filter_fn(|metadata| {
        !metadata.is_span() || metadata.target().starts_with("telcoin")
    });
    let resource = Resource::builder()
        .with_service_name(service.to_string())
        .with_schema_url([KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION"))], SCHEMA_URL)
        .build();
    let mut meter_provider = None;
    if let Some(meter_url) = meter_url {
        meter_provider = init_meter_provider(resource.clone(), &meter_url);
        if let Some(meter_provider) = meter_provider.as_ref() {
            let metrics_layer = MetricsLayer::new(meter_provider.clone())
                .with_filter(otel_filter.clone())
                .with_filter(env_filter.clone());
            let layer: BoxedLayer<Registry> = Box::new(metrics_layer);
            layers.add_layer(layer);
        }
    }
    let mut tracer_provider = None;
    if let Some(tracing_url) = tracing_url {
        tracer_provider = init_tracer_provider(resource, &tracing_url);
        if let Some(tracer_provider) = tracer_provider.as_ref() {
            let tracer = tracer_provider.tracer("tracing-otel-subscriber");
            // filter only `tn` targets
            let tracer =
                OpenTelemetryLayer::new(tracer).with_filter(otel_filter).with_filter(env_filter);
            let layer: BoxedLayer<Registry> = Box::new(tracer);
            layers.add_layer(layer);
        }
    }

    LayersGuard { layers: Some(layers), tracer_provider, meter_provider }
}
