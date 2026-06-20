use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::{
    propagation::TraceContextPropagator,
    resource::Resource,
    runtime,
    trace::{Sampler, TracerProvider},
};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Returned by `init` — shuts down the OTEL tracer provider on drop.
pub struct ShutdownGuard;

impl Drop for ShutdownGuard {
    fn drop(&mut self) {
        global::shutdown_tracer_provider();
    }
}

/// Initialise tracing + OTEL for a binary. Call once at the top of `main`.
///
/// Backend is controlled by `OTEL_EXPORTER_OTLP_ENDPOINT` env var
/// (default: `http://localhost:4317`). Swap endpoint to switch backends.
/// Sampling rate controlled by `OTEL_SAMPLE_RATE` (default: 0.1 = 10%).
pub fn init(service_name: &'static str) -> ShutdownGuard {
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".into());

    let sample_rate: f64 = std::env::var("OTEL_SAMPLE_RATE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.1);

    global::set_text_map_propagator(TraceContextPropagator::new());

    let resource = Resource::new(vec![
        opentelemetry::KeyValue::new("service.name", service_name),
        opentelemetry::KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
    ]);

    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .expect("OTLP span exporter build failed");

    let provider = TracerProvider::builder()
        .with_batch_exporter(exporter, runtime::Tokio)
        .with_resource(resource)
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            sample_rate,
        ))))
        .build();

    // Get an SDK tracer (implements PreSampledTracer, required by OpenTelemetryLayer).
    let tracer = provider.tracer(service_name);

    // Register as global provider so application code can use `global::tracer(...)`.
    global::set_tracer_provider(provider);

    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .with(OpenTelemetryLayer::new(tracer))
        .init();

    ShutdownGuard
}
