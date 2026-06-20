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
#[must_use = "dropping ShutdownGuard immediately shuts down the OTEL exporter"]
pub struct ShutdownGuard {
    provider: opentelemetry_sdk::trace::TracerProvider,
}

impl Drop for ShutdownGuard {
    fn drop(&mut self) {
        if let Err(e) = self.provider.shutdown() {
            eprintln!("OTEL shutdown error: {e}");
        }
    }
}

/// Initialise tracing + OTEL for a binary. Call once at the top of `main`.
///
/// Backend is controlled by `OTEL_EXPORTER_OTLP_ENDPOINT` env var
/// (default: `http://localhost:4317`). Swap endpoint to switch backends.
/// Sampling rate controlled by `OTEL_SAMPLE_RATE` (default: 0.1 = 10%).
pub fn init(service_name: &str) -> ShutdownGuard {
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".into());

    let sample_rate: f64 = match std::env::var("OTEL_SAMPLE_RATE") {
        Ok(v) => v.parse().unwrap_or_else(|_| {
            eprintln!("WARN: OTEL_SAMPLE_RATE={v:?} is not a valid f64, using default 0.1");
            0.1
        }),
        Err(_) => 0.1,
    };

    global::set_text_map_propagator(TraceContextPropagator::new());

    let resource = Resource::new(vec![
        opentelemetry::KeyValue::new("service.name", service_name.to_string()),
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
    let tracer = provider.tracer(service_name.to_string());

    // Register as global provider so application code can use `global::tracer(...)`.
    global::set_tracer_provider(provider.clone());

    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .with(OpenTelemetryLayer::new(tracer))
        .init();

    ShutdownGuard { provider }
}
