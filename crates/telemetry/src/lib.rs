use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{LogExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::{
    logs::LoggerProvider,
    propagation::TraceContextPropagator,
    resource::Resource,
    runtime,
    trace::{Sampler, TracerProvider},
};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Returned by `init` — shuts down the OTEL providers on drop.
#[must_use = "dropping ShutdownGuard immediately shuts down the OTEL exporters"]
pub struct ShutdownGuard {
    tracer_provider: TracerProvider,
    logger_provider: LoggerProvider,
}

impl Drop for ShutdownGuard {
    fn drop(&mut self) {
        if let Err(e) = self.tracer_provider.shutdown() {
            eprintln!("OTEL tracer shutdown error: {e}");
        }
        if let Err(e) = self.logger_provider.shutdown() {
            eprintln!("OTEL logger shutdown error: {e}");
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

    let sample_rate: f64 = std::env::var("OTEL_SAMPLE_RATE").map_or(0.1, |v| {
        v.parse().unwrap_or_else(|_| {
            eprintln!("WARN: OTEL_SAMPLE_RATE={v:?} is not a valid f64, using default 0.1");
            0.1
        })
    });

    global::set_text_map_propagator(TraceContextPropagator::new());

    let resource = Resource::new(vec![
        opentelemetry::KeyValue::new("service.name", service_name.to_string()),
        opentelemetry::KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
    ]);

    let span_exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&endpoint)
        .build()
        .expect("OTLP span exporter build failed");

    let tracer_provider = TracerProvider::builder()
        .with_batch_exporter(span_exporter, runtime::Tokio)
        .with_resource(resource.clone())
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            sample_rate,
        ))))
        .build();

    let tracer = tracer_provider.tracer(service_name.to_string());
    global::set_tracer_provider(tracer_provider.clone());

    let log_exporter = LogExporter::builder()
        .with_tonic()
        .with_endpoint(&endpoint)
        .build()
        .expect("OTLP log exporter build failed");

    let logger_provider = LoggerProvider::builder()
        .with_batch_exporter(log_exporter, runtime::Tokio)
        .with_resource(resource)
        .build();

    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .with(OpenTelemetryLayer::new(tracer))
        .with(OpenTelemetryTracingBridge::new(&logger_provider))
        .init();

    ShutdownGuard { tracer_provider, logger_provider }
}
