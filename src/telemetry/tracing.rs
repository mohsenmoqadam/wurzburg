use std::{sync::OnceLock, time::Duration};

use anyhow::Result;

use opentelemetry::{KeyValue, trace::TracerProvider};

use opentelemetry_otlp::{SpanExporter, WithExportConfig};

use opentelemetry_sdk::{
    propagation::TraceContextPropagator,
    resource::Resource,
    trace::{BatchConfigBuilder, BatchSpanProcessor, Sampler, SdkTracerProvider},
};

use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};

use crate::config::TelemetryConfig;

/// Global tracer provider used for graceful shutdown.
/// Stored once so we can flush spans on exit.
static TRACER_PROVIDER: OnceLock<SdkTracerProvider> = OnceLock::new();

/// Initialize logging and OpenTelemetry tracing.
pub fn init(config: &TelemetryConfig) -> Result<()> {
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    // Log level filter (fallback: info)
    let filter = EnvFilter::try_new(&config.log_level).unwrap_or_else(|_| EnvFilter::new("info"));

    // Logging only when telemetry is disabled
    if !config.enabled {
        tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer())
            .init();

        tracing::warn!("telemetry disabled");
        return Ok(());
    }

    // OTLP exporter (gRPC)
    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(config.otlp_endpoint.clone())
        .build()?;

    // Service metadata attached to all spans
    let resource = Resource::builder()
        .with_attributes([
            KeyValue::new("service.name", config.service_name.clone()),
            KeyValue::new("service.version", config.service_version.clone()),
            KeyValue::new("deployment.environment", config.environment.clone()),
        ])
        .build();

    // Batch export configuration
    let batch_config = BatchConfigBuilder::default()
        .with_max_queue_size(config.batch_max_queue)
        .with_max_export_batch_size(config.batch_size)
        .with_scheduled_delay(Duration::from_millis(config.batch_delay_ms))
        .build();

    // Batch span processor
    let batch_processor = BatchSpanProcessor::builder(exporter)
        .with_batch_config(batch_config)
        .build();

    // Tracer provider with sampling
    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(resource)
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            config.sampling_ratio,
        ))))
        .with_span_processor(batch_processor)
        .build();

    // Store provider for shutdown
    TRACER_PROVIDER.set(tracer_provider.clone()).ok();

    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    // Tracer used by tracing-opentelemetry
    let tracer = tracer_provider.tracer(config.service_name.clone());

    let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    // Log formatting
    let fmt_layer = match config.log_format.as_str() {
        "json" => tracing_subscriber::fmt::layer().json().boxed(),
        _ => tracing_subscriber::fmt::layer().pretty().boxed(),
    };

    // Build tracing subscriber
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .with(telemetry_layer)
        .init();

    tracing::info!(
        service = %config.service_name,
        version = %config.service_version,
        environment = %config.environment,
        endpoint = %config.otlp_endpoint,
        sampling_ratio = config.sampling_ratio,
        "telemetry initialized"
    );

    Ok(())
}

/// Flush remaining spans and shutdown exporters.
pub fn shutdown() {
    if let Some(provider) = TRACER_PROVIDER.get()
        && let Err(error) = provider.shutdown()
    {
        tracing::warn!(error = %error, "telemetry shutdown did not flush every span");
    }

    tracing::info!("telemetry shutdown");
}

/// Flush telemetry without allowing an unavailable collector to hold process
/// termination open indefinitely. Telemetry is diagnostic and must never become
/// a correctness dependency for a financial service.
pub async fn shutdown_with_timeout(deadline: Duration) {
    let Some(provider) = TRACER_PROVIDER.get().cloned() else {
        return;
    };

    // A detached OS thread is intentional here. Tokio waits for spawn_blocking
    // tasks while dropping the runtime, which could negate this deadline if an
    // exporter becomes permanently stuck.
    let (result_sender, result_receiver) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("wurzburg-otel-shutdown".to_string())
        .spawn(move || {
            let _ = result_sender.send(provider.shutdown());
        })
        .expect("failed to start telemetry shutdown thread");

    match tokio::time::timeout(deadline, result_receiver).await {
        Ok(Ok(Ok(()))) => tracing::info!("telemetry shutdown complete"),
        Ok(Ok(Err(error))) => {
            tracing::warn!(error = %error, "telemetry shutdown did not flush every span")
        }
        Ok(Err(_)) => tracing::warn!("telemetry shutdown thread ended without a result"),
        Err(_) => tracing::warn!(
            timeout_ms = deadline.as_millis(),
            "telemetry shutdown deadline exceeded"
        ),
    }
}
