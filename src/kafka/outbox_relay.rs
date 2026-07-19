use std::{sync::Arc, time::Duration};

use futures::{StreamExt, stream};
use opentelemetry::trace::TraceContextExt;
use rand::RngExt;
use tokio::sync::watch;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::{config::KafkaOutboxRelayConfig, db::oracle::OracleRepository};

use super::AppKafkaProducer;

pub struct OutboxRelayHandle {
    shutdown: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl OutboxRelayHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        let _ = self.task.await;
    }
}

pub fn start_outbox_relay(
    repository: Arc<OracleRepository>,
    producer: AppKafkaProducer,
    config: KafkaOutboxRelayConfig,
) -> Option<OutboxRelayHandle> {
    if !config.enabled {
        tracing::info!(worker.name = "kafka-outbox-relay", "outbox relay disabled");
        return None;
    }
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(run_outbox_relay(repository, producer, config, receiver));
    Some(OutboxRelayHandle { shutdown, task })
}

async fn run_outbox_relay(
    repository: Arc<OracleRepository>,
    producer: AppKafkaProducer,
    config: KafkaOutboxRelayConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    let poll_interval = Duration::from_millis(config.poll_interval_ms);
    loop {
        if *shutdown.borrow() {
            break;
        }
        match repository
            .claim_outbox_batch(
                config.worker_id.clone(),
                config.batch_size,
                config.lease_duration_ms,
            )
            .await
        {
            Ok(events) if events.is_empty() => {
                tokio::select! {
                    _ = tokio::time::sleep(poll_interval) => {},
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { break; }
                    }
                }
            }
            Ok(events) => {
                stream::iter(events)
                    .for_each_concurrent(usize::from(config.batch_size), |event| {
                        publish_claimed_event(repository.clone(), producer.clone(), &config, event)
                    })
                    .await;
            }
            Err(error) => {
                tracing::error!(
                    error.kind = error.diagnostic_kind(),
                    "failed to claim Oracle outbox events"
                );
                tokio::time::sleep(poll_interval).await;
            }
        }
    }
    tracing::info!(worker.name = "kafka-outbox-relay", "outbox relay stopped");
}

async fn publish_claimed_event(
    repository: Arc<OracleRepository>,
    producer: AppKafkaProducer,
    config: &KafkaOutboxRelayConfig,
    event: crate::db::oracle::ClaimedOutboxEvent,
) {
    let span = tracing::info_span!(
        "kafka.outbox.publish",
        messaging.system = "kafka",
        messaging.operation.name = "publish",
        messaging.destination.name = %config.topic,
        messaging.message.id = %event.envelope.event_id,
        event.type = %event.envelope.event_type,
        operation.id = %event.envelope.operation_id,
        retry.count = event.attempt_count
    );
    link_original_trace(&span, &event.headers);
    async move {
        let event_id = event.envelope.event_id;
        if producer
            .send_internal(
                &config.topic,
                &event.partition_key,
                &event.envelope,
                &event.headers,
            )
            .await
            .is_ok()
        {
            if let Err(error) = repository
                .mark_outbox_published(event_id, config.worker_id.clone())
                .await
            {
                tracing::error!(
                    error.kind = error.diagnostic_kind(),
                    "broker acknowledged event but Oracle publication finalization failed"
                );
            }
        } else {
            let dead_letter = event.attempt_count >= config.max_attempts;
            let backoff = retry_backoff(config, event.attempt_count);
            if let Err(error) = repository
                .reschedule_outbox_event(
                    event_id,
                    config.worker_id.clone(),
                    dead_letter,
                    backoff.as_millis() as u64,
                )
                .await
            {
                tracing::error!(
                    error.kind = error.diagnostic_kind(),
                    "failed to persist outbox delivery outcome"
                );
            } else {
                tracing::warn!(dead_letter, "Kafka event was not acknowledged");
            }
        }
    }
    .instrument(span)
    .await;
}

fn retry_backoff(config: &KafkaOutboxRelayConfig, attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(31);
    let base = config
        .initial_backoff_ms
        .saturating_mul(1_u64 << exponent)
        .min(config.max_backoff_ms);
    let jitter_ceiling = (base / 5).max(1);
    let jitter = rand::rng().random_range(0..=jitter_ceiling);
    Duration::from_millis(base.saturating_add(jitter).min(config.max_backoff_ms))
}

fn link_original_trace(
    span: &tracing::Span,
    headers: &crate::kafka::contract::InternalEventHeaders,
) {
    let Some(traceparent) = headers.traceparent.as_deref() else {
        return;
    };
    let mut carrier =
        std::collections::HashMap::from([("traceparent".to_string(), traceparent.to_string())]);
    if let Some(tracestate) = headers.tracestate.as_deref() {
        carrier.insert("tracestate".to_string(), tracestate.to_string());
    }
    let context =
        opentelemetry::global::get_text_map_propagator(|propagator| propagator.extract(&carrier));
    let linked = context.span().span_context().clone();
    if linked.is_valid() {
        span.add_link(linked);
    }
}

#[cfg(test)]
mod tests {
    use super::retry_backoff;
    use crate::config::Settings;

    #[test]
    fn retry_backoff_is_bounded_by_configuration() {
        let settings = Settings::new().expect("settings should load");
        let config = &settings.kafka.outbox_relay;
        for attempt in [1, 2, 10, u32::MAX] {
            assert!(
                retry_backoff(config, attempt).as_millis() <= u128::from(config.max_backoff_ms)
            );
        }
    }
}
