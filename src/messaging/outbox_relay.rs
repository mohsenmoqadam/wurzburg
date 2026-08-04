use std::{sync::Arc, time::Duration};

use futures::{StreamExt, stream};
use rand::RngExt;
use tokio::sync::watch;
use tracing::Instrument;

use crate::{
    config::KafkaOutboxRelayConfig,
    db::oracle::{ClaimedOutboxDelivery, OracleRepository},
    messaging::contract::InternalEventEnvelope,
};

use super::MessageProducer;

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
    producer: MessageProducer,
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
    producer: MessageProducer,
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
                if *shutdown.borrow() {
                    break;
                }
                tracing::error!(
                    error.kind = error.diagnostic_kind(),
                    "failed to claim Oracle outbox events"
                );
                tokio::select! {
                    _ = tokio::time::sleep(poll_interval) => {},
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { break; }
                    }
                }
            }
        }
    }
    tracing::info!(worker.name = "kafka-outbox-relay", "outbox relay stopped");
}

async fn publish_claimed_event(
    repository: Arc<OracleRepository>,
    producer: MessageProducer,
    config: &KafkaOutboxRelayConfig,
    event: crate::db::oracle::ClaimedOutboxEvent,
) {
    let span = tracing::info_span!(
        "kafka.outbox.publish",
        messaging.system = "kafka",
        messaging.operation.name = "publish",
        messaging.message.id = %event.event_id,
        event.type = %event.event_type,
        operation.id = %event.operation_id,
        retry.count = event.attempt_count
    );
    link_original_trace(&span, &event.headers);
    async move {
        let event_id = event.event_id;
        let publication = match &event.delivery {
            ClaimedOutboxDelivery::Internal => {
                let envelope = serde_json::from_value::<InternalEventEnvelope<serde_json::Value>>(
                    event.payload.clone(),
                );
                match envelope {
                    Ok(envelope) => {
                        producer
                            .send_internal(
                                &config.topic,
                                &event.partition_key,
                                &envelope,
                                &event.headers,
                            )
                            .await
                    }
                    Err(error) => Err(anyhow::anyhow!("invalid claimed internal event: {error}")),
                }
            }
            ClaimedOutboxDelivery::Provider { topic, provider_id } => {
                tracing::Span::current().record("provider.id", provider_id.to_string());
                producer
                    .send_provider(
                        topic,
                        &event.partition_key,
                        event.event_id,
                        &event.event_type,
                        event.schema_version,
                        &event.payload,
                    )
                    .await
            }
        };
        if publication.is_ok() {
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
    headers: &crate::messaging::contract::InternalEventHeaders,
) {
    headers.link_to_span(span);
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
