use std::sync::Arc;

use anyhow::{Context, Result};
use opentelemetry::trace::TraceContextExt;
use rdkafka::{
    Message,
    config::ClientConfig,
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::Headers,
};
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::{
    config::{KafkaConfig, KafkaReceiptConsumerConfig},
    db::oracle::OracleRepository,
    domain::card_policy::PolicyMaterializationReceipt,
    kafka::{
        contract::{InternalEventEnvelope, RuntimeMaterializationReceipt},
        producer::apply_security_config,
    },
};

pub struct ReceiptConsumerHandle {
    shutdown: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl ReceiptConsumerHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        let _ = self.task.await;
    }
}

pub fn start_receipt_consumer(
    repository: Arc<OracleRepository>,
    kafka: KafkaConfig,
) -> Result<Option<ReceiptConsumerHandle>> {
    if !kafka.materialization_receipts.enabled {
        tracing::info!(
            worker.name = "materialization-receipt-consumer",
            "receipt consumer disabled"
        );
        return Ok(None);
    }
    let consumer = build_consumer(&kafka, &kafka.materialization_receipts)?;
    consumer
        .subscribe(&[&kafka.materialization_receipts.topic])
        .context("failed to subscribe to materialization receipt topic")?;
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(run_consumer(repository, consumer, receiver));
    Ok(Some(ReceiptConsumerHandle { shutdown, task }))
}

fn build_consumer(
    kafka: &KafkaConfig,
    config: &KafkaReceiptConsumerConfig,
) -> Result<StreamConsumer> {
    let mut client = ClientConfig::new();
    client
        .set("bootstrap.servers", &kafka.bootstrap_servers)
        .set("client.id", &config.client_id)
        .set("group.id", &config.group_id)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("session.timeout.ms", config.session_timeout_ms.to_string())
        .set(
            "max.poll.interval.ms",
            config.max_poll_interval_ms.to_string(),
        )
        .set("auto.offset.reset", &config.auto_offset_reset)
        .set("security.protocol", &kafka.security_protocol);
    apply_security_config(&mut client, kafka)?;
    client
        .create()
        .context("failed to create Kafka receipt consumer")
}

async fn run_consumer(
    repository: Arc<OracleRepository>,
    consumer: StreamConsumer,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break; }
            }
            message = consumer.recv() => match message {
                Ok(message) => {
                    let span = tracing::info_span!(
                        "kafka.receipt.process",
                        messaging.system = "kafka",
                        messaging.operation.name = "process",
                        messaging.destination.name = message.topic(),
                        messaging.kafka.partition = message.partition(),
                        messaging.kafka.offset = message.offset()
                    );
                    link_incoming_trace(&span, &message);
                    match process_message(&repository, &message).instrument(span).await {
                        Ok(()) => {
                            if consumer.commit_message(&message, CommitMode::Sync).is_err() {
                                tracing::error!(error.kind = "offset_commit", "failed to commit materialization receipt offset");
                            }
                        }
                        Err(ReceiptProcessingError::Contract(error_code)) => {
                            let payload_sha256 = message.payload().map(payload_sha256);
                            match repository.record_kafka_poison_message(
                                message.topic().to_string(),
                                message.partition(),
                                message.offset(),
                                payload_sha256,
                                error_code,
                            ).await {
                                Ok(()) => {
                                    if consumer.commit_message(&message, CommitMode::Sync).is_err() {
                                        tracing::error!(error.kind = "offset_commit", "failed to commit durable poison receipt offset");
                                    }
                                }
                                Err(error) => tracing::error!(error.kind = error.diagnostic_kind(), "failed to persist poison receipt evidence"),
                            }
                        }
                        Err(ReceiptProcessingError::Database) => {
                            tracing::error!(error.kind = "receipt_database", "materialization receipt remains uncommitted for retry");
                        }
                    }
                }
                Err(_) => tracing::error!(error.kind = "kafka_consume", "failed to receive materialization receipt"),
            }
        }
    }
    tracing::info!(
        worker.name = "materialization-receipt-consumer",
        "receipt consumer stopped"
    );
}

async fn process_message(
    repository: &OracleRepository,
    message: &rdkafka::message::BorrowedMessage<'_>,
) -> Result<(), ReceiptProcessingError> {
    let payload = message
        .payload()
        .ok_or(ReceiptProcessingError::Contract("RECEIPT_PAYLOAD_MISSING"))?;
    let envelope: InternalEventEnvelope<RuntimeMaterializationReceipt> =
        serde_json::from_slice(payload)
            .map_err(|_| ReceiptProcessingError::Contract("RECEIPT_ENVELOPE_INVALID"))?;
    if envelope.event_type != "RUNTIME_PROFILE_MATERIALIZED" {
        return Err(ReceiptProcessingError::Contract(
            "RECEIPT_EVENT_TYPE_INVALID",
        ));
    }
    if envelope.event_id != envelope.payload.receipt_event_id
        || envelope.operation_id != envelope.payload.operation_id
        || envelope.aggregate_id != envelope.payload.aggregate_id
    {
        return Err(ReceiptProcessingError::Contract(
            "RECEIPT_IDENTITY_MISMATCH",
        ));
    }
    match envelope.payload.profile_type.as_str() {
        "CPOL" => {
            let profile_id =
                envelope
                    .payload
                    .profile_id
                    .ok_or(ReceiptProcessingError::Contract(
                        "RECEIPT_PROFILE_ID_MISSING",
                    ))?;
            repository
                .apply_policy_materialization_receipt(PolicyMaterializationReceipt {
                    receipt_event_id: envelope.payload.receipt_event_id,
                    operation_id: envelope.payload.operation_id,
                    card_range_id: envelope.payload.aggregate_id,
                    card_policy_profile_id: profile_id,
                    materialized_version: envelope.payload.materialized_version,
                    runtime_key: envelope.payload.runtime_key,
                    materialized_at: envelope.payload.materialized_at,
                })
                .await
                .map_err(|_| ReceiptProcessingError::Database)?;
        }
        "CRCTL" => {
            if envelope.payload.profile_id.is_some() {
                return Err(ReceiptProcessingError::Contract(
                    "RECEIPT_PROFILE_ID_UNEXPECTED",
                ));
            }
            repository
                .apply_range_control_receipt(envelope.payload)
                .await
                .map_err(|_| ReceiptProcessingError::Database)?;
        }
        _ => {
            return Err(ReceiptProcessingError::Contract(
                "RECEIPT_PROFILE_TYPE_INVALID",
            ));
        }
    }
    Ok(())
}

enum ReceiptProcessingError {
    Contract(&'static str),
    Database,
}

fn payload_sha256(payload: &[u8]) -> String {
    format!("{:x}", Sha256::digest(payload))
}

fn link_incoming_trace(span: &tracing::Span, message: &rdkafka::message::BorrowedMessage<'_>) {
    let Some(headers) = message.headers() else {
        return;
    };
    let mut carrier = std::collections::HashMap::new();
    for header in headers.iter() {
        if matches!(header.key, "traceparent" | "tracestate")
            && let Some(value) = header
                .value
                .and_then(|value| std::str::from_utf8(value).ok())
        {
            carrier.insert(header.key.to_string(), value.to_string());
        }
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
    use super::payload_sha256;

    #[test]
    fn poison_evidence_hash_is_stable_without_storing_payload() {
        assert_eq!(payload_sha256(b"invalid"), payload_sha256(b"invalid"));
        assert_ne!(payload_sha256(b"invalid"), payload_sha256(b"other"));
    }
}
