use anyhow::{Context, Result, bail};
use rdkafka::{
    config::ClientConfig,
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
};
use serde::Serialize;
use std::time::Duration;

use crate::{
    config::KafkaConfig,
    kafka::contract::{InternalEventEnvelope, InternalEventHeaders},
};

#[derive(Clone)]
pub struct AppKafkaProducer {
    producer: FutureProducer,
    delivery_timeout: Duration,
}

impl AppKafkaProducer {
    pub fn new(config: &KafkaConfig) -> Result<Self> {
        config.validate()?;
        let producer_config = &config.producer;
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("client.id", &producer_config.client_id)
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("max.in.flight.requests.per.connection", "5")
            .set(
                "delivery.timeout.ms",
                producer_config.delivery_timeout_ms.to_string(),
            )
            .set(
                "message.timeout.ms",
                producer_config.delivery_timeout_ms.to_string(),
            )
            .set(
                "request.timeout.ms",
                producer_config.request_timeout_ms.to_string(),
            )
            .set(
                "message.max.bytes",
                producer_config.max_request_size.to_string(),
            )
            .set("retries", producer_config.retries.to_string())
            .set("linger.ms", producer_config.linger_ms.to_string())
            .set("compression.type", &producer_config.compression_type)
            .set("security.protocol", &config.security_protocol);

        apply_security_config(&mut client_config, config)?;

        let producer = client_config
            .create()
            .context("failed to create Kafka producer")?;

        Ok(Self {
            producer,
            delivery_timeout: producer_config.delivery_timeout(),
        })
    }

    #[tracing::instrument(
        skip(self, envelope, headers),
        fields(
            messaging.system = "kafka",
            messaging.operation.name = "publish",
            messaging.destination.name = topic,
            messaging.message.id = %envelope.event_id,
            event.type = %envelope.event_type,
            operation.id = %envelope.operation_id
        )
    )]
    pub async fn send_internal<T: Serialize>(
        &self,
        topic: &str,
        partition_key: &str,
        envelope: &InternalEventEnvelope<T>,
        headers: &InternalEventHeaders,
    ) -> Result<()> {
        let payload = serde_json::to_vec(envelope).context("failed to serialize internal event")?;
        let schema_version = envelope.schema_version.to_string();
        let event_id = envelope.event_id.to_string();
        let operation_id = envelope.operation_id.to_string();

        let mut kafka_headers = OwnedHeaders::new()
            .insert(Header {
                key: "event_id",
                value: Some(event_id.as_str()),
            })
            .insert(Header {
                key: "event_type",
                value: Some(envelope.event_type.as_str()),
            })
            .insert(Header {
                key: "schema_version",
                value: Some(schema_version.as_str()),
            })
            .insert(Header {
                key: "operation_id",
                value: Some(operation_id.as_str()),
            })
            .insert(Header {
                key: "correlation_id",
                value: Some(headers.correlation_id.as_str()),
            })
            .insert(Header {
                key: "request_id",
                value: Some(headers.request_id.as_str()),
            });

        let causation_id = headers.causation_id.map(|value| value.to_string());
        for (key, value) in [
            ("causation_id", causation_id.as_deref()),
            ("traceparent", headers.traceparent.as_deref()),
            ("tracestate", headers.tracestate.as_deref()),
        ] {
            if let Some(value) = value {
                kafka_headers = kafka_headers.insert(Header {
                    key,
                    value: Some(value),
                });
            }
        }

        let record = FutureRecord::to(topic)
            .key(partition_key)
            .payload(payload.as_slice())
            .headers(kafka_headers);

        self.producer
            .send(record, Timeout::After(self.delivery_timeout))
            .await
            .map_err(|(error, _)| anyhow::anyhow!("Kafka delivery failed: {error}"))?;
        Ok(())
    }
}

pub(crate) fn apply_security_config(
    client_config: &mut ClientConfig,
    config: &KafkaConfig,
) -> Result<()> {
    if let Some(cert) = config
        .security_cert
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        client_config.set("ssl.ca.location", cert);
    }

    if config.security_protocol.contains("SASL") {
        let (Some(mechanism), Some(username), Some(password)) = (
            config.sasl_mechanism.as_deref(),
            config.sasl_username.as_deref(),
            config.sasl_password.as_deref(),
        ) else {
            bail!("Kafka SASL mechanism, username, and password are required");
        };
        client_config
            .set("sasl.mechanism", mechanism)
            .set("sasl.username", username)
            .set("sasl.password", password);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::config::Settings;

    #[test]
    fn loaded_producer_configuration_is_consistent_with_idempotent_delivery() {
        let settings = Settings::new().expect("test settings should load");
        settings
            .kafka
            .validate()
            .expect("producer config should be valid");
        assert!(
            settings.kafka.producer.delivery_timeout_ms
                > settings.kafka.producer.request_timeout_ms
        );
    }
}
