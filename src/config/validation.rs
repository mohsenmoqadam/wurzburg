use anyhow::Result;

use super::model::KafkaConfig;

impl KafkaConfig {
    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            !self.bootstrap_servers.trim().is_empty(),
            "Kafka bootstrap_servers must not be empty"
        );
        anyhow::ensure!(
            matches!(
                self.security_protocol.as_str(),
                "PLAINTEXT" | "SSL" | "SASL_PLAINTEXT" | "SASL_SSL"
            ),
            "unsupported Kafka security_protocol"
        );
        if self.security_protocol.contains("SASL") {
            anyhow::ensure!(
                self.sasl_mechanism
                    .as_deref()
                    .is_some_and(|v| !v.is_empty()),
                "Kafka SASL mechanism is required"
            );
            anyhow::ensure!(
                self.sasl_username.as_deref().is_some_and(|v| !v.is_empty()),
                "Kafka SASL username is required"
            );
            anyhow::ensure!(
                self.sasl_password.as_deref().is_some_and(|v| !v.is_empty()),
                "Kafka SASL password is required"
            );
        }
        anyhow::ensure!(
            self.producer.delivery_timeout_ms > self.producer.request_timeout_ms,
            "Kafka delivery_timeout_ms must exceed request_timeout_ms"
        );
        anyhow::ensure!(
            matches!(self.producer.compression_type.as_str(), "none" | "lz4"),
            "unsupported Kafka compression_type; this build supports none and lz4"
        );
        if self.outbox_relay.enabled {
            anyhow::ensure!(
                !self.outbox_relay.topic.trim().is_empty(),
                "Kafka outbox topic is required"
            );
            anyhow::ensure!(
                !self.outbox_relay.worker_id.trim().is_empty(),
                "Kafka outbox worker_id is required"
            );
            anyhow::ensure!(
                self.outbox_relay.batch_size > 0,
                "Kafka outbox batch_size must be positive"
            );
            anyhow::ensure!(
                self.outbox_relay.lease_duration_ms > self.producer.delivery_timeout_ms,
                "Kafka outbox lease must exceed producer delivery timeout"
            );
            anyhow::ensure!(
                self.outbox_relay.max_attempts > 0,
                "Kafka outbox max_attempts must be positive"
            );
            anyhow::ensure!(
                self.outbox_relay.initial_backoff_ms <= self.outbox_relay.max_backoff_ms,
                "Kafka outbox backoff bounds are invalid"
            );
        }
        if self.materialization_receipts.enabled {
            anyhow::ensure!(
                !self.materialization_receipts.topic.trim().is_empty(),
                "Kafka receipt topic is required"
            );
            anyhow::ensure!(
                !self.materialization_receipts.group_id.trim().is_empty(),
                "Kafka receipt group_id is required"
            );
            anyhow::ensure!(
                matches!(
                    self.materialization_receipts.auto_offset_reset.as_str(),
                    "earliest" | "latest" | "error"
                ),
                "invalid Kafka auto_offset_reset"
            );
        }
        Ok(())
    }
}
