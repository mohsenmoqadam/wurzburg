use anyhow::{Context, Result};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use serde_json::Value;
use std::time::Duration;

use crate::config::Settings;

#[derive(Clone)]
pub struct AppKafkaProducer {
    producer: FutureProducer,
    timeout: Duration,
}

impl AppKafkaProducer {
    pub fn new(config: &Settings) -> Result<Self> {
        let mut client_config = ClientConfig::new();
        let prod_cfg = &config.kafka.producer;
        client_config
            .set("bootstrap.servers", &prod_cfg.bootstrap_servers)
            .set("client.id", &prod_cfg.client_id)
            .set(
                "message.timeout.ms",
                &prod_cfg.message_timeout_ms.to_string(),
            )
            .set("security.protocol", &prod_cfg.security_protocol)
            .set("ssl.ca.location", &prod_cfg.security_cert);

        if prod_cfg.security_protocol != "PLAINTEXT" {
            if let (Some(mech), Some(user), Some(pass)) = (
                &prod_cfg.sasl_mechanism,
                &prod_cfg.sasl_username,
                &prod_cfg.sasl_password,
            ) {
                client_config.set("sasl.mechanism", mech);
                client_config.set("sasl.username", user);
                client_config.set("sasl.password", pass);
            } else {
                return Err(anyhow::anyhow!(
                    "SASL credentials (mechanism, username, password) must be provided when security_protocol is not PLAINTEXT"
                ));
            }
        }

        let producer: FutureProducer = client_config
            .create()
            .context("Failed to create Kafka producer")?;

        let timeout = std::time::Duration::from_millis(config.kafka.producer.message_timeout_ms);

        Ok(Self { producer, timeout })
    }

    pub async fn send_json(&self, topic: &str, key: &str, payload: &Value) -> Result<()> {
        let payload_str = serde_json::to_string(payload)?;

        let record = FutureRecord::to(topic).key(key).payload(&payload_str);

        self.producer
            .send(record, Timeout::After(self.timeout))
            .await
            .map_err(|(e, _)| anyhow::anyhow!("Failed to send message to Kafka: {}", e))?;

        Ok(())
    }
}
