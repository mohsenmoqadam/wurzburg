use anyhow::{Context, Result, bail};
use rdkafka::{
    ClientConfig,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    types::RDKafkaErrorCode,
    util::Timeout,
};

use crate::config::KafkaConfig;

use super::producer::apply_security_config;

/// Broker-native topic administration used by controlled platform workflows.
/// Provider SCRAM and ACL provisioning belongs to the later Provider worker and
/// is intentionally absent until its security and recovery contract is built.
#[derive(Clone)]
pub struct AppKafkaAdmin {
    client: std::sync::Arc<AdminClient<DefaultClientContext>>,
    request_timeout: std::time::Duration,
    partitions: i32,
    replication_factor: i32,
}

impl AppKafkaAdmin {
    pub fn new(config: &KafkaConfig) -> Result<Self> {
        config.validate()?;
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("client.id", format!("{}-admin", config.producer.client_id))
            .set("security.protocol", &config.security_protocol);
        apply_security_config(&mut client_config, config)?;
        let client = client_config
            .create()
            .context("failed to create Kafka admin client")?;
        Ok(Self {
            client: std::sync::Arc::new(client),
            request_timeout: config.admin.request_timeout(),
            partitions: config
                .admin
                .partitions
                .try_into()
                .context("Kafka partition count exceeds i32")?,
            replication_factor: config
                .admin
                .replication_factor
                .try_into()
                .context("Kafka replication factor exceeds i32")?,
        })
    }

    #[tracing::instrument(skip(self), fields(messaging.system="kafka", messaging.operation.name="create_topic", messaging.destination.name=topic_name))]
    pub async fn create_topic(&self, topic_name: &str) -> Result<()> {
        validate_topic_name(topic_name)?;
        let topic = NewTopic::new(
            topic_name,
            self.partitions,
            TopicReplication::Fixed(self.replication_factor),
        );
        let options = AdminOptions::new()
            .request_timeout(Some(self.request_timeout))
            .operation_timeout(Some(self.request_timeout));
        let results = self
            .client
            .create_topics([&topic], &options)
            .await
            .context("Kafka create-topic request failed")?;
        match results.into_iter().next() {
            Some(Ok(_)) | Some(Err((_, RDKafkaErrorCode::TopicAlreadyExists))) => Ok(()),
            Some(Err((_, code))) => bail!("Kafka create-topic operation failed with code {code:?}"),
            None => bail!("Kafka create-topic response was empty"),
        }
    }

    #[tracing::instrument(skip(self), fields(messaging.system="kafka", messaging.operation.name="delete_topic", messaging.destination.name=topic_name))]
    pub async fn delete_topic(&self, topic_name: &str) -> Result<()> {
        validate_topic_name(topic_name)?;
        let options = AdminOptions::new()
            .request_timeout(Some(self.request_timeout))
            .operation_timeout(Some(self.request_timeout));
        let results = self
            .client
            .delete_topics(&[topic_name], &options)
            .await
            .context("Kafka delete-topic request failed")?;
        match results.into_iter().next() {
            Some(Ok(_)) | Some(Err((_, RDKafkaErrorCode::UnknownTopicOrPartition))) => Ok(()),
            Some(Err((_, code))) => bail!("Kafka delete-topic operation failed with code {code:?}"),
            None => bail!("Kafka delete-topic response was empty"),
        }
    }

    #[tracing::instrument(skip(self, topic_names), fields(messaging.system="kafka", messaging.operation.name="verify_topics"))]
    pub fn verify_topics<'a>(&self, topic_names: impl IntoIterator<Item = &'a str>) -> Result<()> {
        for topic_name in topic_names {
            validate_topic_name(topic_name)?;
            let metadata = self
                .client
                .inner()
                .fetch_metadata(Some(topic_name), Timeout::After(self.request_timeout))
                .with_context(|| format!("failed to fetch Kafka metadata for {topic_name}"))?;
            let topic = metadata
                .topics()
                .iter()
                .find(|topic| topic.name() == topic_name)
                .with_context(|| format!("Kafka topic {topic_name} does not exist"))?;
            if let Some(error) = topic.error() {
                bail!("Kafka topic {topic_name} metadata failed with code {error:?}");
            }
            if topic.partitions().is_empty() {
                bail!("Kafka topic {topic_name} has no partitions");
            }
            if topic
                .partitions()
                .iter()
                .any(|partition| partition.error().is_some() || partition.leader() < 0)
            {
                bail!("Kafka topic {topic_name} has an unavailable partition");
            }
        }
        Ok(())
    }
}

fn validate_topic_name(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 249
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        bail!("Kafka topic name is invalid");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_topic_name;

    #[test]
    fn topic_names_accept_contract_names_and_reject_unsafe_input() {
        assert!(validate_topic_name("wurzburg.runtime-projection.commands.v1").is_ok());
        assert!(validate_topic_name("bad topic").is_err());
        assert!(validate_topic_name("").is_err());
    }
}
