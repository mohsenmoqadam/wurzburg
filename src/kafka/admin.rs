use anyhow::{Context, Result, bail};
use rdkafka::{
    ClientConfig,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    types::RDKafkaErrorCode,
    util::Timeout,
};

use crate::config::KafkaConfig;

use super::{
    native_admin::{self, AclOperation, AclResource, KafkaAdminError, ProviderAcl},
    producer::apply_security_config,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderKafkaAccessSpec {
    pub topic_name: String,
    pub username: String,
    pub consumer_group: String,
}

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
        if let (Some(username), Some(password)) = (
            config.admin.sasl_username.as_deref(),
            config.admin.sasl_password.as_deref(),
        ) {
            client_config
                .set("sasl.username", username)
                .set("sasl.password", password);
        }
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

    #[tracing::instrument(
        skip(self, password),
        fields(
            messaging.system = "kafka",
            messaging.operation.name = "provider_access.provision"
        )
    )]
    pub async fn provision_provider_access(
        &self,
        spec: ProviderKafkaAccessSpec,
        mut password: Vec<u8>,
        scram_iterations: i32,
    ) -> std::result::Result<(), KafkaAdminError> {
        validate_provider_access_spec(&spec)?;
        self.create_topic(&spec.topic_name)
            .await
            .map_err(|_| KafkaAdminError::Broker(-1))?;
        let client = self.client.clone();
        let timeout = self.request_timeout;
        tokio::task::spawn_blocking(move || {
            let result = native_admin::upsert_scram_sha512(
                client.as_ref(),
                &spec.username,
                &password,
                scram_iterations,
                timeout,
            )
            .and_then(|()| {
                for acl in provider_acls(&spec) {
                    if !native_admin::acl_exists(client.as_ref(), acl, timeout)? {
                        native_admin::create_acl(client.as_ref(), acl, timeout)?;
                    }
                }
                verify_provider_access_native(client.as_ref(), &spec, timeout)
            });
            password.fill(0);
            result
        })
        .await
        .map_err(|_| KafkaAdminError::NativeContract("blocking_task_join"))?
    }

    #[tracing::instrument(
        skip(self),
        fields(
            messaging.system = "kafka",
            messaging.operation.name = "provider_access.verify"
        )
    )]
    pub async fn verify_provider_access(
        &self,
        spec: ProviderKafkaAccessSpec,
    ) -> std::result::Result<(), KafkaAdminError> {
        validate_provider_access_spec(&spec)?;
        self.verify_topics([spec.topic_name.as_str()])
            .map_err(|_| KafkaAdminError::Broker(-1))?;
        let client = self.client.clone();
        let timeout = self.request_timeout;
        tokio::task::spawn_blocking(move || {
            verify_provider_access_native(client.as_ref(), &spec, timeout)
        })
        .await
        .map_err(|_| KafkaAdminError::NativeContract("blocking_task_join"))?
    }

    #[tracing::instrument(
        skip(self),
        fields(
            messaging.system = "kafka",
            messaging.operation.name = "provider_access.revoke"
        )
    )]
    pub async fn revoke_provider_access(
        &self,
        spec: ProviderKafkaAccessSpec,
    ) -> std::result::Result<(), KafkaAdminError> {
        validate_provider_access_spec(&spec)?;
        let client = self.client.clone();
        let timeout = self.request_timeout;
        tokio::task::spawn_blocking(move || {
            for acl in provider_acls(&spec) {
                if native_admin::acl_exists(client.as_ref(), acl, timeout)? {
                    native_admin::delete_acl(client.as_ref(), acl, timeout)?;
                }
            }
            if native_admin::verify_scram_sha512(client.as_ref(), &spec.username, timeout)? {
                native_admin::delete_scram_sha512(client.as_ref(), &spec.username, timeout)?;
            }
            Ok(())
        })
        .await
        .map_err(|_| KafkaAdminError::NativeContract("blocking_task_join"))?
    }
}

fn verify_provider_access_native(
    client: &AdminClient<DefaultClientContext>,
    spec: &ProviderKafkaAccessSpec,
    timeout: std::time::Duration,
) -> std::result::Result<(), KafkaAdminError> {
    if !native_admin::verify_scram_sha512(client, &spec.username, timeout)? {
        return Err(KafkaAdminError::NativeContract("scram_verification"));
    }
    for acl in provider_acls(spec) {
        if !native_admin::acl_exists(client, acl, timeout)? {
            return Err(KafkaAdminError::NativeContract("acl_verification"));
        }
    }
    Ok(())
}

fn provider_acls(spec: &ProviderKafkaAccessSpec) -> [ProviderAcl<'_>; 3] {
    [
        ProviderAcl {
            resource: AclResource::Topic(&spec.topic_name),
            username: &spec.username,
            operation: AclOperation::Read,
        },
        ProviderAcl {
            resource: AclResource::Topic(&spec.topic_name),
            username: &spec.username,
            operation: AclOperation::Describe,
        },
        ProviderAcl {
            resource: AclResource::Group(&spec.consumer_group),
            username: &spec.username,
            operation: AclOperation::Read,
        },
    ]
}

fn validate_provider_access_spec(
    spec: &ProviderKafkaAccessSpec,
) -> std::result::Result<(), KafkaAdminError> {
    validate_topic_name(&spec.topic_name).map_err(|_| KafkaAdminError::InvalidInput)?;
    for value in [&spec.username, &spec.consumer_group] {
        if value.is_empty()
            || value.len() > 249
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            return Err(KafkaAdminError::InvalidInput);
        }
    }
    Ok(())
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
