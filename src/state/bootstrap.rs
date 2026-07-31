use anyhow::{Context, Result};
use deadpool_redis::{Config as RedisConfig, PoolConfig, Runtime};
use std::sync::Arc;

use crate::config::Settings;
use crate::db::oracle::{
    OracleConnectConfig, OracleHealthRepository, OraclePool, OracleRepository,
};
use crate::ledger::{LedgerClient, start_ledger_worker};
use crate::messaging::{MessageBrokerAdmin, MessageProducer};
use crate::object_storage::ObjectStorage;
use crate::security::provider_kafka_cipher::ProviderKafkaCredentialFactory;

use super::AppState;

impl AppState {
    /// Builds all process-level dependency clients. Infrastructure creation is
    /// intentionally excluded and belongs to explicit deployment init jobs.
    pub async fn new(config: Settings) -> Result<Self> {
        let config_arc = Arc::new(config.clone());

        let oracle_config = OracleConnectConfig::from_driver_config(&config.database)
            .context("Invalid Oracle database configuration")?;
        let oracle_pool = OraclePool::connect(oracle_config)
            .await
            .context("Failed to connect to Oracle")?;
        let oracle_health = OracleHealthRepository::new(oracle_pool.clone());
        let db = Arc::new(OracleRepository::new(oracle_pool));

        let mut redis_config = RedisConfig::from_url(config.redis.url.clone());
        redis_config.pool = Some(PoolConfig::new(config.redis.pool_max_open as usize));
        let redis = redis_config
            .create_pool(Some(Runtime::Tokio1))
            .context("Failed to create Redis pool")?;

        let message_producer = MessageProducer::new(&config.kafka)?;
        let message_broker_admin = MessageBrokerAdmin::new(&config.kafka)?;
        let provider_kafka_credentials = config
            .provider_kafka_access
            .enabled
            .then(|| {
                ProviderKafkaCredentialFactory::from_config(
                    &config.kafka,
                    &config.provider_kafka_access,
                )
            })
            .transpose()
            .map_err(|error| {
                anyhow::anyhow!(
                    "failed to initialize Provider Kafka credential encryption: {}",
                    error.diagnostic_kind()
                )
            })?
            .map(Arc::new);

        let (ledger_client, ledger_receiver) = LedgerClient::new(&config)?;
        let ledger_worker = start_ledger_worker(config_arc.clone(), ledger_receiver);
        let object_storage = Arc::new(ObjectStorage::new(&config.object_storage)?);

        Ok(Self {
            config: config_arc,
            db,
            oracle_health: Some(oracle_health),
            redis,
            message_producer,
            message_broker_admin,
            ledger_client,
            ledger_worker,
            provider_kafka_credentials,
            object_storage,
        })
    }
}
