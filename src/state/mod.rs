mod bootstrap;

use deadpool_redis::Pool as RedisPool;
use std::sync::Arc;

use crate::config::Settings;
use crate::db::oracle::{OracleHealthRepository, OracleRepository};
use crate::ledger::{LedgerClient, LedgerWorkerHandle};
use crate::messaging::{MessageBrokerAdmin, MessageProducer};
use crate::object_storage::ObjectStorage;
use crate::security::provider_kafka_cipher::ProviderKafkaCredentialFactory;

/// Immutable dependency container shared by HTTP handlers and background
/// workers. Resource construction belongs to the bootstrap submodule.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Settings>,
    pub db: Arc<OracleRepository>,
    pub oracle_health: Option<OracleHealthRepository>,
    pub redis: RedisPool,
    pub message_producer: MessageProducer,
    pub message_broker_admin: MessageBrokerAdmin,
    pub ledger_client: LedgerClient,
    pub ledger_worker: LedgerWorkerHandle,
    pub provider_kafka_credentials: Option<Arc<ProviderKafkaCredentialFactory>>,
    pub object_storage: Arc<ObjectStorage>,
}
