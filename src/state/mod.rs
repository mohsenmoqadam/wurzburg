mod bootstrap;

use deadpool_redis::Pool as RedisPool;
use std::sync::Arc;

use crate::config::Settings;
use crate::db::oracle::{OracleHealthRepository, OracleRepository};
use crate::kafka::{AppKafkaAdmin, AppKafkaProducer};
use crate::object_storage::ObjectStorage;
use crate::security::provider_kafka_cipher::ProviderKafkaCredentialFactory;
use crate::tigerbeetle::{AppTbClient, TigerBeetleWorkerHandle};

/// Immutable dependency container shared by HTTP handlers and background
/// workers. Resource construction belongs to the bootstrap submodule.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Settings>,
    pub db: Arc<OracleRepository>,
    pub oracle_health: Option<OracleHealthRepository>,
    pub redis: RedisPool,
    pub kafka_producer: AppKafkaProducer,
    pub kafka_admin: AppKafkaAdmin,
    pub tb_client: AppTbClient,
    pub tb_worker: TigerBeetleWorkerHandle,
    pub provider_kafka_credentials: Option<Arc<ProviderKafkaCredentialFactory>>,
    pub object_storage: Arc<ObjectStorage>,
}
