use anyhow::{Context, Result};
use deadpool_redis::{Config as RedisConfig, Pool as RedisPool, PoolConfig, Runtime};
use std::sync::Arc;

use crate::config::Settings;
use crate::db::oracle::{
    OracleConnectConfig, OracleHealthRepository, OraclePool, OracleRepository,
};
use crate::kafka::{AppKafkaAdmin, AppKafkaProducer};
use crate::tigerbeetle::{AppTbClient, start_tb_worker};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Settings>,
    pub db: Arc<OracleRepository>,
    pub oracle_health: Option<OracleHealthRepository>,
    pub redis: RedisPool,
    pub kafka_producer: AppKafkaProducer,
    pub kafka_admin: AppKafkaAdmin,
    pub tb_client: AppTbClient,
}

impl AppState {
    pub async fn new(config: Settings) -> Result<Self> {
        let config_arc = Arc::new(config.clone());

        // 1. Setup Oracle pool. Wurzburg is Oracle-only for final persistence.
        let oracle_config = OracleConnectConfig::from_driver_config(&config.database)
            .context("Invalid Oracle database configuration")?;
        let oracle_pool = OraclePool::connect(oracle_config)
            .await
            .context("Failed to connect to Oracle")?;
        let oracle_health = OracleHealthRepository::new(oracle_pool.clone());
        let db = Arc::new(OracleRepository::new(oracle_pool));

        // 2. Setup Redis Pool
        let mut redis_cfg = RedisConfig::from_url(config.redis.url.clone());
        // Configure deadpool max size based on config
        redis_cfg.pool = Some(PoolConfig::new(config.redis.pool_max_open as usize));

        let redis = redis_cfg
            .create_pool(Some(Runtime::Tokio1))
            .context("Failed to create Redis pool")?;

        // 3. Setup Kafka Clients
        let kafka_producer = AppKafkaProducer::new(&config)?;
        let kafka_admin = AppKafkaAdmin::new(&config)?;

        // 4. Setup TigerBeetle Client & Background Worker
        let (tb_client, tb_receiver) = AppTbClient::new(&config)?;
        let worker_config = config_arc.clone();
        tokio::spawn(async move {
            if let Err(e) = start_tb_worker(worker_config, tb_receiver).await {
                tracing::error!("TigerBeetle worker failed: {:?}", e);
            }
        });

        Ok(Self {
            config: config_arc,
            db,
            oracle_health: Some(oracle_health),
            redis,
            kafka_producer,
            kafka_admin,
            tb_client,
        })
    }
}
