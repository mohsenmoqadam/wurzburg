use std::sync::Arc;
use anyhow::{Context, Result};
use deadpool_redis::{Config as RedisConfig, Pool as RedisPool, Runtime, PoolConfig};
use sqlx::postgres::PgPoolOptions;

use crate::config::Settings;
use crate::db::traits::AppRepository;
use crate::db::postgres::PgRepository;
use crate::kafka::{AppKafkaAdmin, AppKafkaProducer};
use crate::tigerbeetle::{AppTbClient, start_tb_worker};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Settings>,
    pub db: Arc<dyn AppRepository>, 
    pub redis: RedisPool,
    pub kafka_producer: AppKafkaProducer,
    pub kafka_admin: AppKafkaAdmin,
    pub tb_client: AppTbClient, 
}

impl AppState {
    pub async fn new(config: Settings) -> Result<Self> {
        let config_arc = Arc::new(config.clone());

        // 1. Setup Database Pool based on active_driver
        let db: Arc<dyn AppRepository> = if config.database.active_driver == "postgres" {
            let pg_cfg = &config.database.postgres;
            let pool = PgPoolOptions::new()
                .max_connections(pg_cfg.max_connections)
                .min_connections(pg_cfg.min_connections)
                .acquire_timeout(pg_cfg.acquire_timeout())
                .idle_timeout(pg_cfg.idle_timeout())
                .max_lifetime(pg_cfg.max_lifetime())
                .connect(&pg_cfg.url)
                .await
                .context("Failed to connect to PostgreSQL")?;  
            Arc::new(PgRepository::new(pool))
        }  else if config.database.active_driver == "oracle" {
            anyhow::bail!("Oracle driver is not implemented yet");
        } else {
            anyhow::bail!("Unsupported database driver: {}", config.database.active_driver);
        };

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
            if let Err(e) = start_tb_worker(worker_config, tb_receiver).await 
                { tracing::error!("TigerBeetle worker failed: {:?}", e); }
            });

        Ok(Self {
            config: config_arc,
            db,
            redis,
            kafka_producer,
            kafka_admin,
            tb_client,
        })
    }
}
