use std::{sync::Arc, time::Duration};

use oracle::pool::{GetMode, Pool, PoolBuilder};
use tokio::task;

use crate::{
    config::DatabaseConfig,
    db::error::{DbError, DbResult},
};

#[derive(Debug, Clone)]
pub struct OracleConnectConfig {
    pub username: String,
    pub password: String,
    pub connect_string: String,
    pub min_connections: u32,
    pub max_connections: u32,
    pub connection_increment: u32,
    pub acquire_timeout: Duration,
    pub idle_timeout: Duration,
    pub max_lifetime: Duration,
    pub statement_cache_capacity: u32,
}

impl OracleConnectConfig {
    pub fn from_driver_config(config: &DatabaseConfig) -> DbResult<Self> {
        let min_connections = config.min_connections.max(1);
        let max_connections = config.max_connections.max(min_connections);
        let connection_increment = max_connections.saturating_sub(min_connections).min(4);

        Ok(Self {
            username: config.username.clone(),
            password: config.password.clone(),
            connect_string: config.connect_string.clone(),
            min_connections,
            max_connections,
            connection_increment,
            acquire_timeout: config.acquire_timeout(),
            idle_timeout: config.idle_timeout(),
            max_lifetime: config.max_lifetime(),
            statement_cache_capacity: config
                .statement_cache_capacity
                .try_into()
                .unwrap_or(u32::MAX),
        })
    }
}

#[derive(Clone)]
pub struct OraclePool {
    inner: Arc<Pool>,
}

impl OraclePool {
    pub async fn connect(config: OracleConnectConfig) -> DbResult<Self> {
        let pool = task::spawn_blocking(move || build_pool(config))
            .await
            .map_err(|error| {
                DbError::BlockingTask(format!("Oracle pool build task failed: {error}"))
            })??;

        Ok(Self {
            inner: Arc::new(pool),
        })
    }

    pub async fn with_connection<F, T>(&self, operation: F) -> DbResult<T>
    where
        F: FnOnce(&oracle::Connection) -> DbResult<T> + Send + 'static,
        T: Send + 'static,
    {
        let pool = self.inner.clone();
        task::spawn_blocking(move || {
            let connection = pool.get().map_err(|error| {
                DbError::Connection(format!("failed to acquire Oracle connection: {error}"))
            })?;

            operation(&connection)
        })
        .await
        .map_err(|error| DbError::BlockingTask(format!("Oracle connection task failed: {error}")))?
    }
}

fn build_pool(config: OracleConnectConfig) -> DbResult<Pool> {
    let mut builder = PoolBuilder::new(config.username, config.password, config.connect_string);
    builder
        .min_connections(config.min_connections)
        .max_connections(config.max_connections)
        .connection_increment(config.connection_increment)
        .get_mode(GetMode::Wait)
        .stmt_cache_size(config.statement_cache_capacity)
        .driver_name("wurzburg-oracle");

    builder
        .ping_timeout(config.acquire_timeout)
        .map_err(|error| {
            DbError::Configuration(format!(
                "invalid Oracle ping timeout configuration: {error}"
            ))
        })?;
    builder.timeout(config.idle_timeout).map_err(|error| {
        DbError::Configuration(format!(
            "invalid Oracle idle timeout configuration: {error}"
        ))
    })?;
    builder
        .max_lifetime_connection(config.max_lifetime)
        .map_err(|error| {
            DbError::Configuration(format!(
                "invalid Oracle max lifetime configuration: {error}"
            ))
        })?;

    builder
        .build()
        .map_err(|error| DbError::Connection(format!("failed to build Oracle pool: {error}")))
}
