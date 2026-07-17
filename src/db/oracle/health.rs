use serde::Serialize;

use crate::db::{
    error::{DbError, DbResult},
    oracle::OraclePool,
};

#[derive(Debug, Clone, Serialize)]
pub struct OracleHealth {
    pub database_time_utc: String,
}

#[derive(Clone)]
pub struct OracleHealthRepository {
    pool: OraclePool,
}

impl OracleHealthRepository {
    pub fn new(pool: OraclePool) -> Self {
        Self { pool }
    }

    pub async fn check(&self) -> DbResult<OracleHealth> {
        self.pool
            .with_connection(|connection| {
                let database_time_utc: String = connection
                    .query_row_as(
                        "SELECT TO_CHAR(SYSTIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') FROM dual",
                        &[],
                    )
                    .map_err(|error| {
                        DbError::Query(format!("Oracle health check query failed: {error}"))
                    })?;

                Ok(OracleHealth { database_time_utc })
            })
            .await
    }
}
