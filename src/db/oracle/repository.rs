use crate::db::{oracle::OraclePool, traits::AppRepository};

#[derive(Clone)]
pub struct OracleRepository {
    pub pool: OraclePool,
}

impl OracleRepository {
    pub fn new(pool: OraclePool) -> Self {
        Self { pool }
    }
}

impl AppRepository for OracleRepository {}
