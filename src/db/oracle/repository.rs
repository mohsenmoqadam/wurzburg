use crate::db::oracle::OraclePool;

#[derive(Clone)]
pub struct OracleRepository {
    pub pool: OraclePool,
}

impl OracleRepository {
    pub fn new(pool: OraclePool) -> Self {
        Self { pool }
    }
}
