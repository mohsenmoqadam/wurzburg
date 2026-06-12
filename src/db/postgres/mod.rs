use sqlx::PgPool;

pub mod provider;
pub mod user; 
pub mod transaction;
pub mod priority;

#[derive(Clone)]
pub struct PgRepository {
    pub pool: PgPool,
}

impl PgRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl crate::db::traits::AppRepository for PgRepository {}