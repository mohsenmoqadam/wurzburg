use crate::db::error::{DbError, DbResult};

pub(crate) fn commit(connection: &oracle::Connection, context: &str) -> DbResult<()> {
    connection
        .commit()
        .map_err(|error| DbError::Query(format!("failed to commit {context}: {error}")))
}

pub(crate) fn rollback(connection: &oracle::Connection, context: &str) -> DbResult<()> {
    connection
        .rollback()
        .map_err(|error| DbError::Query(format!("failed to rollback {context}: {error}")))
}
