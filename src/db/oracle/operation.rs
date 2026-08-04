use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

use crate::db::{
    error::{DbError, DbResult},
    oracle::{
        OracleRepository,
        types::{raw16_to_uuid, uuid_to_raw16},
    },
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IntegrationOperationStatus {
    Pending,
    Publishing,
    Published,
    Materialized,
    DeadLetter,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IntegrationOperationView {
    pub operation_id: Uuid,
    pub event_id: Uuid,
    pub event_type: String,
    pub aggregate_type: String,
    pub aggregate_id: Uuid,
    pub status: IntegrationOperationStatus,
    pub attempt_count: i64,
    pub created_at: DateTime<Utc>,
    pub published_at: Option<DateTime<Utc>>,
    pub materialized_at: Option<DateTime<Utc>>,
}

impl OracleRepository {
    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="integration_operations.get", operation.id=%operation_id))]
    pub async fn get_integration_operation(
        &self,
        operation_id: Uuid,
    ) -> DbResult<Option<IntegrationOperationView>> {
        self.pool.with_connection(move |connection| {
            let raw = uuid_to_raw16(operation_id).to_vec();
            let mut rows = connection.query(
                "SELECT e.outbox_event_id,e.event_type,o.aggregate_type,o.aggregate_id,o.status,(SELECT NVL(SUM(attempt_count),0) FROM integration_outbox WHERE operation_id=o.operation_id),TO_CHAR(SYS_EXTRACT_UTC(o.created_at),'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'),TO_CHAR(SYS_EXTRACT_UTC(MAX(e.published_at)),'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'),TO_CHAR(SYS_EXTRACT_UTC(o.materialized_at),'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') FROM integration_operations o JOIN integration_outbox e ON e.operation_id=o.operation_id AND e.event_sequence=1 WHERE o.operation_id=:1 GROUP BY e.outbox_event_id,e.event_type,o.aggregate_type,o.aggregate_id,o.status,o.operation_id,o.created_at,o.materialized_at",
                &[&raw],
            ).map_err(|error| DbError::Query(format!("failed to fetch integration operation: {error}")))?;
            let Some(row) = rows.next() else { return Ok(None) };
            let row = row.map_err(|error| DbError::Query(format!("failed to read integration operation: {error}")))?;
            let event_raw: Vec<u8> = row.get(0).map_err(map_error)?;
            let aggregate_raw: Vec<u8> = row.get(3).map_err(map_error)?;
            let operation_status: String = row.get(4).map_err(map_error)?;
            let materialized_at: Option<String> = row.get(8).map_err(map_error)?;
            let status = match operation_status.as_str() { "PENDING" => IntegrationOperationStatus::Pending, "PUBLISHING" => IntegrationOperationStatus::Publishing, "PUBLISHED" => IntegrationOperationStatus::Published, "MATERIALIZED" => IntegrationOperationStatus::Materialized, "DEAD_LETTER" => IntegrationOperationStatus::DeadLetter, _ => return Err(DbError::Query("unknown integration operation status".to_string())) };
            Ok(Some(IntegrationOperationView {
                operation_id, event_id: raw16_to_uuid(&event_raw)?, event_type: row.get(1).map_err(map_error)?, aggregate_type: row.get(2).map_err(map_error)?, aggregate_id: raw16_to_uuid(&aggregate_raw)?, status, attempt_count: row.get(5).map_err(map_error)?, created_at: parse_time(row.get::<_, String>(6).map_err(map_error)?)?, published_at: row.get::<_, Option<String>>(7).map_err(map_error)?.map(parse_time).transpose()?, materialized_at: materialized_at.map(parse_time).transpose()?,
            }))
        }).await
    }
}

fn parse_time(value: String) -> DbResult<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(&value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|error| DbError::Query(format!("invalid operation timestamp: {error}")))
}
fn map_error(error: oracle::Error) -> DbError {
    DbError::Query(format!("failed to map integration operation: {error}"))
}
