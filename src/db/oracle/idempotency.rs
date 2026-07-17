use async_trait::async_trait;
use chrono::{DateTime, Utc};
use oracle::Row;
use uuid::Uuid;

use crate::{
    db::{
        error::{DbError, DbResult},
        oracle::{
            OracleRepository,
            types::{raw16_to_uuid, uuid_to_raw16},
        },
        traits::IdempotencyRepository,
    },
    domain::idempotency::{IdempotencyRecord, IdempotencyStatus, NewIdempotencyRecord},
};

#[async_trait]
impl IdempotencyRepository for OracleRepository {
    async fn get_idempotency_record(
        &self,
        operation_type: &str,
        idempotency_key: &str,
    ) -> DbResult<Option<IdempotencyRecord>> {
        let operation_type = operation_type.to_string();
        let idempotency_key = idempotency_key.to_string();

        self.pool
            .with_connection(move |connection| {
                let mut rows = connection
                    .query(
                        idempotency_select_sql(),
                        &[&operation_type, &idempotency_key],
                    )
                    .map_err(|error| {
                        DbError::Query(format!("failed to fetch idempotency record: {error}"))
                    })?;

                match rows.next() {
                    Some(Ok(row)) => map_idempotency_row(&row).map(Some),
                    Some(Err(error)) => Err(DbError::Query(format!(
                        "failed to read idempotency record: {error}"
                    ))),
                    None => Ok(None),
                }
            })
            .await
    }

    async fn create_idempotency_record(&self, record: NewIdempotencyRecord) -> DbResult<()> {
        self.pool
            .with_transaction("idempotency record creation", move |connection| {
                let idempotency_record_id = uuid_to_raw16(record.idempotency_record_id).to_vec();
                let actor_provider_id = record
                    .actor_provider_id
                    .map(|value| uuid_to_raw16(value).to_vec());
                let actor_user_id = record
                    .actor_user_id
                    .map(|value| uuid_to_raw16(value).to_vec());

                connection
                    .execute(
                        idempotency_insert_sql(),
                        &[
                            &idempotency_record_id,
                            &record.operation_type,
                            &record.idempotency_key,
                            &record.request_hash,
                            &IdempotencyStatus::InProgress.as_db_value(),
                            &record.created_by_subject,
                            &record.created_by_client_id,
                            &actor_provider_id,
                            &actor_user_id,
                            &record.correlation_id,
                            &record.request_id,
                        ],
                    )
                    .map_err(|error| {
                        if is_unique_constraint_violation(&error) {
                            DbError::Conflict("idempotency record already exists".to_string())
                        } else {
                            DbError::Query(format!("failed to create idempotency record: {error}"))
                        }
                    })?;

                Ok(())
            })
            .await
    }

    async fn complete_idempotency_record(
        &self,
        operation_type: &str,
        idempotency_key: &str,
        resource_type: &str,
        resource_id: Uuid,
        response_snapshot: serde_json::Value,
    ) -> DbResult<()> {
        let operation_type = operation_type.to_string();
        let idempotency_key = idempotency_key.to_string();
        let resource_type = resource_type.to_string();
        let resource_id = uuid_to_raw16(resource_id).to_vec();
        let response_snapshot = response_snapshot.to_string();

        self.pool
            .with_transaction("idempotency record completion", move |connection| {
                let statement = connection
                    .execute(
                        idempotency_complete_sql(),
                        &[
                            &resource_type,
                            &resource_id,
                            &response_snapshot,
                            &operation_type,
                            &idempotency_key,
                        ],
                    )
                    .map_err(|error| {
                        DbError::Query(format!("failed to complete idempotency record: {error}"))
                    })?;

                let updated = statement.row_count().map_err(|error| {
                    DbError::Query(format!(
                        "failed to read idempotency completion row count: {error}"
                    ))
                })?;

                if updated == 0 {
                    return Err(DbError::Query(
                        "idempotency record was not found for completion".to_string(),
                    ));
                }

                Ok(())
            })
            .await
    }
}

pub(crate) fn idempotency_insert_sql() -> &'static str {
    r#"
    INSERT INTO idempotency_records (
        idempotency_record_id,
        operation_type,
        idempotency_key,
        request_hash,
        status,
        created_by_subject,
        created_by_client_id,
        actor_provider_id,
        actor_user_id,
        correlation_id,
        request_id
    )
    VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)
    "#
}

pub(crate) fn idempotency_complete_sql() -> &'static str {
    r#"
    UPDATE idempotency_records
    SET status = 'COMPLETED',
        resource_type = :1,
        resource_id = :2,
        response_snapshot = :3,
        updated_at = SYSTIMESTAMP,
        completed_at = SYSTIMESTAMP
    WHERE operation_type = :4
      AND idempotency_key = :5
      AND status = 'IN_PROGRESS'
    "#
}

pub(crate) fn idempotency_select_sql() -> &'static str {
    r#"
    SELECT
        idempotency_record_id,
        operation_type,
        idempotency_key,
        request_hash,
        status,
        resource_type,
        resource_id,
        JSON_SERIALIZE(response_snapshot RETURNING CLOB) AS response_snapshot,
        JSON_SERIALIZE(error_snapshot RETURNING CLOB) AS error_snapshot,
        created_by_subject,
        created_by_client_id,
        actor_provider_id,
        actor_user_id,
        correlation_id,
        request_id,
        TO_CHAR(SYS_EXTRACT_UTC(created_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS created_at,
        TO_CHAR(SYS_EXTRACT_UTC(updated_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS updated_at,
        TO_CHAR(SYS_EXTRACT_UTC(completed_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS completed_at
    FROM idempotency_records
    WHERE operation_type = :1
      AND idempotency_key = :2
    "#
}

fn map_idempotency_row(row: &Row) -> DbResult<IdempotencyRecord> {
    let idempotency_record_id: Vec<u8> = row.get(0).map_err(read_error)?;
    let status: String = row.get(4).map_err(read_error)?;
    let resource_id: Option<Vec<u8>> = row.get(6).map_err(read_error)?;
    let response_snapshot: Option<String> = row.get(7).map_err(read_error)?;
    let error_snapshot: Option<String> = row.get(8).map_err(read_error)?;
    let actor_provider_id: Option<Vec<u8>> = row.get(11).map_err(read_error)?;
    let actor_user_id: Option<Vec<u8>> = row.get(12).map_err(read_error)?;
    let created_at: String = row.get(15).map_err(read_error)?;
    let updated_at: String = row.get(16).map_err(read_error)?;
    let completed_at: Option<String> = row.get(17).map_err(read_error)?;

    Ok(IdempotencyRecord {
        idempotency_record_id: raw16_to_uuid(&idempotency_record_id)?,
        operation_type: row.get(1).map_err(read_error)?,
        idempotency_key: row.get(2).map_err(read_error)?,
        request_hash: row.get(3).map_err(read_error)?,
        status: IdempotencyStatus::from_db_value(&status)
            .ok_or_else(|| DbError::Query(format!("unknown idempotency status `{status}`")))?,
        resource_type: row.get(5).map_err(read_error)?,
        resource_id: resource_id.as_deref().map(raw16_to_uuid).transpose()?,
        response_snapshot: response_snapshot
            .as_deref()
            .map(parse_json_snapshot)
            .transpose()?,
        error_snapshot: error_snapshot
            .as_deref()
            .map(parse_json_snapshot)
            .transpose()?,
        created_by_subject: row.get(9).map_err(read_error)?,
        created_by_client_id: row.get(10).map_err(read_error)?,
        actor_provider_id: actor_provider_id
            .as_deref()
            .map(raw16_to_uuid)
            .transpose()?,
        actor_user_id: actor_user_id.as_deref().map(raw16_to_uuid).transpose()?,
        correlation_id: row.get(13).map_err(read_error)?,
        request_id: row.get(14).map_err(read_error)?,
        created_at: parse_utc(&created_at)?,
        updated_at: parse_utc(&updated_at)?,
        completed_at: completed_at.as_deref().map(parse_utc).transpose()?,
    })
}

fn parse_json_snapshot(value: &str) -> DbResult<serde_json::Value> {
    serde_json::from_str(value)
        .map_err(|error| DbError::Query(format!("invalid JSON snapshot in Oracle row: {error}")))
}

fn parse_utc(value: &str) -> DbResult<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)
        .map_err(|error| {
            DbError::Query(format!("invalid Oracle UTC timestamp `{value}`: {error}"))
        })?
        .with_timezone(&Utc))
}

fn read_error(error: oracle::Error) -> DbError {
    DbError::Query(format!("failed to read Oracle idempotency row: {error}"))
}

fn is_unique_constraint_violation(error: &oracle::Error) -> bool {
    error.to_string().contains("ORA-00001")
}

#[cfg(test)]
mod tests {
    use super::{idempotency_complete_sql, idempotency_insert_sql, idempotency_select_sql};

    #[test]
    fn insert_sql_persists_trusted_request_context() {
        let sql = idempotency_insert_sql();

        assert!(sql.contains("created_by_subject"));
        assert!(sql.contains("created_by_client_id"));
        assert!(sql.contains("actor_provider_id"));
        assert!(sql.contains("actor_user_id"));
        assert!(sql.contains("correlation_id"));
        assert!(sql.contains("request_id"));
    }

    #[test]
    fn completion_sql_only_completes_in_progress_record() {
        let sql = idempotency_complete_sql();

        assert!(sql.contains("status = 'COMPLETED'"));
        assert!(sql.contains("AND status = 'IN_PROGRESS'"));
        assert!(sql.contains("completed_at = SYSTIMESTAMP"));
    }

    #[test]
    fn select_sql_serializes_json_snapshots() {
        let sql = idempotency_select_sql();

        assert!(sql.contains("JSON_SERIALIZE(response_snapshot"));
        assert!(sql.contains("JSON_SERIALIZE(error_snapshot"));
    }
}
