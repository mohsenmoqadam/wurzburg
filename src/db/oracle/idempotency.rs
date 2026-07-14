use anyhow::{Result, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use oracle::Row;
use uuid::Uuid;

use crate::{
    db::{
        oracle::{
            OracleRepository,
            transaction::is_unique_constraint_violation,
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
    ) -> Result<Option<IdempotencyRecord>> {
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
                        crate::db::error::DbError::Query(format!(
                            "failed to fetch idempotency record: {error}"
                        ))
                    })?;

                match rows.next() {
                    Some(Ok(row)) => map_idempotency_row(&row).map(Some).map_err(|error| {
                        crate::db::error::DbError::Query(format!(
                            "invalid idempotency row: {error}"
                        ))
                    }),
                    Some(Err(error)) => Err(crate::db::error::DbError::Query(format!(
                        "failed to read idempotency record: {error}"
                    ))),
                    None => Ok(None),
                }
            })
            .await
            .map_err(Into::into)
    }

    async fn create_idempotency_record(&self, record: NewIdempotencyRecord) -> Result<()> {
        self.pool
            .with_transaction("idempotency record creation", move |connection| {
                let id = uuid_to_raw16(record.id).to_vec();
                connection
                    .execute(
                        r#"
                        INSERT INTO idempotency_records (
                            id, operation_type, idempotency_key, request_hash,
                            status, created_by_subject
                        )
                        VALUES (:1, :2, :3, :4, 'IN_PROGRESS', :5)
                        "#,
                        &[
                            &id,
                            &record.operation_type,
                            &record.idempotency_key,
                            &record.request_hash,
                            &record.created_by_subject,
                        ],
                    )
                    .map_err(|error| {
                        if is_unique_constraint_violation(&error) {
                            crate::db::error::DbError::Conflict(
                                "idempotency record already exists".to_string(),
                            )
                        } else {
                            crate::db::error::DbError::Query(format!(
                                "failed to create idempotency record: {error}"
                            ))
                        }
                    })?;
                Ok(())
            })
            .await
            .map_err(Into::into)
    }

    async fn complete_idempotency_record(
        &self,
        operation_type: &str,
        idempotency_key: &str,
        resource_type: &str,
        resource_id: Uuid,
        response_snapshot: serde_json::Value,
    ) -> Result<()> {
        let operation_type = operation_type.to_string();
        let idempotency_key = idempotency_key.to_string();
        let resource_type = resource_type.to_string();
        let resource_id = uuid_to_raw16(resource_id).to_vec();
        let response_snapshot = response_snapshot.to_string();

        self.pool
            .with_transaction("idempotency record completion", move |connection| {
                connection
                    .execute(
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
                        "#,
                        &[
                            &resource_type,
                            &resource_id,
                            &response_snapshot,
                            &operation_type,
                            &idempotency_key,
                        ],
                    )
                    .map_err(|error| {
                        crate::db::error::DbError::Query(format!(
                            "failed to complete idempotency record: {error}"
                        ))
                    })?;
                Ok(())
            })
            .await
            .map_err(Into::into)
    }
}

fn idempotency_select_sql() -> &'static str {
    r#"
    SELECT
        id,
        operation_type,
        idempotency_key,
        request_hash,
        status,
        resource_type,
        resource_id,
        JSON_SERIALIZE(response_snapshot RETURNING CLOB) AS response_snapshot,
        created_by_subject,
        TO_CHAR(SYS_EXTRACT_UTC(created_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS created_at,
        TO_CHAR(SYS_EXTRACT_UTC(updated_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS updated_at,
        TO_CHAR(SYS_EXTRACT_UTC(completed_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS completed_at
    FROM idempotency_records
    WHERE operation_type = :1
      AND idempotency_key = :2
    "#
}

fn map_idempotency_row(row: &Row) -> Result<IdempotencyRecord> {
    let id: Vec<u8> = row.get(0)?;
    let status: String = row.get(4)?;
    let resource_id: Option<Vec<u8>> = row.get(6)?;
    let response_snapshot: Option<String> = row.get(7)?;
    let created_at: String = row.get(9)?;
    let updated_at: String = row.get(10)?;
    let completed_at: Option<String> = row.get(11)?;

    Ok(IdempotencyRecord {
        id: raw16_to_uuid(&id)?,
        operation_type: row.get(1)?,
        idempotency_key: row.get(2)?,
        request_hash: row.get(3)?,
        status: IdempotencyStatus::from_db_value(&status)
            .ok_or_else(|| anyhow!("unknown idempotency status `{status}`"))?,
        resource_type: row.get(5)?,
        resource_id: resource_id.as_deref().map(raw16_to_uuid).transpose()?,
        response_snapshot: response_snapshot
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?,
        created_by_subject: row.get(8)?,
        created_at: parse_utc(&created_at)?,
        updated_at: parse_utc(&updated_at)?,
        completed_at: completed_at.as_deref().map(parse_utc).transpose()?,
    })
}

fn parse_utc(value: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)?.with_timezone(&Utc))
}
