use oracle::Row;
use serde_json::Value;
use uuid::Uuid;

use crate::{
    db::{
        error::{DbError, DbResult},
        oracle::{OracleRepository, types::raw16_to_uuid},
    },
    kafka::contract::{InternalEventEnvelope, InternalEventHeaders},
};

#[derive(Debug, Clone)]
pub struct ClaimedOutboxEvent {
    pub envelope: InternalEventEnvelope<Value>,
    pub headers: InternalEventHeaders,
    pub partition_key: String,
    pub attempt_count: u32,
}

impl OracleRepository {
    #[tracing::instrument(
        skip(self),
        fields(db.system = "oracle", db.operation.name = "integration_outbox.claim")
    )]
    pub async fn claim_outbox_batch(
        &self,
        worker_id: String,
        batch_size: u16,
        lease_duration_ms: u64,
    ) -> DbResult<Vec<ClaimedOutboxEvent>> {
        self.pool
            .with_transaction("claim integration outbox batch", move |connection| {
                connection
                    .execute(
                        "UPDATE integration_outbox SET status = 'PENDING', locked_by = NULL, locked_until = NULL, updated_at = SYSTIMESTAMP WHERE status = 'PUBLISHING' AND locked_until < SYSTIMESTAMP",
                        &[],
                    )
                    .map_err(|error| DbError::Query(format!("failed to recover expired outbox leases: {error}")))?;

                let mut rows = connection
                    .query(
                        outbox_claim_select_sql(),
                        &[],
                    )
                    .map_err(|error| DbError::Query(format!("failed to select outbox events: {error}")))?;

                let mut claimed = Vec::with_capacity(usize::from(batch_size));
                while claimed.len() < usize::from(batch_size) {
                    let Some(row) = rows.next() else { break };
                    let row = row.map_err(|error| DbError::Query(format!("failed to read outbox event: {error}")))?;
                    let event = map_outbox_event(&row)?;
                    let event_id = crate::db::oracle::types::uuid_to_raw16(event.envelope.event_id).to_vec();
                    connection
                        .execute(
                            "UPDATE integration_outbox SET status = 'PUBLISHING', attempt_count = attempt_count + 1, locked_by = :1, locked_until = SYSTIMESTAMP + NUMTODSINTERVAL(:2 / 1000, 'SECOND'), updated_at = SYSTIMESTAMP WHERE outbox_event_id = :3",
                            &[&worker_id, &(lease_duration_ms as i64), &event_id],
                        )
                        .map_err(|error| DbError::Query(format!("failed to lease outbox event: {error}")))?;
                    claimed.push(ClaimedOutboxEvent {
                        attempt_count: event.attempt_count.saturating_add(1),
                        ..event
                    });
                }
                Ok(claimed)
            })
            .await
    }

    #[tracing::instrument(
        skip(self),
        fields(db.system = "oracle", db.operation.name = "integration_outbox.published", messaging.message.id = %event_id)
    )]
    pub async fn mark_outbox_published(&self, event_id: Uuid, worker_id: String) -> DbResult<()> {
        self.pool
            .with_transaction("complete integration outbox publication", move |connection| {
                let event_id = crate::db::oracle::types::uuid_to_raw16(event_id).to_vec();
                let statement = connection
                    .execute(
                        "UPDATE integration_outbox SET status = 'PUBLISHED', published_at = SYSTIMESTAMP, locked_by = NULL, locked_until = NULL, dead_letter_reason = NULL, updated_at = SYSTIMESTAMP WHERE outbox_event_id = :1 AND status = 'PUBLISHING' AND locked_by = :2",
                        &[&event_id, &worker_id],
                    )
                    .map_err(|error| DbError::Query(format!("failed to complete outbox event: {error}")))?;
                if statement.row_count().map_err(|error| DbError::Query(format!("failed to inspect outbox completion: {error}")))? != 1 {
                    return Err(DbError::Conflict("outbox publication lease is no longer owned by this worker".to_string()));
                }
                Ok(())
            })
            .await
    }

    #[tracing::instrument(
        skip(self),
        fields(db.system = "oracle", db.operation.name = "integration_outbox.retry", messaging.message.id = %event_id)
    )]
    pub async fn reschedule_outbox_event(
        &self,
        event_id: Uuid,
        worker_id: String,
        dead_letter: bool,
        backoff_ms: u64,
    ) -> DbResult<()> {
        self.pool
            .with_transaction("reschedule integration outbox publication", move |connection| {
                let event_id = crate::db::oracle::types::uuid_to_raw16(event_id).to_vec();
                let statement = if dead_letter {
                    connection.execute(
                        "UPDATE integration_outbox SET status='DEAD_LETTER', next_attempt_at=NULL, locked_by=NULL, locked_until=NULL, dead_letter_reason='KAFKA_DELIVERY_ATTEMPTS_EXHAUSTED', updated_at=SYSTIMESTAMP WHERE outbox_event_id=:1 AND status='PUBLISHING' AND locked_by=:2",
                        &[&event_id, &worker_id],
                    )
                } else {
                    connection.execute(
                        "UPDATE integration_outbox SET status='PENDING', next_attempt_at=SYSTIMESTAMP + NUMTODSINTERVAL(:1 / 1000, 'SECOND'), locked_by=NULL, locked_until=NULL, dead_letter_reason=NULL, updated_at=SYSTIMESTAMP WHERE outbox_event_id=:2 AND status='PUBLISHING' AND locked_by=:3",
                        &[&(backoff_ms as i64), &event_id, &worker_id],
                    )
                }
                    .map_err(|error| DbError::Query(format!("failed to reschedule outbox event: {error}")))?;
                if statement.row_count().map_err(|error| DbError::Query(format!("failed to inspect outbox retry: {error}")))? != 1 {
                    return Err(DbError::Conflict("outbox retry lease is no longer owned by this worker".to_string()));
                }
                Ok(())
            })
            .await
    }
}

pub(crate) fn outbox_claim_select_sql() -> &'static str {
    r#"
    SELECT outbox_event_id, operation_id, event_type, aggregate_type,
           aggregate_id, partition_key,
           JSON_SERIALIZE(payload_json RETURNING CLOB) AS payload_json,
           JSON_SERIALIZE(headers_json RETURNING CLOB) AS headers_json,
           attempt_count
    FROM integration_outbox
    WHERE status = 'PENDING'
      AND (next_attempt_at IS NULL OR next_attempt_at <= SYSTIMESTAMP)
    ORDER BY created_at, outbox_event_id
    FOR UPDATE SKIP LOCKED
    "#
}

fn map_outbox_event(row: &Row) -> DbResult<ClaimedOutboxEvent> {
    let outbox_event_id_raw: Vec<u8> = row.get(0).map_err(row_error)?;
    let operation_id_raw: Vec<u8> = row.get(1).map_err(row_error)?;
    let outbox_event_id = raw16_to_uuid(&outbox_event_id_raw)?;
    let operation_id = raw16_to_uuid(&operation_id_raw)?;
    let event_type: String = row.get(2).map_err(row_error)?;
    let aggregate_type: String = row.get(3).map_err(row_error)?;
    let aggregate_id_raw: Vec<u8> = row.get(4).map_err(row_error)?;
    let aggregate_id = raw16_to_uuid(&aggregate_id_raw)?;
    let partition_key: String = row.get(5).map_err(row_error)?;
    let payload_json: String = row.get(6).map_err(row_error)?;
    let headers_json: Option<String> = row.get(7).map_err(row_error)?;
    let attempt_count: i64 = row.get(8).map_err(row_error)?;

    let mut envelope: InternalEventEnvelope<Value> =
        serde_json::from_str(&payload_json).map_err(|_| {
            DbError::Query("stored outbox payload violates the internal event contract".to_string())
        })?;
    if envelope.event_id != outbox_event_id
        || envelope.operation_id != operation_id
        || envelope.event_type != event_type
        || envelope.aggregate_type != aggregate_type
        || envelope.aggregate_id != aggregate_id
    {
        return Err(DbError::Query(
            "stored outbox envelope does not match indexed event columns".to_string(),
        ));
    }
    // Canonicalize producer casing from legacy rows without changing identity.
    envelope.producer = envelope.producer.to_ascii_lowercase();

    let headers = headers_json
        .map(|value| serde_json::from_str(&value))
        .transpose()
        .map_err(|_| {
            DbError::Query("stored outbox headers violate the internal event contract".to_string())
        })?
        .unwrap_or_default();
    Ok(ClaimedOutboxEvent {
        envelope,
        headers,
        partition_key,
        attempt_count: attempt_count.try_into().unwrap_or(u32::MAX),
    })
}

fn row_error(error: oracle::Error) -> DbError {
    DbError::Query(format!("failed to map outbox event: {error}"))
}

#[cfg(test)]
mod tests {
    use super::outbox_claim_select_sql;

    #[test]
    fn claim_query_serializes_native_oracle_json_for_the_driver() {
        let sql = outbox_claim_select_sql();
        assert!(sql.contains("JSON_SERIALIZE(payload_json RETURNING CLOB)"));
        assert!(sql.contains("JSON_SERIALIZE(headers_json RETURNING CLOB)"));
        assert!(sql.contains("FOR UPDATE SKIP LOCKED"));
    }
}
