use oracle::Row;
use serde_json::Value;
use uuid::Uuid;

use crate::{
    db::{
        error::{DbError, DbResult},
        oracle::{OracleRepository, types::raw16_to_uuid},
    },
    messaging::contract::{InternalEventEnvelope, InternalEventHeaders},
};

/// Creates the durable parent for one or more ordered integration events.
///
/// This must run in the same Oracle transaction as the business mutation and
/// all associated outbox inserts. Runtime publication is never used as proof
/// that the canonical business mutation committed.
pub(crate) fn insert_integration_operation(
    connection: &oracle::Connection,
    operation_id: Uuid,
    operation_type: &str,
    aggregate_type: &str,
    aggregate_id: Uuid,
    event_count: u32,
) -> DbResult<()> {
    connection
        .execute(
            "INSERT INTO integration_operations (operation_id,operation_type,aggregate_type,aggregate_id,status,event_count,published_event_count) VALUES (:1,:2,:3,:4,'PENDING',:5,0)",
            &[
                &crate::db::oracle::types::uuid_to_raw16(operation_id).to_vec(),
                &operation_type,
                &aggregate_type,
                &crate::db::oracle::types::uuid_to_raw16(aggregate_id).to_vec(),
                &i64::from(event_count),
            ],
        )
        .map_err(|error| {
            DbError::Query(format!("failed to insert integration operation: {error}"))
        })?;
    Ok(())
}

pub(crate) fn mark_integration_operation_materialized(
    connection: &oracle::Connection,
    operation_id: Uuid,
) -> DbResult<()> {
    let statement = connection
        .execute(
            "UPDATE integration_operations SET status='MATERIALIZED',materialized_at=SYSTIMESTAMP,updated_at=SYSTIMESTAMP,completed_at=SYSTIMESTAMP,safe_error_code=NULL WHERE operation_id=:1 AND status IN ('PENDING','PUBLISHING','PUBLISHED','MATERIALIZED')",
            &[&crate::db::oracle::types::uuid_to_raw16(operation_id).to_vec()],
        )
        .map_err(|error| {
            DbError::Query(format!("failed to materialize integration operation: {error}"))
        })?;
    if statement.row_count().map_err(|error| {
        DbError::Query(format!(
            "failed to inspect operation materialization: {error}"
        ))
    })? != 1
    {
        return Err(DbError::Conflict(
            "integration operation cannot accept materialization".to_string(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct ClaimedOutboxEvent {
    pub event_id: Uuid,
    pub operation_id: Uuid,
    pub event_type: String,
    pub schema_version: u16,
    pub delivery: ClaimedOutboxDelivery,
    pub payload: Value,
    pub headers: InternalEventHeaders,
    pub partition_key: String,
    pub attempt_count: u32,
}

#[derive(Debug, Clone)]
pub enum ClaimedOutboxDelivery {
    Internal,
    Provider { provider_id: Uuid, topic: String },
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
                    let event_id = crate::db::oracle::types::uuid_to_raw16(event.event_id).to_vec();
                    connection
                        .execute(
                            "UPDATE integration_outbox SET status = 'PUBLISHING', attempt_count = attempt_count + 1, locked_by = :1, locked_until = SYSTIMESTAMP + NUMTODSINTERVAL(:2 / 1000, 'SECOND'), updated_at = SYSTIMESTAMP WHERE outbox_event_id = :3",
                            &[&worker_id, &(lease_duration_ms as i64), &event_id],
                        )
                        .map_err(|error| DbError::Query(format!("failed to lease outbox event: {error}")))?;
                    connection.execute(
                        "UPDATE integration_operations SET status=CASE WHEN status='PENDING' THEN 'PUBLISHING' ELSE status END,updated_at=SYSTIMESTAMP WHERE operation_id=:1",
                        &[&crate::db::oracle::types::uuid_to_raw16(event.operation_id).to_vec()],
                    ).map_err(|error| DbError::Query(format!("failed to mark integration operation publishing: {error}")))?;
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
                connection.execute(
                    "UPDATE integration_operations SET published_event_count=(SELECT COUNT(*) FROM integration_outbox WHERE operation_id=integration_operations.operation_id AND status IN ('PUBLISHED','SUPPRESSED')),status=CASE WHEN status='MATERIALIZED' THEN 'MATERIALIZED' WHEN event_count=(SELECT COUNT(*) FROM integration_outbox WHERE operation_id=integration_operations.operation_id AND status IN ('PUBLISHED','SUPPRESSED')) THEN 'PUBLISHED' ELSE 'PUBLISHING' END,updated_at=SYSTIMESTAMP WHERE operation_id=(SELECT operation_id FROM integration_outbox WHERE outbox_event_id=:1)",
                    &[&event_id],
                ).map_err(|error| DbError::Query(format!("failed to update integration operation publication: {error}")))?;
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
                if dead_letter {
                    connection.execute(
                        "UPDATE integration_operations SET status=CASE WHEN status='MATERIALIZED' THEN status ELSE 'DEAD_LETTER' END,safe_error_code=CASE WHEN status='MATERIALIZED' THEN safe_error_code ELSE 'KAFKA_DELIVERY_ATTEMPTS_EXHAUSTED' END,updated_at=SYSTIMESTAMP WHERE operation_id=(SELECT operation_id FROM integration_outbox WHERE outbox_event_id=:1)",
                        &[&event_id],
                    ).map_err(|error| DbError::Query(format!("failed to dead-letter integration operation: {error}")))?;
                }
                Ok(())
            })
            .await
    }
}

pub(crate) fn outbox_claim_select_sql() -> &'static str {
    r#"
    SELECT o.outbox_event_id, o.operation_id, o.event_type, o.aggregate_type,
           o.aggregate_id, o.partition_key,
           JSON_SERIALIZE(o.payload_json RETURNING CLOB) AS payload_json,
           JSON_SERIALIZE(o.headers_json RETURNING CLOB) AS headers_json,
           o.attempt_count, o.schema_version, o.delivery_channel,
           o.provider_id, k.topic_name
    FROM integration_outbox o
    LEFT JOIN provider_kafka_access k
      ON k.provider_id = o.provider_id
     AND k.credential_status = 'ACTIVE'
    WHERE o.status = 'PENDING'
      AND (o.next_attempt_at IS NULL OR o.next_attempt_at <= SYSTIMESTAMP)
      AND (o.delivery_channel = 'INTERNAL' OR k.topic_name IS NOT NULL)
    ORDER BY o.created_at, o.outbox_event_id
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

    let schema_version: i64 = row.get(9).map_err(row_error)?;
    let schema_version = u16::try_from(schema_version)
        .map_err(|_| DbError::Query("stored outbox schema version is invalid".to_string()))?;
    let delivery_channel: String = row.get(10).map_err(row_error)?;
    let payload: Value = serde_json::from_str(&payload_json)
        .map_err(|_| DbError::Query("stored outbox payload is invalid JSON".to_string()))?;
    let delivery = match delivery_channel.as_str() {
        "INTERNAL" => {
            let envelope: InternalEventEnvelope<Value> = serde_json::from_value(payload.clone())
                .map_err(|_| {
                    DbError::Query(
                        "stored outbox payload violates the internal event contract".to_string(),
                    )
                })?;
            if envelope.event_id != outbox_event_id
                || envelope.operation_id != operation_id
                || envelope.event_type != event_type
                || envelope.aggregate_type != aggregate_type
                || envelope.aggregate_id != aggregate_id
            {
                return Err(DbError::Query(
                    "stored internal outbox envelope does not match indexed event columns"
                        .to_string(),
                ));
            }
            ClaimedOutboxDelivery::Internal
        }
        "PROVIDER" => {
            let provider_raw: Option<Vec<u8>> = row.get(11).map_err(row_error)?;
            let topic: Option<String> = row.get(12).map_err(row_error)?;
            let provider_id = provider_raw
                .as_deref()
                .map(raw16_to_uuid)
                .transpose()?
                .ok_or_else(|| {
                    DbError::Query("provider outbox event has no provider identity".to_string())
                })?;
            let topic = topic.ok_or_else(|| {
                DbError::Conflict("provider Kafka access is not active".to_string())
            })?;
            let payload_event_id = payload.get("event_id").and_then(Value::as_str);
            let payload_event_type = payload.get("event_type").and_then(Value::as_str);
            let payload_version = payload.get("schema_version").and_then(Value::as_u64);
            let payload_provider = payload.get("provider_id").and_then(Value::as_str);
            if payload_event_id != Some(outbox_event_id.to_string().as_str())
                || payload_event_type != Some(event_type.as_str())
                || payload_version != Some(u64::from(schema_version))
                || payload_provider != Some(provider_id.to_string().as_str())
            {
                return Err(DbError::Query(
                    "stored provider outbox envelope does not match indexed event columns"
                        .to_string(),
                ));
            }
            ClaimedOutboxDelivery::Provider { provider_id, topic }
        }
        _ => {
            return Err(DbError::Query(
                "stored outbox delivery channel is invalid".to_string(),
            ));
        }
    };

    let headers = headers_json
        .map(|value| serde_json::from_str(&value))
        .transpose()
        .map_err(|_| {
            DbError::Query("stored outbox headers violate the internal event contract".to_string())
        })?
        .unwrap_or_default();
    Ok(ClaimedOutboxEvent {
        event_id: outbox_event_id,
        operation_id,
        event_type,
        schema_version,
        delivery,
        payload,
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
        assert!(sql.contains("JSON_SERIALIZE(o.payload_json RETURNING CLOB)"));
        assert!(sql.contains("JSON_SERIALIZE(o.headers_json RETURNING CLOB)"));
        assert!(sql.contains("o.delivery_channel = 'INTERNAL' OR k.topic_name IS NOT NULL"));
        assert!(sql.contains("FOR UPDATE SKIP LOCKED"));
    }
}
