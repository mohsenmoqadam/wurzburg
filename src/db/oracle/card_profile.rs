use uuid::Uuid;

use crate::{
    db::{
        error::{DbError, DbResult},
        oracle::{OracleRepository, types::uuid_to_raw16},
    },
    kafka::contract::RuntimeMaterializationReceipt,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CardProfileReceiptOutcome {
    Materialized,
    Replayed,
    Mismatch,
}

impl OracleRepository {
    #[tracing::instrument(skip(self, receipt), fields(db.system="oracle", db.operation.name="card_profile.receipt", operation.id=%receipt.operation_id, messaging.message.id=%receipt.receipt_event_id))]
    pub async fn apply_card_profile_receipt(
        &self,
        receipt: RuntimeMaterializationReceipt,
    ) -> DbResult<CardProfileReceiptOutcome> {
        self.pool
            .with_transaction("card profile materialization receipt", move |connection| {
                let duplicate = connection
                    .query_row_as::<i64>(
                        "SELECT COUNT(*) FROM runtime_materialization_receipts WHERE receipt_event_id=:1",
                        &[&raw(receipt.receipt_event_id)],
                    )
                    .map_err(|error| query("failed to check card receipt replay", error))?;
                if duplicate > 0 {
                    return Ok(CardProfileReceiptOutcome::Replayed);
                }

                let payload = serde_json::to_string(&receipt).map_err(|error| {
                    DbError::Query(format!("failed to serialize card receipt: {error}"))
                })?;
                connection.execute(
                    "INSERT INTO integration_inbox (inbox_event_id,source_system,source_event_id,event_type,aggregate_type,aggregate_id,payload_json) VALUES (:1,'WOLFSBURG',:2,'RUNTIME_PROFILE_MATERIALIZED','CARD',:3,:4)",
                    &[&raw(Uuid::new_v4()), &raw(receipt.receipt_event_id), &raw(receipt.aggregate_id), &payload],
                ).map_err(|error| query("failed to insert card receipt inbox", error))?;

                let expected = connection.query(
                    "SELECT state_version,card_number FROM cards WHERE card_id=:1 AND publication_operation_id=:2 FOR UPDATE",
                    &[&raw(receipt.aggregate_id), &raw(receipt.operation_id)],
                ).map_err(|error| query("failed to lock pending card projection", error))?
                    .next()
                    .transpose()
                    .map_err(|error| query("failed to read pending card projection", error))?
                    .map(|row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                        ))
                    })
                    .transpose()
                    .map_err(|error| query("failed to map card state version", error))?;
                let expected_key = expected
                    .as_ref()
                    .map(|(_, card_number)| format!("CP:{card_number}"));
                let matches = receipt.profile_type == "CP"
                    && receipt.profile_id.is_none()
                    && expected
                        .as_ref()
                        .is_some_and(|(version, _)| *version == receipt.materialized_version)
                    && expected_key.as_deref() == Some(receipt.runtime_key.as_str());
                if !matches {
                    let error = serde_json::json!({"code":"CARD_PROFILE_RECEIPT_MISMATCH"}).to_string();
                    connection.execute(
                        "UPDATE integration_inbox SET status='FAILED',processed_at=SYSTIMESTAMP,error_json=:1 WHERE source_system='WOLFSBURG' AND source_event_id=:2",
                        &[&error, &raw(receipt.receipt_event_id)],
                    ).map_err(|error| query("failed to reject card receipt", error))?;
                    return Ok(CardProfileReceiptOutcome::Mismatch);
                }

                let at = receipt.materialized_at.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
                connection.execute(
                    "INSERT INTO runtime_materialization_receipts (runtime_materialization_receipt_id,receipt_event_id,operation_id,profile_type,aggregate_id,profile_id,materialized_version,redis_key,materialized_at) VALUES (:1,:2,:3,'CP',:4,NULL,:5,:6,TO_TIMESTAMP_TZ(:7,'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'))",
                    &[&raw(Uuid::new_v4()), &raw(receipt.receipt_event_id), &raw(receipt.operation_id), &raw(receipt.aggregate_id), &receipt.materialized_version, &receipt.runtime_key, &at],
                ).map_err(|error| query("failed to insert card materialization receipt", error))?;
                let statement = connection.execute(
                    "UPDATE cards SET materialized_version=:1,materialized_at=TO_TIMESTAMP_TZ(:2,'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'),publication_operation_id=NULL,updated_at=SYSTIMESTAMP WHERE card_id=:3 AND publication_operation_id=:4 AND state_version=:1",
                    &[&receipt.materialized_version, &at, &raw(receipt.aggregate_id), &raw(receipt.operation_id)],
                ).map_err(|error| query("failed to finalize card materialization", error))?;
                if statement.row_count().map_err(|error| query("failed to inspect card receipt update", error))? != 1 {
                    return Err(DbError::Conflict("card projection changed during receipt processing".to_string()));
                }
                connection.execute(
                    "UPDATE integration_inbox SET status='PROCESSED',processed_at=SYSTIMESTAMP WHERE source_system='WOLFSBURG' AND source_event_id=:1",
                    &[&raw(receipt.receipt_event_id)],
                ).map_err(|error| query("failed to complete card receipt inbox", error))?;
                Ok(CardProfileReceiptOutcome::Materialized)
            })
            .await
    }
}

fn raw(value: Uuid) -> Vec<u8> {
    uuid_to_raw16(value).to_vec()
}

fn query(context: &str, error: oracle::Error) -> DbError {
    DbError::Query(format!("{context}: {error}"))
}
