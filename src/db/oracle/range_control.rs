use uuid::Uuid;

use crate::{
    db::{
        error::{DbError, DbResult},
        oracle::{
            OracleRepository,
            audit::insert_audit_log,
            card_range::fetch_card_range,
            types::{raw16_to_uuid, uuid_to_raw16},
        },
    },
    domain::audit::{AuditAction, NewAuditLog, TrustedAuditContext},
    messaging::contract::RuntimeMaterializationReceipt,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeControlReceiptOutcome {
    Materialized,
    Replayed,
    Mismatch,
}

impl OracleRepository {
    #[tracing::instrument(skip(self, receipt), fields(db.system="oracle", db.operation.name="range_control.receipt", operation.id=%receipt.operation_id, messaging.message.id=%receipt.receipt_event_id))]
    pub async fn apply_range_control_receipt(
        &self,
        receipt: RuntimeMaterializationReceipt,
    ) -> DbResult<RangeControlReceiptOutcome> {
        self.pool.with_transaction("range control materialization receipt", move |connection| {
            if let Some(outcome) = classify_existing_receipt(connection, &receipt)? {
                return Ok(outcome);
            }
            insert_inbox(connection, &receipt)?;
            let expected = lock_pending_range(connection, receipt.operation_id)?;
            let expected_key = format!("CRCTL:{}", receipt.aggregate_id);
            let matches = receipt.profile_type == "CRCTL"
                && receipt.profile_id.is_none()
                && expected.as_ref().is_some_and(|(range_id, version)| *range_id == receipt.aggregate_id && *version == receipt.materialized_version)
                && receipt.runtime_key == expected_key;
            if !matches {
                fail_inbox(connection, receipt.receipt_event_id, "RANGE_CONTROL_RECEIPT_MISMATCH")?;
                return Ok(RangeControlReceiptOutcome::Mismatch);
            }
            let before = fetch_card_range(connection, receipt.aggregate_id)?;
            insert_receipt(connection, &receipt)?;
            let statement = connection.execute("UPDATE card_ranges SET materialized_operational_version=:1, range_control_operation_id=NULL, updated_at=SYSTIMESTAMP WHERE card_range_id=:2 AND range_control_operation_id=:3 AND operational_version=:1", &[&receipt.materialized_version, &uuid_to_raw16(receipt.aggregate_id).to_vec(), &uuid_to_raw16(receipt.operation_id).to_vec()]).map_err(|error| DbError::Query(format!("failed to finalize range control: {error}")))?;
            if statement.row_count().map_err(|error| DbError::Query(format!("failed to inspect range control finalization: {error}")))? != 1 { return Err(DbError::Conflict("range control changed during receipt processing".to_string())); }
            complete_inbox(connection, receipt.receipt_event_id)?;
            super::outbox::mark_integration_operation_materialized(connection, receipt.operation_id)?;
            let after = fetch_card_range(connection, receipt.aggregate_id)?;
            insert_audit_log(connection, NewAuditLog {
                audit_log_id: Uuid::new_v4(),
                entity_type: "CARD_RANGE".to_string(),
                entity_id: receipt.aggregate_id,
                action_type: AuditAction::StateTransition,
                reason: Some("Wolfsburg confirmed CRCTL materialization".to_string()),
                old_values: Some(before.replay_snapshot()),
                new_values: Some(serde_json::json!({
                    "card_range": after.replay_snapshot(),
                    "receipt_event_id": receipt.receipt_event_id,
                    "operation_id": receipt.operation_id,
                    "materialized_version": receipt.materialized_version,
                })),
                context: TrustedAuditContext {
                    actor_subject: "wolfsburg".to_string(),
                    actor_client_id: Some("wolfsburg-materializer".to_string()),
                    actor_provider_id: None,
                    actor_user_id: None,
                    actor_issuer: Some("internal-kafka".to_string()),
                    source_ip: None,
                    correlation_id: receipt.operation_id.to_string(),
                    request_id: receipt.receipt_event_id.to_string(),
                },
            })?;
            Ok(RangeControlReceiptOutcome::Materialized)
        }).await
    }
}

fn classify_existing_receipt(
    connection: &oracle::Connection,
    receipt: &RuntimeMaterializationReceipt,
) -> DbResult<Option<RangeControlReceiptOutcome>> {
    let mut rows = connection.query(
        "SELECT receipt_event_id, operation_id, aggregate_id, materialized_version, redis_key FROM runtime_materialization_receipts WHERE receipt_event_id=:1 OR (operation_id=:2 AND profile_type='CRCTL' AND materialized_version=:3)",
        &[&uuid_to_raw16(receipt.receipt_event_id).to_vec(), &uuid_to_raw16(receipt.operation_id).to_vec(), &receipt.materialized_version],
    ).map_err(|error| DbError::Query(format!("failed to check range receipt replay: {error}")))?;
    if let Some(row) = rows.next() {
        let row = row.map_err(|error| {
            DbError::Query(format!("failed to read range receipt replay: {error}"))
        })?;
        let stored_event: Vec<u8> = row.get(0).map_err(map_error)?;
        let stored_operation: Vec<u8> = row.get(1).map_err(map_error)?;
        let stored_aggregate: Vec<u8> = row.get(2).map_err(map_error)?;
        let stored_version: i64 = row.get(3).map_err(map_error)?;
        let stored_key: String = row.get(4).map_err(map_error)?;
        let exact = receipt.profile_type == "CRCTL"
            && receipt.profile_id.is_none()
            && raw16_to_uuid(&stored_event)? == receipt.receipt_event_id
            && raw16_to_uuid(&stored_operation)? == receipt.operation_id
            && raw16_to_uuid(&stored_aggregate)? == receipt.aggregate_id
            && stored_version == receipt.materialized_version
            && stored_key == receipt.runtime_key;
        return Ok(Some(if exact {
            RangeControlReceiptOutcome::Replayed
        } else {
            RangeControlReceiptOutcome::Mismatch
        }));
    }
    let failed = connection.query_row_as::<i64>(
        "SELECT COUNT(*) FROM integration_inbox WHERE source_system='WOLFSBURG' AND source_event_id=:1 AND status='FAILED'",
        &[&uuid_to_raw16(receipt.receipt_event_id).to_vec()],
    ).map_err(|error| DbError::Query(format!("failed to check rejected range receipt: {error}")))?;
    Ok((failed > 0).then_some(RangeControlReceiptOutcome::Mismatch))
}

fn insert_inbox(
    connection: &oracle::Connection,
    receipt: &RuntimeMaterializationReceipt,
) -> DbResult<()> {
    let payload = serde_json::to_string(receipt)
        .map_err(|error| DbError::Query(format!("failed to serialize range receipt: {error}")))?;
    connection.execute("INSERT INTO integration_inbox (inbox_event_id, source_system, source_event_id, event_type, aggregate_type, aggregate_id, payload_json) VALUES (:1,'WOLFSBURG',:2,'RUNTIME_PROFILE_MATERIALIZED','CARD_RANGE',:3,:4)", &[&uuid_to_raw16(Uuid::new_v4()).to_vec(), &uuid_to_raw16(receipt.receipt_event_id).to_vec(), &uuid_to_raw16(receipt.aggregate_id).to_vec(), &payload]).map_err(|error| DbError::Query(format!("failed to insert range receipt inbox: {error}")))?;
    Ok(())
}

fn lock_pending_range(
    connection: &oracle::Connection,
    operation_id: Uuid,
) -> DbResult<Option<(Uuid, i64)>> {
    let mut rows = connection.query("SELECT card_range_id, operational_version FROM card_ranges WHERE range_control_operation_id=:1 FOR UPDATE", &[&uuid_to_raw16(operation_id).to_vec()]).map_err(|error| DbError::Query(format!("failed to lock pending range control: {error}")))?;
    match rows.next() {
        Some(Ok(row)) => {
            let raw: Vec<u8> = row.get(0).map_err(|error| {
                DbError::Query(format!("failed to map pending range id: {error}"))
            })?;
            let version: i64 = row.get(1).map_err(|error| {
                DbError::Query(format!("failed to map pending range version: {error}"))
            })?;
            Ok(Some((raw16_to_uuid(&raw)?, version)))
        }
        Some(Err(error)) => Err(DbError::Query(format!(
            "failed to read pending range control: {error}"
        ))),
        None => Ok(None),
    }
}

fn insert_receipt(
    connection: &oracle::Connection,
    receipt: &RuntimeMaterializationReceipt,
) -> DbResult<()> {
    let at = receipt
        .materialized_at
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string();
    connection.execute("INSERT INTO runtime_materialization_receipts (runtime_materialization_receipt_id, receipt_event_id, operation_id, profile_type, aggregate_id, profile_id, materialized_version, redis_key, materialized_at) VALUES (:1,:2,:3,'CRCTL',:4,NULL,:5,:6,TO_TIMESTAMP_TZ(:7,'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'))", &[&uuid_to_raw16(Uuid::new_v4()).to_vec(), &uuid_to_raw16(receipt.receipt_event_id).to_vec(), &uuid_to_raw16(receipt.operation_id).to_vec(), &uuid_to_raw16(receipt.aggregate_id).to_vec(), &receipt.materialized_version, &receipt.runtime_key, &at]).map_err(|error| DbError::Query(format!("failed to insert range materialization receipt: {error}")))?;
    Ok(())
}

fn complete_inbox(connection: &oracle::Connection, id: Uuid) -> DbResult<()> {
    connection.execute("UPDATE integration_inbox SET status='PROCESSED', processed_at=SYSTIMESTAMP WHERE source_system='WOLFSBURG' AND source_event_id=:1", &[&uuid_to_raw16(id).to_vec()]).map(|_| ()).map_err(|error| DbError::Query(format!("failed to complete range receipt inbox: {error}")))
}
fn fail_inbox(connection: &oracle::Connection, id: Uuid, code: &str) -> DbResult<()> {
    let error = serde_json::json!({"code":code}).to_string();
    connection.execute("UPDATE integration_inbox SET status='FAILED', processed_at=SYSTIMESTAMP, error_json=:1 WHERE source_system='WOLFSBURG' AND source_event_id=:2", &[&error, &uuid_to_raw16(id).to_vec()]).map(|_| ()).map_err(|error| DbError::Query(format!("failed to reject range receipt inbox: {error}")))
}

fn map_error(error: oracle::Error) -> DbError {
    DbError::Query(format!("failed to map range receipt replay: {error}"))
}
