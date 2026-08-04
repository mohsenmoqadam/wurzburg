use serde::Deserialize;
use uuid::Uuid;

use crate::{
    api::command::DurableMutationContext,
    db::{
        error::{DbError, DbResult},
        oracle::{
            OracleRepository,
            card_issuance_result::{
                IssuedCardFinalizationSnapshot, IssuedCardProvisioningIntent,
                load_issued_card_intent,
            },
            idempotency::complete_idempotency_record,
            provider_credit::CreditMovementWalPayload,
            provider_user::{ExistingCardProvisioningIntent, resume_provider_user_intent},
            types::{raw16_to_uuid, uuid_to_raw16},
        },
    },
    domain::provider_credit::CreditMovementIntent,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedWalRecovery {
    pub operation_id: Uuid,
    pub operation_type: String,
    pub aggregate_id: Uuid,
    pub attempt_count: u32,
}

#[derive(Debug, Clone)]
pub enum WalRecoveryWork {
    ProviderUser {
        context: DurableMutationContext,
        intent: ExistingCardProvisioningIntent,
    },
    CardIssuance {
        context: DurableMutationContext,
        intent: IssuedCardProvisioningIntent,
        finalization: IssuedCardFinalizationSnapshot,
    },
    ProviderCredit {
        context: DurableMutationContext,
        intent: CreditMovementIntent,
        card_number: String,
    },
}

#[derive(Deserialize)]
struct ProviderUserWalPayload {
    command_context: DurableMutationContext,
}

#[derive(Deserialize)]
struct IssuanceWalPayload {
    command_context: DurableMutationContext,
    finalization: IssuedCardFinalizationSnapshot,
    batch_id: Uuid,
}

impl OracleRepository {
    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="card_issuance_batches.recover_completion", limit))]
    pub async fn recover_ready_issuance_batches(&self, limit: u16) -> DbResult<u16> {
        self.pool
            .with_transaction("recover ready issuance batches", move |connection| {
                let rows = connection.query(
                    "SELECT b.card_issuance_batch_id,JSON_SERIALIZE(b.result_command_context_json RETURNING CLOB) FROM card_issuance_batches b WHERE b.status='PROCESSING_RESULT' AND b.result_command_context_json IS NOT NULL AND NOT EXISTS (SELECT 1 FROM card_issuance_batch_rows r WHERE r.card_issuance_batch_id=b.card_issuance_batch_id AND (r.result_status IS NULL OR r.result_status='RECOVERY_REQUIRED')) ORDER BY b.updated_at FOR UPDATE SKIP LOCKED",
                    &[],
                ).map_err(|error| query("failed to select recoverable issuance batches", error))?;
                let mut ready = Vec::new();
                for row in rows {
                    if ready.len() >= usize::from(limit) { break; }
                    let row = row.map_err(|error| query("failed to read recoverable issuance batch", error))?;
                    ready.push((row_uuid(&row, 0)?, row.get::<_, String>(1).map_err(read)?));
                }
                for (batch_id, context_json) in &ready {
                    let context: DurableMutationContext = serde_json::from_str(context_json)
                        .map_err(|error| DbError::Query(format!("invalid issuance batch recovery context: {error}")))?;
                    let issued = outcome_count(connection, *batch_id, "ISSUED")?;
                    let rejected = outcome_count(connection, *batch_id, "REJECTED")?;
                    let failed = outcome_count(connection, *batch_id, "FAILED")?;
                    let request_count: i64 = connection.query_row_as(
                        "SELECT request_count FROM card_issuance_batches WHERE card_issuance_batch_id=:1",
                        &[&raw(*batch_id)],
                    ).map_err(|error| query("failed to count recoverable issuance batch", error))?;
                    if issued + rejected + failed != request_count { continue; }
                    let status = if failed == 0 { "COMPLETED" } else { "PARTIALLY_COMPLETED" };
                    connection.execute(
                        "UPDATE card_issuance_batches SET status=:1,issued_count=:2,rejected_count=:3,failed_count=:4,updated_by_subject=:5,updated_at=SYSTIMESTAMP,completed_at=SYSTIMESTAMP WHERE card_issuance_batch_id=:6 AND status='PROCESSING_RESULT'",
                        &[&status, &issued, &rejected, &failed, &context.audit.actor_subject, &raw(*batch_id)],
                    ).map_err(|error| query("failed to recover issuance batch completion", error))?;
                    let batch = super::card_issuance::fetch_batch(connection, *batch_id)?
                        .ok_or_else(|| DbError::Query("recovered issuance batch disappeared".to_string()))?;
                    complete_idempotency_record(
                        connection,
                        &context.operation_type,
                        &context.idempotency_key,
                        "card_issuance_batch",
                        *batch_id,
                        serde_json::to_value(batch).map_err(|error| DbError::Query(format!("failed to serialize recovered issuance batch: {error}")))?,
                    )?;
                }
                u16::try_from(ready.len()).map_err(|_| DbError::Query("issuance recovery count overflow".to_string()))
            })
            .await
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="operation_wal.claim", worker.id=worker_id))]
    pub async fn claim_recoverable_wal(
        &self,
        worker_id: String,
        batch_size: u16,
        lease_duration_ms: u64,
        stale_after_ms: u64,
    ) -> DbResult<Vec<ClaimedWalRecovery>> {
        self.pool
            .with_transaction("claim recoverable operation WAL", move |connection| {
                connection.execute(
                    "UPDATE operation_wal SET locked_by=NULL,locked_until=NULL,updated_at=SYSTIMESTAMP WHERE locked_until<SYSTIMESTAMP AND status<>'COMPLETED'",
                    &[],
                ).map_err(|error| query("failed to release stale WAL leases", error))?;
                let rows = connection.query(
                    "SELECT operation_id,operation_type,aggregate_id,attempt_count FROM operation_wal WHERE ((operation_type IN ('PROVIDER_USER_ACCOUNT_PROVISION','CARD_ISSUANCE_ACCOUNT_PROVISION') AND status IN ('PENDING','EXTERNAL_IN_FLIGHT','EXTERNAL_VERIFIED','FAILED')) OR (operation_type='PROVIDER_CREDIT_MOVEMENT' AND status IN ('PENDING','EXTERNAL_IN_FLIGHT','FAILED'))) AND locked_by IS NULL AND (next_attempt_at IS NULL OR next_attempt_at<=SYSTIMESTAMP) AND (status='FAILED' OR updated_at<=SYSTIMESTAMP-NUMTODSINTERVAL(:1/1000,'SECOND')) ORDER BY created_at FOR UPDATE SKIP LOCKED",
                    &[&(stale_after_ms as i64)],
                ).map_err(|error| query("failed to select recoverable WAL", error))?;
                let mut claimed = Vec::new();
                for row in rows {
                    if claimed.len() >= usize::from(batch_size) { break; }
                    let row = row.map_err(|error| query("failed to read recoverable WAL", error))?;
                    let operation_id = row_uuid(&row, 0)?;
                    let attempt_count: i64 = row.get(3).map_err(read)?;
                    claimed.push(ClaimedWalRecovery {
                        operation_id,
                        operation_type: row.get(1).map_err(read)?,
                        aggregate_id: row_uuid(&row, 2)?,
                        attempt_count: u32::try_from(attempt_count).map_err(|_| DbError::Query("invalid WAL attempt count".to_string()))?,
                    });
                }
                for job in &claimed {
                    connection.execute(
                        "UPDATE operation_wal SET status='EXTERNAL_IN_FLIGHT',attempt_count=attempt_count+1,locked_by=:1,locked_until=SYSTIMESTAMP+NUMTODSINTERVAL(:2/1000,'SECOND'),updated_at=SYSTIMESTAMP WHERE operation_id=:3 AND locked_by IS NULL AND status IN ('PENDING','EXTERNAL_IN_FLIGHT','EXTERNAL_VERIFIED','FAILED')",
                        &[&worker_id, &(lease_duration_ms as i64), &raw(job.operation_id)],
                    ).map_err(|error| query("failed to lease recoverable WAL", error))?;
                }
                Ok(claimed)
            })
            .await
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="operation_wal.load_recovery", operation.id=%job.operation_id))]
    pub async fn load_wal_recovery_work(
        &self,
        job: ClaimedWalRecovery,
    ) -> DbResult<Option<WalRecoveryWork>> {
        self.pool.with_connection(move |connection| {
            let request_json: String = connection.query_row_as(
                "SELECT JSON_SERIALIZE(request_json RETURNING CLOB) FROM operation_wal WHERE operation_id=:1 AND status='EXTERNAL_IN_FLIGHT'",
                &[&raw(job.operation_id)],
            ).map_err(|error| query("failed to load WAL recovery payload", error))?;
            match job.operation_type.as_str() {
                "PROVIDER_USER_ACCOUNT_PROVISION" => {
                    let payload: ProviderUserWalPayload = serde_json::from_str(&request_json)
                        .map_err(|error| DbError::Query(format!("invalid provider-user WAL payload: {error}")))?;
                    Ok(resume_provider_user_intent(connection, job.aggregate_id)?.map(|intent| WalRecoveryWork::ProviderUser { context: payload.command_context, intent }))
                }
                "CARD_ISSUANCE_ACCOUNT_PROVISION" => {
                    let payload: IssuanceWalPayload = serde_json::from_str(&request_json)
                        .map_err(|error| DbError::Query(format!("invalid issuance WAL payload: {error}")))?;
                    Ok(load_issued_card_intent(connection, payload.batch_id, job.aggregate_id)?.map(|intent| WalRecoveryWork::CardIssuance { context: payload.command_context, intent, finalization: payload.finalization }))
                }
                "PROVIDER_CREDIT_MOVEMENT" => {
                    let payload: CreditMovementWalPayload = serde_json::from_str(&request_json)
                        .map_err(|error| DbError::Query(format!("invalid provider-credit WAL payload: {error}")))?;
                    let card_number: String = connection.query_row_as(
                        "SELECT c.card_number FROM provider_credit_movements m JOIN cards c ON c.card_id=m.card_id WHERE m.operation_id=:1",
                        &[&raw(job.operation_id)],
                    ).map_err(|error| query("failed to load provider-credit recovery card", error))?;
                    Ok(Some(WalRecoveryWork::ProviderCredit {
                        context: payload.command_context,
                        intent: payload.intent,
                        card_number,
                    }))
                }
                _ => Ok(None),
            }
        }).await
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="operation_wal.reschedule", operation.id=%operation_id))]
    pub async fn reschedule_wal_recovery(
        &self,
        operation_id: Uuid,
        worker_id: String,
        backoff_ms: u64,
        exhausted: bool,
        safe_error_code: &'static str,
    ) -> DbResult<()> {
        self.pool.with_transaction("reschedule operation WAL recovery", move |connection| {
            let status = if exhausted { "DEAD_LETTER" } else { "FAILED" };
            let next_attempt = if exhausted { None } else { Some(backoff_ms as i64) };
            let error = serde_json::json!({"code":safe_error_code}).to_string();
            let statement = connection.execute(
                "UPDATE operation_wal SET status=:1,error_json=:2,next_attempt_at=CASE WHEN :3 IS NULL THEN NULL ELSE SYSTIMESTAMP+NUMTODSINTERVAL(:4/1000,'SECOND') END,locked_by=NULL,locked_until=NULL,updated_at=SYSTIMESTAMP WHERE operation_id=:5 AND locked_by=:6 AND status='EXTERNAL_IN_FLIGHT'",
                &[&status, &error, &next_attempt, &next_attempt, &raw(operation_id), &worker_id],
            ).map_err(|error| query("failed to reschedule operation WAL", error))?;
            if statement.row_count().map_err(read)? != 1 {
                return Err(DbError::Conflict("operation WAL lease is no longer owned".to_string()));
            }
            Ok(())
        }).await
    }
}

fn raw(value: Uuid) -> Vec<u8> {
    uuid_to_raw16(value).to_vec()
}
fn row_uuid(row: &oracle::Row, index: usize) -> DbResult<Uuid> {
    let value: Vec<u8> = row.get(index).map_err(read)?;
    raw16_to_uuid(&value)
}
fn query(context: &str, error: oracle::Error) -> DbError {
    DbError::Query(format!("{context}: {error}"))
}
fn read(error: oracle::Error) -> DbError {
    DbError::Query(format!("failed to map operation WAL recovery row: {error}"))
}

fn outcome_count(connection: &oracle::Connection, batch_id: Uuid, status: &str) -> DbResult<i64> {
    connection
        .query_row_as(
            "SELECT COUNT(*) FROM card_issuance_batch_rows WHERE card_issuance_batch_id=:1 AND result_status=:2",
            &[&raw(batch_id), &status],
        )
        .map_err(|error| query("failed to count recoverable issuance outcomes", error))
}
