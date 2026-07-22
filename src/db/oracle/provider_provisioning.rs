use uuid::Uuid;

use crate::db::{
    error::{DbError, DbResult},
    oracle::{
        OracleRepository,
        audit::insert_audit_log,
        provider::{fetch_provider_for_command, provisioning_audit_context},
        types::raw16_to_uuid,
    },
};
use crate::domain::audit::{AuditAction, NewAuditLog};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedProviderProvisioningJob {
    pub job_id: Uuid,
    pub provider_id: Uuid,
    pub attempt_count: u32,
}

impl OracleRepository {
    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="provider_provisioning.claim", worker.id=worker_id))]
    pub async fn claim_provider_core_jobs(
        &self,
        worker_id: String,
        batch_size: u16,
        lease_duration_ms: u64,
    ) -> DbResult<Vec<ClaimedProviderProvisioningJob>> {
        self.pool
            .with_transaction("claim provider core provisioning jobs", move |connection| {
                connection.execute(
                    "UPDATE provider_provisioning_jobs SET status='PENDING',locked_by=NULL,locked_until=NULL,updated_at=SYSTIMESTAMP WHERE job_type='TIGERBEETLE_PROVISION' AND status='RUNNING' AND locked_until<SYSTIMESTAMP",
                    &[],
                ).map_err(|error| DbError::Query(format!("failed to release stale provider provisioning leases: {error}")))?;

                let rows = connection.query(
                    "SELECT provider_provisioning_job_id,provider_id,attempt_count FROM provider_provisioning_jobs WHERE job_type='TIGERBEETLE_PROVISION' AND status='PENDING' AND (next_attempt_at IS NULL OR next_attempt_at<=SYSTIMESTAMP) ORDER BY created_at FOR UPDATE SKIP LOCKED",
                    &[],
                ).map_err(|error| DbError::Query(format!("failed to select provider provisioning jobs: {error}")))?;
                let mut jobs = Vec::new();
                for row in rows {
                    if jobs.len() >= usize::from(batch_size) { break; }
                    let row = row.map_err(|error| DbError::Query(format!("failed to read provider provisioning job: {error}")))?;
                    let job_id: Vec<u8> = row.get(0).map_err(read_error)?;
                    let provider_id: Vec<u8> = row.get(1).map_err(read_error)?;
                    let attempt_count: i64 = row.get(2).map_err(read_error)?;
                    jobs.push(ClaimedProviderProvisioningJob {
                        job_id: raw16_to_uuid(&job_id)?,
                        provider_id: raw16_to_uuid(&provider_id)?,
                        attempt_count: attempt_count.try_into().map_err(|_| DbError::Query("invalid provisioning attempt count".to_string()))?,
                    });
                }
                for job in &jobs {
                    connection.execute(
                        "UPDATE provider_provisioning_jobs SET status='RUNNING',attempt_count=attempt_count+1,locked_by=:1,locked_until=SYSTIMESTAMP+NUMTODSINTERVAL(:2/1000,'SECOND'),updated_at=SYSTIMESTAMP WHERE provider_provisioning_job_id=:3 AND status='PENDING'",
                        &[&worker_id, &(lease_duration_ms as i64), &job.job_id.as_bytes().to_vec()],
                    ).map_err(|error| DbError::Query(format!("failed to lease provider provisioning job: {error}")))?;
                }
                Ok(jobs)
            })
            .await
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="provider_provisioning.retry", job_id=%job_id))]
    pub async fn retry_or_fail_provider_core_job(
        &self,
        job_id: Uuid,
        provider_id: Uuid,
        worker_id: String,
        next_backoff_ms: u64,
        exhausted: bool,
        error_code: &'static str,
    ) -> DbResult<()> {
        self.pool
            .with_transaction("reschedule provider core provisioning", move |connection| {
                let job_id = job_id.as_bytes().to_vec();
                let provider_id_raw = provider_id.as_bytes().to_vec();
                if exhausted {
                    let previous = fetch_provider_for_command(connection, provider_id)?;
                    let updated_job = connection.execute(
                        "UPDATE provider_provisioning_jobs SET status='FAILED',next_attempt_at=NULL,locked_by=NULL,locked_until=NULL,error_code=:1,error_message=NULL,updated_at=SYSTIMESTAMP WHERE provider_provisioning_job_id=:2 AND status='RUNNING' AND locked_by=:3",
                        &[&error_code, &job_id, &worker_id],
                    ).map_err(|error| DbError::Query(format!("failed to fail provider provisioning job: {error}")))?;
                    if updated_job.row_count().map_err(read_error)? != 1 {
                        return Err(DbError::Conflict(
                            "provider provisioning lease is no longer owned by this worker".to_string(),
                        ));
                    }
                    connection.execute(
                        "UPDATE provider_ledger_accounts SET status='FAILED_PROVISIONING',updated_at=SYSTIMESTAMP WHERE provider_id=:1 AND status='PROVISIONING'",
                        &[&provider_id_raw],
                    ).map_err(|error| DbError::Query(format!("failed to mark provider account provisioning failed: {error}")))?;
                    connection.execute(
                        "UPDATE providers SET status='FAILED',updated_at=SYSTIMESTAMP WHERE provider_id=:1 AND status='PENDING_PROVISIONING'",
                        &[&provider_id_raw],
                    ).map_err(|error| DbError::Query(format!("failed to mark provider provisioning failed: {error}")))?;
                    let failed = fetch_provider_for_command(connection, provider_id)?;
                    let failed_snapshot = failed.replay_snapshot();
                    connection.execute(
                        "UPDATE idempotency_records SET response_snapshot=:1,updated_at=SYSTIMESTAMP WHERE resource_type='provider' AND resource_id=:2 AND status='COMPLETED'",
                        &[&failed_snapshot.to_string(), &provider_id_raw],
                    ).map_err(|error| DbError::Query(format!("failed to refresh failed provider idempotency snapshot: {error}")))?;
                    insert_audit_log(
                        connection,
                        NewAuditLog {
                            audit_log_id: Uuid::new_v4(),
                            entity_type: "PROVIDER".to_string(),
                            entity_id: provider_id,
                            action_type: AuditAction::StateTransition,
                            reason: Some("provider core provisioning exhausted recovery attempts".to_string()),
                            old_values: Some(previous.replay_snapshot()),
                            new_values: Some(failed_snapshot),
                            context: provisioning_audit_context(provider_id, "failed"),
                        },
                    )?;
                } else {
                    let updated_job = connection.execute(
                        "UPDATE provider_provisioning_jobs SET status='PENDING',next_attempt_at=SYSTIMESTAMP+NUMTODSINTERVAL(:1/1000,'SECOND'),locked_by=NULL,locked_until=NULL,error_code=:2,error_message=NULL,updated_at=SYSTIMESTAMP WHERE provider_provisioning_job_id=:3 AND status='RUNNING' AND locked_by=:4",
                        &[&(next_backoff_ms as i64), &error_code, &job_id, &worker_id],
                    ).map_err(|error| DbError::Query(format!("failed to reschedule provider provisioning job: {error}")))?;
                    if updated_job.row_count().map_err(read_error)? != 1 {
                        return Err(DbError::Conflict(
                            "provider provisioning lease is no longer owned by this worker".to_string(),
                        ));
                    }
                }
                Ok(())
            })
            .await
    }
}

fn read_error(error: oracle::Error) -> DbError {
    DbError::Query(format!(
        "failed to read Oracle provider provisioning row: {error}"
    ))
}
