use uuid::Uuid;

use crate::{
    api::command::MutationCommandContext,
    db::{
        error::{DbError, DbResult},
        oracle::{
            OracleRepository,
            audit::insert_audit_log,
            provider::provisioning_audit_context,
            types::{raw16_to_uuid, uuid_to_raw16},
        },
    },
    domain::{
        audit::{AuditAction, NewAuditLog, TrustedAuditContext},
        idempotency::IdempotencyStatus,
        provider::NewProviderKafkaCredential,
    },
};

use super::idempotency::{
    complete_idempotency_record, fetch_idempotency_record, insert_idempotency_record,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderKafkaJobType {
    Provision,
    Rotate,
    Suspend,
    Resume,
}

impl ProviderKafkaJobType {
    fn from_db_value(value: &str) -> DbResult<Self> {
        match value {
            "KAFKA_PROVISION" => Ok(Self::Provision),
            "KAFKA_ROTATE" => Ok(Self::Rotate),
            "KAFKA_SUSPEND" => Ok(Self::Suspend),
            "KAFKA_RESUME" => Ok(Self::Resume),
            _ => Err(DbError::Query(
                "unknown Provider Kafka provisioning job type".to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderKafkaAccessRecord {
    pub provider_kafka_access_id: Uuid,
    pub provider_id: Uuid,
    pub topic_name: String,
    pub username: String,
    pub consumer_group: String,
    pub credential_id: Option<Uuid>,
    pub password_ciphertext: Option<String>,
    pub encryption_key_version: Option<String>,
    pub credential_version: Option<u64>,
    pub security_protocol: String,
    pub sasl_mechanism: String,
    pub bootstrap_servers: Vec<String>,
    pub credential_status: String,
}

impl ProviderKafkaAccessRecord {
    pub fn safe_snapshot(&self) -> serde_json::Value {
        serde_json::json!({
            "provider_id": self.provider_id,
            "topic": self.topic_name,
            "username": self.username,
            "consumer_group": self.consumer_group,
            "credential_version": self.credential_version,
            "security_protocol": self.security_protocol,
            "sasl_mechanism": self.sasl_mechanism,
            "credential_status": self.credential_status
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedProviderKafkaJob {
    pub job_id: Uuid,
    pub provider_id: Uuid,
    pub job_type: ProviderKafkaJobType,
    pub attempt_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderKafkaJobStatusRecord {
    pub operation_id: Uuid,
    pub operation_type: String,
    pub status: String,
    pub attempt_count: u32,
    pub error_code: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderKafkaStatusRecord {
    pub provider_id: Uuid,
    pub access_status: String,
    pub active_credential_version: Option<u64>,
    pub candidate_credential_version: Option<u64>,
    pub latest_operation: Option<ProviderKafkaJobStatusRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderKafkaCredentialReadOutcome {
    NotConfigured,
    NotReady,
    Available(Box<ProviderKafkaAccessRecord>),
}

#[derive(Debug, Clone, Copy)]
pub struct ProviderKafkaRetryDecision {
    pub backoff_ms: u64,
    pub exhausted: bool,
    pub error_code: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderKafkaCommandAction {
    Provision,
    Rotate,
    Suspend,
    Resume,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProviderKafkaCommandOutcome {
    Applied(serde_json::Value),
    Replayed(serde_json::Value),
    NotConfigured,
    InvalidState,
    VersionConflict,
    IdempotencyConflict,
    IdempotencyInProgress,
    IdempotencyInvalidState,
}

impl OracleRepository {
    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="provider_kafka.status", provider_id=%provider_id))]
    pub async fn get_provider_kafka_status(
        &self,
        provider_id: Uuid,
    ) -> DbResult<Option<ProviderKafkaStatusRecord>> {
        self.pool
            .with_connection(move |connection| {
                let provider_id_raw = uuid_to_raw16(provider_id).to_vec();
                let access_status = match connection.query_row_as::<String>(
                    "SELECT credential_status FROM provider_kafka_access WHERE provider_id=:1",
                    &[&provider_id_raw],
                ) {
                    Ok(value) => value,
                    Err(error) if error.kind() == oracle::ErrorKind::NoDataFound => {
                        return Ok(None);
                    }
                    Err(error) => return Err(query_error(error)),
                };
                let active_credential_version = fetch_credential_version(
                    connection,
                    &provider_id_raw,
                    "ACTIVE",
                )?;
                let candidate_credential_version = fetch_credential_version(
                    connection,
                    &provider_id_raw,
                    "CANDIDATE",
                )?;
                let latest_operation = match connection.query_row(
                    "SELECT provider_provisioning_job_id,job_type,status,attempt_count,error_code FROM provider_provisioning_jobs WHERE provider_id=:1 AND job_type IN ('KAFKA_PROVISION','KAFKA_ROTATE','KAFKA_SUSPEND','KAFKA_RESUME') ORDER BY created_at DESC FETCH FIRST 1 ROWS ONLY",
                    &[&provider_id_raw],
                ) {
                    Ok(row) => {
                        let operation_id: Vec<u8> = row.get(0).map_err(read_error)?;
                        let attempt_count: i64 = row.get(3).map_err(read_error)?;
                        Some(ProviderKafkaJobStatusRecord {
                            operation_id: raw16_to_uuid(&operation_id)?,
                            operation_type: row.get(1).map_err(read_error)?,
                            status: row.get(2).map_err(read_error)?,
                            attempt_count: attempt_count.try_into().map_err(|_| {
                                DbError::Query(
                                    "invalid Provider Kafka attempt count".to_string(),
                                )
                            })?,
                            error_code: row.get(4).map_err(read_error)?,
                        })
                    }
                    Err(error) if error.kind() == oracle::ErrorKind::NoDataFound => None,
                    Err(error) => return Err(query_error(error)),
                };
                Ok(Some(ProviderKafkaStatusRecord {
                    provider_id,
                    access_status,
                    active_credential_version,
                    candidate_credential_version,
                    latest_operation,
                }))
            })
            .await
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="provider_kafka.next_credential_version", provider_id=%provider_id))]
    pub async fn next_provider_kafka_credential_version(&self, provider_id: Uuid) -> DbResult<u64> {
        self.pool
            .with_connection(move |connection| {
                let version = connection
                    .query_row_as::<i64>(
                        "SELECT NVL(MAX(credential_version),0)+1 FROM provider_kafka_credentials WHERE provider_id=:1",
                        &[&uuid_to_raw16(provider_id).to_vec()],
                    )
                    .map_err(query_error)?;
                version.try_into().map_err(|_| {
                    DbError::Query("invalid next Provider Kafka credential version".to_string())
                })
            })
            .await
    }

    #[tracing::instrument(skip(self, context, credential, reason), fields(db.system="oracle", db.operation.name="provider_kafka.command", provider_id=%provider_id, provider.kafka.action=?action))]
    pub async fn command_provider_kafka_access_atomic(
        &self,
        context: MutationCommandContext,
        provider_id: Uuid,
        action: ProviderKafkaCommandAction,
        credential: Option<NewProviderKafkaCredential>,
        reason: String,
    ) -> DbResult<ProviderKafkaCommandOutcome> {
        let operation_type = context.operation_type.clone();
        let idempotency_key = context.idempotency_key.as_str().to_string();
        let request_hash = context.request_hash.clone();
        self.pool
            .with_transaction("command Provider Kafka access", move |connection| {
                if let Some(existing) =
                    fetch_idempotency_record(connection, &operation_type, &idempotency_key)?
                {
                    return classify_command_idempotency(&existing, &request_hash);
                }

                let access_id = match connection.query_row_as::<Vec<u8>>(
                    "SELECT provider_kafka_access_id FROM provider_kafka_access WHERE provider_id=:1 FOR UPDATE",
                    &[&uuid_to_raw16(provider_id).to_vec()],
                ) {
                    Ok(value) => raw16_to_uuid(&value)?,
                    Err(error) if error.kind() == oracle::ErrorKind::NoDataFound => {
                        return Ok(ProviderKafkaCommandOutcome::NotConfigured);
                    }
                    Err(error) => return Err(query_error(error)),
                };
                let previous = fetch_access(connection, provider_id, CredentialSelection::Any)?
                    .ok_or_else(|| {
                        DbError::Query("Provider Kafka access was not found".to_string())
                    })?;
                if !command_state_is_valid(action, &previous.credential_status) {
                    return Ok(ProviderKafkaCommandOutcome::InvalidState);
                }

                if let Some(credential) = &credential {
                    let expected_version = connection
                        .query_row_as::<i64>(
                            "SELECT NVL(MAX(credential_version),0)+1 FROM provider_kafka_credentials WHERE provider_id=:1",
                            &[&uuid_to_raw16(provider_id).to_vec()],
                        )
                        .map_err(query_error)?;
                    if u64::try_from(expected_version).ok()
                        != Some(credential.credential_version)
                    {
                        return Ok(ProviderKafkaCommandOutcome::VersionConflict);
                    }
                }

                insert_idempotency_record(connection, context.new_idempotency_record())?;
                let job_id = Uuid::new_v4();
                let (job_type, target_status) = command_job_and_status(action);
                if let Some(credential) = credential {
                    let version = i64::try_from(credential.credential_version).map_err(|_| {
                        DbError::Query(
                            "Provider Kafka credential version exceeds Oracle NUMBER".to_string(),
                        )
                    })?;
                    connection.execute(
                        "INSERT INTO provider_kafka_credentials (provider_kafka_credential_id,provider_kafka_access_id,provider_id,credential_version,password_ciphertext,encryption_key_version,status) VALUES (:1,:2,:3,:4,:5,:6,'CANDIDATE')",
                        &[&uuid_to_raw16(credential.provider_kafka_credential_id).to_vec(), &uuid_to_raw16(access_id).to_vec(), &uuid_to_raw16(provider_id).to_vec(), &version, &credential.password_ciphertext, &credential.encryption_key_version],
                    ).map_err(query_error)?;
                }
                connection.execute(
                    "UPDATE provider_kafka_access SET credential_status=:1,updated_at=SYSTIMESTAMP WHERE provider_id=:2",
                    &[&target_status, &uuid_to_raw16(provider_id).to_vec()],
                ).map_err(query_error)?;
                connection.execute(
                    "INSERT INTO provider_provisioning_jobs (provider_provisioning_job_id,provider_id,job_type,status,request_json,result_json) VALUES (:1,:2,:3,'PENDING','{}','{}')",
                    &[&uuid_to_raw16(job_id).to_vec(), &uuid_to_raw16(provider_id).to_vec(), &job_type],
                ).map_err(query_error)?;
                let current = fetch_access(connection, provider_id, CredentialSelection::Any)?
                    .ok_or_else(|| DbError::Query("Provider Kafka access was not found".to_string()))?;
                let snapshot = serde_json::json!({
                    "provider_id": provider_id,
                    "operation_id": job_id,
                    "credential_status": target_status,
                    "credential_version": current.credential_version
                });
                insert_audit_log(connection, NewAuditLog {
                    audit_log_id: Uuid::new_v4(),
                    entity_type: "PROVIDER_KAFKA_ACCESS".to_string(),
                    entity_id: provider_id,
                    action_type: if matches!(action, ProviderKafkaCommandAction::Rotate | ProviderKafkaCommandAction::Provision) { AuditAction::SecretRotate } else { AuditAction::StateTransition },
                    reason: Some(reason),
                    old_values: Some(previous.safe_snapshot()),
                    new_values: Some(current.safe_snapshot()),
                    context: context.audit_context(),
                })?;
                complete_idempotency_record(
                    connection,
                    &operation_type,
                    &idempotency_key,
                    "provider_kafka_access",
                    provider_id,
                    snapshot.clone(),
                )?;
                Ok(ProviderKafkaCommandOutcome::Applied(snapshot))
            })
            .await
    }

    #[tracing::instrument(skip(self, audit_context), fields(db.system="oracle", db.operation.name="provider_kafka.read_secret", provider_id=%provider_id))]
    pub async fn read_active_provider_kafka_credential(
        &self,
        provider_id: Uuid,
        audit_context: TrustedAuditContext,
    ) -> DbResult<ProviderKafkaCredentialReadOutcome> {
        self.pool
            .with_transaction(
                "read and audit Provider Kafka credential",
                move |connection| {
                    let Some(access) =
                        fetch_access(connection, provider_id, CredentialSelection::Any)?
                    else {
                        return Ok(ProviderKafkaCredentialReadOutcome::NotConfigured);
                    };
                    if !matches!(access.credential_status.as_str(), "ACTIVE" | "ROTATING") {
                        return Ok(ProviderKafkaCredentialReadOutcome::NotReady);
                    }
                    let Some(active) =
                        fetch_access(connection, provider_id, CredentialSelection::Active)?
                    else {
                        return Ok(ProviderKafkaCredentialReadOutcome::NotReady);
                    };
                    insert_audit_log(
                        connection,
                        NewAuditLog {
                            audit_log_id: Uuid::new_v4(),
                            entity_type: "PROVIDER_KAFKA_CREDENTIAL".to_string(),
                            entity_id: active.credential_id.ok_or_else(|| {
                                DbError::Query(
                                    "active Provider Kafka credential ID is missing".to_string(),
                                )
                            })?,
                            action_type: AuditAction::SecretRead,
                            reason: Some(
                                "authorized Provider Kafka credential retrieval".to_string(),
                            ),
                            old_values: None,
                            new_values: Some(access.safe_snapshot()),
                            context: audit_context,
                        },
                    )?;
                    Ok(ProviderKafkaCredentialReadOutcome::Available(Box::new(
                        active,
                    )))
                },
            )
            .await
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="provider_kafka.get_candidate", provider_id=%provider_id))]
    pub async fn get_provider_kafka_candidate(
        &self,
        provider_id: Uuid,
    ) -> DbResult<Option<ProviderKafkaAccessRecord>> {
        self.pool
            .with_connection(move |connection| {
                fetch_access(connection, provider_id, CredentialSelection::Candidate)
            })
            .await
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="provider_kafka.get_active", provider_id=%provider_id))]
    pub async fn get_active_provider_kafka_access(
        &self,
        provider_id: Uuid,
    ) -> DbResult<Option<ProviderKafkaAccessRecord>> {
        self.pool
            .with_connection(move |connection| {
                fetch_access(connection, provider_id, CredentialSelection::Active)
            })
            .await
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="provider_kafka.claim", worker.id=worker_id))]
    pub async fn claim_provider_kafka_jobs(
        &self,
        worker_id: String,
        batch_size: u16,
        lease_duration_ms: u64,
    ) -> DbResult<Vec<ClaimedProviderKafkaJob>> {
        self.pool
            .with_transaction("claim Provider Kafka jobs", move |connection| {
                connection
                    .execute(
                        "UPDATE provider_provisioning_jobs SET status='PENDING',locked_by=NULL,locked_until=NULL,updated_at=SYSTIMESTAMP WHERE job_type IN ('KAFKA_PROVISION','KAFKA_ROTATE','KAFKA_SUSPEND','KAFKA_RESUME') AND status='RUNNING' AND locked_until<SYSTIMESTAMP",
                        &[],
                    )
                    .map_err(query_error)?;
                let rows = connection
                    .query(
                        "SELECT provider_provisioning_job_id,provider_id,job_type,attempt_count FROM provider_provisioning_jobs WHERE job_type IN ('KAFKA_PROVISION','KAFKA_ROTATE','KAFKA_SUSPEND','KAFKA_RESUME') AND status='PENDING' AND (next_attempt_at IS NULL OR next_attempt_at<=SYSTIMESTAMP) ORDER BY created_at FOR UPDATE SKIP LOCKED",
                        &[],
                    )
                    .map_err(query_error)?;
                let mut jobs = Vec::new();
                for row in rows {
                    if jobs.len() >= usize::from(batch_size) {
                        break;
                    }
                    let row = row.map_err(query_error)?;
                    let job_id: Vec<u8> = row.get(0).map_err(read_error)?;
                    let provider_id: Vec<u8> = row.get(1).map_err(read_error)?;
                    let job_type: String = row.get(2).map_err(read_error)?;
                    let attempts: i64 = row.get(3).map_err(read_error)?;
                    jobs.push(ClaimedProviderKafkaJob {
                        job_id: raw16_to_uuid(&job_id)?,
                        provider_id: raw16_to_uuid(&provider_id)?,
                        job_type: ProviderKafkaJobType::from_db_value(&job_type)?,
                        attempt_count: attempts.try_into().map_err(|_| {
                            DbError::Query(
                                "invalid Provider Kafka provisioning attempt count".to_string(),
                            )
                        })?,
                    });
                }
                for job in &jobs {
                    let updated = connection
                        .execute(
                            "UPDATE provider_provisioning_jobs SET status='RUNNING',attempt_count=attempt_count+1,locked_by=:1,locked_until=SYSTIMESTAMP+NUMTODSINTERVAL(:2/1000,'SECOND'),updated_at=SYSTIMESTAMP WHERE provider_provisioning_job_id=:3 AND status='PENDING'",
                            &[&worker_id, &(lease_duration_ms as i64), &uuid_to_raw16(job.job_id).to_vec()],
                        )
                        .map_err(query_error)?;
                    if updated.row_count().map_err(read_error)? != 1 {
                        return Err(DbError::Conflict(
                            "Provider Kafka job lease was not acquired".to_string(),
                        ));
                    }
                }
                Ok(jobs)
            })
            .await
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="provider_kafka.complete", provider_id=%provider_id, job_id=%job_id))]
    pub async fn complete_provider_kafka_job(
        &self,
        job_id: Uuid,
        provider_id: Uuid,
        job_type: ProviderKafkaJobType,
        worker_id: String,
    ) -> DbResult<()> {
        self.pool
            .with_transaction("complete Provider Kafka provisioning", move |connection| {
                let previous = fetch_access(connection, provider_id, CredentialSelection::Any)?
                    .ok_or_else(|| {
                        DbError::Query("Provider Kafka access was not found".to_string())
                    })?;
                complete_owned_job(connection, job_id, &worker_id)?;
                match job_type {
                    ProviderKafkaJobType::Provision | ProviderKafkaJobType::Rotate => {
                        let candidate = fetch_access(
                            connection,
                            provider_id,
                            CredentialSelection::Candidate,
                        )?
                        .ok_or_else(|| {
                            DbError::Conflict(
                                "Provider Kafka candidate credential was not found".to_string(),
                            )
                        })?;
                        let credential_id = candidate.credential_id.ok_or_else(|| {
                            DbError::Query(
                                "Provider Kafka candidate credential ID is missing".to_string(),
                            )
                        })?;
                        connection
                            .execute(
                                "UPDATE provider_kafka_credentials SET status='SUPERSEDED',superseded_at=SYSTIMESTAMP,updated_at=SYSTIMESTAMP WHERE provider_id=:1 AND status='ACTIVE'",
                                &[&uuid_to_raw16(provider_id).to_vec()],
                            )
                            .map_err(query_error)?;
                        let activated = connection
                            .execute(
                                "UPDATE provider_kafka_credentials SET status='ACTIVE',activated_at=SYSTIMESTAMP,updated_at=SYSTIMESTAMP WHERE provider_kafka_credential_id=:1 AND status='CANDIDATE'",
                                &[&uuid_to_raw16(credential_id).to_vec()],
                            )
                            .map_err(query_error)?;
                        if activated.row_count().map_err(read_error)? != 1 {
                            return Err(DbError::Conflict(
                                "Provider Kafka candidate credential is no longer available"
                                    .to_string(),
                            ));
                        }
                        connection
                            .execute(
                                "UPDATE provider_kafka_access SET credential_status='ACTIVE',rotated_at=CASE WHEN :1='KAFKA_ROTATE' THEN SYSTIMESTAMP ELSE rotated_at END,updated_at=SYSTIMESTAMP WHERE provider_id=:2",
                                &[&job_type_db_value(job_type), &uuid_to_raw16(provider_id).to_vec()],
                            )
                            .map_err(query_error)?;
                    }
                    ProviderKafkaJobType::Suspend => {
                        connection
                            .execute(
                                "UPDATE provider_kafka_access SET credential_status='SUSPENDED',updated_at=SYSTIMESTAMP WHERE provider_id=:1 AND credential_status='SUSPENDING'",
                                &[&uuid_to_raw16(provider_id).to_vec()],
                            )
                            .map_err(query_error)?;
                    }
                    ProviderKafkaJobType::Resume => {
                        connection
                            .execute(
                                "UPDATE provider_kafka_access SET credential_status='ACTIVE',updated_at=SYSTIMESTAMP WHERE provider_id=:1 AND credential_status='RESUMING'",
                                &[&uuid_to_raw16(provider_id).to_vec()],
                            )
                            .map_err(query_error)?;
                    }
                }
                let current = fetch_access(connection, provider_id, CredentialSelection::Any)?
                    .ok_or_else(|| {
                        DbError::Query("Provider Kafka access was not found".to_string())
                    })?;
                insert_audit_log(
                    connection,
                    NewAuditLog {
                        audit_log_id: Uuid::new_v4(),
                        entity_type: "PROVIDER_KAFKA_ACCESS".to_string(),
                        entity_id: provider_id,
                        action_type: AuditAction::StateTransition,
                        reason: Some(format!(
                            "Provider Kafka {} completed and broker state verified",
                            job_type_db_value(job_type)
                        )),
                        old_values: Some(previous.safe_snapshot()),
                        new_values: Some(current.safe_snapshot()),
                        context: provisioning_audit_context(provider_id, "kafka-complete"),
                    },
                )?;
                Ok(())
            })
            .await
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="provider_kafka.retry", provider_id=%provider_id, job_id=%job_id))]
    pub async fn retry_or_fail_provider_kafka_job(
        &self,
        job_id: Uuid,
        provider_id: Uuid,
        job_type: ProviderKafkaJobType,
        worker_id: String,
        decision: ProviderKafkaRetryDecision,
    ) -> DbResult<()> {
        self.pool
            .with_transaction("reschedule Provider Kafka provisioning", move |connection| {
                let updated = if decision.exhausted {
                    connection.execute(
                        "UPDATE provider_provisioning_jobs SET status='FAILED',next_attempt_at=NULL,locked_by=NULL,locked_until=NULL,error_code=:1,error_message=NULL,updated_at=SYSTIMESTAMP WHERE provider_provisioning_job_id=:2 AND status='RUNNING' AND locked_by=:3",
                        &[&decision.error_code, &uuid_to_raw16(job_id).to_vec(), &worker_id],
                    ).map_err(query_error)?
                } else {
                    connection.execute(
                        "UPDATE provider_provisioning_jobs SET status='PENDING',next_attempt_at=SYSTIMESTAMP+NUMTODSINTERVAL(:1/1000,'SECOND'),locked_by=NULL,locked_until=NULL,error_code=:2,error_message=NULL,updated_at=SYSTIMESTAMP WHERE provider_provisioning_job_id=:3 AND status='RUNNING' AND locked_by=:4",
                        &[&(decision.backoff_ms as i64), &decision.error_code, &uuid_to_raw16(job_id).to_vec(), &worker_id],
                    ).map_err(query_error)?
                };
                if updated.row_count().map_err(read_error)? != 1 {
                    return Err(DbError::Conflict(
                        "Provider Kafka provisioning lease is no longer owned".to_string(),
                    ));
                }
                if decision.exhausted {
                    if matches!(
                        job_type,
                        ProviderKafkaJobType::Provision | ProviderKafkaJobType::Rotate
                    ) {
                        connection.execute(
                            "UPDATE provider_kafka_credentials SET status='FAILED',updated_at=SYSTIMESTAMP WHERE provider_id=:1 AND status='CANDIDATE'",
                            &[&uuid_to_raw16(provider_id).to_vec()],
                        ).map_err(query_error)?;
                    }
                    let restored_status = match job_type {
                        ProviderKafkaJobType::Provision => "FAILED",
                        ProviderKafkaJobType::Rotate | ProviderKafkaJobType::Suspend => "ACTIVE",
                        ProviderKafkaJobType::Resume => "SUSPENDED",
                    };
                    connection.execute(
                        "UPDATE provider_kafka_access SET credential_status=:1,updated_at=SYSTIMESTAMP WHERE provider_id=:2",
                        &[&restored_status, &uuid_to_raw16(provider_id).to_vec()],
                    ).map_err(query_error)?;
                }
                Ok(())
            })
            .await
    }
}

#[derive(Clone, Copy)]
enum CredentialSelection {
    Active,
    Candidate,
    Any,
}

fn fetch_access(
    connection: &oracle::Connection,
    provider_id: Uuid,
    selection: CredentialSelection,
) -> DbResult<Option<ProviderKafkaAccessRecord>> {
    let provider_id_raw = uuid_to_raw16(provider_id).to_vec();
    let row = match connection.query_row(
        "SELECT provider_kafka_access_id,provider_id,topic_name,username,consumer_group,security_protocol,sasl_mechanism,JSON_SERIALIZE(bootstrap_servers_json RETURNING CLOB),credential_status FROM provider_kafka_access WHERE provider_id=:1",
        &[&provider_id_raw],
    ) {
        Ok(row) => row,
        Err(error) if error.kind() == oracle::ErrorKind::NoDataFound => return Ok(None),
        Err(error) => return Err(query_error(error)),
    };
    let access_id: Vec<u8> = row.get(0).map_err(read_error)?;
    let provider_raw: Vec<u8> = row.get(1).map_err(read_error)?;
    let brokers: String = row.get(7).map_err(read_error)?;
    let credential = fetch_credential(connection, &provider_id_raw, selection)?;
    Ok(Some(ProviderKafkaAccessRecord {
        provider_kafka_access_id: raw16_to_uuid(&access_id)?,
        provider_id: raw16_to_uuid(&provider_raw)?,
        topic_name: row.get(2).map_err(read_error)?,
        username: row.get(3).map_err(read_error)?,
        consumer_group: row.get(4).map_err(read_error)?,
        credential_id: credential.as_ref().map(|value| value.0),
        password_ciphertext: credential.as_ref().map(|value| value.1.clone()),
        encryption_key_version: credential.as_ref().map(|value| value.2.clone()),
        credential_version: credential.as_ref().map(|value| value.3),
        security_protocol: row.get(5).map_err(read_error)?,
        sasl_mechanism: row.get(6).map_err(read_error)?,
        bootstrap_servers: serde_json::from_str(&brokers).map_err(|_| {
            DbError::Query("invalid Provider Kafka broker configuration".to_string())
        })?,
        credential_status: row.get(8).map_err(read_error)?,
    }))
}

fn fetch_credential(
    connection: &oracle::Connection,
    provider_id: &[u8],
    selection: CredentialSelection,
) -> DbResult<Option<(Uuid, String, String, u64)>> {
    let row = match selection {
        CredentialSelection::Active | CredentialSelection::Candidate => {
            let status = match selection {
                CredentialSelection::Active => "ACTIVE",
                CredentialSelection::Candidate => "CANDIDATE",
                CredentialSelection::Any => unreachable!(),
            };
            connection.query_row(
                "SELECT provider_kafka_credential_id,password_ciphertext,encryption_key_version,credential_version FROM provider_kafka_credentials WHERE provider_id=:1 AND status=:2",
                &[&provider_id, &status],
            )
        }
        CredentialSelection::Any => connection.query_row(
            "SELECT provider_kafka_credential_id,password_ciphertext,encryption_key_version,credential_version FROM provider_kafka_credentials WHERE provider_id=:1 AND status IN ('ACTIVE','CANDIDATE') ORDER BY CASE status WHEN 'ACTIVE' THEN 1 ELSE 2 END FETCH FIRST 1 ROWS ONLY",
            &[&provider_id],
        ),
    };
    let row = match row {
        Ok(row) => row,
        Err(error) if error.kind() == oracle::ErrorKind::NoDataFound => return Ok(None),
        Err(error) => return Err(query_error(error)),
    };
    let credential_id: Vec<u8> = row.get(0).map_err(read_error)?;
    let version: i64 = row.get(3).map_err(read_error)?;
    Ok(Some((
        raw16_to_uuid(&credential_id)?,
        row.get(1).map_err(read_error)?,
        row.get(2).map_err(read_error)?,
        version
            .try_into()
            .map_err(|_| DbError::Query("invalid Provider Kafka credential version".to_string()))?,
    )))
}

fn fetch_credential_version(
    connection: &oracle::Connection,
    provider_id: &[u8],
    status: &str,
) -> DbResult<Option<u64>> {
    match connection.query_row_as::<i64>(
        "SELECT credential_version FROM provider_kafka_credentials WHERE provider_id=:1 AND status=:2",
        &[&provider_id, &status],
    ) {
        Ok(version) => version
            .try_into()
            .map(Some)
            .map_err(|_| DbError::Query("invalid Provider Kafka credential version".to_string())),
        Err(error) if error.kind() == oracle::ErrorKind::NoDataFound => Ok(None),
        Err(error) => Err(query_error(error)),
    }
}

fn complete_owned_job(
    connection: &oracle::Connection,
    job_id: Uuid,
    worker_id: &str,
) -> DbResult<()> {
    let updated = connection
        .execute(
            "UPDATE provider_provisioning_jobs SET status='SUCCEEDED',completed_at=SYSTIMESTAMP,locked_by=NULL,locked_until=NULL,error_code=NULL,error_message=NULL,updated_at=SYSTIMESTAMP WHERE provider_provisioning_job_id=:1 AND status='RUNNING' AND locked_by=:2",
            &[&uuid_to_raw16(job_id).to_vec(), &worker_id],
        )
        .map_err(query_error)?;
    if updated.row_count().map_err(read_error)? != 1 {
        return Err(DbError::Conflict(
            "Provider Kafka provisioning lease is no longer owned".to_string(),
        ));
    }
    Ok(())
}

fn job_type_db_value(job_type: ProviderKafkaJobType) -> &'static str {
    match job_type {
        ProviderKafkaJobType::Provision => "KAFKA_PROVISION",
        ProviderKafkaJobType::Rotate => "KAFKA_ROTATE",
        ProviderKafkaJobType::Suspend => "KAFKA_SUSPEND",
        ProviderKafkaJobType::Resume => "KAFKA_RESUME",
    }
}

fn command_state_is_valid(action: ProviderKafkaCommandAction, status: &str) -> bool {
    match action {
        ProviderKafkaCommandAction::Provision => status == "FAILED",
        ProviderKafkaCommandAction::Rotate => status == "ACTIVE",
        ProviderKafkaCommandAction::Suspend => status == "ACTIVE",
        ProviderKafkaCommandAction::Resume => status == "SUSPENDED",
    }
}

fn command_job_and_status(action: ProviderKafkaCommandAction) -> (&'static str, &'static str) {
    match action {
        ProviderKafkaCommandAction::Provision => ("KAFKA_PROVISION", "PROVISIONING"),
        ProviderKafkaCommandAction::Rotate => ("KAFKA_ROTATE", "ROTATING"),
        ProviderKafkaCommandAction::Suspend => ("KAFKA_SUSPEND", "SUSPENDING"),
        ProviderKafkaCommandAction::Resume => ("KAFKA_RESUME", "RESUMING"),
    }
}

fn classify_command_idempotency(
    existing: &crate::domain::idempotency::IdempotencyRecord,
    request_hash: &str,
) -> DbResult<ProviderKafkaCommandOutcome> {
    if existing.request_hash != request_hash {
        return Ok(ProviderKafkaCommandOutcome::IdempotencyConflict);
    }
    Ok(match existing.status {
        IdempotencyStatus::Completed => existing
            .response_snapshot
            .clone()
            .map(ProviderKafkaCommandOutcome::Replayed)
            .unwrap_or(ProviderKafkaCommandOutcome::IdempotencyInvalidState),
        IdempotencyStatus::InProgress => ProviderKafkaCommandOutcome::IdempotencyInProgress,
        IdempotencyStatus::Failed | IdempotencyStatus::Conflict => {
            ProviderKafkaCommandOutcome::IdempotencyInvalidState
        }
    })
}

fn query_error(error: oracle::Error) -> DbError {
    DbError::Query(format!("Provider Kafka Oracle operation failed: {error}"))
}

fn read_error(error: oracle::Error) -> DbError {
    DbError::Query(format!("failed to read Provider Kafka Oracle row: {error}"))
}
