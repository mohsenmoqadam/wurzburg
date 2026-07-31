use chrono::{DateTime, Utc};
use oracle::Row;
use serde::Serialize;
use uuid::Uuid;

use crate::{
    api::command::MutationCommandContext,
    db::{
        error::{DbError, DbResult},
        oracle::{
            OracleRepository,
            audit::insert_audit_log,
            idempotency::{
                complete_idempotency_record, fetch_idempotency_record, insert_idempotency_record,
            },
            types::{raw16_to_uuid, uuid_to_raw16},
        },
    },
    domain::{
        audit::{AuditAction, NewAuditLog, TrustedAuditContext},
        idempotency::IdempotencyStatus,
        provider::{
            DesiredProviderOperationalProfile, ProviderOperationalProfileRecord,
            ProviderOperationalProfileStatus, ProviderStatus,
        },
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OperationalProfileMutationDisposition {
    Activated,
    Scheduled,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SetProviderOperationalProfileResult {
    pub disposition: OperationalProfileMutationDisposition,
    pub profile: ProviderOperationalProfileRecord,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetProviderOperationalProfilePersistenceOutcome {
    Applied(Box<SetProviderOperationalProfileResult>),
    Replayed(serde_json::Value),
    ProviderNotFound,
    ProviderInactive,
    IdempotencyConflict,
    IdempotencyInProgress,
    IdempotencyInvalidState,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CancelProviderOperationalProfilePersistenceOutcome {
    Applied(Box<ProviderOperationalProfileRecord>),
    Replayed(serde_json::Value),
    ProviderNotFound,
    ProfileNotFound,
    InvalidState,
    IdempotencyConflict,
    IdempotencyInProgress,
    IdempotencyInvalidState,
}

impl OracleRepository {
    #[tracing::instrument(skip(self, context, desired), fields(db.system="oracle", db.operation.name="provider_operational_profiles.set", provider_id=%provider_id))]
    pub async fn set_provider_operational_profile_atomic(
        &self,
        context: MutationCommandContext,
        provider_id: Uuid,
        desired: DesiredProviderOperationalProfile,
    ) -> DbResult<SetProviderOperationalProfilePersistenceOutcome> {
        self.pool
            .with_transaction("set provider operational profile", move |connection| {
                if let Some(existing) = fetch_idempotency_record(
                    connection,
                    &context.operation_type,
                    context.idempotency_key.as_str(),
                )? {
                    return classify_set_idempotency(&existing, &context.request_hash);
                }

                let Some(provider_status) = lock_provider(connection, provider_id)? else {
                    return Ok(SetProviderOperationalProfilePersistenceOutcome::ProviderNotFound);
                };
                if provider_status == ProviderStatus::Inactive {
                    return Ok(SetProviderOperationalProfilePersistenceOutcome::ProviderInactive);
                }

                promote_due_for_provider(connection, provider_id)?;
                insert_idempotency_record(connection, context.new_idempotency_record())?;

                let profile_id = Uuid::new_v4();
                let version: i64 = connection
                    .query_row_as(
                        "SELECT NVL(MAX(version),0)+1 FROM provider_operational_profiles WHERE provider_id=:1",
                        &[&raw(provider_id)],
                    )
                    .map_err(query_error)?;
                let immediate: i32 = connection
                    .query_row_as(
                        "SELECT CASE WHEN TO_TIMESTAMP_TZ(:1,'YYYY-MM-DD\"T\"HH24:MI:SS.FF3TZH:TZM')<=SYSTIMESTAMP THEN 1 ELSE 0 END FROM dual",
                        &[&format_utc(desired.effective_at)],
                    )
                    .map_err(query_error)?;

                let replaced_candidate = fetch_by_status(
                    connection,
                    provider_id,
                    ProviderOperationalProfileStatus::Scheduled,
                )?;
                if let Some(candidate) = &replaced_candidate {
                    cancel_locked_profile(
                        connection,
                        candidate,
                        None,
                        &context,
                        "replaced by a newer operational profile schedule",
                    )?;
                }

                let replaced_active = if immediate == 1 {
                    let active = fetch_by_status(
                        connection,
                        provider_id,
                        ProviderOperationalProfileStatus::Active,
                    )?;
                    if let Some(active) = &active {
                        supersede_profile(connection, active, None, &context.audit_context())?;
                    }
                    active
                } else {
                    None
                };
                let disposition = if immediate == 1 {
                    OperationalProfileMutationDisposition::Activated
                } else {
                    OperationalProfileMutationDisposition::Scheduled
                };

                insert_profile(
                    connection,
                    profile_id,
                    provider_id,
                    version,
                    &desired,
                    disposition,
                    &context.actor.subject,
                )?;
                for previous_id in replaced_candidate
                    .iter()
                    .map(|value| value.provider_operational_profile_id)
                    .chain(
                        replaced_active
                            .iter()
                            .map(|value| value.provider_operational_profile_id),
                    )
                {
                    link_replacement(connection, previous_id, profile_id)?;
                }
                let profile = fetch_profile(connection, profile_id)?;
                insert_audit_log(
                    connection,
                    NewAuditLog {
                        audit_log_id: Uuid::new_v4(),
                        entity_type: "PROVIDER_OPERATIONAL_PROFILE".to_string(),
                        entity_id: profile_id,
                        action_type: AuditAction::Insert,
                        reason: Some(desired.reason),
                        old_values: None,
                        new_values: Some(profile.replay_snapshot()),
                        context: context.audit_context(),
                    },
                )?;
                let result = SetProviderOperationalProfileResult {
                    disposition,
                    profile,
                };
                let snapshot = serde_json::json!({
                    "disposition": result.disposition,
                    "profile": result.profile.replay_snapshot(),
                });
                complete_idempotency_record(
                    connection,
                    &context.operation_type,
                    context.idempotency_key.as_str(),
                    "provider_operational_profile",
                    profile_id,
                    snapshot,
                )?;
                Ok(SetProviderOperationalProfilePersistenceOutcome::Applied(
                    Box::new(result),
                ))
            })
            .await
    }

    #[tracing::instrument(skip(self, context), fields(db.system="oracle", db.operation.name="provider_operational_profiles.cancel", provider_id=%provider_id, provider_operational_profile_id=%profile_id))]
    pub async fn cancel_provider_operational_profile_atomic(
        &self,
        context: MutationCommandContext,
        provider_id: Uuid,
        profile_id: Uuid,
        reason: String,
    ) -> DbResult<CancelProviderOperationalProfilePersistenceOutcome> {
        self.pool
            .with_transaction("cancel provider operational profile", move |connection| {
                if let Some(existing) = fetch_idempotency_record(
                    connection,
                    &context.operation_type,
                    context.idempotency_key.as_str(),
                )? {
                    return classify_cancel_idempotency(&existing, &context.request_hash);
                }
                if lock_provider(connection, provider_id)?.is_none() {
                    return Ok(
                        CancelProviderOperationalProfilePersistenceOutcome::ProviderNotFound,
                    );
                }
                promote_due_for_provider(connection, provider_id)?;
                let Some(profile) = fetch_optional_profile(connection, profile_id)? else {
                    return Ok(CancelProviderOperationalProfilePersistenceOutcome::ProfileNotFound);
                };
                if profile.provider_id != provider_id
                    || profile.status != ProviderOperationalProfileStatus::Scheduled
                {
                    return Ok(CancelProviderOperationalProfilePersistenceOutcome::InvalidState);
                }
                insert_idempotency_record(connection, context.new_idempotency_record())?;
                cancel_locked_profile(connection, &profile, None, &context, &reason)?;
                let cancelled = fetch_profile(connection, profile_id)?;
                let snapshot = cancelled.replay_snapshot();
                complete_idempotency_record(
                    connection,
                    &context.operation_type,
                    context.idempotency_key.as_str(),
                    "provider_operational_profile",
                    profile_id,
                    snapshot,
                )?;
                Ok(CancelProviderOperationalProfilePersistenceOutcome::Applied(
                    Box::new(cancelled),
                ))
            })
            .await
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="provider_operational_profiles.get_current", provider_id=%provider_id))]
    pub async fn get_current_provider_operational_profile(
        &self,
        provider_id: Uuid,
    ) -> DbResult<Option<ProviderOperationalProfileRecord>> {
        self.pool
            .with_transaction("resolve provider operational profile", move |connection| {
                if lock_provider(connection, provider_id)?.is_none() {
                    return Ok(None);
                }
                promote_due_for_provider(connection, provider_id)?;
                fetch_by_status(
                    connection,
                    provider_id,
                    ProviderOperationalProfileStatus::Active,
                )
            })
            .await
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="provider_operational_profiles.list", provider_id=%provider_id, limit))]
    pub async fn list_provider_operational_profiles(
        &self,
        provider_id: Uuid,
        before_version: Option<i64>,
        limit: u16,
    ) -> DbResult<Option<Vec<ProviderOperationalProfileRecord>>> {
        self.pool
            .with_transaction("list provider operational profiles", move |connection| {
                if lock_provider(connection, provider_id)?.is_none() {
                    return Ok(None);
                }
                promote_due_for_provider(connection, provider_id)?;
                let before_again = before_version;
                let rows = connection
                    .query(
                        &format!(
                            "SELECT {} FROM provider_operational_profiles WHERE provider_id=:1 AND (:2 IS NULL OR version<:3) ORDER BY version DESC FETCH FIRST :4 ROWS ONLY",
                            columns()
                        ),
                        &[&raw(provider_id), &before_version, &before_again, &i64::from(limit)],
                    )
                    .map_err(query_error)?;
                let mut profiles = Vec::new();
                for row in rows {
                    profiles.push(map_row(&row.map_err(query_error)?)?);
                }
                Ok(Some(profiles))
            })
            .await
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="provider_operational_profiles.promote_due", batch_size))]
    pub async fn promote_due_provider_operational_profiles(
        &self,
        batch_size: u16,
    ) -> DbResult<u64> {
        self.pool
            .with_transaction("promote due provider operational profiles", move |connection| {
                let rows = connection
                    .query(
                        "SELECT provider_id FROM providers WHERE provider_id IN (SELECT provider_id FROM provider_operational_profiles WHERE status='SCHEDULED' AND effective_at<=SYSTIMESTAMP AND ROWNUM<=:1) FOR UPDATE SKIP LOCKED",
                        &[&i64::from(batch_size)],
                    )
                    .map_err(query_error)?;
                let mut provider_ids = Vec::new();
                for row in rows {
                    let raw_id: Vec<u8> = row.map_err(query_error)?.get(0).map_err(read_error)?;
                    provider_ids.push(raw16_to_uuid(&raw_id)?);
                }
                let mut promoted = 0_u64;
                for provider_id in provider_ids {
                    if promote_due_for_provider(connection, provider_id)?.is_some() {
                        promoted += 1;
                    }
                }
                Ok(promoted)
            })
            .await
    }
}

pub(crate) fn promote_due_for_provider(
    connection: &oracle::Connection,
    provider_id: Uuid,
) -> DbResult<Option<ProviderOperationalProfileRecord>> {
    let Some(candidate) = fetch_due_scheduled(connection, provider_id)? else {
        return Ok(None);
    };
    let audit_context = system_audit_context();
    if let Some(active) = fetch_by_status(
        connection,
        provider_id,
        ProviderOperationalProfileStatus::Active,
    )? {
        supersede_profile(
            connection,
            &active,
            Some(candidate.provider_operational_profile_id),
            &audit_context,
        )?;
    }
    let updated = connection
        .execute(
            "UPDATE provider_operational_profiles SET status='ACTIVE',activated_at=SYSTIMESTAMP,updated_by_subject='wurzburg-system',updated_at=SYSTIMESTAMP WHERE provider_operational_profile_id=:1 AND status='SCHEDULED' AND effective_at<=SYSTIMESTAMP",
            &[&raw(candidate.provider_operational_profile_id)],
        )
        .map_err(query_error)?
        .row_count()
        .map_err(query_error)?;
    if updated != 1 {
        return Err(DbError::Query(
            "due operational profile promotion affected an unexpected row count".to_string(),
        ));
    }
    let active = fetch_profile(connection, candidate.provider_operational_profile_id)?;
    insert_audit_log(
        connection,
        NewAuditLog {
            audit_log_id: Uuid::new_v4(),
            entity_type: "PROVIDER_OPERATIONAL_PROFILE".to_string(),
            entity_id: active.provider_operational_profile_id,
            action_type: AuditAction::StateTransition,
            reason: Some("scheduled operational profile reached effective time".to_string()),
            old_values: Some(candidate.replay_snapshot()),
            new_values: Some(active.replay_snapshot()),
            context: audit_context,
        },
    )?;
    Ok(Some(active))
}

fn supersede_profile(
    connection: &oracle::Connection,
    active: &ProviderOperationalProfileRecord,
    replacement_id: Option<Uuid>,
    audit_context: &TrustedAuditContext,
) -> DbResult<()> {
    connection
        .execute(
            "UPDATE provider_operational_profiles SET status='SUPERSEDED',superseded_by_profile_id=:1,superseded_at=SYSTIMESTAMP,updated_by_subject=:2,updated_at=SYSTIMESTAMP WHERE provider_operational_profile_id=:3 AND status='ACTIVE'",
            &[&replacement_id.map(raw), &audit_context.actor_subject, &raw(active.provider_operational_profile_id)],
        )
        .map_err(query_error)?;
    let superseded = fetch_profile(connection, active.provider_operational_profile_id)?;
    insert_audit_log(
        connection,
        NewAuditLog {
            audit_log_id: Uuid::new_v4(),
            entity_type: "PROVIDER_OPERATIONAL_PROFILE".to_string(),
            entity_id: active.provider_operational_profile_id,
            action_type: AuditAction::StateTransition,
            reason: Some("operational profile superseded".to_string()),
            old_values: Some(active.replay_snapshot()),
            new_values: Some(superseded.replay_snapshot()),
            context: audit_context.clone(),
        },
    )
}

fn link_replacement(
    connection: &oracle::Connection,
    previous_id: Uuid,
    replacement_id: Uuid,
) -> DbResult<()> {
    connection
        .execute(
            "UPDATE provider_operational_profiles SET superseded_by_profile_id=:1,updated_at=SYSTIMESTAMP WHERE provider_operational_profile_id=:2 AND status IN ('SUPERSEDED','CANCELLED')",
            &[&raw(replacement_id), &raw(previous_id)],
        )
        .map_err(query_error)?;
    Ok(())
}

fn cancel_locked_profile(
    connection: &oracle::Connection,
    profile: &ProviderOperationalProfileRecord,
    replacement_id: Option<Uuid>,
    context: &MutationCommandContext,
    reason: &str,
) -> DbResult<()> {
    connection
        .execute(
            "UPDATE provider_operational_profiles SET status='CANCELLED',superseded_by_profile_id=:1,cancelled_at=SYSTIMESTAMP,updated_by_subject=:2,updated_at=SYSTIMESTAMP WHERE provider_operational_profile_id=:3 AND status='SCHEDULED'",
            &[&replacement_id.map(raw), &context.actor.subject, &raw(profile.provider_operational_profile_id)],
        )
        .map_err(query_error)?;
    let cancelled = fetch_profile(connection, profile.provider_operational_profile_id)?;
    insert_audit_log(
        connection,
        NewAuditLog {
            audit_log_id: Uuid::new_v4(),
            entity_type: "PROVIDER_OPERATIONAL_PROFILE".to_string(),
            entity_id: profile.provider_operational_profile_id,
            action_type: AuditAction::StateTransition,
            reason: Some(reason.to_string()),
            old_values: Some(profile.replay_snapshot()),
            new_values: Some(cancelled.replay_snapshot()),
            context: context.audit_context(),
        },
    )
}

fn insert_profile(
    connection: &oracle::Connection,
    profile_id: Uuid,
    provider_id: Uuid,
    version: i64,
    desired: &DesiredProviderOperationalProfile,
    disposition: OperationalProfileMutationDisposition,
    subject: &str,
) -> DbResult<()> {
    let profile_json = serde_json::to_string(&desired.controls).map_err(|error| {
        DbError::Query(format!(
            "failed to serialize provider operational controls: {error}"
        ))
    })?;
    let status = match disposition {
        OperationalProfileMutationDisposition::Activated => "ACTIVE",
        OperationalProfileMutationDisposition::Scheduled => "SCHEDULED",
    };
    let activated = matches!(
        disposition,
        OperationalProfileMutationDisposition::Activated
    );
    connection
        .execute(
            "INSERT INTO provider_operational_profiles (provider_operational_profile_id,provider_id,status,version,effective_at,profile_json,created_by_subject,updated_by_subject,change_reason,activated_at) VALUES (:1,:2,:3,:4,TO_TIMESTAMP_TZ(:5,'YYYY-MM-DD\"T\"HH24:MI:SS.FF3TZH:TZM'),:6,:7,:8,:9,CASE WHEN :10=1 THEN SYSTIMESTAMP ELSE NULL END)",
            &[&raw(profile_id),&raw(provider_id),&status,&version,&format_utc(desired.effective_at),&profile_json,&subject,&subject,&desired.reason,&i32::from(activated)],
        )
        .map_err(query_error)?;
    Ok(())
}

pub(crate) fn lock_provider(
    connection: &oracle::Connection,
    provider_id: Uuid,
) -> DbResult<Option<ProviderStatus>> {
    let mut rows = connection
        .query(
            "SELECT status FROM providers WHERE provider_id=:1 FOR UPDATE",
            &[&raw(provider_id)],
        )
        .map_err(query_error)?;
    match rows.next() {
        Some(Ok(row)) => {
            let status: String = row.get(0).map_err(read_error)?;
            ProviderStatus::from_db_value(&status)
                .map(Some)
                .ok_or_else(|| DbError::Query("unknown provider lifecycle status".to_string()))
        }
        Some(Err(error)) => Err(query_error(error)),
        None => Ok(None),
    }
}

fn fetch_due_scheduled(
    connection: &oracle::Connection,
    provider_id: Uuid,
) -> DbResult<Option<ProviderOperationalProfileRecord>> {
    fetch_optional_with_sql(
        connection,
        &format!(
            "SELECT {} FROM provider_operational_profiles WHERE provider_id=:1 AND status='SCHEDULED' AND effective_at<=SYSTIMESTAMP FOR UPDATE",
            columns()
        ),
        &[&raw(provider_id)],
    )
}

fn fetch_by_status(
    connection: &oracle::Connection,
    provider_id: Uuid,
    status: ProviderOperationalProfileStatus,
) -> DbResult<Option<ProviderOperationalProfileRecord>> {
    fetch_optional_with_sql(
        connection,
        &format!(
            "SELECT {} FROM provider_operational_profiles WHERE provider_id=:1 AND status=:2",
            columns()
        ),
        &[&raw(provider_id), &status.as_db_value()],
    )
}

fn fetch_profile(
    connection: &oracle::Connection,
    profile_id: Uuid,
) -> DbResult<ProviderOperationalProfileRecord> {
    fetch_optional_profile(connection, profile_id)?.ok_or_else(|| {
        DbError::Query("provider operational profile disappeared during transaction".to_string())
    })
}

fn fetch_optional_profile(
    connection: &oracle::Connection,
    profile_id: Uuid,
) -> DbResult<Option<ProviderOperationalProfileRecord>> {
    fetch_optional_with_sql(
        connection,
        &format!(
            "SELECT {} FROM provider_operational_profiles WHERE provider_operational_profile_id=:1",
            columns()
        ),
        &[&raw(profile_id)],
    )
}

fn fetch_optional_with_sql(
    connection: &oracle::Connection,
    sql: &str,
    binds: &[&dyn oracle::sql_type::ToSql],
) -> DbResult<Option<ProviderOperationalProfileRecord>> {
    let mut rows = connection.query(sql, binds).map_err(query_error)?;
    match rows.next() {
        Some(Ok(row)) => map_row(&row).map(Some),
        Some(Err(error)) => Err(query_error(error)),
        None => Ok(None),
    }
}

fn map_row(row: &Row) -> DbResult<ProviderOperationalProfileRecord> {
    let profile_id: Vec<u8> = row.get(0).map_err(read_error)?;
    let provider_id: Vec<u8> = row.get(1).map_err(read_error)?;
    let status: String = row.get(2).map_err(read_error)?;
    let profile_json: String = row.get(5).map_err(read_error)?;
    let superseded_by: Option<Vec<u8>> = row.get(6).map_err(read_error)?;
    Ok(ProviderOperationalProfileRecord {
        provider_operational_profile_id: raw16_to_uuid(&profile_id)?,
        provider_id: raw16_to_uuid(&provider_id)?,
        status: ProviderOperationalProfileStatus::from_db_value(&status).ok_or_else(|| {
            DbError::Query(format!(
                "unknown provider operational profile status `{status}`"
            ))
        })?,
        version: row.get(3).map_err(read_error)?,
        effective_at: parse_utc(&row.get::<_, String>(4).map_err(read_error)?)?,
        controls: serde_json::from_str(&profile_json).map_err(|error| {
            DbError::Query(format!(
                "invalid provider operational profile JSON: {error}"
            ))
        })?,
        superseded_by_profile_id: superseded_by.as_deref().map(raw16_to_uuid).transpose()?,
        created_by_subject: row.get(7).map_err(read_error)?,
        updated_by_subject: row.get(8).map_err(read_error)?,
        change_reason: row.get(9).map_err(read_error)?,
        activated_at: parse_optional_utc(row.get(10).map_err(read_error)?)?,
        superseded_at: parse_optional_utc(row.get(11).map_err(read_error)?)?,
        cancelled_at: parse_optional_utc(row.get(12).map_err(read_error)?)?,
        created_at: parse_utc(&row.get::<_, String>(13).map_err(read_error)?)?,
        updated_at: parse_utc(&row.get::<_, String>(14).map_err(read_error)?)?,
    })
}

fn columns() -> &'static str {
    r#"provider_operational_profile_id,provider_id,status,version,TO_CHAR(SYS_EXTRACT_UTC(effective_at),'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'),JSON_SERIALIZE(profile_json RETURNING CLOB),superseded_by_profile_id,created_by_subject,updated_by_subject,change_reason,TO_CHAR(SYS_EXTRACT_UTC(activated_at),'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'),TO_CHAR(SYS_EXTRACT_UTC(superseded_at),'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'),TO_CHAR(SYS_EXTRACT_UTC(cancelled_at),'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'),TO_CHAR(SYS_EXTRACT_UTC(created_at),'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'),TO_CHAR(SYS_EXTRACT_UTC(updated_at),'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')"#
}

fn classify_set_idempotency(
    existing: &crate::domain::idempotency::IdempotencyRecord,
    hash: &str,
) -> DbResult<SetProviderOperationalProfilePersistenceOutcome> {
    if existing.request_hash != hash {
        return Ok(SetProviderOperationalProfilePersistenceOutcome::IdempotencyConflict);
    }
    Ok(match existing.status {
        IdempotencyStatus::Completed => existing
            .response_snapshot
            .clone()
            .map(SetProviderOperationalProfilePersistenceOutcome::Replayed)
            .unwrap_or(SetProviderOperationalProfilePersistenceOutcome::IdempotencyInvalidState),
        IdempotencyStatus::InProgress => {
            SetProviderOperationalProfilePersistenceOutcome::IdempotencyInProgress
        }
        _ => SetProviderOperationalProfilePersistenceOutcome::IdempotencyInvalidState,
    })
}

fn classify_cancel_idempotency(
    existing: &crate::domain::idempotency::IdempotencyRecord,
    hash: &str,
) -> DbResult<CancelProviderOperationalProfilePersistenceOutcome> {
    if existing.request_hash != hash {
        return Ok(CancelProviderOperationalProfilePersistenceOutcome::IdempotencyConflict);
    }
    Ok(match existing.status {
        IdempotencyStatus::Completed => existing
            .response_snapshot
            .clone()
            .map(CancelProviderOperationalProfilePersistenceOutcome::Replayed)
            .unwrap_or(CancelProviderOperationalProfilePersistenceOutcome::IdempotencyInvalidState),
        IdempotencyStatus::InProgress => {
            CancelProviderOperationalProfilePersistenceOutcome::IdempotencyInProgress
        }
        _ => CancelProviderOperationalProfilePersistenceOutcome::IdempotencyInvalidState,
    })
}

fn system_audit_context() -> TrustedAuditContext {
    let operation_id = Uuid::new_v4().to_string();
    TrustedAuditContext {
        actor_subject: "wurzburg-system".to_string(),
        actor_client_id: Some("provider-operational-profile-scheduler".to_string()),
        actor_provider_id: None,
        actor_user_id: None,
        actor_issuer: Some("wurzburg-internal".to_string()),
        source_ip: None,
        correlation_id: operation_id.clone(),
        request_id: operation_id,
    }
}

fn raw(value: Uuid) -> Vec<u8> {
    uuid_to_raw16(value).to_vec()
}

fn format_utc(value: DateTime<Utc>) -> String {
    value.format("%Y-%m-%dT%H:%M:%S%.3f+00:00").to_string()
}

fn parse_utc(value: &str) -> DbResult<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)
        .map_err(|error| DbError::Query(format!("invalid Oracle UTC timestamp: {error}")))?
        .with_timezone(&Utc))
}

fn parse_optional_utc(value: Option<String>) -> DbResult<Option<DateTime<Utc>>> {
    value.as_deref().map(parse_utc).transpose()
}

fn query_error(error: oracle::Error) -> DbError {
    DbError::Query(format!(
        "provider operational profile query failed: {error}"
    ))
}

fn read_error(error: oracle::Error) -> DbError {
    DbError::Query(format!(
        "failed to read provider operational profile row: {error}"
    ))
}
