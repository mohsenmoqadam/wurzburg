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
        audit::{AuditAction, NewAuditLog},
        idempotency::IdempotencyStatus,
        provider_fee::{
            DesiredProviderFeeProfile, FeePayer, FeePolicy, ProviderFeeMaterializationReceipt,
            ProviderFeeProfile, ProviderFeeProfileStatus,
        },
    },
    messaging::contract::{InternalEventEnvelope, InternalEventHeaders},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FeeProfileMutationDisposition {
    Created,
    Updated,
    PublicationPending,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SetProviderFeeProfileResult {
    pub disposition: FeeProfileMutationDisposition,
    pub profile: ProviderFeeProfile,
    pub operation_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetProviderFeeProfilePersistenceOutcome {
    Applied(Box<SetProviderFeeProfileResult>),
    Replayed(serde_json::Value),
    ProviderNotFound,
    ContractInvalid(String),
    DraftFrozen,
    IdempotencyConflict,
    IdempotencyInProgress,
    IdempotencyInvalidState,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProviderFeeReceiptPersistenceOutcome {
    Activated(Box<ProviderFeeProfile>),
    Replayed,
    Mismatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FeeProfileAttachmentPreparation {
    Ready { operation_id: Option<Uuid> },
    Missing,
    PublicationPending,
}

impl OracleRepository {
    #[tracing::instrument(skip(self, context, desired), fields(db.system="oracle", db.operation.name="provider_fee_profiles.set", provider_id=%provider_id))]
    pub async fn set_provider_fee_profile_atomic(
        &self,
        context: MutationCommandContext,
        provider_id: Uuid,
        desired: DesiredProviderFeeProfile,
    ) -> DbResult<SetProviderFeeProfilePersistenceOutcome> {
        let operation_type = context.operation_type.clone();
        let key = context.idempotency_key.as_str().to_string();
        let hash = context.request_hash.clone();
        let retry = (operation_type.clone(), key.clone(), hash.clone());
        let pool = self.pool.clone();
        let headers = InternalEventHeaders::from_current_span(
            context.request.correlation_id.clone(),
            context.request.request_id.to_string(),
        );
        let result = self
            .pool
            .with_transaction(
                "atomic provider fee profile configuration",
                move |connection| {
                    if let Some(existing) =
                        fetch_idempotency_record(connection, &operation_type, &key)?
                    {
                        return classify_idempotency(&existing, &hash);
                    }
                    if let Err(error) = desired.validate() {
                        return Ok(SetProviderFeeProfilePersistenceOutcome::ContractInvalid(
                            error.to_string(),
                        ));
                    }
                    if !lock_provider(connection, provider_id)? {
                        return Ok(SetProviderFeeProfilePersistenceOutcome::ProviderNotFound);
                    }
                    if let Some(existing) =
                        fetch_idempotency_record(connection, &operation_type, &key)?
                    {
                        return classify_idempotency(&existing, &hash);
                    }
                    let draft = fetch_fee_profile_by_status(
                        connection,
                        provider_id,
                        ProviderFeeProfileStatus::Draft,
                    )?;
                    if draft
                        .as_ref()
                        .is_some_and(|value| value.publication_operation_id.is_some())
                    {
                        return Ok(SetProviderFeeProfilePersistenceOutcome::DraftFrozen);
                    }
                    let profile_id = draft
                        .as_ref()
                        .map(|value| value.provider_fee_profile_id)
                        .unwrap_or_else(Uuid::new_v4);
                    let version = draft
                        .as_ref()
                        .map(|value| value.version)
                        .unwrap_or(next_version(connection, provider_id)?);
                    let operation_id =
                        (active_range_count(connection, provider_id)? > 0).then(Uuid::new_v4);
                    if let Some(operation_id) = operation_id {
                        insert_fee_profile_outbox(
                            connection,
                            operation_id,
                            provider_id,
                            profile_id,
                            version,
                            &desired.fee_policy,
                            &headers,
                        )?;
                    }
                    insert_idempotency_record(connection, context.new_idempotency_record())?;
                    let reason = desired.reason.trim().to_string();
                    let (profile, action, old_values, disposition) = if let Some(draft) = draft {
                        update_draft(
                            connection,
                            profile_id,
                            &desired.fee_policy,
                            operation_id,
                            &context.actor.subject,
                            &reason,
                        )?;
                        (
                            fetch_fee_profile(connection, profile_id)?,
                            AuditAction::Update,
                            Some(draft.replay_snapshot()),
                            if operation_id.is_some() {
                                FeeProfileMutationDisposition::PublicationPending
                            } else {
                                FeeProfileMutationDisposition::Updated
                            },
                        )
                    } else {
                        insert_draft(
                            connection,
                            profile_id,
                            provider_id,
                            &desired.fee_policy,
                            version,
                            operation_id,
                            &context.actor.subject,
                            &reason,
                        )?;
                        (
                            fetch_fee_profile(connection, profile_id)?,
                            AuditAction::Insert,
                            None,
                            if operation_id.is_some() {
                                FeeProfileMutationDisposition::PublicationPending
                            } else {
                                FeeProfileMutationDisposition::Created
                            },
                        )
                    };
                    let response = SetProviderFeeProfileResult {
                        disposition,
                        profile: profile.clone(),
                        operation_id,
                    };
                    let snapshot = serde_json::to_value(&response).map_err(|error| {
                        DbError::Query(format!("failed to serialize fee response: {error}"))
                    })?;
                    insert_audit_log(
                        connection,
                        NewAuditLog {
                            audit_log_id: Uuid::new_v4(),
                            entity_type: "PROVIDER_FEE_PROFILE".to_string(),
                            entity_id: profile_id,
                            action_type: action,
                            reason: Some(reason),
                            old_values,
                            new_values: Some(profile.replay_snapshot()),
                            context: context.audit_context(),
                        },
                    )?;
                    complete_idempotency_record(
                        connection,
                        &operation_type,
                        &key,
                        "provider_fee_profile",
                        profile_id,
                        snapshot,
                    )?;
                    Ok(SetProviderFeeProfilePersistenceOutcome::Applied(Box::new(
                        response,
                    )))
                },
            )
            .await;
        match result {
            Err(DbError::Conflict(_)) => {
                pool.with_connection(move |connection| {
                    let existing = fetch_idempotency_record(connection, &retry.0, &retry.1)?
                        .ok_or_else(|| {
                            DbError::Query(
                                "concurrent fee idempotency winner was not visible".to_string(),
                            )
                        })?;
                    classify_idempotency(&existing, &retry.2)
                })
                .await
            }
            other => other,
        }
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="provider_fee_profiles.get_current", provider_id=%provider_id))]
    pub async fn get_current_provider_fee_profile(
        &self,
        provider_id: Uuid,
    ) -> DbResult<Option<ProviderFeeProfile>> {
        self.pool
            .with_connection(move |connection| {
                fetch_fee_profile_by_status(
                    connection,
                    provider_id,
                    ProviderFeeProfileStatus::Active,
                )
            })
            .await
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="provider_fee_profiles.get", provider_id=%provider_id, fee_profile_id=%profile_id))]
    pub async fn get_provider_fee_profile(
        &self,
        provider_id: Uuid,
        profile_id: Uuid,
    ) -> DbResult<Option<ProviderFeeProfile>> {
        self.pool
            .with_connection(move |connection| {
                Ok(fetch_optional_fee_profile(connection, profile_id)?
                    .filter(|value| value.provider_id == provider_id))
            })
            .await
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="provider_fee_profiles.list", provider_id=%provider_id, limit))]
    pub async fn list_provider_fee_profiles(
        &self,
        provider_id: Uuid,
        before_version: Option<i64>,
        limit: u16,
    ) -> DbResult<Vec<ProviderFeeProfile>> {
        self.pool.with_connection(move |connection| {
            let limit = i64::from(limit);
            let rows = connection.query(&format!("SELECT {} FROM provider_fee_profiles WHERE provider_id = :1 AND (:2 IS NULL OR version < :3) ORDER BY version DESC FETCH FIRST :4 ROWS ONLY", columns()), &[&raw(provider_id), &before_version, &before_version, &limit])
                .map_err(|error| DbError::Query(format!("failed to list fee profiles: {error}")))?;
            rows.map(|row| row.map_err(|error| DbError::Query(format!("failed to read fee profile list: {error}"))).and_then(|row| map_row(&row))).collect()
        }).await
    }

    #[tracing::instrument(skip(self, receipt), fields(messaging.system="kafka", messaging.operation.name="process", messaging.message.id=%receipt.receipt_event_id, operation_id=%receipt.operation_id, provider_id=%receipt.provider_id))]
    pub async fn apply_provider_fee_materialization_receipt(
        &self,
        receipt: ProviderFeeMaterializationReceipt,
    ) -> DbResult<ProviderFeeReceiptPersistenceOutcome> {
        self.pool.with_transaction("provider fee materialization receipt", move |connection| {
            if let Some(outcome) = classify_receipt_replay(connection, &receipt)? { return Ok(outcome); }
            insert_inbox(connection, &receipt)?;
            let pending = fetch_by_operation_for_update(connection, receipt.operation_id)?;
            let matches = pending.as_ref().is_some_and(|profile| {
                profile.provider_id == receipt.provider_id && profile.provider_fee_profile_id == receipt.provider_fee_profile_id
                    && profile.version == receipt.materialized_version && receipt.runtime_key == format!("FEE:{}", receipt.provider_id)
            });
            if !matches {
                fail_inbox(connection, &receipt)?;
                return Ok(ProviderFeeReceiptPersistenceOutcome::Mismatch);
            }
            let pending = pending.expect("validated pending fee profile must exist");
            let previous = fetch_fee_profile_by_status(connection, receipt.provider_id, ProviderFeeProfileStatus::Active)?;
            insert_runtime_receipt(connection, &receipt)?;
            connection.execute("UPDATE provider_fee_profiles SET status='SUPERSEDED', superseded_by_profile_id=:1, superseded_at=SYSTIMESTAMP, updated_at=SYSTIMESTAMP WHERE provider_id=:2 AND status='ACTIVE'", &[&raw(receipt.provider_fee_profile_id), &raw(receipt.provider_id)])
                .map_err(|error| DbError::Query(format!("failed to supersede fee profile: {error}")))?;
            let updated = connection.execute("UPDATE provider_fee_profiles SET status='ACTIVE', activated_at=SYSTIMESTAMP, updated_at=SYSTIMESTAMP WHERE provider_fee_profile_id=:1 AND publication_operation_id=:2 AND status='DRAFT'", &[&raw(receipt.provider_fee_profile_id), &raw(receipt.operation_id)])
                .map_err(|error| DbError::Query(format!("failed to activate fee profile: {error}")))?;
            if updated.row_count().map_err(|error| DbError::Query(format!("failed to read fee activation count: {error}")))? != 1 {
                return Err(DbError::Query("pending fee profile changed before activation".to_string()));
            }
            complete_inbox(connection, receipt.receipt_event_id)?;
            let active = fetch_fee_profile(connection, receipt.provider_fee_profile_id)?;
            insert_audit_log(connection, NewAuditLog {
                audit_log_id: Uuid::new_v4(), entity_type: "PROVIDER_FEE_PROFILE".to_string(), entity_id: active.provider_fee_profile_id,
                action_type: AuditAction::StateTransition, reason: Some("Wolfsburg confirmed FEE materialization".to_string()),
                old_values: Some(pending.replay_snapshot()), new_values: Some(serde_json::json!({"activated_profile": active.replay_snapshot(), "previous_active_profile_id": previous.map(|value| value.provider_fee_profile_id), "receipt_event_id": receipt.receipt_event_id})),
                context: crate::domain::audit::TrustedAuditContext { actor_subject:"wolfsburg".to_string(), actor_client_id:Some("wolfsburg-materializer".to_string()), actor_provider_id:None, actor_user_id:None, actor_issuer:Some("internal-kafka".to_string()), source_ip:None, correlation_id:receipt.operation_id.to_string(), request_id:receipt.receipt_event_id.to_string() },
            })?;
            Ok(ProviderFeeReceiptPersistenceOutcome::Activated(Box::new(active)))
        }).await
    }
}

pub(crate) fn prepare_fee_profile_for_attachment(
    connection: &oracle::Connection,
    context: &MutationCommandContext,
    provider_id: Uuid,
    reason: &str,
    headers: &InternalEventHeaders,
) -> DbResult<FeeProfileAttachmentPreparation> {
    if let Some(draft) =
        fetch_fee_profile_by_status(connection, provider_id, ProviderFeeProfileStatus::Draft)?
    {
        if draft.publication_operation_id.is_some() {
            return Ok(FeeProfileAttachmentPreparation::PublicationPending);
        }
        let operation_id = Uuid::new_v4();
        insert_fee_profile_outbox(
            connection,
            operation_id,
            provider_id,
            draft.provider_fee_profile_id,
            draft.version,
            &draft.fee_policy,
            headers,
        )?;
        let updated = connection.execute("UPDATE provider_fee_profiles SET publication_operation_id=:1, updated_by_subject=:2, change_reason=:3, updated_at=SYSTIMESTAMP WHERE provider_fee_profile_id=:4 AND status='DRAFT' AND publication_operation_id IS NULL", &[&raw(operation_id), &context.actor.subject, &reason, &raw(draft.provider_fee_profile_id)])
            .map_err(|error| DbError::Query(format!("failed to freeze fee publication: {error}")))?;
        if updated
            .row_count()
            .map_err(|error| DbError::Query(format!("failed to read fee freeze count: {error}")))?
            != 1
        {
            return Ok(FeeProfileAttachmentPreparation::PublicationPending);
        }
        let frozen = fetch_fee_profile(connection, draft.provider_fee_profile_id)?;
        insert_audit_log(
            connection,
            NewAuditLog {
                audit_log_id: Uuid::new_v4(),
                entity_type: "PROVIDER_FEE_PROFILE".to_string(),
                entity_id: draft.provider_fee_profile_id,
                action_type: AuditAction::Update,
                reason: Some(reason.to_string()),
                old_values: Some(draft.replay_snapshot()),
                new_values: Some(frozen.replay_snapshot()),
                context: context.audit_context(),
            },
        )?;
        return Ok(FeeProfileAttachmentPreparation::Ready {
            operation_id: Some(operation_id),
        });
    }
    if fetch_fee_profile_by_status(connection, provider_id, ProviderFeeProfileStatus::Active)?
        .is_some()
    {
        return Ok(FeeProfileAttachmentPreparation::Ready { operation_id: None });
    }
    Ok(FeeProfileAttachmentPreparation::Missing)
}

pub(crate) fn insert_fee_profile_outbox(
    connection: &oracle::Connection,
    operation_id: Uuid,
    provider_id: Uuid,
    profile_id: Uuid,
    version: i64,
    policy: &FeePolicy,
    headers: &InternalEventHeaders,
) -> DbResult<()> {
    let event_id = Uuid::new_v4();
    let envelope = InternalEventEnvelope::new(
        event_id,
        "PROVIDER_FEE_PROFILE_PUBLISH_REQUESTED",
        "PROVIDER",
        provider_id,
        operation_id,
        serde_json::json!({"provider_id":provider_id, "provider_fee_profile_id":profile_id, "fee_profile_version":version, "fee_policy":policy}),
    );
    let payload = serde_json::to_string(&envelope)
        .map_err(|error| DbError::Query(format!("failed to serialize fee event: {error}")))?;
    let headers = serde_json::to_string(headers)
        .map_err(|error| DbError::Query(format!("failed to serialize fee headers: {error}")))?;
    connection.execute("INSERT INTO integration_outbox (outbox_event_id, operation_id, event_type, aggregate_type, aggregate_id, partition_key, payload_json, headers_json, status, next_attempt_at) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,'PENDING',SYSTIMESTAMP)", &[&raw(event_id), &raw(operation_id), &"PROVIDER_FEE_PROFILE_PUBLISH_REQUESTED", &"PROVIDER", &raw(provider_id), &provider_id.to_string(), &payload, &headers])
        .map_err(|error| DbError::Query(format!("failed to insert fee outbox: {error}")))?;
    Ok(())
}

fn lock_provider(connection: &oracle::Connection, provider_id: Uuid) -> DbResult<bool> {
    let mut rows = connection
        .query(
            "SELECT provider_id FROM providers WHERE provider_id=:1 FOR UPDATE",
            &[&raw(provider_id)],
        )
        .map_err(|error| DbError::Query(format!("failed to lock provider: {error}")))?;
    Ok(rows.next().is_some())
}

fn active_range_count(connection: &oracle::Connection, provider_id: Uuid) -> DbResult<i64> {
    connection
        .query_row_as(
            "SELECT COUNT(*) FROM card_range_providers WHERE provider_id=:1 AND status='ACTIVE'",
            &[&raw(provider_id)],
        )
        .map_err(|error| DbError::Query(format!("failed to count provider ranges: {error}")))
}

fn next_version(connection: &oracle::Connection, provider_id: Uuid) -> DbResult<i64> {
    connection
        .query_row_as(
            "SELECT NVL(MAX(version),0)+1 FROM provider_fee_profiles WHERE provider_id=:1",
            &[&raw(provider_id)],
        )
        .map_err(|error| DbError::Query(format!("failed to allocate fee version: {error}")))
}

#[allow(clippy::too_many_arguments)]
fn insert_draft(
    connection: &oracle::Connection,
    profile_id: Uuid,
    provider_id: Uuid,
    policy: &FeePolicy,
    version: i64,
    operation_id: Option<Uuid>,
    subject: &str,
    reason: &str,
) -> DbResult<()> {
    connection.execute("INSERT INTO provider_fee_profiles (provider_fee_profile_id,provider_id,rate_bps,fixed_amount_rials,fee_payer,status,version,publication_operation_id,created_by_subject,updated_by_subject,change_reason) VALUES (:1,:2,:3,:4,:5,'DRAFT',:6,:7,:8,:9,:10)", &[&raw(profile_id),&raw(provider_id),&policy.rate_bps,&policy.fixed_amount_rials,&policy.fee_payer.as_db_value(),&version,&operation_id.map(raw),&subject,&subject,&reason]).map_err(|error| DbError::Query(format!("failed to insert fee profile: {error}")))?;
    Ok(())
}

fn update_draft(
    connection: &oracle::Connection,
    profile_id: Uuid,
    policy: &FeePolicy,
    operation_id: Option<Uuid>,
    subject: &str,
    reason: &str,
) -> DbResult<()> {
    let result=connection.execute("UPDATE provider_fee_profiles SET rate_bps=:1,fixed_amount_rials=:2,fee_payer=:3,publication_operation_id=:4,updated_by_subject=:5,change_reason=:6,updated_at=SYSTIMESTAMP WHERE provider_fee_profile_id=:7 AND status='DRAFT' AND publication_operation_id IS NULL", &[&policy.rate_bps,&policy.fixed_amount_rials,&policy.fee_payer.as_db_value(),&operation_id.map(raw),&subject,&reason,&raw(profile_id)]).map_err(|error| DbError::Query(format!("failed to update fee profile: {error}")))?;
    if result
        .row_count()
        .map_err(|error| DbError::Query(format!("failed to read fee update count: {error}")))?
        != 1
    {
        return Err(DbError::Query(
            "fee draft changed before update".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn fetch_fee_profile_by_status(
    connection: &oracle::Connection,
    provider_id: Uuid,
    status: ProviderFeeProfileStatus,
) -> DbResult<Option<ProviderFeeProfile>> {
    let mut rows = connection
        .query(
            &format!(
                "SELECT {} FROM provider_fee_profiles WHERE provider_id=:1 AND status=:2",
                columns()
            ),
            &[&raw(provider_id), &status.as_db_value()],
        )
        .map_err(|error| DbError::Query(format!("failed to fetch fee status: {error}")))?;
    match rows.next() {
        Some(Ok(row)) => map_row(&row).map(Some),
        Some(Err(error)) => Err(DbError::Query(format!(
            "failed to read fee status: {error}"
        ))),
        None => Ok(None),
    }
}

fn fetch_fee_profile(
    connection: &oracle::Connection,
    profile_id: Uuid,
) -> DbResult<ProviderFeeProfile> {
    fetch_optional_fee_profile(connection, profile_id)?
        .ok_or_else(|| DbError::Query("fee profile not visible".to_string()))
}
fn fetch_optional_fee_profile(
    connection: &oracle::Connection,
    profile_id: Uuid,
) -> DbResult<Option<ProviderFeeProfile>> {
    let mut rows = connection
        .query(
            &format!(
                "SELECT {} FROM provider_fee_profiles WHERE provider_fee_profile_id=:1",
                columns()
            ),
            &[&raw(profile_id)],
        )
        .map_err(|error| DbError::Query(format!("failed to fetch fee profile: {error}")))?;
    match rows.next() {
        Some(Ok(row)) => map_row(&row).map(Some),
        Some(Err(error)) => Err(DbError::Query(format!(
            "failed to read fee profile: {error}"
        ))),
        None => Ok(None),
    }
}
fn fetch_by_operation_for_update(
    connection: &oracle::Connection,
    operation_id: Uuid,
) -> DbResult<Option<ProviderFeeProfile>> {
    let mut rows=connection.query(&format!("SELECT {} FROM provider_fee_profiles WHERE publication_operation_id=:1 AND status='DRAFT' FOR UPDATE",columns()), &[&raw(operation_id)]).map_err(|error|DbError::Query(format!("failed to lock pending fee: {error}")))?;
    match rows.next() {
        Some(Ok(row)) => map_row(&row).map(Some),
        Some(Err(error)) => Err(DbError::Query(format!(
            "failed to read pending fee: {error}"
        ))),
        None => Ok(None),
    }
}

fn map_row(row: &Row) -> DbResult<ProviderFeeProfile> {
    let profile: Vec<u8> = row.get(0).map_err(read_error)?;
    let provider: Vec<u8> = row.get(1).map_err(read_error)?;
    let fixed: String = row.get(3).map_err(read_error)?;
    let payer: String = row.get(4).map_err(read_error)?;
    let status: String = row.get(5).map_err(read_error)?;
    let superseded: Option<Vec<u8>> = row.get(7).map_err(read_error)?;
    let operation: Option<Vec<u8>> = row.get(8).map_err(read_error)?;
    let activated: Option<String> = row.get(12).map_err(read_error)?;
    let superseded_at: Option<String> = row.get(13).map_err(read_error)?;
    let created: String = row.get(14).map_err(read_error)?;
    let updated: String = row.get(15).map_err(read_error)?;
    Ok(ProviderFeeProfile {
        provider_fee_profile_id: raw16_to_uuid(&profile)?,
        provider_id: raw16_to_uuid(&provider)?,
        fee_policy: FeePolicy {
            rate_bps: row.get(2).map_err(read_error)?,
            fixed_amount_rials: fixed
                .parse()
                .map_err(|_| DbError::Query("invalid fixed fee".to_string()))?,
            fee_payer: FeePayer::from_db_value(&payer)
                .ok_or_else(|| DbError::Query("unknown fee payer".to_string()))?,
        },
        status: ProviderFeeProfileStatus::from_db_value(&status)
            .ok_or_else(|| DbError::Query("unknown fee status".to_string()))?,
        version: row.get(6).map_err(read_error)?,
        superseded_by_profile_id: superseded.as_deref().map(raw16_to_uuid).transpose()?,
        publication_operation_id: operation.as_deref().map(raw16_to_uuid).transpose()?,
        created_by_subject: row.get(9).map_err(read_error)?,
        updated_by_subject: row.get(10).map_err(read_error)?,
        change_reason: row.get(11).map_err(read_error)?,
        activated_at: activated.as_deref().map(parse_utc).transpose()?,
        superseded_at: superseded_at.as_deref().map(parse_utc).transpose()?,
        created_at: parse_utc(&created)?,
        updated_at: parse_utc(&updated)?,
    })
}
fn columns() -> &'static str {
    r#"provider_fee_profile_id,provider_id,rate_bps,TO_CHAR(fixed_amount_rials,'FM99999999999999999999999999999999999999'),fee_payer,status,version,superseded_by_profile_id,publication_operation_id,created_by_subject,updated_by_subject,change_reason,TO_CHAR(activated_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'),TO_CHAR(superseded_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'),TO_CHAR(created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'),TO_CHAR(updated_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')"#
}

fn classify_idempotency(
    existing: &crate::domain::idempotency::IdempotencyRecord,
    hash: &str,
) -> DbResult<SetProviderFeeProfilePersistenceOutcome> {
    if existing.request_hash != hash {
        return Ok(SetProviderFeeProfilePersistenceOutcome::IdempotencyConflict);
    }
    Ok(match existing.status {
        IdempotencyStatus::Completed => existing
            .response_snapshot
            .clone()
            .map(SetProviderFeeProfilePersistenceOutcome::Replayed)
            .unwrap_or(SetProviderFeeProfilePersistenceOutcome::IdempotencyInvalidState),
        IdempotencyStatus::InProgress => {
            SetProviderFeeProfilePersistenceOutcome::IdempotencyInProgress
        }
        _ => SetProviderFeeProfilePersistenceOutcome::IdempotencyInvalidState,
    })
}

fn classify_receipt_replay(
    connection: &oracle::Connection,
    receipt: &ProviderFeeMaterializationReceipt,
) -> DbResult<Option<ProviderFeeReceiptPersistenceOutcome>> {
    let mut rows=connection.query("SELECT receipt_event_id,operation_id,aggregate_id,profile_id,materialized_version,redis_key FROM runtime_materialization_receipts WHERE receipt_event_id=:1 OR (operation_id=:2 AND profile_type='FEE' AND materialized_version=:3)",&[&raw(receipt.receipt_event_id),&raw(receipt.operation_id),&receipt.materialized_version]).map_err(|error|DbError::Query(format!("failed to check fee receipt: {error}")))?;
    if let Some(row) = rows.next() {
        let row =
            row.map_err(|error| DbError::Query(format!("failed to read fee receipt: {error}")))?;
        let exact = raw16_to_uuid(&row.get::<_, Vec<u8>>(0).map_err(read_error)?)?
            == receipt.receipt_event_id
            && raw16_to_uuid(&row.get::<_, Vec<u8>>(1).map_err(read_error)?)?
                == receipt.operation_id
            && raw16_to_uuid(&row.get::<_, Vec<u8>>(2).map_err(read_error)?)?
                == receipt.provider_id
            && raw16_to_uuid(&row.get::<_, Vec<u8>>(3).map_err(read_error)?)?
                == receipt.provider_fee_profile_id
            && row.get::<_, i64>(4).map_err(read_error)? == receipt.materialized_version
            && row.get::<_, String>(5).map_err(read_error)? == receipt.runtime_key;
        return Ok(Some(if exact {
            ProviderFeeReceiptPersistenceOutcome::Replayed
        } else {
            ProviderFeeReceiptPersistenceOutcome::Mismatch
        }));
    }
    let failed:i64=connection.query_row_as("SELECT COUNT(*) FROM integration_inbox WHERE source_system='WOLFSBURG' AND source_event_id=:1 AND status='FAILED'",&[&raw(receipt.receipt_event_id)]).map_err(|error|DbError::Query(format!("failed to check rejected fee receipt: {error}")))?;
    Ok((failed > 0).then_some(ProviderFeeReceiptPersistenceOutcome::Mismatch))
}
fn insert_inbox(
    connection: &oracle::Connection,
    receipt: &ProviderFeeMaterializationReceipt,
) -> DbResult<()> {
    let payload = serde_json::to_string(receipt)
        .map_err(|error| DbError::Query(format!("failed to serialize fee receipt: {error}")))?;
    connection.execute("INSERT INTO integration_inbox (inbox_event_id,source_system,source_event_id,event_type,aggregate_type,aggregate_id,payload_json,status) VALUES (:1,'WOLFSBURG',:2,'RUNTIME_PROFILE_MATERIALIZED','PROVIDER',:3,:4,'RECEIVED')",&[&raw(Uuid::new_v4()),&raw(receipt.receipt_event_id),&raw(receipt.provider_id),&payload]).map_err(|error|DbError::Query(format!("failed to insert fee inbox: {error}")))?;
    Ok(())
}
fn fail_inbox(
    connection: &oracle::Connection,
    receipt: &ProviderFeeMaterializationReceipt,
) -> DbResult<()> {
    let error=serde_json::json!({"code":"PROVIDER_FEE_MATERIALIZATION_MISMATCH","operation_id":receipt.operation_id,"profile_id":receipt.provider_fee_profile_id}).to_string();
    connection.execute("UPDATE integration_inbox SET status='FAILED',processed_at=SYSTIMESTAMP,error_json=:1 WHERE source_system='WOLFSBURG' AND source_event_id=:2",&[&error,&raw(receipt.receipt_event_id)]).map_err(|error|DbError::Query(format!("failed to reject fee receipt: {error}")))?;
    Ok(())
}
fn complete_inbox(connection: &oracle::Connection, event_id: Uuid) -> DbResult<()> {
    connection.execute("UPDATE integration_inbox SET status='PROCESSED',processed_at=SYSTIMESTAMP WHERE source_system='WOLFSBURG' AND source_event_id=:1",&[&raw(event_id)]).map_err(|error|DbError::Query(format!("failed to complete fee inbox: {error}")))?;
    Ok(())
}
fn insert_runtime_receipt(
    connection: &oracle::Connection,
    receipt: &ProviderFeeMaterializationReceipt,
) -> DbResult<()> {
    let at = receipt
        .materialized_at
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string();
    connection.execute(r#"INSERT INTO runtime_materialization_receipts (runtime_materialization_receipt_id,receipt_event_id,operation_id,profile_type,aggregate_id,profile_id,materialized_version,redis_key,materialized_at) VALUES (:1,:2,:3,'FEE',:4,:5,:6,:7,TO_TIMESTAMP_TZ(:8,'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'))"#,&[&raw(Uuid::new_v4()),&raw(receipt.receipt_event_id),&raw(receipt.operation_id),&raw(receipt.provider_id),&raw(receipt.provider_fee_profile_id),&receipt.materialized_version,&receipt.runtime_key,&at]).map_err(|error|DbError::Query(format!("failed to persist fee receipt: {error}")))?;
    Ok(())
}
fn parse_utc(value: &str) -> DbResult<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)
        .map_err(|error| DbError::Query(format!("invalid fee UTC timestamp: {error}")))?
        .with_timezone(&Utc))
}
fn raw(value: Uuid) -> Vec<u8> {
    uuid_to_raw16(value).to_vec()
}
fn read_error(error: oracle::Error) -> DbError {
    DbError::Query(format!("failed to read Oracle fee row: {error}"))
}
