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
        card_policy::{
            CardPolicyProfile, CardPolicyStatus, CardPolicyTerms, DesiredCardPolicy,
            PolicyMaterializationReceipt,
        },
        card_range::{FundingMode, LimitCalendar, WithdrawalLimitAuthority},
        idempotency::IdempotencyStatus,
    },
    kafka::contract::{InternalEventEnvelope, InternalEventHeaders},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PolicyMutationDisposition {
    Created,
    Updated,
    PublicationPending,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SetCardPolicyResult {
    pub disposition: PolicyMutationDisposition,
    pub profile: CardPolicyProfile,
    pub operation_id: Option<Uuid>,
    pub funding_mode: FundingMode,
    pub withdrawal_limit_authority: WithdrawalLimitAuthority,
    pub limit_calendar: Option<LimitCalendar>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetCardPolicyPersistenceOutcome {
    Applied(Box<SetCardPolicyResult>),
    Replayed(serde_json::Value),
    RangeNotFound,
    ContractInvalid(String),
    DraftFrozen,
    IdempotencyConflict,
    IdempotencyInProgress,
    IdempotencyInvalidState,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PolicyReceiptPersistenceOutcome {
    Activated(Box<CardPolicyProfile>),
    Replayed,
    Mismatch,
}

#[derive(Debug, Clone, PartialEq)]
struct LockedRangePolicyContext {
    funding_mode: FundingMode,
    authority: WithdrawalLimitAuthority,
    limit_calendar: Option<LimitCalendar>,
    active_provider_count: i64,
}

impl OracleRepository {
    #[tracing::instrument(
        skip(self, command_context, desired_policy),
        fields(db.system = "oracle", db.operation.name = "card_policies.set")
    )]
    pub async fn set_card_policy_atomic(
        &self,
        command_context: MutationCommandContext,
        card_range_id: Uuid,
        desired_policy: DesiredCardPolicy,
    ) -> DbResult<SetCardPolicyPersistenceOutcome> {
        let operation_type = command_context.operation_type.clone();
        let idempotency_key = command_context.idempotency_key.as_str().to_string();
        let request_hash = command_context.request_hash.clone();
        let retry_operation_type = operation_type.clone();
        let retry_idempotency_key = idempotency_key.clone();
        let retry_request_hash = request_hash.clone();
        let pool = self.pool.clone();
        let event_headers = InternalEventHeaders::from_current_span(
            command_context.request.correlation_id.clone(),
            command_context.request.request_id.to_string(),
        );

        let result = self
            .pool
            .with_transaction("atomic card policy configuration", move |connection| {
                if let Some(existing) = traced_db_step("idempotency.lookup", || {
                    fetch_idempotency_record(connection, &operation_type, &idempotency_key)
                })? {
                    return classify_idempotency(&existing, &request_hash);
                }

                let Some(range) = traced_db_step("card_policies.lock_range", || {
                    lock_range_policy_context(connection, card_range_id)
                })?
                else {
                    return Ok(SetCardPolicyPersistenceOutcome::RangeNotFound);
                };
                let contract_validation = match range.authority {
                    WithdrawalLimitAuthority::Platform => desired_policy.validate_for_platform(),
                    WithdrawalLimitAuthority::Cms => desired_policy.validate_for_cms(),
                };
                if let Err(error) = contract_validation {
                    return Ok(SetCardPolicyPersistenceOutcome::ContractInvalid(
                        error.to_string(),
                    ));
                }

                // Re-read after the range row lock. A concurrent request may have
                // completed while this command was waiting for the aggregate.
                if let Some(existing) = traced_db_step("idempotency.recheck", || {
                    fetch_idempotency_record(connection, &operation_type, &idempotency_key)
                })? {
                    return classify_idempotency(&existing, &request_hash);
                }

                let existing_draft = traced_db_step("card_policies.find_draft", || {
                    fetch_policy_by_status(connection, card_range_id, CardPolicyStatus::Draft)
                })?;
                if existing_draft
                    .as_ref()
                    .is_some_and(|draft| draft.publication_operation_id.is_some())
                {
                    return Ok(SetCardPolicyPersistenceOutcome::DraftFrozen);
                }

                let terms = desired_policy.terms.clone();
                let reason = desired_policy.reason.trim().to_string();
                let terms_json = serde_json::to_string(&terms).map_err(|error| {
                    DbError::Query(format!("failed to serialize card policy terms: {error}"))
                })?;
                let policy_id = existing_draft
                    .as_ref()
                    .map(|draft| draft.card_policy_profile_id)
                    .unwrap_or_else(Uuid::new_v4);
                let version = existing_draft
                    .as_ref()
                    .map(|draft| draft.version)
                    .unwrap_or(next_policy_version(connection, card_range_id)?);
                let operation_id = (range.active_provider_count > 0).then(Uuid::new_v4);

                if let Some(operation_id) = operation_id {
                    traced_db_step("integration_outbox.insert_policy", || {
                        insert_policy_outbox(
                            connection,
                            operation_id,
                            card_range_id,
                            policy_id,
                            version,
                            range.funding_mode,
                            range.authority,
                            range.limit_calendar.as_ref(),
                            &terms,
                            &event_headers,
                        )
                    })?;
                }

                traced_db_step("idempotency.insert", || {
                    insert_idempotency_record(connection, command_context.new_idempotency_record())
                })?;

                let (profile, action, old_values, disposition) =
                    if let Some(existing_draft) = existing_draft {
                        traced_db_step("card_policies.update_draft", || {
                            update_draft_policy(
                                connection,
                                policy_id,
                                &terms_json,
                                operation_id,
                                &command_context.actor.subject,
                                &reason,
                            )
                        })?;
                        let updated = fetch_policy(connection, policy_id)?;
                        let disposition = if operation_id.is_some() {
                            PolicyMutationDisposition::PublicationPending
                        } else {
                            PolicyMutationDisposition::Updated
                        };
                        (
                            updated,
                            AuditAction::Update,
                            Some(existing_draft.replay_snapshot()),
                            disposition,
                        )
                    } else {
                        traced_db_step("card_policies.insert_draft", || {
                            insert_draft_policy(
                                connection,
                                policy_id,
                                card_range_id,
                                &terms_json,
                                version,
                                operation_id,
                                &command_context.actor.subject,
                                &reason,
                            )
                        })?;
                        let created = fetch_policy(connection, policy_id)?;
                        let disposition = if operation_id.is_some() {
                            PolicyMutationDisposition::PublicationPending
                        } else {
                            PolicyMutationDisposition::Created
                        };
                        (created, AuditAction::Insert, None, disposition)
                    };

                let result = SetCardPolicyResult {
                    disposition,
                    profile: profile.clone(),
                    operation_id,
                    funding_mode: range.funding_mode,
                    withdrawal_limit_authority: range.authority,
                    limit_calendar: range.limit_calendar,
                };
                let response_snapshot = serde_json::to_value(&result).map_err(|error| {
                    DbError::Query(format!("failed to serialize policy response: {error}"))
                })?;

                traced_db_step("audit_logs.insert", || {
                    insert_audit_log(
                        connection,
                        NewAuditLog {
                            audit_log_id: Uuid::new_v4(),
                            entity_type: "CARD_POLICY_PROFILE".to_string(),
                            entity_id: policy_id,
                            action_type: action,
                            reason: Some(reason),
                            old_values,
                            new_values: Some(profile.replay_snapshot()),
                            context: command_context.audit_context(),
                        },
                    )
                })?;
                traced_db_step("idempotency.complete", || {
                    complete_idempotency_record(
                        connection,
                        &operation_type,
                        &idempotency_key,
                        "card_policy_profile",
                        policy_id,
                        response_snapshot,
                    )
                })?;

                Ok(SetCardPolicyPersistenceOutcome::Applied(Box::new(result)))
            })
            .await;

        match result {
            Err(DbError::Conflict(_)) => {
                pool.with_connection(move |connection| {
                    let existing = fetch_idempotency_record(
                        connection,
                        &retry_operation_type,
                        &retry_idempotency_key,
                    )?
                    .ok_or_else(|| {
                        DbError::Query(
                            "concurrent policy idempotency winner was not visible".to_string(),
                        )
                    })?;
                    classify_idempotency(&existing, &retry_request_hash)
                })
                .await
            }
            other => other,
        }
    }

    #[tracing::instrument(skip(self), fields(db.system = "oracle", db.operation.name = "card_policies.get_current"))]
    pub async fn get_current_card_policy(
        &self,
        card_range_id: Uuid,
    ) -> DbResult<Option<CardPolicyProfile>> {
        self.pool
            .with_connection(move |connection| {
                // "Current" is the operational policy only. A replacement
                // DRAFT is observable through history/by-ID APIs but cannot be
                // mistaken for runtime configuration before its receipt.
                fetch_policy_by_status(connection, card_range_id, CardPolicyStatus::Active)
            })
            .await
    }

    #[tracing::instrument(skip(self), fields(db.system = "oracle", db.operation.name = "card_policies.get"))]
    pub async fn get_card_policy(
        &self,
        card_range_id: Uuid,
        policy_id: Uuid,
    ) -> DbResult<Option<CardPolicyProfile>> {
        self.pool
            .with_connection(move |connection| {
                let policy = fetch_optional_policy(connection, policy_id)?;
                Ok(policy.filter(|profile| profile.card_range_id == card_range_id))
            })
            .await
    }

    #[tracing::instrument(skip(self), fields(db.system = "oracle", db.operation.name = "card_policies.list"))]
    pub async fn list_card_policies(
        &self,
        card_range_id: Uuid,
        before_version: Option<i64>,
        limit: u16,
    ) -> DbResult<Vec<CardPolicyProfile>> {
        self.pool
            .with_connection(move |connection| {
                let fetch_limit = i64::from(limit);
                let rows = connection
                    .query(
                        &policy_list_sql(),
                        &[
                            &card_range_id_raw(card_range_id),
                            &before_version,
                            &before_version,
                            &fetch_limit,
                        ],
                    )
                    .map_err(|error| {
                        DbError::Query(format!("failed to list card policies: {error}"))
                    })?;
                rows.map(|row| {
                    row.map_err(|error| {
                        DbError::Query(format!("failed to read card policy list row: {error}"))
                    })
                    .and_then(|row| map_policy_row(&row))
                })
                .collect()
            })
            .await
    }

    /// Atomically records Wolfsburg's receipt and performs the policy switch.
    /// A mismatch is committed as failed inbox evidence while leaving both the
    /// pending and currently active policy unchanged.
    #[tracing::instrument(
        skip(self, receipt),
        fields(
            messaging.system = "kafka",
            messaging.operation.name = "process",
            messaging.message.id = %receipt.receipt_event_id,
            operation_id = %receipt.operation_id,
            card_range_id = %receipt.card_range_id,
            policy_id = %receipt.card_policy_profile_id,
            policy_version = receipt.materialized_version
        )
    )]
    pub async fn apply_policy_materialization_receipt(
        &self,
        receipt: PolicyMaterializationReceipt,
    ) -> DbResult<PolicyReceiptPersistenceOutcome> {
        self.pool
            .with_transaction("card policy materialization receipt", move |connection| {
                if let Some(outcome) = traced_db_step("integration_inbox.check_replay", || {
                    classify_existing_receipt(connection, &receipt)
                })? {
                    return Ok(outcome);
                }

                traced_db_step("integration_inbox.insert", || {
                    insert_receipt_inbox(connection, &receipt)
                })?;
                let pending = traced_db_step("card_policies.lock_pending", || {
                    fetch_policy_by_operation_for_update(connection, receipt.operation_id)
                })?;
                let expected_key = pending
                    .as_ref()
                    .map(|profile| expected_runtime_key(connection, profile.card_range_id))
                    .transpose()?
                    .flatten();
                let matches = pending.as_ref().is_some_and(|profile| {
                    profile.card_range_id == receipt.card_range_id
                        && profile.card_policy_profile_id == receipt.card_policy_profile_id
                        && profile.version == receipt.materialized_version
                        && expected_key.as_deref() == Some(receipt.runtime_key.as_str())
                });

                if !matches {
                    traced_db_step("integration_inbox.reject", || {
                        mark_receipt_inbox_failed(connection, &receipt)
                    })?;
                    return Ok(PolicyReceiptPersistenceOutcome::Mismatch);
                }

                let pending = pending.expect("validated pending policy must exist");
                let previous_active = fetch_policy_by_status(
                    connection,
                    receipt.card_range_id,
                    CardPolicyStatus::Active,
                )?;
                traced_db_step("runtime_receipts.insert", || {
                    insert_materialization_receipt(connection, &receipt)
                })?;
                traced_db_step("card_policies.supersede_active", || {
                    supersede_active_policy(
                        connection,
                        receipt.card_range_id,
                        receipt.card_policy_profile_id,
                    )
                })?;
                traced_db_step("card_policies.activate_pending", || {
                    activate_pending_policy(connection, &receipt)
                })?;
                traced_db_step("integration_inbox.complete", || {
                    mark_receipt_inbox_processed(connection, receipt.receipt_event_id)
                })?;

                let activated = fetch_policy(connection, receipt.card_policy_profile_id)?;
                traced_db_step("audit_logs.insert_receipt", || {
                    insert_audit_log(
                        connection,
                        NewAuditLog {
                            audit_log_id: Uuid::new_v4(),
                            entity_type: "CARD_POLICY_PROFILE".to_string(),
                            entity_id: receipt.card_policy_profile_id,
                            action_type: AuditAction::StateTransition,
                            reason: Some("Wolfsburg confirmed CPOL materialization".to_string()),
                            old_values: Some(pending.replay_snapshot()),
                            new_values: Some(serde_json::json!({
                                "activated_policy": activated.replay_snapshot(),
                                "previous_active_policy_id": previous_active
                                    .map(|profile| profile.card_policy_profile_id),
                                "receipt_event_id": receipt.receipt_event_id,
                                "operation_id": receipt.operation_id,
                                "materialized_version": receipt.materialized_version,
                            })),
                            context: crate::domain::audit::TrustedAuditContext {
                                actor_subject: "wolfsburg".to_string(),
                                actor_client_id: Some("wolfsburg-materializer".to_string()),
                                actor_provider_id: None,
                                actor_user_id: None,
                                actor_issuer: Some("internal-kafka".to_string()),
                                source_ip: None,
                                correlation_id: receipt.operation_id.to_string(),
                                request_id: receipt.receipt_event_id.to_string(),
                            },
                        },
                    )
                })?;

                Ok(PolicyReceiptPersistenceOutcome::Activated(Box::new(
                    activated,
                )))
            })
            .await
    }
}

fn traced_db_step<T>(
    operation_name: &'static str,
    operation: impl FnOnce() -> DbResult<T>,
) -> DbResult<T> {
    let span = tracing::info_span!(
        "oracle.command.step",
        db.system = "oracle",
        db.operation.name = operation_name
    );
    let _guard = span.enter();
    operation()
}

fn classify_existing_receipt(
    connection: &oracle::Connection,
    receipt: &PolicyMaterializationReceipt,
) -> DbResult<Option<PolicyReceiptPersistenceOutcome>> {
    let mut rows = connection
        .query(
            policy_receipt_replay_sql(),
            &[
                &card_range_id_raw(receipt.receipt_event_id),
                &card_range_id_raw(receipt.operation_id),
                &receipt.materialized_version,
            ],
        )
        .map_err(|error| {
            DbError::Query(format!("failed to check policy receipt replay: {error}"))
        })?;
    if let Some(row) = rows.next() {
        let row = row.map_err(|error| {
            DbError::Query(format!("failed to read policy receipt replay: {error}"))
        })?;
        let stored_receipt_event: Vec<u8> = row.get(0).map_err(read_error)?;
        let stored_operation: Vec<u8> = row.get(1).map_err(read_error)?;
        let stored_aggregate: Vec<u8> = row.get(2).map_err(read_error)?;
        let stored_profile: Vec<u8> = row.get(3).map_err(read_error)?;
        let stored_version: i64 = row.get(4).map_err(read_error)?;
        let stored_key: String = row.get(5).map_err(read_error)?;
        let exact = raw16_to_uuid(&stored_receipt_event)? == receipt.receipt_event_id
            && raw16_to_uuid(&stored_operation)? == receipt.operation_id
            && raw16_to_uuid(&stored_aggregate)? == receipt.card_range_id
            && raw16_to_uuid(&stored_profile)? == receipt.card_policy_profile_id
            && stored_version == receipt.materialized_version
            && stored_key == receipt.runtime_key;
        return Ok(Some(if exact {
            PolicyReceiptPersistenceOutcome::Replayed
        } else {
            PolicyReceiptPersistenceOutcome::Mismatch
        }));
    }

    let failed_inbox_count = connection
        .query_row_as::<i64>(
            "SELECT COUNT(*) FROM integration_inbox WHERE source_system = 'WOLFSBURG' AND source_event_id = :1 AND status = 'FAILED'",
            &[&card_range_id_raw(receipt.receipt_event_id)],
        )
        .map_err(|error| DbError::Query(format!("failed to check rejected policy receipt: {error}")))?;
    Ok((failed_inbox_count > 0).then_some(PolicyReceiptPersistenceOutcome::Mismatch))
}

fn insert_receipt_inbox(
    connection: &oracle::Connection,
    receipt: &PolicyMaterializationReceipt,
) -> DbResult<()> {
    let payload = serde_json::to_string(receipt)
        .map_err(|error| DbError::Query(format!("failed to serialize policy receipt: {error}")))?;
    connection
        .execute(
            policy_receipt_inbox_insert_sql(),
            &[
                &card_range_id_raw(Uuid::new_v4()),
                &"WOLFSBURG",
                &card_range_id_raw(receipt.receipt_event_id),
                &"RUNTIME_PROFILE_MATERIALIZED",
                &"CARD_RANGE",
                &card_range_id_raw(receipt.card_range_id),
                &payload,
            ],
        )
        .map_err(|error| {
            DbError::Query(format!("failed to insert policy receipt inbox: {error}"))
        })?;
    Ok(())
}

fn fetch_policy_by_operation_for_update(
    connection: &oracle::Connection,
    operation_id: Uuid,
) -> DbResult<Option<CardPolicyProfile>> {
    let sql = format!(
        "SELECT {} FROM card_policy_profiles WHERE publication_operation_id = :1 AND status = 'DRAFT' FOR UPDATE",
        policy_select_columns()
    );
    let mut rows = connection
        .query(&sql, &[&card_range_id_raw(operation_id)])
        .map_err(|error| DbError::Query(format!("failed to lock pending policy: {error}")))?;
    match rows.next() {
        Some(Ok(row)) => map_policy_row(&row).map(Some),
        Some(Err(error)) => Err(DbError::Query(format!(
            "failed to read pending policy: {error}"
        ))),
        None => Ok(None),
    }
}

fn expected_runtime_key(
    connection: &oracle::Connection,
    card_range_id: Uuid,
) -> DbResult<Option<String>> {
    let mode = connection
        .query_row_as::<String>(
            "SELECT funding_mode FROM card_ranges WHERE card_range_id = :1",
            &[&card_range_id_raw(card_range_id)],
        )
        .map_err(|error| {
            DbError::Query(format!("failed to resolve policy runtime key: {error}"))
        })?;
    let prefix = match FundingMode::from_db_value(&mode) {
        Some(FundingMode::SingleProvider) => "CPOL:SingleProvider",
        Some(FundingMode::MultiProvider) => "CPOL:MultiProvider",
        None => return Ok(None),
    };
    Ok(Some(format!("{prefix}:{card_range_id}")))
}

fn mark_receipt_inbox_failed(
    connection: &oracle::Connection,
    receipt: &PolicyMaterializationReceipt,
) -> DbResult<()> {
    let safe_error = serde_json::json!({
        "code": "POLICY_MATERIALIZATION_MISMATCH",
        "operation_id": receipt.operation_id,
        "profile_id": receipt.card_policy_profile_id,
        "materialized_version": receipt.materialized_version,
    })
    .to_string();
    connection
        .execute(
            "UPDATE integration_inbox SET status = 'FAILED', processed_at = SYSTIMESTAMP, error_json = :1 WHERE source_system = 'WOLFSBURG' AND source_event_id = :2",
            &[&safe_error, &card_range_id_raw(receipt.receipt_event_id)],
        )
        .map_err(|error| DbError::Query(format!("failed to reject policy receipt: {error}")))?;
    Ok(())
}

fn insert_materialization_receipt(
    connection: &oracle::Connection,
    receipt: &PolicyMaterializationReceipt,
) -> DbResult<()> {
    let materialized_at = receipt
        .materialized_at
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string();
    connection
        .execute(
            policy_receipt_insert_sql(),
            &[
                &card_range_id_raw(Uuid::new_v4()),
                &card_range_id_raw(receipt.receipt_event_id),
                &card_range_id_raw(receipt.operation_id),
                &"CPOL",
                &card_range_id_raw(receipt.card_range_id),
                &card_range_id_raw(receipt.card_policy_profile_id),
                &receipt.materialized_version,
                &receipt.runtime_key,
                &materialized_at,
            ],
        )
        .map_err(|error| DbError::Query(format!("failed to persist policy receipt: {error}")))?;
    Ok(())
}

fn supersede_active_policy(
    connection: &oracle::Connection,
    card_range_id: Uuid,
    replacement_policy_id: Uuid,
) -> DbResult<()> {
    connection
        .execute(
            "UPDATE card_policy_profiles SET status = 'SUPERSEDED', superseded_by_profile_id = :1, superseded_at = SYSTIMESTAMP, updated_at = SYSTIMESTAMP WHERE card_range_id = :2 AND status = 'ACTIVE'",
            &[
                &card_range_id_raw(replacement_policy_id),
                &card_range_id_raw(card_range_id),
            ],
        )
        .map_err(|error| DbError::Query(format!("failed to supersede active policy: {error}")))?;
    Ok(())
}

fn activate_pending_policy(
    connection: &oracle::Connection,
    receipt: &PolicyMaterializationReceipt,
) -> DbResult<()> {
    let statement = connection
        .execute(
            "UPDATE card_policy_profiles SET status = 'ACTIVE', activated_at = SYSTIMESTAMP, updated_at = SYSTIMESTAMP WHERE card_policy_profile_id = :1 AND publication_operation_id = :2 AND status = 'DRAFT'",
            &[
                &card_range_id_raw(receipt.card_policy_profile_id),
                &card_range_id_raw(receipt.operation_id),
            ],
        )
        .map_err(|error| DbError::Query(format!("failed to activate pending policy: {error}")))?;
    if statement.row_count().map_err(|error| {
        DbError::Query(format!("failed to read policy activation count: {error}"))
    })? != 1
    {
        return Err(DbError::Query(
            "pending policy changed before receipt activation".to_string(),
        ));
    }
    Ok(())
}

fn mark_receipt_inbox_processed(
    connection: &oracle::Connection,
    receipt_event_id: Uuid,
) -> DbResult<()> {
    connection
        .execute(
            "UPDATE integration_inbox SET status = 'PROCESSED', processed_at = SYSTIMESTAMP WHERE source_system = 'WOLFSBURG' AND source_event_id = :1",
            &[&card_range_id_raw(receipt_event_id)],
        )
        .map_err(|error| DbError::Query(format!("failed to complete policy receipt inbox: {error}")))?;
    Ok(())
}

fn classify_idempotency(
    existing: &crate::domain::idempotency::IdempotencyRecord,
    request_hash: &str,
) -> DbResult<SetCardPolicyPersistenceOutcome> {
    if existing.request_hash != request_hash {
        return Ok(SetCardPolicyPersistenceOutcome::IdempotencyConflict);
    }
    Ok(match existing.status {
        IdempotencyStatus::Completed => existing
            .response_snapshot
            .clone()
            .map(SetCardPolicyPersistenceOutcome::Replayed)
            .unwrap_or(SetCardPolicyPersistenceOutcome::IdempotencyInvalidState),
        IdempotencyStatus::InProgress => SetCardPolicyPersistenceOutcome::IdempotencyInProgress,
        IdempotencyStatus::Failed | IdempotencyStatus::Conflict => {
            SetCardPolicyPersistenceOutcome::IdempotencyInvalidState
        }
    })
}

fn lock_range_policy_context(
    connection: &oracle::Connection,
    card_range_id: Uuid,
) -> DbResult<Option<LockedRangePolicyContext>> {
    let raw_id = card_range_id_raw(card_range_id);
    let mut rows = connection
        .query(policy_range_lock_sql(), &[&raw_id])
        .map_err(|error| DbError::Query(format!("failed to lock policy card range: {error}")))?;
    let Some(row) = rows.next() else {
        return Ok(None);
    };
    let row = row.map_err(|error| {
        DbError::Query(format!("failed to read locked policy card range: {error}"))
    })?;
    let funding_mode: String = row.get(0).map_err(read_error)?;
    let authority: String = row.get(1).map_err(read_error)?;
    let calendar_json: Option<String> = row.get(2).map_err(read_error)?;
    Ok(Some(LockedRangePolicyContext {
        funding_mode: FundingMode::from_db_value(&funding_mode)
            .ok_or_else(|| DbError::Query("unknown policy funding mode".to_string()))?,
        authority: WithdrawalLimitAuthority::from_db_value(&authority)
            .ok_or_else(|| DbError::Query("unknown policy authority".to_string()))?,
        limit_calendar: calendar_json
            .as_deref()
            .map(super::card_range::limit_calendar_from_json)
            .transpose()?,
        active_provider_count: row.get(3).map_err(read_error)?,
    }))
}

fn next_policy_version(connection: &oracle::Connection, card_range_id: Uuid) -> DbResult<i64> {
    connection
        .query_row_as::<i64>(
            policy_next_version_sql(),
            &[&card_range_id_raw(card_range_id)],
        )
        .map_err(|error| DbError::Query(format!("failed to allocate policy version: {error}")))
}

#[allow(clippy::too_many_arguments)]
fn insert_draft_policy(
    connection: &oracle::Connection,
    policy_id: Uuid,
    card_range_id: Uuid,
    terms_json: &str,
    version: i64,
    operation_id: Option<Uuid>,
    actor_subject: &str,
    reason: &str,
) -> DbResult<()> {
    connection
        .execute(
            policy_insert_sql(),
            &[
                &card_range_id_raw(policy_id),
                &card_range_id_raw(card_range_id),
                &terms_json,
                &CardPolicyStatus::Draft.as_db_value(),
                &version,
                &operation_id.map(card_range_id_raw),
                &actor_subject,
                &actor_subject,
                &reason,
            ],
        )
        .map_err(|error| DbError::Query(format!("failed to insert draft policy: {error}")))?;
    Ok(())
}

fn update_draft_policy(
    connection: &oracle::Connection,
    policy_id: Uuid,
    terms_json: &str,
    operation_id: Option<Uuid>,
    actor_subject: &str,
    reason: &str,
) -> DbResult<()> {
    let statement = connection
        .execute(
            policy_update_draft_sql(),
            &[
                &terms_json,
                &operation_id.map(card_range_id_raw),
                &actor_subject,
                &reason,
                &card_range_id_raw(policy_id),
            ],
        )
        .map_err(|error| DbError::Query(format!("failed to update draft policy: {error}")))?;
    if statement.row_count().map_err(|error| {
        DbError::Query(format!("failed to read draft policy update count: {error}"))
    })? != 1
    {
        return Err(DbError::Query(
            "draft policy changed before update completed".to_string(),
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn insert_policy_outbox(
    connection: &oracle::Connection,
    operation_id: Uuid,
    card_range_id: Uuid,
    policy_id: Uuid,
    version: i64,
    funding_mode: FundingMode,
    authority: WithdrawalLimitAuthority,
    limit_calendar: Option<&LimitCalendar>,
    terms: &CardPolicyTerms,
    headers: &InternalEventHeaders,
) -> DbResult<()> {
    let event_id = Uuid::new_v4();
    let envelope = InternalEventEnvelope::new(
        event_id,
        "CARD_POLICY_PROFILE_PUBLISH_REQUESTED",
        "CARD_RANGE",
        card_range_id,
        operation_id,
        serde_json::json!({
            "card_range_id": card_range_id,
            "card_policy_profile_id": policy_id,
            "policy_version": version,
            "funding_mode": funding_mode.as_db_value(),
            "withdrawal_limit_authority": authority.as_db_value(),
            "withdrawal_limits": terms.withdrawal_limits,
            "calendar": limit_calendar,
        }),
    );
    let payload = serde_json::to_string(&envelope)
        .map_err(|error| DbError::Query(format!("failed to serialize policy event: {error}")))?;
    let headers = serde_json::to_string(headers)
        .map_err(|error| DbError::Query(format!("failed to serialize policy headers: {error}")))?;
    let partition_key = card_range_id.to_string();

    connection
        .execute(
            policy_outbox_insert_sql(),
            &[
                &card_range_id_raw(event_id),
                &card_range_id_raw(operation_id),
                &"CARD_POLICY_PROFILE_PUBLISH_REQUESTED",
                &"CARD_RANGE",
                &card_range_id_raw(card_range_id),
                &partition_key,
                &payload,
                &headers,
            ],
        )
        .map_err(|error| {
            DbError::Query(format!("failed to insert policy outbox event: {error}"))
        })?;
    Ok(())
}

fn fetch_policy_by_status(
    connection: &oracle::Connection,
    card_range_id: Uuid,
    status: CardPolicyStatus,
) -> DbResult<Option<CardPolicyProfile>> {
    let mut rows = connection
        .query(
            &policy_select_by_status_sql(),
            &[&card_range_id_raw(card_range_id), &status.as_db_value()],
        )
        .map_err(|error| DbError::Query(format!("failed to fetch policy by status: {error}")))?;
    match rows.next() {
        Some(Ok(row)) => map_policy_row(&row).map(Some),
        Some(Err(error)) => Err(DbError::Query(format!(
            "failed to read policy status row: {error}"
        ))),
        None => Ok(None),
    }
}

fn fetch_policy(connection: &oracle::Connection, policy_id: Uuid) -> DbResult<CardPolicyProfile> {
    fetch_optional_policy(connection, policy_id)?
        .ok_or_else(|| DbError::Query("card policy was not visible after persistence".to_string()))
}

fn fetch_optional_policy(
    connection: &oracle::Connection,
    policy_id: Uuid,
) -> DbResult<Option<CardPolicyProfile>> {
    let mut rows = connection
        .query(&policy_select_sql(), &[&card_range_id_raw(policy_id)])
        .map_err(|error| DbError::Query(format!("failed to fetch card policy: {error}")))?;
    match rows.next() {
        Some(Ok(row)) => map_policy_row(&row).map(Some),
        Some(Err(error)) => Err(DbError::Query(format!(
            "failed to read card policy: {error}"
        ))),
        None => Ok(None),
    }
}

fn map_policy_row(row: &Row) -> DbResult<CardPolicyProfile> {
    let policy_id: Vec<u8> = row.get(0).map_err(read_error)?;
    let card_range_id: Vec<u8> = row.get(1).map_err(read_error)?;
    let terms_json: String = row.get(2).map_err(read_error)?;
    let status: String = row.get(3).map_err(read_error)?;
    let superseded_by: Option<Vec<u8>> = row.get(5).map_err(read_error)?;
    let publication_operation: Option<Vec<u8>> = row.get(6).map_err(read_error)?;
    let activated_at: Option<String> = row.get(10).map_err(read_error)?;
    let superseded_at: Option<String> = row.get(11).map_err(read_error)?;
    let created_at: String = row.get(12).map_err(read_error)?;
    let updated_at: String = row.get(13).map_err(read_error)?;

    Ok(CardPolicyProfile {
        card_policy_profile_id: raw16_to_uuid(&policy_id)?,
        card_range_id: raw16_to_uuid(&card_range_id)?,
        terms: serde_json::from_str(&terms_json)
            .map_err(|error| DbError::Query(format!("invalid policy JSON in Oracle: {error}")))?,
        status: CardPolicyStatus::from_db_value(&status)
            .ok_or_else(|| DbError::Query("unknown card policy status".to_string()))?,
        version: row.get(4).map_err(read_error)?,
        superseded_by_profile_id: superseded_by.as_deref().map(raw16_to_uuid).transpose()?,
        publication_operation_id: publication_operation
            .as_deref()
            .map(raw16_to_uuid)
            .transpose()?,
        created_by_subject: row.get(7).map_err(read_error)?,
        updated_by_subject: row.get(8).map_err(read_error)?,
        change_reason: row.get(9).map_err(read_error)?,
        activated_at: activated_at.as_deref().map(parse_utc).transpose()?,
        superseded_at: superseded_at.as_deref().map(parse_utc).transpose()?,
        created_at: parse_utc(&created_at)?,
        updated_at: parse_utc(&updated_at)?,
    })
}

fn parse_utc(value: &str) -> DbResult<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)
        .map_err(|error| DbError::Query(format!("invalid policy UTC timestamp: {error}")))?
        .with_timezone(&Utc))
}

fn card_range_id_raw(value: Uuid) -> Vec<u8> {
    uuid_to_raw16(value).to_vec()
}

fn read_error(error: oracle::Error) -> DbError {
    DbError::Query(format!("failed to read Oracle policy row: {error}"))
}

pub(crate) fn policy_range_lock_sql() -> &'static str {
    r#"
    SELECT
        cr.funding_mode,
        cr.withdrawal_limit_authority,
        JSON_SERIALIZE(cr.limit_calendar_json RETURNING CLOB) AS limit_calendar_json,
        (SELECT COUNT(*) FROM card_range_providers crp
         WHERE crp.card_range_id = cr.card_range_id AND crp.status = 'ACTIVE')
    FROM card_ranges cr
    WHERE cr.card_range_id = :1
    FOR UPDATE
    "#
}

pub(crate) fn policy_insert_sql() -> &'static str {
    r#"
    INSERT INTO card_policy_profiles (
        card_policy_profile_id, card_range_id, profile_json, status, version,
        publication_operation_id, created_by_subject, updated_by_subject,
        change_reason
    ) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9)
    "#
}

pub(crate) fn policy_update_draft_sql() -> &'static str {
    r#"
    UPDATE card_policy_profiles
    SET profile_json = :1,
        publication_operation_id = :2,
        updated_by_subject = :3,
        change_reason = :4,
        updated_at = SYSTIMESTAMP
    WHERE card_policy_profile_id = :5
      AND status = 'DRAFT'
      AND publication_operation_id IS NULL
    "#
}

pub(crate) fn policy_next_version_sql() -> &'static str {
    "SELECT NVL(MAX(version), 0) + 1 FROM card_policy_profiles WHERE card_range_id = :1"
}

pub(crate) fn policy_outbox_insert_sql() -> &'static str {
    r#"
    INSERT INTO integration_outbox (
        outbox_event_id, operation_id, event_type, aggregate_type, aggregate_id,
        partition_key, payload_json, headers_json, status, next_attempt_at
    ) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, 'PENDING', SYSTIMESTAMP)
    "#
}

pub(crate) fn policy_receipt_replay_sql() -> &'static str {
    r#"
    SELECT receipt_event_id, operation_id, aggregate_id, profile_id,
           materialized_version, redis_key
    FROM runtime_materialization_receipts
    WHERE receipt_event_id = :1
       OR (operation_id = :2 AND profile_type = 'CPOL' AND materialized_version = :3)
    "#
}

pub(crate) fn policy_receipt_inbox_insert_sql() -> &'static str {
    r#"
    INSERT INTO integration_inbox (
        inbox_event_id, source_system, source_event_id, event_type,
        aggregate_type, aggregate_id, payload_json, status
    ) VALUES (:1, :2, :3, :4, :5, :6, :7, 'RECEIVED')
    "#
}

pub(crate) fn policy_receipt_insert_sql() -> &'static str {
    r#"
    INSERT INTO runtime_materialization_receipts (
        runtime_materialization_receipt_id, receipt_event_id, operation_id,
        profile_type, aggregate_id, profile_id, materialized_version,
        redis_key, materialized_at
    ) VALUES (
        :1, :2, :3, :4, :5, :6, :7, :8,
        FROM_TZ(TO_TIMESTAMP(:9, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'), 'UTC')
    )
    "#
}

fn policy_select_columns() -> &'static str {
    r#"
        card_policy_profile_id,
        card_range_id,
        JSON_SERIALIZE(profile_json RETURNING CLOB) AS profile_json,
        status,
        version,
        superseded_by_profile_id,
        publication_operation_id,
        created_by_subject,
        updated_by_subject,
        change_reason,
        TO_CHAR(SYS_EXTRACT_UTC(activated_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS activated_at,
        TO_CHAR(SYS_EXTRACT_UTC(superseded_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS superseded_at,
        TO_CHAR(SYS_EXTRACT_UTC(created_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS created_at,
        TO_CHAR(SYS_EXTRACT_UTC(updated_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS updated_at
    "#
}

fn policy_select_sql() -> String {
    format!(
        "SELECT {} FROM card_policy_profiles WHERE card_policy_profile_id = :1",
        policy_select_columns()
    )
}

fn policy_select_by_status_sql() -> String {
    format!(
        "SELECT {} FROM card_policy_profiles WHERE card_range_id = :1 AND status = :2",
        policy_select_columns()
    )
}

fn policy_list_sql() -> String {
    format!(
        "SELECT {} FROM card_policy_profiles WHERE card_range_id = :1 AND (:2 IS NULL OR version < :3) ORDER BY version DESC FETCH NEXT :4 ROWS ONLY",
        policy_select_columns()
    )
}

#[cfg(test)]
mod tests {
    use super::{
        policy_list_sql, policy_outbox_insert_sql, policy_receipt_insert_sql,
        policy_receipt_replay_sql, policy_update_draft_sql,
    };

    #[test]
    fn policy_history_uses_distinct_oracle_bind_positions() {
        let sql = policy_list_sql();
        assert!(sql.contains("(:2 IS NULL OR version < :3)"));
        assert!(sql.contains("FETCH NEXT :4 ROWS ONLY"));
    }

    #[test]
    fn draft_update_refuses_a_frozen_publication() {
        let sql = policy_update_draft_sql();
        assert!(sql.contains("status = 'DRAFT'"));
        assert!(sql.contains("publication_operation_id IS NULL"));
    }

    #[test]
    fn policy_outbox_stores_operation_and_retry_state() {
        let sql = policy_outbox_insert_sql();
        assert!(sql.contains("operation_id"));
        assert!(sql.contains("'PENDING'"));
        assert!(sql.contains("SYSTIMESTAMP"));
    }

    #[test]
    fn receipt_sql_supports_replay_and_durable_activation_proof() {
        assert!(policy_receipt_replay_sql().contains("operation_id = :2"));
        assert!(policy_receipt_replay_sql().contains("profile_type = 'CPOL'"));
        assert!(policy_receipt_insert_sql().contains("materialized_version"));
        assert!(policy_receipt_insert_sql().contains("materialized_at"));
    }
}
