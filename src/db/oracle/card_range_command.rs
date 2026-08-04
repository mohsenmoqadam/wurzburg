use uuid::Uuid;

use crate::{
    api::command::MutationCommandContext,
    db::{
        error::{DbError, DbResult},
        oracle::{
            OracleRepository,
            audit::insert_audit_log,
            card_range::{card_range_lock_sql, card_range_select_sql, fetch_card_range},
            idempotency::{
                complete_idempotency_record, fetch_idempotency_record, insert_idempotency_record,
            },
            types::uuid_to_raw16,
        },
    },
    domain::{
        audit::{AuditAction, NewAuditLog},
        card_range::{
            CardRange, CardRangeControlChange, CardRangeStatus, DraftCardRangeUpdate, FundingMode,
        },
        idempotency::IdempotencyStatus,
    },
    messaging::contract::{InternalEventEnvelope, InternalEventHeaders},
};

#[derive(Debug, Clone)]
pub enum CardRangeMutation {
    UpdateDraft(DraftCardRangeUpdate),
    Activate { reason: String },
    Suspend { reason: String },
    UpdateControls(CardRangeControlChange),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CardRangeMutationResult {
    pub card_range: CardRange,
    pub operation_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CardRangeMutationPersistenceOutcome {
    Applied(Box<CardRangeMutationResult>),
    Replayed(serde_json::Value),
    NotFound,
    InvalidTransition,
    Immutable,
    PrerequisitesMissing,
    FeeProfilesMissing,
    PublicationPending,
    Overlap,
    ContractInvalid(String),
    IdempotencyConflict,
    IdempotencyInProgress,
    IdempotencyInvalidState,
}

impl OracleRepository {
    #[tracing::instrument(skip(self, context, mutation), fields(db.system = "oracle", db.operation.name = "card_ranges.mutate", card_range_id = %card_range_id))]
    pub async fn mutate_card_range_atomic(
        &self,
        context: MutationCommandContext,
        card_range_id: Uuid,
        mutation: CardRangeMutation,
    ) -> DbResult<CardRangeMutationPersistenceOutcome> {
        let operation_type = context.operation_type.clone();
        let key = context.idempotency_key.as_str().to_string();
        let request_hash = context.request_hash.clone();
        let headers = InternalEventHeaders::from_current_span(
            context.request.correlation_id.clone(),
            context.request.request_id.to_string(),
        );
        self.pool
            .with_transaction("atomic card range mutation", move |connection| {
                if let Some(existing) = fetch_idempotency_record(connection, &operation_type, &key)?
                {
                    return classify_idempotency(&existing, &request_hash);
                }
                connection
                    .query_row_as::<String>(card_range_lock_sql(), &[&"CARD_RANGE_STRUCTURE"])
                    .map_err(|error| {
                        DbError::Query(format!("failed to lock card range structure: {error}"))
                    })?;
                let current = match fetch_range_for_update(connection, card_range_id)? {
                    Some(value) => value,
                    None => return Ok(CardRangeMutationPersistenceOutcome::NotFound),
                };
                if let Some(existing) = fetch_idempotency_record(connection, &operation_type, &key)?
                {
                    return classify_idempotency(&existing, &request_hash);
                }

                let before = current.replay_snapshot();
                let operation_id = match &mutation {
                    CardRangeMutation::UpdateDraft(update) => {
                        if current.status != CardRangeStatus::Draft {
                            return Ok(CardRangeMutationPersistenceOutcome::Immutable);
                        }
                        let desired = match update.apply_to(&current) {
                            Ok(value) => value,
                            Err(error) => {
                                return Ok(CardRangeMutationPersistenceOutcome::ContractInvalid(
                                    error.to_string(),
                                ));
                            }
                        };
                        if draft_values_equal(&current, &desired) {
                            return Ok(CardRangeMutationPersistenceOutcome::ContractInvalid(
                                "requested draft values do not change the card range".to_string(),
                            ));
                        }
                        if overlaps_other(
                            connection,
                            card_range_id,
                            &desired.numbers.start,
                            &desired.numbers.end,
                        )? {
                            return Ok(CardRangeMutationPersistenceOutcome::Overlap);
                        }
                        insert_idempotency_record(connection, context.new_idempotency_record())?;
                        update_draft(connection, &desired, &context.actor.subject)?;
                        None
                    }
                    CardRangeMutation::Activate { reason } => {
                        if !valid_reason(reason) {
                            return Ok(CardRangeMutationPersistenceOutcome::ContractInvalid(
                                "change reason must contain 1 through 1000 characters".to_string(),
                            ));
                        }
                        if current.status == CardRangeStatus::Active {
                            return Ok(CardRangeMutationPersistenceOutcome::InvalidTransition);
                        }
                        if current.range_control_operation_id.is_some() {
                            return Ok(CardRangeMutationPersistenceOutcome::PublicationPending);
                        }
                        match activation_ready(connection, &current)? {
                            ActivationReadiness::Ready => {}
                            ActivationReadiness::CorePrerequisitesMissing => {
                                return Ok(
                                    CardRangeMutationPersistenceOutcome::PrerequisitesMissing,
                                );
                            }
                            ActivationReadiness::FeeProfilesMissing => {
                                return Ok(CardRangeMutationPersistenceOutcome::FeeProfilesMissing);
                            }
                        }
                        insert_idempotency_record(connection, context.new_idempotency_record())?;
                        let operation_id = Uuid::new_v4();
                        update_runtime_state(
                            connection,
                            card_range_id,
                            CardRangeStatus::Active,
                            current.issuance_enabled,
                            current.cms_operation_mode.as_db_value(),
                            operation_id,
                            &context.actor.subject,
                        )?;
                        Some(operation_id)
                    }
                    CardRangeMutation::Suspend { reason } => {
                        if !valid_reason(reason) {
                            return Ok(CardRangeMutationPersistenceOutcome::ContractInvalid(
                                "change reason must contain 1 through 1000 characters".to_string(),
                            ));
                        }
                        if current.status != CardRangeStatus::Active
                            || current.range_control_operation_id.is_some()
                        {
                            return Ok(if current.range_control_operation_id.is_some() {
                                CardRangeMutationPersistenceOutcome::PublicationPending
                            } else {
                                CardRangeMutationPersistenceOutcome::InvalidTransition
                            });
                        }
                        insert_idempotency_record(connection, context.new_idempotency_record())?;
                        let operation_id = Uuid::new_v4();
                        update_runtime_state(
                            connection,
                            card_range_id,
                            CardRangeStatus::Suspended,
                            current.issuance_enabled,
                            current.cms_operation_mode.as_db_value(),
                            operation_id,
                            &context.actor.subject,
                        )?;
                        Some(operation_id)
                    }
                    CardRangeMutation::UpdateControls(change) => {
                        if let Err(error) = change.validate() {
                            return Ok(CardRangeMutationPersistenceOutcome::ContractInvalid(
                                error.to_string(),
                            ));
                        }
                        if current.status == CardRangeStatus::Draft {
                            return Ok(CardRangeMutationPersistenceOutcome::InvalidTransition);
                        }
                        if current.range_control_operation_id.is_some() {
                            return Ok(CardRangeMutationPersistenceOutcome::PublicationPending);
                        }
                        if current.issuance_enabled == change.issuance_enabled
                            && current.cms_operation_mode == change.cms_operation_mode
                        {
                            return Ok(CardRangeMutationPersistenceOutcome::ContractInvalid(
                                "requested operational controls do not change the card range"
                                    .to_string(),
                            ));
                        }
                        insert_idempotency_record(connection, context.new_idempotency_record())?;
                        let operation_id = Uuid::new_v4();
                        update_runtime_state(
                            connection,
                            card_range_id,
                            current.status,
                            change.issuance_enabled,
                            change.cms_operation_mode.as_db_value(),
                            operation_id,
                            &context.actor.subject,
                        )?;
                        Some(operation_id)
                    }
                };

                let updated = fetch_card_range(connection, card_range_id)?;
                if let Some(operation_id) = operation_id {
                    insert_control_outbox(connection, &updated, operation_id, &headers)?;
                }
                let reason = match &mutation {
                    CardRangeMutation::UpdateDraft(value) => &value.reason,
                    CardRangeMutation::Activate { reason }
                    | CardRangeMutation::Suspend { reason } => reason,
                    CardRangeMutation::UpdateControls(value) => &value.reason,
                };
                insert_audit_log(
                    connection,
                    NewAuditLog {
                        audit_log_id: Uuid::new_v4(),
                        entity_type: "CARD_RANGE".to_string(),
                        entity_id: card_range_id,
                        action_type: match mutation {
                            CardRangeMutation::UpdateDraft(_)
                            | CardRangeMutation::UpdateControls(_) => AuditAction::Update,
                            _ => AuditAction::StateTransition,
                        },
                        reason: Some(reason.clone()),
                        old_values: Some(before),
                        new_values: Some(updated.replay_snapshot()),
                        context: context.audit_context(),
                    },
                )?;
                let result = CardRangeMutationResult {
                    card_range: updated,
                    operation_id,
                };
                let snapshot = serde_json::to_value(&result.card_range).map_err(|error| {
                    DbError::Query(format!(
                        "failed to serialize range mutation result: {error}"
                    ))
                })?;
                complete_idempotency_record(
                    connection,
                    &operation_type,
                    &key,
                    "card_range",
                    card_range_id,
                    serde_json::json!({"card_range": snapshot, "operation_id": operation_id}),
                )?;
                Ok(CardRangeMutationPersistenceOutcome::Applied(Box::new(
                    result,
                )))
            })
            .await
    }
}

fn fetch_range_for_update(
    connection: &oracle::Connection,
    id: Uuid,
) -> DbResult<Option<CardRange>> {
    let sql = format!("{} FOR UPDATE", card_range_select_sql().trim());
    let raw = uuid_to_raw16(id).to_vec();
    match connection.query_row(&sql, &[&raw]) {
        Ok(row) => crate::db::oracle::card_range::map_card_range_row(&row).map(Some),
        Err(error) if error.kind() == oracle::ErrorKind::NoDataFound => Ok(None),
        Err(error) => Err(DbError::Query(format!(
            "failed to lock card range: {error}"
        ))),
    }
}

fn overlaps_other(
    connection: &oracle::Connection,
    id: Uuid,
    start: &str,
    end: &str,
) -> DbResult<bool> {
    let raw = uuid_to_raw16(id).to_vec();
    connection.query_row_as::<i64>("SELECT COUNT(*) FROM card_ranges WHERE card_range_id <> :1 AND start_card_number <= :2 AND end_card_number >= :3", &[&raw, &end, &start])
        .map(|count| count > 0).map_err(|error| DbError::Query(format!("failed to check updated range overlap: {error}")))
}

fn update_draft(
    connection: &oracle::Connection,
    desired: &crate::domain::card_range::NewCardRange,
    actor: &str,
) -> DbResult<()> {
    let raw = uuid_to_raw16(desired.card_range_id).to_vec();
    let calendar = desired.limit_calendar.as_ref().map(|value| {
        serde_json::json!({
            "timezone": value.timezone,
            "week_starts_on": match value.week_starts_on {
                crate::domain::card_range::WeekStartDay::Saturday => "Saturday",
                crate::domain::card_range::WeekStartDay::Sunday => "Sunday",
                crate::domain::card_range::WeekStartDay::Monday => "Monday",
            },
            "window_mode": "Calendar"
        })
        .to_string()
    });
    let metadata = desired.metadata_json.to_string();
    let statement = connection.execute("UPDATE card_ranges SET start_card_number=:1, end_card_number=:2, funding_mode=:3, withdrawal_limit_authority=:4, limit_calendar_json=:5, issuance_enabled=:6, cms_operation_mode=:7, metadata_json=:8, updated_by_subject=:9, updated_at=SYSTIMESTAMP WHERE card_range_id=:10 AND status='DRAFT'", &[&desired.numbers.start, &desired.numbers.end, &desired.funding_mode.as_db_value(), &desired.withdrawal_limit_authority.as_db_value(), &calendar, &i32::from(desired.issuance_enabled), &desired.cms_operation_mode.as_db_value(), &metadata, &actor, &raw])
        .map_err(|error| DbError::Query(format!("failed to update draft card range: {error}")))?;
    if statement.row_count().map_err(|error| {
        DbError::Query(format!(
            "failed to inspect draft card range update: {error}"
        ))
    })? != 1
    {
        return Err(DbError::Conflict(
            "draft card range changed during update".to_string(),
        ));
    }
    Ok(())
}

enum ActivationReadiness {
    Ready,
    CorePrerequisitesMissing,
    FeeProfilesMissing,
}

fn activation_ready(
    connection: &oracle::Connection,
    range: &CardRange,
) -> DbResult<ActivationReadiness> {
    let raw = uuid_to_raw16(range.card_range_id).to_vec();
    let providers = connection
        .query_row_as::<i64>(
            "SELECT COUNT(*) FROM card_range_providers WHERE card_range_id=:1 AND status='ACTIVE'",
            &[&raw],
        )
        .map_err(|error| DbError::Query(format!("failed to count eligible providers: {error}")))?;
    let policies = connection
        .query_row_as::<i64>(
            "SELECT COUNT(*) FROM card_policy_profiles WHERE card_range_id=:1 AND status='ACTIVE'",
            &[&raw],
        )
        .map_err(|error| DbError::Query(format!("failed to check active policy: {error}")))?;
    let provider_shape_ready = match range.funding_mode {
        FundingMode::SingleProvider => providers == 1,
        FundingMode::MultiProvider => providers >= 1,
    };
    if policies != 1 || !provider_shape_ready {
        return Ok(ActivationReadiness::CorePrerequisitesMissing);
    }
    let missing_fee_profiles = connection.query_row_as::<i64>(
        "SELECT COUNT(*) FROM card_range_providers crp WHERE crp.card_range_id=:1 AND crp.status='ACTIVE' AND NOT EXISTS (SELECT 1 FROM provider_fee_profiles pfp WHERE pfp.provider_id=crp.provider_id AND pfp.status='ACTIVE')",
        &[&raw],
    ).map_err(|error| DbError::Query(format!("failed to check active provider fee profiles: {error}")))?;
    Ok(if missing_fee_profiles == 0 {
        ActivationReadiness::Ready
    } else {
        ActivationReadiness::FeeProfilesMissing
    })
}

fn update_runtime_state(
    connection: &oracle::Connection,
    id: Uuid,
    status: CardRangeStatus,
    issuance: bool,
    cms_mode: &str,
    operation_id: Uuid,
    actor: &str,
) -> DbResult<()> {
    let statement = connection.execute("UPDATE card_ranges SET status=:1, issuance_enabled=:2, cms_operation_mode=:3, operational_version=operational_version+1, range_control_operation_id=:4, updated_by_subject=:5, updated_at=SYSTIMESTAMP WHERE card_range_id=:6 AND range_control_operation_id IS NULL", &[&status.as_db_value(), &i32::from(issuance), &cms_mode, &uuid_to_raw16(operation_id).to_vec(), &actor, &uuid_to_raw16(id).to_vec()])
        .map_err(|error| DbError::Query(format!("failed to update range runtime state: {error}")))?;
    if statement.row_count().map_err(|error| {
        DbError::Query(format!("failed to inspect range runtime update: {error}"))
    })? != 1
    {
        return Err(DbError::Conflict(
            "card range runtime state changed during update".to_string(),
        ));
    }
    Ok(())
}

fn draft_values_equal(
    current: &CardRange,
    desired: &crate::domain::card_range::NewCardRange,
) -> bool {
    current.numbers == desired.numbers
        && current.funding_mode == desired.funding_mode
        && current.withdrawal_limit_authority == desired.withdrawal_limit_authority
        && current.limit_calendar == desired.limit_calendar
        && current.issuance_enabled == desired.issuance_enabled
        && current.cms_operation_mode == desired.cms_operation_mode
        && current.metadata_json == desired.metadata_json
}

fn insert_control_outbox(
    connection: &oracle::Connection,
    range: &CardRange,
    operation_id: Uuid,
    headers: &InternalEventHeaders,
) -> DbResult<()> {
    super::outbox::insert_integration_operation(
        connection,
        operation_id,
        "CARD_RANGE_CONTROL_PUBLISH",
        "CARD_RANGE",
        range.card_range_id,
        1,
    )?;
    let provider_rows = connection.query("SELECT provider_id FROM card_range_providers WHERE card_range_id=:1 AND status='ACTIVE' ORDER BY provider_id", &[&uuid_to_raw16(range.card_range_id).to_vec()]).map_err(|error| DbError::Query(format!("failed to list eligible providers: {error}")))?;
    let mut providers = Vec::new();
    for row in provider_rows {
        let raw: Vec<u8> = row
            .map_err(|error| DbError::Query(format!("failed to read eligible provider: {error}")))?
            .get(0)
            .map_err(|error| DbError::Query(format!("failed to map eligible provider: {error}")))?;
        providers.push(crate::db::oracle::types::raw16_to_uuid(&raw)?);
    }
    let event_id = Uuid::new_v4();
    let envelope = InternalEventEnvelope::new(
        event_id,
        "CARD_RANGE_CONTROL_PUBLISH_REQUESTED",
        "CARD_RANGE",
        range.card_range_id,
        operation_id,
        serde_json::json!({"card_range_id": range.card_range_id, "range_status": range.status, "issuance_enabled": range.issuance_enabled, "cms_operation_mode": range.cms_operation_mode, "operational_version": range.operational_version, "eligible_provider_ids": providers}),
    );
    let payload = serde_json::to_string(&envelope).map_err(|error| {
        DbError::Query(format!("failed to serialize range control event: {error}"))
    })?;
    let headers = serde_json::to_string(headers).map_err(|error| {
        DbError::Query(format!(
            "failed to serialize range control headers: {error}"
        ))
    })?;
    connection.execute("INSERT INTO integration_outbox (outbox_event_id, operation_id, event_type, aggregate_type, aggregate_id, partition_key, payload_json, headers_json) VALUES (:1,:2,:3,:4,:5,:6,:7,:8)", &[&uuid_to_raw16(event_id).to_vec(), &uuid_to_raw16(operation_id).to_vec(), &"CARD_RANGE_CONTROL_PUBLISH_REQUESTED", &"CARD_RANGE", &uuid_to_raw16(range.card_range_id).to_vec(), &range.card_range_id.to_string(), &payload, &headers]).map_err(|error| DbError::Query(format!("failed to insert range control outbox event: {error}")))?;
    Ok(())
}

fn valid_reason(reason: &str) -> bool {
    let reason = reason.trim();
    !reason.is_empty() && reason.len() <= 1000 && !reason.chars().any(char::is_control)
}

fn classify_idempotency(
    existing: &crate::domain::idempotency::IdempotencyRecord,
    request_hash: &str,
) -> DbResult<CardRangeMutationPersistenceOutcome> {
    if existing.request_hash != request_hash {
        return Ok(CardRangeMutationPersistenceOutcome::IdempotencyConflict);
    }
    Ok(match existing.status {
        IdempotencyStatus::Completed => existing
            .response_snapshot
            .clone()
            .map(CardRangeMutationPersistenceOutcome::Replayed)
            .unwrap_or(CardRangeMutationPersistenceOutcome::IdempotencyInvalidState),
        IdempotencyStatus::InProgress => CardRangeMutationPersistenceOutcome::IdempotencyInProgress,
        _ => CardRangeMutationPersistenceOutcome::IdempotencyInvalidState,
    })
}
