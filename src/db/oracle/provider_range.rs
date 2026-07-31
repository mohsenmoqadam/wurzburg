use uuid::Uuid;

use crate::{
    api::command::MutationCommandContext,
    db::{
        error::{DbError, DbResult},
        oracle::{
            OracleRepository,
            audit::insert_audit_log,
            card_policy::insert_policy_outbox,
            idempotency::{
                complete_idempotency_record, fetch_idempotency_record, insert_idempotency_record,
            },
            provider_fee::{FeeProfileAttachmentPreparation, prepare_fee_profile_for_attachment},
            types::{raw16_to_uuid, uuid_to_raw16},
        },
    },
    domain::{
        audit::{AuditAction, NewAuditLog},
        card_policy::CardPolicyTerms,
        card_range::{FundingMode, WithdrawalLimitAuthority},
        idempotency::IdempotencyStatus,
    },
    messaging::contract::{InternalEventEnvelope, InternalEventHeaders},
};

#[derive(Debug, Clone, PartialEq)]
pub struct ProviderRangeAssignmentResult {
    pub provider_id: Uuid,
    pub card_range_id: Uuid,
    pub range_control_operation_id: Uuid,
    pub policy_operation_id: Option<Uuid>,
    pub fee_operation_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProviderRangeAssignmentOutcome {
    Applied(ProviderRangeAssignmentResult),
    Replayed(serde_json::Value),
    ProviderNotFound,
    ProviderNotActive,
    RangeNotFound,
    PolicyMissing,
    FeeProfileMissing,
    SingleProviderOccupied,
    PublicationPending,
    IdempotencyConflict,
    IdempotencyInProgress,
    IdempotencyInvalidState,
}

impl OracleRepository {
    #[tracing::instrument(skip(self, context), fields(db.system="oracle", db.operation.name="provider_range.assign", provider_id=%provider_id, card_range_id=%card_range_id))]
    pub async fn assign_provider_range_atomic(
        &self,
        context: MutationCommandContext,
        provider_id: Uuid,
        card_range_id: Uuid,
        reason: String,
    ) -> DbResult<ProviderRangeAssignmentOutcome> {
        let event_headers = InternalEventHeaders::from_current_span(
            context.request.correlation_id.clone(),
            context.request.request_id.to_string(),
        );
        let operation_type = context.operation_type.clone();
        let idempotency_key = context.idempotency_key.as_str().to_string();
        let request_hash = context.request_hash.clone();
        let provider_raw = uuid_to_raw16(provider_id).to_vec();

        self.pool.with_transaction("assign provider card range", move |connection| {
            if let Some(existing) = fetch_idempotency_record(connection, &operation_type, &idempotency_key)? {
                return classify_idempotency(&existing, &request_hash);
            }
            let provider_status = select_optional_string(
                connection,
                "SELECT status FROM providers WHERE provider_id=:1 FOR UPDATE",
                &provider_raw,
            )?;
            let Some(provider_status) = provider_status else {
                return Ok(ProviderRangeAssignmentOutcome::ProviderNotFound);
            };
            if provider_status != "ACTIVE" {
                return Ok(ProviderRangeAssignmentOutcome::ProviderNotActive);
            }

            let range_raw = uuid_to_raw16(card_range_id).to_vec();
            let mut range_rows = connection.query(
                "SELECT funding_mode,withdrawal_limit_authority,JSON_SERIALIZE(limit_calendar_json RETURNING CLOB),status,issuance_enabled,cms_operation_mode,operational_version,materialized_operational_version,range_control_operation_id FROM card_ranges WHERE card_range_id=:1 FOR UPDATE",
                &[&range_raw],
            ).map_err(|error| DbError::Query(format!("failed to lock provider target range: {error}")))?;
            let Some(range_row) = range_rows.next() else {
                return Ok(ProviderRangeAssignmentOutcome::RangeNotFound);
            };
            let range_row = range_row.map_err(|error| DbError::Query(format!("failed to read provider target range: {error}")))?;
            let funding_mode_value: String = range_row.get(0).map_err(read_error)?;
            let authority_value: String = range_row.get(1).map_err(read_error)?;
            let calendar_json: Option<String> = range_row.get(2).map_err(read_error)?;
            let range_status: String = range_row.get(3).map_err(read_error)?;
            let issuance_enabled: i32 = range_row.get(4).map_err(read_error)?;
            let cms_operation_mode: String = range_row.get(5).map_err(read_error)?;
            let operational_version: i64 = range_row.get(6).map_err(read_error)?;
            let materialized_version: i64 = range_row.get(7).map_err(read_error)?;
            let control_operation: Option<Vec<u8>> = range_row.get(8).map_err(read_error)?;
            let unpublished_bootstrap = operational_version == 1
                && materialized_version == 0
                && control_operation.is_none();
            if operational_version != materialized_version && !unpublished_bootstrap {
                return Ok(ProviderRangeAssignmentOutcome::PublicationPending);
            }
            let funding_mode = FundingMode::from_db_value(&funding_mode_value)
                .ok_or_else(|| DbError::Query("unknown card range funding mode".to_string()))?;
            let authority = WithdrawalLimitAuthority::from_db_value(&authority_value)
                .ok_or_else(|| DbError::Query("unknown withdrawal authority".to_string()))?;

            let active_count: i64 = connection.query_row_as(
                "SELECT COUNT(*) FROM card_range_providers WHERE card_range_id=:1 AND status='ACTIVE'",
                &[&range_raw],
            ).map_err(|error| DbError::Query(format!("failed to count target range providers: {error}")))?;
            let current_range = select_optional_raw(
                connection,
                "SELECT card_range_id FROM card_range_providers WHERE provider_id=:1 AND status='ACTIVE' FOR UPDATE",
                &provider_raw,
            )?.map(|raw| raw16_to_uuid(&raw)).transpose()?;
            if funding_mode == FundingMode::SingleProvider
                && active_count > 0
                && current_range != Some(card_range_id)
            {
                return Ok(ProviderRangeAssignmentOutcome::SingleProviderOccupied);
            }

            // Multi-provider additions consume the already materialized policy.
            // Only the first provider freezes and publishes an initial draft.
            let active_policy_count: i64 = connection.query_row_as(
                "SELECT COUNT(*) FROM card_policy_profiles WHERE card_range_id=:1 AND status='ACTIVE'",
                &[&range_raw],
            ).map_err(|error| DbError::Query(format!("failed to check active range policy: {error}")))?;
            let mut draft_policy = None;
            if active_policy_count == 0 {
                let mut policy_rows = connection.query(
                    "SELECT card_policy_profile_id,version,JSON_SERIALIZE(profile_json RETURNING CLOB),publication_operation_id FROM card_policy_profiles WHERE card_range_id=:1 AND status='DRAFT' FOR UPDATE",
                    &[&range_raw],
                ).map_err(|error| DbError::Query(format!("failed to lock range policy draft: {error}")))?;
                let Some(policy_row) = policy_rows.next() else { return Ok(ProviderRangeAssignmentOutcome::PolicyMissing); };
                let policy_row = policy_row.map_err(|error| DbError::Query(format!("failed to read range policy draft: {error}")))?;
                let operation: Option<Vec<u8>> = policy_row.get(3).map_err(read_error)?;
                if operation.is_some() { return Ok(ProviderRangeAssignmentOutcome::PublicationPending); }
                draft_policy = Some((
                    policy_row.get::<_, Vec<u8>>(0).map_err(read_error)?,
                    policy_row.get::<_, i64>(1).map_err(read_error)?,
                    policy_row.get::<_, String>(2).map_err(read_error)?,
                ));
            }

            let fee_operation_id = match prepare_fee_profile_for_attachment(
                connection, &context, provider_id, &reason, &event_headers,
            )? {
                FeeProfileAttachmentPreparation::Ready { operation_id } => operation_id,
                FeeProfileAttachmentPreparation::Missing => return Ok(ProviderRangeAssignmentOutcome::FeeProfileMissing),
                FeeProfileAttachmentPreparation::PublicationPending => return Ok(ProviderRangeAssignmentOutcome::PublicationPending),
            };

            insert_idempotency_record(connection, context.new_idempotency_record())?;
            if let Some(current_range) = current_range.filter(|value| *value != card_range_id) {
                connection.execute(
                    "UPDATE card_range_providers SET status='SUSPENDED',updated_by_subject=:1,updated_at=SYSTIMESTAMP WHERE provider_id=:2 AND card_range_id=:3 AND status='ACTIVE'",
                    &[&context.actor.subject, &uuid_to_raw16(provider_id).to_vec(), &uuid_to_raw16(current_range).to_vec()],
                ).map_err(|error| DbError::Query(format!("failed to preserve prior provider range assignment: {error}")))?;
            }
            connection.execute(
                "MERGE INTO card_range_providers target USING (SELECT :1 card_range_id,:2 provider_id FROM dual) source ON (target.card_range_id=source.card_range_id AND target.provider_id=source.provider_id) WHEN MATCHED THEN UPDATE SET target.status='ACTIVE',target.updated_by_subject=:3,target.updated_at=SYSTIMESTAMP WHEN NOT MATCHED THEN INSERT (card_range_id,provider_id,status,created_by_subject,updated_by_subject) VALUES (source.card_range_id,source.provider_id,'ACTIVE',:3,:3)",
                &[&range_raw, &uuid_to_raw16(provider_id).to_vec(), &context.actor.subject],
            ).map_err(|error| DbError::Query(format!("failed to assign provider to range: {error}")))?;

            let policy_operation_id = if let Some((policy_id_raw, policy_version, policy_json)) = draft_policy {
                let operation_id = Uuid::new_v4();
                let policy_id = raw16_to_uuid(&policy_id_raw)?;
                let terms: CardPolicyTerms = serde_json::from_str(&policy_json)
                    .map_err(|error| DbError::Query(format!("invalid draft policy JSON: {error}")))?;
                let calendar = calendar_json
                    .as_deref()
                    .map(super::card_range::limit_calendar_from_json)
                    .transpose()?;
                insert_policy_outbox(connection, operation_id, card_range_id, policy_id, policy_version, funding_mode, authority, calendar.as_ref(), &terms, &event_headers)?;
                connection.execute(
                    "UPDATE card_policy_profiles SET publication_operation_id=:1,updated_by_subject=:2,updated_at=SYSTIMESTAMP WHERE card_policy_profile_id=:3 AND status='DRAFT' AND publication_operation_id IS NULL",
                    &[&uuid_to_raw16(operation_id).to_vec(), &context.actor.subject, &policy_id_raw],
                ).map_err(|error| DbError::Query(format!("failed to freeze range policy publication: {error}")))?;
                Some(operation_id)
            } else { None };

            let range_control_operation_id = Uuid::new_v4();
            let next_version = operational_version + 1;
            connection.execute(
                "UPDATE card_ranges SET operational_version=:1,range_control_operation_id=:2,updated_by_subject=:3,updated_at=SYSTIMESTAMP WHERE card_range_id=:4",
                &[&next_version, &uuid_to_raw16(range_control_operation_id).to_vec(), &context.actor.subject, &range_raw],
            ).map_err(|error| DbError::Query(format!("failed to advance range control version: {error}")))?;
            insert_range_control_outbox(
                connection,
                &RangeControlProjection {
                    card_range_id,
                    operation_id: range_control_operation_id,
                    version: next_version,
                    range_status: &range_status,
                    issuance_enabled: issuance_enabled != 0,
                    cms_operation_mode: &cms_operation_mode,
                },
                &event_headers,
            )?;

            let result = ProviderRangeAssignmentResult { provider_id, card_range_id, range_control_operation_id, policy_operation_id, fee_operation_id };
            let snapshot = serde_json::json!({
                "provider_id": provider_id,
                "card_range_id": card_range_id,
                "status": "ACTIVE",
                "range_control_operation_id": range_control_operation_id,
                "policy_operation_id": policy_operation_id,
                "fee_operation_id": fee_operation_id
            });
            insert_audit_log(connection, NewAuditLog {
                audit_log_id: Uuid::new_v4(), entity_type: "CARD_RANGE_PROVIDER".to_string(), entity_id: provider_id,
                action_type: AuditAction::StateTransition, reason: Some(reason), old_values: current_range.map(|value| serde_json::json!({"card_range_id": value, "status": "ACTIVE"})),
                new_values: Some(snapshot.clone()), context: context.audit_context(),
            })?;
            complete_idempotency_record(connection, &operation_type, &idempotency_key, "provider", provider_id, snapshot)?;
            Ok(ProviderRangeAssignmentOutcome::Applied(result))
        }).await
    }
}

pub(crate) struct RangeControlProjection<'a> {
    pub card_range_id: Uuid,
    pub operation_id: Uuid,
    pub version: i64,
    pub range_status: &'a str,
    pub issuance_enabled: bool,
    pub cms_operation_mode: &'a str,
}

pub(crate) fn insert_range_control_outbox(
    connection: &oracle::Connection,
    projection: &RangeControlProjection<'_>,
    headers: &InternalEventHeaders,
) -> DbResult<()> {
    let rows = connection.query(
        "SELECT provider_id FROM card_range_providers WHERE card_range_id=:1 AND status='ACTIVE' ORDER BY provider_id",
        &[&uuid_to_raw16(projection.card_range_id).to_vec()],
    ).map_err(|error| DbError::Query(format!("failed to list range control providers: {error}")))?;
    let mut providers = Vec::new();
    for row in rows {
        let raw: Vec<u8> = row
            .map_err(|error| {
                DbError::Query(format!("failed to read range control provider: {error}"))
            })?
            .get(0)
            .map_err(read_error)?;
        providers.push(raw16_to_uuid(&raw)?);
    }
    let event_id = Uuid::new_v4();
    let envelope = InternalEventEnvelope::new(
        event_id,
        "CARD_RANGE_CONTROL_PUBLISH_REQUESTED",
        "CARD_RANGE",
        projection.card_range_id,
        projection.operation_id,
        serde_json::json!({
            "card_range_id": projection.card_range_id, "range_status": projection.range_status,
            "issuance_enabled": projection.issuance_enabled,
            "cms_operation_mode": projection.cms_operation_mode,
            "operational_version": projection.version, "eligible_provider_ids": providers
        }),
    );
    let payload = serde_json::to_string(&envelope).map_err(|error| {
        DbError::Query(format!("failed to serialize range control event: {error}"))
    })?;
    let headers = serde_json::to_string(headers).map_err(|error| {
        DbError::Query(format!(
            "failed to serialize range control headers: {error}"
        ))
    })?;
    connection.execute(
        "INSERT INTO integration_outbox (outbox_event_id,operation_id,event_type,aggregate_type,aggregate_id,partition_key,payload_json,headers_json) VALUES (:1,:2,'CARD_RANGE_CONTROL_PUBLISH_REQUESTED','CARD_RANGE',:3,:4,:5,:6)",
        &[&uuid_to_raw16(event_id).to_vec(), &uuid_to_raw16(projection.operation_id).to_vec(), &uuid_to_raw16(projection.card_range_id).to_vec(), &projection.card_range_id.to_string(), &payload, &headers],
    ).map_err(|error| DbError::Query(format!("failed to insert range control outbox event: {error}")))?;
    Ok(())
}

fn select_optional_string(
    connection: &oracle::Connection,
    sql: &str,
    id: &[u8],
) -> DbResult<Option<String>> {
    let mut rows = connection.query(sql, &[&id]).map_err(|error| {
        DbError::Query(format!("failed to select provider range fact: {error}"))
    })?;
    match rows.next() {
        Some(Ok(row)) => row.get(0).map(Some).map_err(read_error),
        Some(Err(error)) => Err(DbError::Query(format!(
            "failed to read provider range fact: {error}"
        ))),
        None => Ok(None),
    }
}

fn select_optional_raw(
    connection: &oracle::Connection,
    sql: &str,
    id: &[u8],
) -> DbResult<Option<Vec<u8>>> {
    let mut rows = connection.query(sql, &[&id]).map_err(|error| {
        DbError::Query(format!("failed to select provider range identity: {error}"))
    })?;
    match rows.next() {
        Some(Ok(row)) => row.get(0).map(Some).map_err(read_error),
        Some(Err(error)) => Err(DbError::Query(format!(
            "failed to read provider range identity: {error}"
        ))),
        None => Ok(None),
    }
}

fn classify_idempotency(
    existing: &crate::domain::idempotency::IdempotencyRecord,
    hash: &str,
) -> DbResult<ProviderRangeAssignmentOutcome> {
    if existing.request_hash != hash {
        return Ok(ProviderRangeAssignmentOutcome::IdempotencyConflict);
    }
    Ok(match existing.status {
        IdempotencyStatus::Completed => existing
            .response_snapshot
            .clone()
            .map(ProviderRangeAssignmentOutcome::Replayed)
            .unwrap_or(ProviderRangeAssignmentOutcome::IdempotencyInvalidState),
        IdempotencyStatus::InProgress => ProviderRangeAssignmentOutcome::IdempotencyInProgress,
        _ => ProviderRangeAssignmentOutcome::IdempotencyInvalidState,
    })
}

fn read_error(error: oracle::Error) -> DbError {
    DbError::Query(format!("failed to read Oracle provider range row: {error}"))
}
