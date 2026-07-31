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
            provider_operational_profile::promote_due_for_provider,
            provider_range::{RangeControlProjection, insert_range_control_outbox},
            types::{raw16_to_uuid, uuid_to_raw16},
        },
    },
    domain::{
        audit::{AuditAction, NewAuditLog},
        idempotency::IdempotencyStatus,
        provider::{Provider, ProviderStatus},
    },
    messaging::contract::InternalEventHeaders,
};

#[derive(Debug, Clone, PartialEq)]
pub enum ProviderLifecycleOutcome {
    Applied(Box<Provider>),
    Replayed(serde_json::Value),
    NotFound,
    InvalidTransition,
    PrerequisitesMissing,
    PublicationPending,
    IdempotencyConflict,
    IdempotencyInProgress,
    IdempotencyInvalidState,
}

impl OracleRepository {
    #[tracing::instrument(skip(self, context), fields(db.system="oracle", db.operation.name="providers.transition", provider_id=%provider_id, provider.target_status=target.as_db_value()))]
    pub async fn transition_provider_atomic(
        &self,
        context: MutationCommandContext,
        provider_id: Uuid,
        target: ProviderStatus,
        reason: String,
    ) -> DbResult<ProviderLifecycleOutcome> {
        let event_headers = InternalEventHeaders::from_current_span(
            context.request.correlation_id.clone(),
            context.request.request_id.to_string(),
        );
        let operation_type = context.operation_type.clone();
        let idempotency_key = context.idempotency_key.as_str().to_string();
        let request_hash = context.request_hash.clone();
        let result = self.pool.with_transaction("provider lifecycle transition", move |connection| {
            if let Some(existing) = fetch_idempotency_record(connection, &operation_type, &idempotency_key)? {
                return classify_idempotency(&existing, &request_hash);
            }
            let mut rows = connection.query(
                "SELECT status FROM providers WHERE provider_id=:1 FOR UPDATE",
                &[&uuid_to_raw16(provider_id).to_vec()],
            ).map_err(|error| DbError::Query(format!("failed to lock provider lifecycle: {error}")))?;
            let Some(row) = rows.next() else { return Ok(ProviderLifecycleOutcome::NotFound); };
            let current_value: String = row.map_err(|error| DbError::Query(format!("failed to read provider lifecycle: {error}")))?.get(0).map_err(read_error)?;
            let current = ProviderStatus::from_db_value(&current_value).ok_or_else(|| DbError::Query("unknown provider lifecycle status".to_string()))?;
            promote_due_for_provider(connection, provider_id)?;
            if !current.can_transition_to(target) {
                return Ok(ProviderLifecycleOutcome::InvalidTransition);
            }
            if target == ProviderStatus::Active {
                let active_accounts: i64 = connection.query_row_as(
                    "SELECT COUNT(*) FROM provider_ledger_accounts WHERE provider_id=:1 AND status='ACTIVE'",
                    &[&uuid_to_raw16(provider_id).to_vec()],
                ).map_err(|error| DbError::Query(format!("failed to verify provider accounts: {error}")))?;
                let active_profiles: i64 = connection.query_row_as(
                    "SELECT COUNT(*) FROM provider_operational_profiles WHERE provider_id=:1 AND status='ACTIVE' AND effective_at<=SYSTIMESTAMP",
                    &[&uuid_to_raw16(provider_id).to_vec()],
                ).map_err(|error| DbError::Query(format!("failed to verify provider profile: {error}")))?;
                if active_accounts != 4 || active_profiles != 1 {
                    return Ok(ProviderLifecycleOutcome::PrerequisitesMissing);
                }
            }

            let active_range = if matches!(target, ProviderStatus::Suspended | ProviderStatus::Inactive) {
                let mut relation_rows = connection.query(
                    "SELECT card_range_id FROM card_range_providers WHERE provider_id=:1 AND status='ACTIVE' FOR UPDATE",
                    &[&uuid_to_raw16(provider_id).to_vec()],
                ).map_err(|error| DbError::Query(format!("failed to lock provider range lifecycle: {error}")))?;
                match relation_rows.next() {
                    Some(Ok(row)) => {
                        let raw: Vec<u8> = row.get(0).map_err(read_error)?;
                        Some(raw16_to_uuid(&raw)?)
                    }
                    Some(Err(error)) => return Err(DbError::Query(format!("failed to read provider range lifecycle: {error}"))),
                    None => None,
                }
            } else { None };

            insert_idempotency_record(connection, context.new_idempotency_record())?;
            connection.execute(
                "UPDATE providers SET status=:1,updated_by_subject=:2,updated_at=SYSTIMESTAMP WHERE provider_id=:3",
                &[&target.as_db_value(), &context.actor.subject, &uuid_to_raw16(provider_id).to_vec()],
            ).map_err(|error| DbError::Query(format!("failed to transition provider lifecycle: {error}")))?;

            if let Some(card_range_id) = active_range {
                let range_raw = uuid_to_raw16(card_range_id).to_vec();
                let mut range_rows = connection.query(
                    "SELECT status,issuance_enabled,cms_operation_mode,operational_version,materialized_operational_version FROM card_ranges WHERE card_range_id=:1 FOR UPDATE",
                    &[&range_raw],
                ).map_err(|error| DbError::Query(format!("failed to lock affected provider range: {error}")))?;
                let range_row = range_rows.next().ok_or_else(|| DbError::Query("provider range relationship points to missing range".to_string()))?
                    .map_err(|error| DbError::Query(format!("failed to read affected provider range: {error}")))?;
                let mut range_status: String = range_row.get(0).map_err(read_error)?;
                let issuance_enabled: i32 = range_row.get(1).map_err(read_error)?;
                let cms_mode: String = range_row.get(2).map_err(read_error)?;
                let version: i64 = range_row.get(3).map_err(read_error)?;
                let materialized: i64 = range_row.get(4).map_err(read_error)?;
                if version != materialized {
                    return Err(DbError::Conflict("provider range publication pending".to_string()));
                }
                connection.execute(
                    "UPDATE card_range_providers SET status='SUSPENDED',updated_by_subject=:1,updated_at=SYSTIMESTAMP WHERE provider_id=:2 AND card_range_id=:3 AND status='ACTIVE'",
                    &[&context.actor.subject, &uuid_to_raw16(provider_id).to_vec(), &range_raw],
                ).map_err(|error| DbError::Query(format!("failed to suspend provider range eligibility: {error}")))?;
                let remaining: i64 = connection.query_row_as(
                    "SELECT COUNT(*) FROM card_range_providers WHERE card_range_id=:1 AND status='ACTIVE'",
                    &[&range_raw],
                ).map_err(|error| DbError::Query(format!("failed to count remaining providers: {error}")))?;
                if remaining == 0 { range_status = "SUSPENDED".to_string(); }
                let next_version = version + 1;
                let control_operation_id = Uuid::new_v4();
                connection.execute(
                    "UPDATE card_ranges SET status=:1,operational_version=:2,range_control_operation_id=:3,updated_by_subject=:4,updated_at=SYSTIMESTAMP WHERE card_range_id=:5",
                    &[&range_status, &next_version, &uuid_to_raw16(control_operation_id).to_vec(), &context.actor.subject, &range_raw],
                ).map_err(|error| DbError::Query(format!("failed to update affected range control: {error}")))?;
                insert_range_control_outbox(
                    connection,
                    &RangeControlProjection {
                        card_range_id,
                        operation_id: control_operation_id,
                        version: next_version,
                        range_status: &range_status,
                        issuance_enabled: issuance_enabled != 0,
                        cms_operation_mode: &cms_mode,
                    },
                    &event_headers,
                )?;
            }

            let provider = super::provider::fetch_provider_for_command(connection, provider_id)?;
            let snapshot = provider.replay_snapshot();
            insert_audit_log(connection, NewAuditLog {
                audit_log_id: Uuid::new_v4(), entity_type: "PROVIDER".to_string(), entity_id: provider_id,
                action_type: AuditAction::StateTransition, reason: Some(reason),
                old_values: Some(serde_json::json!({"status": current})), new_values: Some(snapshot.clone()), context: context.audit_context(),
            })?;
            complete_idempotency_record(connection, &operation_type, &idempotency_key, "provider", provider_id, snapshot)?;
            Ok(ProviderLifecycleOutcome::Applied(Box::new(provider)))
        }).await;
        match result {
            Err(DbError::Conflict(message)) if message == "provider range publication pending" => {
                Ok(ProviderLifecycleOutcome::PublicationPending)
            }
            other => other,
        }
    }
}

fn classify_idempotency(
    existing: &crate::domain::idempotency::IdempotencyRecord,
    hash: &str,
) -> DbResult<ProviderLifecycleOutcome> {
    if existing.request_hash != hash {
        return Ok(ProviderLifecycleOutcome::IdempotencyConflict);
    }
    Ok(match existing.status {
        IdempotencyStatus::Completed => existing
            .response_snapshot
            .clone()
            .map(ProviderLifecycleOutcome::Replayed)
            .unwrap_or(ProviderLifecycleOutcome::IdempotencyInvalidState),
        IdempotencyStatus::InProgress => ProviderLifecycleOutcome::IdempotencyInProgress,
        _ => ProviderLifecycleOutcome::IdempotencyInvalidState,
    })
}

fn read_error(error: oracle::Error) -> DbError {
    DbError::Query(format!(
        "failed to read Oracle provider lifecycle row: {error}"
    ))
}
