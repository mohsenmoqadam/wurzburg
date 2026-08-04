use std::collections::{BTreeMap, BTreeSet};

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
        user_card::{
            CardProfileRefreshReason, FundingOrderAppliedSource, FundingOrderResult,
            FundingOrderSource, mask_card_number,
        },
    },
    messaging::contract::InternalEventHeaders,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FundingOrderPersistenceOutcome {
    Applied(FundingOrderResult),
    Replayed(FundingOrderResult),
    IdempotencyConflict,
    IdempotencyInProgress,
    CardNotFound,
    CardNotActive,
    CardOwnerMismatch,
    StateVersionConflict,
    PublicationPending,
    CardProfileLocked,
    SourceSetMismatch,
}

pub struct FundingOrderCommand {
    pub card_number: String,
    pub expected_state_version: i64,
    pub expected_owner: Option<Uuid>,
    pub sources: Vec<FundingOrderSource>,
    pub reason: String,
    pub operation_id: Uuid,
    pub headers: InternalEventHeaders,
}

impl OracleRepository {
    #[tracing::instrument(skip(self, context), fields(db.system="oracle", db.operation.name="card_funding_order.idempotency"))]
    pub async fn find_funding_order_replay(
        &self,
        context: &MutationCommandContext,
    ) -> DbResult<Option<FundingOrderPersistenceOutcome>> {
        let operation_type = context.operation_type.clone();
        let key = context.idempotency_key.as_str().to_string();
        let hash = context.request_hash.clone();
        self.pool
            .with_connection(move |connection| {
                fetch_idempotency_record(connection, &operation_type, &key)?
                    .map(|record| classify_idempotency(&record, &hash))
                    .transpose()
            })
            .await
    }

    #[tracing::instrument(skip(self, context, command), fields(db.system="oracle", db.operation.name="card_funding_order.update", operation.id=%command.operation_id))]
    pub async fn update_card_funding_order_atomic(
        &self,
        context: &MutationCommandContext,
        command: FundingOrderCommand,
    ) -> DbResult<FundingOrderPersistenceOutcome> {
        let context = context.clone();
        let card_number = command.card_number;
        let expected_state_version = command.expected_state_version;
        let expected_owner = command.expected_owner;
        let sources = command.sources;
        let reason = command.reason;
        let operation_id = command.operation_id;
        let headers = command.headers;
        self.pool.with_transaction("update card funding order", move |connection| {
            if let Some(existing) = fetch_idempotency_record(connection, &context.operation_type, context.idempotency_key.as_str())? {
                return classify_idempotency(&existing, &context.request_hash);
            }
            let mut rows = connection.query(
                "SELECT card_id,user_id,status,state_version,publication_operation_id FROM cards WHERE card_number=:1 FOR UPDATE",
                &[&card_number],
            ).map_err(|error| query("failed to lock card funding order", error))?;
            let Some(row) = rows.next() else { return Ok(FundingOrderPersistenceOutcome::CardNotFound) };
            let row = row.map_err(|error| query("failed to read locked card", error))?;
            let card_id = row_uuid(&row, 0)?;
            let user_id = row_uuid(&row, 1)?;
            let status: String = row.get(2).map_err(read)?;
            let state_version: i64 = row.get(3).map_err(read)?;
            let pending_operation: Option<Vec<u8>> = row.get(4).map_err(read)?;
            if status != "ACTIVE" { return Ok(FundingOrderPersistenceOutcome::CardNotActive); }
            if expected_owner.is_some_and(|owner| owner != user_id) { return Ok(FundingOrderPersistenceOutcome::CardOwnerMismatch); }
            if state_version != expected_state_version { return Ok(FundingOrderPersistenceOutcome::StateVersionConflict); }
            if pending_operation.is_some() { return Ok(FundingOrderPersistenceOutcome::PublicationPending); }

            let current_rows = connection.query(
                "SELECT fs.provider_id,fs.priority,fs.max_amount_rials FROM card_provider_funding_sources fs WHERE fs.card_id=:1 AND fs.status IN ('ACTIVE','SUSPENDED') ORDER BY fs.priority",
                &[&raw(card_id)],
            ).map_err(|error| query("failed to load active funding sources", error))?;
            let mut current = BTreeMap::new();
            for current_row in current_rows {
                let current_row = current_row.map_err(|error| query("failed to read active funding source", error))?;
                current.insert(row_uuid(&current_row, 0)?, (
                    current_row.get::<_, i64>(1).map_err(read)?,
                    current_row.get::<_, Option<i64>>(2).map_err(read)?,
                ));
            }
            let requested: BTreeSet<_> = sources.iter().map(|source| source.provider_id).collect();
            if requested.len() != sources.len() || requested != current.keys().copied().collect() {
                return Ok(FundingOrderPersistenceOutcome::SourceSetMismatch);
            }

            insert_idempotency_record(connection, context.new_idempotency_record())?;
            let old_values = serde_json::json!({"state_version":state_version,"sources":current.iter().map(|(provider_id,(priority,cap))|serde_json::json!({"provider_id":provider_id,"priority":priority,"max_amount_rials":cap})).collect::<Vec<_>>()});
            for (index, source) in sources.iter().enumerate() {
                let temporary_priority = 60_000_i64 + i64::try_from(index).map_err(|_| DbError::Query("funding source index overflow".to_string()))?;
                connection.execute(
                    "UPDATE card_provider_funding_sources SET priority=:1,updated_by_subject=:2,updated_at=SYSTIMESTAMP WHERE card_id=:3 AND provider_id=:4 AND status IN ('ACTIVE','SUSPENDED')",
                    &[&temporary_priority,&context.actor.subject,&raw(card_id),&raw(source.provider_id)],
                ).map_err(|error| query("failed to stage funding priority", error))?;
            }
            let mut applied = Vec::with_capacity(sources.len());
            for (index, source) in sources.iter().enumerate() {
                let priority = u16::try_from(index + 1).map_err(|_| DbError::Query("funding priority overflow".to_string()))?;
                let cap = source.max_amount_rials.map(|value| i64::try_from(value).expect("validated funding cap fits i64"));
                connection.execute(
                    "UPDATE card_provider_funding_sources SET priority=:1,max_amount_rials=:2,updated_by_subject=:3,updated_at=SYSTIMESTAMP WHERE card_id=:4 AND provider_id=:5 AND status IN ('ACTIVE','SUSPENDED')",
                    &[&i64::from(priority),&cap,&context.actor.subject,&raw(card_id),&raw(source.provider_id)],
                ).map_err(|error| query("failed to apply funding priority", error))?;
                applied.push(FundingOrderAppliedSource { provider_id: source.provider_id, priority, max_amount_rials: source.max_amount_rials });
            }
            let next_version = state_version + 1;
            connection.execute(
                "UPDATE cards SET state_version=:1,updated_by_subject=:2,updated_at=SYSTIMESTAMP WHERE card_id=:3 AND state_version=:4",
                &[&next_version,&context.actor.subject,&raw(card_id),&state_version],
            ).map_err(|error| query("failed to advance card state version", error))?;
            super::provider_user::insert_card_projection_outbox(connection,card_id,operation_id,CardProfileRefreshReason::FundingOrderChanged,&headers)?;
            let result = FundingOrderResult { card_id, masked_card_number: mask_card_number(&card_number), state_version: next_version, sources: applied, operation_id, command_status:"APPLIED".to_string(), event_publication_status:"PENDING".to_string(), profile_materialization_status:"PENDING".to_string() };
            insert_audit_log(connection, NewAuditLog { audit_log_id:Uuid::new_v4(),entity_type:"CARD_FUNDING_ORDER".to_string(),entity_id:card_id,action_type:AuditAction::Update,reason:Some(reason),old_values:Some(old_values),new_values:Some(serde_json::to_value(&result).map_err(|error|DbError::Query(format!("failed to serialize funding order audit: {error}")))?),context:context.audit_context() })?;
            complete_idempotency_record(connection,&context.operation_type,context.idempotency_key.as_str(),"card_funding_order",card_id,serde_json::to_value(&result).map_err(|error|DbError::Query(format!("failed to serialize funding order response: {error}")))?)?;
            Ok(FundingOrderPersistenceOutcome::Applied(result))
        }).await
    }
}

fn classify_idempotency(
    record: &crate::domain::idempotency::IdempotencyRecord,
    request_hash: &str,
) -> DbResult<FundingOrderPersistenceOutcome> {
    if record.request_hash != request_hash {
        return Ok(FundingOrderPersistenceOutcome::IdempotencyConflict);
    }
    Ok(match record.status {
        IdempotencyStatus::Completed => record
            .response_snapshot
            .clone()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|error| {
                DbError::Query(format!("invalid funding-order replay snapshot: {error}"))
            })?
            .map(FundingOrderPersistenceOutcome::Replayed)
            .ok_or_else(|| {
                DbError::Query(
                    "completed funding-order idempotency record has no response".to_string(),
                )
            })?,
        IdempotencyStatus::InProgress => FundingOrderPersistenceOutcome::IdempotencyInProgress,
        _ => {
            return Err(DbError::Query(
                "funding-order idempotency record has invalid state".to_string(),
            ));
        }
    })
}

fn raw(value: Uuid) -> Vec<u8> {
    uuid_to_raw16(value).to_vec()
}
fn row_uuid(row: &oracle::Row, index: usize) -> DbResult<Uuid> {
    let value: Vec<u8> = row.get(index).map_err(read)?;
    raw16_to_uuid(&value)
}
fn read(error: oracle::Error) -> DbError {
    DbError::Query(format!("failed to map card funding order: {error}"))
}
fn query(context: &str, error: oracle::Error) -> DbError {
    DbError::Query(format!("{context}: {error}"))
}
