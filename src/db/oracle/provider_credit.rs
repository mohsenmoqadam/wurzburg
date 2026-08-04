use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::{
    api::command::{DurableMutationContext, MutationCommandContext},
    db::{
        error::{DbError, DbResult},
        oracle::{
            OracleRepository,
            audit::insert_audit_log,
            idempotency::{
                complete_idempotency_record, fetch_idempotency_record, insert_idempotency_record,
            },
            outbox::insert_integration_operation,
            provider_event_subscription::provider_event_delivery_gate,
            types::{raw16_to_uuid, uuid_to_raw16},
        },
    },
    domain::{
        audit::{AuditAction, NewAuditLog},
        idempotency::IdempotencyStatus,
        provider_credit::{
            CreditAccountContext, CreditMovementIntent, CreditMovementResult, CreditMovementType,
        },
        provider_event::{
            ProviderEventEnvelope, ProviderEventSubject, ProviderEventType,
            ProviderMoneyMovementEventData,
        },
        user_card::{CardProfileRefreshReason, mask_card_number},
    },
};

#[derive(Debug, Clone)]
pub enum CreditAccountContextOutcome {
    Found(CreditAccountContext),
    ProviderNotFound,
    ProviderNotActive,
    RelationshipNotFound,
    RelationshipNotActive,
    CardNotFound,
    CardNotActive,
    FundingSourceNotActive,
}

#[derive(Debug, Clone)]
pub enum BeginCreditMovementOutcome {
    Prepared(CreditMovementIntent),
    Replayed(CreditMovementResult),
    IdempotencyConflict,
    IdempotencyInProgress,
    ProviderReferenceConflict,
    ContextChanged,
    OperationalProfileChanged,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct CreditMovementWalPayload {
    pub(crate) command_context: DurableMutationContext,
    pub(crate) intent: CreditMovementIntent,
}

impl OracleRepository {
    #[tracing::instrument(skip(self, card_number), fields(db.system="oracle", db.operation.name="provider_credit.context", provider.id=%provider_id, user.id=%user_id))]
    pub async fn resolve_credit_account_context(
        &self,
        provider_id: Uuid,
        user_id: Uuid,
        card_number: String,
    ) -> DbResult<CreditAccountContextOutcome> {
        self.pool
            .with_connection(move |connection| {
                resolve_context(connection, provider_id, user_id, &card_number, false)
            })
            .await
    }

    #[tracing::instrument(skip(self, context), fields(db.system="oracle", db.operation.name="provider_credit.idempotency"))]
    pub async fn find_credit_movement_replay(
        &self,
        context: &MutationCommandContext,
    ) -> DbResult<Option<BeginCreditMovementOutcome>> {
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

    #[tracing::instrument(skip(self, context, intent, card_number), fields(db.system="oracle", db.operation.name="provider_credit.begin", operation.id=%intent.operation_id, provider.id=%intent.provider_id, user.id=%intent.user_id, card.id=%intent.card_id))]
    pub async fn begin_credit_movement_atomic(
        &self,
        context: MutationCommandContext,
        intent: CreditMovementIntent,
        card_number: String,
    ) -> DbResult<BeginCreditMovementOutcome> {
        let outcome = self.pool.with_transaction("begin provider credit movement", move |connection| {
            if let Some(existing) = fetch_idempotency_record(
                connection,
                &context.operation_type,
                context.idempotency_key.as_str(),
            )? {
                return classify_idempotency(&existing, &context.request_hash);
            }
            if let Some(reference) = intent.provider_reference.as_deref()
                && provider_reference_exists(connection, intent.provider_id, reference)?
            {
                return Ok(BeginCreditMovementOutcome::ProviderReferenceConflict);
            }
            let resolved = resolve_context(
                connection,
                intent.provider_id,
                intent.user_id,
                &card_number,
                true,
            )?;
            let CreditAccountContextOutcome::Found(account_context) = resolved else {
                return Ok(BeginCreditMovementOutcome::ContextChanged);
            };
            if account_context.card_id != intent.card_id
                || account_context.provider_user_account_id != intent.provider_user_account_id
                || account_context.provider_owned_account_id != intent.provider_owned_account_id
            {
                return Ok(BeginCreditMovementOutcome::ContextChanged);
            }
            let active_profile: Option<Vec<u8>> = match connection.query_row_as(
                "SELECT provider_operational_profile_id FROM provider_operational_profiles WHERE provider_id=:1 AND status='ACTIVE' AND effective_at<=SYSTIMESTAMP ORDER BY version DESC FETCH FIRST 1 ROWS ONLY",
                &[&raw(intent.provider_id)],
            ) {
                Ok(value) => Some(value),
                Err(error) if error.kind() == oracle::ErrorKind::NoDataFound => None,
                Err(error) => return Err(query("failed to revalidate provider operational profile", error)),
            };
            if active_profile.as_deref().map(raw16_to_uuid).transpose()? != Some(intent.operational_profile_id) {
                return Ok(BeginCreditMovementOutcome::OperationalProfileChanged);
            }

            insert_idempotency_record(connection, context.new_idempotency_record())?;
            let wal_payload = serde_json::to_string(&CreditMovementWalPayload {
                command_context: context.durable(),
                intent: intent.clone(),
            }).map_err(|error| DbError::Query(format!("failed to serialize credit movement WAL: {error}")))?;
            connection.execute(
                "INSERT INTO operation_wal (operation_id,operation_type,aggregate_type,aggregate_id,status,deterministic_external_id,request_json) VALUES (:1,'PROVIDER_CREDIT_MOVEMENT','PROVIDER_CREDIT_MOVEMENT',:2,'PENDING',:3,:4)",
                &[&raw(intent.operation_id), &raw(intent.movement_id), &raw(intent.deterministic_transfer_id), &wal_payload],
            ).map_err(|error| query("failed to insert credit movement WAL", error))?;
            let amount = i64::try_from(intent.amount_rials)
                .map_err(|_| DbError::Query("credit amount exceeds Oracle contract".to_string()))?;
            let expected = intent.expected_remaining_amount_rials
                .map(i64::try_from)
                .transpose()
                .map_err(|_| DbError::Query("expected credit balance exceeds Oracle contract".to_string()))?;
            let metadata = intent.metadata.to_string();
            let insert = connection.execute(
                "INSERT INTO provider_credit_movements (movement_id,operation_id,movement_type,initiated_by,provider_id,user_id,card_id,provider_user_account_id,provider_owned_account_id,amount_rials,expected_remaining_amount_rials,provider_reference,deterministic_transfer_id,status,reason,metadata_json) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,'PENDING',:14,:15)",
                &[&raw(intent.movement_id), &raw(intent.operation_id), &intent.movement_type.as_db_value(), &intent.initiated_by.as_db_value(), &raw(intent.provider_id), &raw(intent.user_id), &raw(intent.card_id), &raw(intent.provider_user_account_id), &raw(intent.provider_owned_account_id), &amount, &expected, &intent.provider_reference, &raw(intent.deterministic_transfer_id), &intent.reason, &metadata],
            );
            if let Err(error) = insert {
                if error.to_string().to_ascii_uppercase().contains("UQ_PCM_PROVIDER_REFERENCE") {
                    // Returning an error is intentional: the transaction must roll back the
                    // IN_PROGRESS idempotency record inserted immediately before this row.
                    return Err(DbError::Conflict("provider credit reference already exists".to_string()));
                }
                return Err(query("failed to insert provider credit movement", error));
            }
            Ok(BeginCreditMovementOutcome::Prepared(intent))
        }).await;
        match outcome {
            Err(DbError::Conflict(message))
                if message == "provider credit reference already exists" =>
            {
                Ok(BeginCreditMovementOutcome::ProviderReferenceConflict)
            }
            other => other,
        }
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="provider_credit.external_in_flight", operation.id=%operation_id))]
    pub async fn mark_credit_movement_external_in_flight(
        &self,
        operation_id: Uuid,
    ) -> DbResult<()> {
        self.pool.with_transaction("mark provider credit movement external in flight", move |connection| {
            update_one(connection,
                "UPDATE operation_wal SET status='EXTERNAL_IN_FLIGHT',attempt_count=attempt_count+1,updated_at=SYSTIMESTAMP WHERE operation_id=:1 AND status IN ('PENDING','FAILED','EXTERNAL_IN_FLIGHT')",
                &[&raw(operation_id)],
                "credit movement WAL cannot enter external execution")
        }).await
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="provider_credit.recovery_required", operation.id=%operation_id))]
    pub async fn mark_credit_movement_recovery_required(
        &self,
        operation_id: Uuid,
        safe_error_code: &'static str,
    ) -> DbResult<()> {
        self.pool.with_transaction("mark provider credit movement recovery required", move |connection| {
            let error = serde_json::json!({"code":safe_error_code}).to_string();
            connection.execute(
                "UPDATE provider_credit_movements SET status='RECOVERY_REQUIRED',safe_error_code=:1,updated_at=SYSTIMESTAMP WHERE operation_id=:2 AND status<>'APPLIED'",
                &[&safe_error_code, &raw(operation_id)],
            ).map_err(|error| query("failed to mark credit movement recovery required", error))?;
            connection.execute(
                "UPDATE operation_wal SET status='FAILED',error_json=:1,locked_by=NULL,locked_until=NULL,updated_at=SYSTIMESTAMP WHERE operation_id=:2 AND status<>'COMPLETED'",
                &[&error, &raw(operation_id)],
            ).map_err(|error| query("failed to reschedule credit movement WAL", error))?;
            Ok(())
        }).await
    }

    #[tracing::instrument(skip(self, context, intent, observed_at), fields(db.system="oracle", db.operation.name="provider_credit.finalize", operation.id=%intent.operation_id, provider.id=%intent.provider_id, user.id=%intent.user_id, card.id=%intent.card_id))]
    pub async fn finalize_credit_movement_atomic(
        &self,
        context: DurableMutationContext,
        intent: CreditMovementIntent,
        observed_remaining_amount_rials: u64,
        observed_at: DateTime<Utc>,
    ) -> DbResult<CreditMovementResult> {
        self.pool.with_transaction("finalize provider credit movement", move |connection| {
            let movement_status: String = connection.query_row_as(
                "SELECT status FROM provider_credit_movements WHERE movement_id=:1 AND operation_id=:2 FOR UPDATE",
                &[&raw(intent.movement_id), &raw(intent.operation_id)],
            ).map_err(|error| query("failed to lock provider credit movement", error))?;
            if movement_status == "APPLIED" {
                return fetch_result(connection, intent.movement_id);
            }
            if !matches!(movement_status.as_str(), "PENDING" | "RECOVERY_REQUIRED") {
                return Err(DbError::Conflict("provider credit movement cannot be finalized".to_string()));
            }
            let current_version: i64 = connection.query_row_as(
                "SELECT state_version FROM cards WHERE card_id=:1 AND status='ACTIVE' FOR UPDATE",
                &[&raw(intent.card_id)],
            ).map_err(|error| query("failed to lock credit movement card", error))?;
            let next_version = current_version.checked_add(1)
                .ok_or_else(|| DbError::Query("card state version overflow".to_string()))?;
            connection.execute(
                "UPDATE cards SET state_version=:1,updated_by_subject=:2,updated_at=SYSTIMESTAMP WHERE card_id=:3 AND state_version=:4",
                &[&next_version, &context.audit.actor_subject, &raw(intent.card_id), &current_version],
            ).map_err(|error| query("failed to advance credit movement card version", error))?;

            insert_integration_operation(connection, intent.operation_id, "PROVIDER_CREDIT_MOVEMENT", "CARD", intent.card_id, 2)?;
            let refresh_reason = match intent.movement_type {
                CreditMovementType::Grant => CardProfileRefreshReason::CreditGranted,
                CreditMovementType::ReturnFullBalance => CardProfileRefreshReason::CreditReturned,
            };
            super::provider_user::insert_card_projection_event(
                connection,
                intent.card_id,
                intent.operation_id,
                1,
                refresh_reason,
                &context.event_headers(),
            )?;

            let event_type = match intent.movement_type {
                CreditMovementType::Grant => ProviderEventType::CreditGranted,
                CreditMovementType::ReturnFullBalance => ProviderEventType::CreditReturned,
            };
            let event_subject = load_event_subject(connection, &intent)?;
            let event_id = Uuid::new_v4();
            let envelope = ProviderEventEnvelope {
                event_id,
                event_type: event_type.as_str().to_string(),
                schema_version: 1,
                occurred_at: observed_at,
                provider_id: intent.provider_id,
                subject: ProviderEventSubject {
                    subject_type: "PROVIDER_USER_ACCOUNT".to_string(),
                    subject_id: intent.provider_user_account_id,
                    user_id: Some(intent.user_id),
                    card_id: Some(intent.card_id),
                    masked_card_number: Some(event_subject.0),
                    provider_customer_reference: Some(event_subject.1),
                },
                data: ProviderMoneyMovementEventData {
                    currency: "IRR".to_string(),
                    amount_rials: intent.amount_rials.to_string(),
                    observed_remaining_credit_rials: observed_remaining_amount_rials.to_string(),
                    balance_observed_at: observed_at,
                    initiated_by: intent.initiated_by.as_db_value().to_string(),
                },
            };
            let payload = serde_json::to_string(&envelope)
                .map_err(|error| DbError::Query(format!("failed to serialize provider credit event: {error}")))?;
            let (delivery_enabled, gate_snapshot) = provider_event_delivery_gate(connection, intent.provider_id, event_type)?;
            let status = if delivery_enabled { "PENDING" } else { "SUPPRESSED" };
            connection.execute(
                "INSERT INTO integration_outbox (outbox_event_id,operation_id,event_sequence,delivery_channel,provider_id,event_type,schema_version,aggregate_type,aggregate_id,partition_key,payload_json,delivery_gate_snapshot_json,status) VALUES (:1,:2,2,'PROVIDER',:3,:4,1,'PROVIDER_USER_ACCOUNT',:5,:6,:7,:8,:9)",
                &[&raw(event_id), &raw(intent.operation_id), &raw(intent.provider_id), &event_type.as_str(), &raw(intent.provider_user_account_id), &intent.provider_id.to_string(), &payload, &gate_snapshot.to_string(), &status],
            ).map_err(|error| query("failed to enqueue provider credit event", error))?;

            insert_credit_transaction_fact(connection, &intent, observed_at)?;

            connection.execute(
                "UPDATE provider_credit_movements SET status='APPLIED',card_state_version=:1,safe_error_code=NULL,updated_at=SYSTIMESTAMP WHERE movement_id=:2 AND status IN ('PENDING','RECOVERY_REQUIRED')",
                &[&next_version, &raw(intent.movement_id)],
            ).map_err(|error| query("failed to apply provider credit movement", error))?;
            let mut result = fetch_result(connection, intent.movement_id)?;
            result.event_publication_status = if delivery_enabled { "PENDING" } else { "SUPPRESSED" }.to_string();
            let response = serde_json::to_value(&result)
                .map_err(|error| DbError::Query(format!("failed to serialize provider credit response: {error}")))?;
            connection.execute(
                "UPDATE operation_wal SET status='EXTERNAL_VERIFIED',response_json=:1,error_json=NULL,locked_by=NULL,locked_until=NULL,updated_at=SYSTIMESTAMP WHERE operation_id=:2 AND status IN ('PENDING','EXTERNAL_IN_FLIGHT','FAILED','EXTERNAL_VERIFIED')",
                &[&response.to_string(), &raw(intent.operation_id)],
            ).map_err(|error| query("failed to verify provider credit WAL", error))?;
            insert_audit_log(connection, NewAuditLog {
                audit_log_id: Uuid::new_v4(),
                entity_type: "PROVIDER_CREDIT_MOVEMENT".to_string(),
                entity_id: intent.movement_id,
                action_type: AuditAction::Insert,
                reason: Some(intent.reason),
                old_values: None,
                new_values: Some(serde_json::json!({
                    "movement_id": intent.movement_id,
                    "operation_id": intent.operation_id,
                    "movement_type": intent.movement_type,
                    "initiated_by": intent.initiated_by,
                    "provider_id": intent.provider_id,
                    "user_id": intent.user_id,
                    "card_id": intent.card_id,
                    "amount_rials": intent.amount_rials,
                    "observed_remaining_amount_rials": observed_remaining_amount_rials,
                    "deterministic_transfer_id": intent.deterministic_transfer_id,
                    "card_state_version": next_version,
                })),
                context: context.audit.clone(),
            })?;
            complete_idempotency_record(
                connection,
                &context.operation_type,
                &context.idempotency_key,
                "provider_credit_movement",
                intent.movement_id,
                response,
            )?;
            Ok(result)
        }).await
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="provider_credit.load_recovery", operation.id=%operation_id))]
    pub async fn load_credit_movement_recovery(
        &self,
        operation_id: Uuid,
    ) -> DbResult<Option<(DurableMutationContext, CreditMovementIntent, String)>> {
        self.pool.with_connection(move |connection| {
            let mut rows = connection.query(
                "SELECT JSON_SERIALIZE(w.request_json RETURNING CLOB),c.card_number FROM operation_wal w JOIN provider_credit_movements m ON m.operation_id=w.operation_id JOIN cards c ON c.card_id=m.card_id WHERE w.operation_id=:1 AND w.status='EXTERNAL_IN_FLIGHT'",
                &[&raw(operation_id)],
            ).map_err(|error| query("failed to load credit movement recovery", error))?;
            let Some(row) = rows.next() else { return Ok(None) };
            let row = row.map_err(|error| query("failed to read credit movement recovery", error))?;
            let payload: String = row.get(0).map_err(read)?;
            let payload: CreditMovementWalPayload = serde_json::from_str(&payload)
                .map_err(|error| DbError::Query(format!("invalid credit movement WAL payload: {error}")))?;
            let card_number: String = row.get(1).map_err(read)?;
            Ok(Some((payload.command_context, payload.intent, card_number)))
        }).await
    }
}

/// Persists the immutable reporting fact in the same Oracle transaction that
/// marks the financial movement applied. The two entries model the actual
/// double-entry transfer; they are deliberately not a balance cache.
fn insert_credit_transaction_fact(
    connection: &oracle::Connection,
    intent: &CreditMovementIntent,
    occurred_at: DateTime<Utc>,
) -> DbResult<()> {
    let amount = i64::try_from(intent.amount_rials)
        .map_err(|_| DbError::Query("credit amount exceeds transaction contract".to_string()))?;
    let occurred_at = occurred_at.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
    let transaction_type = match intent.movement_type {
        CreditMovementType::Grant => "CREDIT_GRANTED",
        CreditMovementType::ReturnFullBalance => "CREDIT_RETURNED",
    };
    connection.execute(
        "INSERT INTO financial_transactions (transaction_id,transaction_type,source_system,source_operation_id,user_id,card_id,amount_rials,currency,status,external_reference,metadata_json,occurred_at) VALUES (:1,:2,'WURZBURG',:3,:4,:5,:6,'IRR','POSTED',:7,'{}',TO_TIMESTAMP_TZ(:8,'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'))",
        &[&raw(intent.movement_id), &transaction_type, &raw(intent.operation_id), &raw(intent.user_id), &raw(intent.card_id), &amount, &intent.provider_reference, &occurred_at],
    ).map_err(|error| query("failed to insert provider credit transaction fact", error))?;

    let (
        first_account,
        first_category,
        first_direction,
        second_account,
        second_category,
        second_direction,
    ) = match intent.movement_type {
        CreditMovementType::Grant => (
            intent.provider_owned_account_id,
            "PROVIDER_OWNED",
            "DEBIT",
            intent.provider_user_account_id,
            "PROVIDER_USER",
            "CREDIT",
        ),
        CreditMovementType::ReturnFullBalance => (
            intent.provider_user_account_id,
            "PROVIDER_USER",
            "DEBIT",
            intent.provider_owned_account_id,
            "PROVIDER_OWNED",
            "CREDIT",
        ),
    };
    for (sequence, account_id, category, direction) in [
        (1_i64, first_account, first_category, first_direction),
        (2_i64, second_account, second_category, second_direction),
    ] {
        connection.execute(
            "INSERT INTO financial_transaction_entries (transaction_entry_id,transaction_id,entry_sequence,provider_id,account_id,account_category,direction,entry_role,amount_rials,tigerbeetle_transfer_id) VALUES (:1,:2,:3,:4,:5,:6,:7,'PRINCIPAL',:8,:9)",
            &[&raw(Uuid::new_v4()), &raw(intent.movement_id), &sequence, &raw(intent.provider_id), &raw(account_id), &category, &direction, &amount, &raw(intent.deterministic_transfer_id)],
        ).map_err(|error| query("failed to insert provider credit transaction entry", error))?;
    }
    Ok(())
}

fn resolve_context(
    connection: &oracle::Connection,
    provider_id: Uuid,
    user_id: Uuid,
    card_number: &str,
    lock: bool,
) -> DbResult<CreditAccountContextOutcome> {
    let provider_status = optional_string(
        connection,
        "SELECT status FROM providers WHERE provider_id=:1",
        &[&raw(provider_id)],
    )?;
    let Some(provider_status) = provider_status else {
        return Ok(CreditAccountContextOutcome::ProviderNotFound);
    };
    if provider_status != "ACTIVE" {
        return Ok(CreditAccountContextOutcome::ProviderNotActive);
    }
    let relationship_status = optional_string(
        connection,
        "SELECT status FROM provider_users WHERE provider_id=:1 AND user_id=:2",
        &[&raw(provider_id), &raw(user_id)],
    )?;
    let Some(relationship_status) = relationship_status else {
        return Ok(CreditAccountContextOutcome::RelationshipNotFound);
    };
    if relationship_status != "ACTIVE" {
        return Ok(CreditAccountContextOutcome::RelationshipNotActive);
    }
    let sql = format!(
        "SELECT c.card_id,c.card_number,c.status,c.state_version,pu.provider_user_id,pu.provider_customer_reference,pua.tigerbeetle_account_id,owned.tigerbeetle_account_id,cms.tigerbeetle_account_id,fs.status FROM cards c JOIN card_provider_funding_sources fs ON fs.card_id=c.card_id AND fs.provider_id=:1 JOIN provider_users pu ON pu.provider_user_id=fs.provider_user_id AND pu.status='ACTIVE' JOIN provider_user_accounts pua ON pua.provider_user_account_id=fs.provider_user_account_id AND pua.status='ACTIVE' JOIN provider_ledger_accounts owned ON owned.provider_id=:2 AND owned.account_category='PROVIDER_OWNED' AND owned.status='ACTIVE' JOIN provider_ledger_accounts cms ON cms.provider_id=:3 AND cms.account_category='CMS_SETTLEMENT' AND cms.status='ACTIVE' WHERE c.card_number=:4 AND c.user_id=:5{}",
        if lock { " FOR UPDATE" } else { "" }
    );
    let mut rows = connection
        .query(
            &sql,
            &[
                &raw(provider_id),
                &raw(provider_id),
                &raw(provider_id),
                &card_number,
                &raw(user_id),
            ],
        )
        .map_err(|error| query("failed to resolve provider credit context", error))?;
    let Some(row) = rows.next() else {
        let card_exists: i64 = connection
            .query_row_as(
                "SELECT COUNT(*) FROM cards WHERE card_number=:1 AND user_id=:2",
                &[&card_number, &raw(user_id)],
            )
            .map_err(|error| query("failed to classify provider credit card", error))?;
        return Ok(if card_exists == 0 {
            CreditAccountContextOutcome::CardNotFound
        } else {
            CreditAccountContextOutcome::FundingSourceNotActive
        });
    };
    let row = row.map_err(|error| query("failed to read provider credit context", error))?;
    let card_status: String = row.get(2).map_err(read)?;
    if card_status != "ACTIVE" {
        return Ok(CreditAccountContextOutcome::CardNotActive);
    }
    let funding_status: String = row.get(9).map_err(read)?;
    if funding_status != "ACTIVE" {
        return Ok(CreditAccountContextOutcome::FundingSourceNotActive);
    }
    let card_number: String = row.get(1).map_err(read)?;
    Ok(CreditAccountContextOutcome::Found(CreditAccountContext {
        provider_id,
        user_id,
        card_id: row_uuid(&row, 0)?,
        masked_card_number: mask_card_number(&card_number),
        card_number,
        provider_user_id: row_uuid(&row, 4)?,
        provider_customer_reference: row.get(5).map_err(read)?,
        provider_user_account_id: row_uuid(&row, 6)?,
        provider_owned_account_id: row_uuid(&row, 7)?,
        cms_settlement_account_id: row_uuid(&row, 8)?,
        card_state_version: row.get(3).map_err(read)?,
    }))
}

fn load_event_subject(
    connection: &oracle::Connection,
    intent: &CreditMovementIntent,
) -> DbResult<(String, String)> {
    let row = connection.query_row(
        "SELECT c.card_number,pu.provider_customer_reference FROM cards c JOIN card_provider_funding_sources fs ON fs.card_id=c.card_id AND fs.provider_id=:1 JOIN provider_users pu ON pu.provider_user_id=fs.provider_user_id WHERE c.card_id=:2 AND c.user_id=:3",
        &[&raw(intent.provider_id), &raw(intent.card_id), &raw(intent.user_id)],
    ).map_err(|error| query("failed to load provider credit event subject", error))?;
    let card_number: String = row.get(0).map_err(read)?;
    let reference: String = row.get(1).map_err(read)?;
    Ok((mask_card_number(&card_number), reference))
}

fn provider_reference_exists(
    connection: &oracle::Connection,
    provider_id: Uuid,
    reference: &str,
) -> DbResult<bool> {
    let count: i64 = connection.query_row_as(
        "SELECT COUNT(*) FROM provider_credit_movements WHERE provider_id=:1 AND provider_reference=:2",
        &[&raw(provider_id), &reference],
    ).map_err(|error| query("failed to check provider credit reference", error))?;
    Ok(count > 0)
}

fn fetch_result(
    connection: &oracle::Connection,
    movement_id: Uuid,
) -> DbResult<CreditMovementResult> {
    let row = connection.query_row(
        "SELECT operation_id,movement_type,provider_id,user_id,card_id,amount_rials,provider_reference,TO_CHAR(SYS_EXTRACT_UTC(created_at),'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') FROM provider_credit_movements WHERE movement_id=:1",
        &[&raw(movement_id)],
    ).map_err(|error| query("failed to load provider credit result", error))?;
    let movement_type: String = row.get(1).map_err(read)?;
    let amount: i64 = row.get(5).map_err(read)?;
    Ok(CreditMovementResult {
        movement_id,
        operation_id: row_uuid(&row, 0)?,
        movement_type: match movement_type.as_str() {
            "GRANT" => CreditMovementType::Grant,
            "RETURN_FULL_BALANCE" => CreditMovementType::ReturnFullBalance,
            _ => {
                return Err(DbError::Query(
                    "unknown provider credit movement type".to_string(),
                ));
            }
        },
        provider_id: row_uuid(&row, 2)?,
        user_id: row_uuid(&row, 3)?,
        card_id: row_uuid(&row, 4)?,
        amount_rials: u64::try_from(amount)
            .map_err(|_| DbError::Query("invalid provider credit amount".to_string()))?,
        provider_reference: row.get(6).map_err(read)?,
        command_status: "APPLIED".to_string(),
        event_publication_status: "PENDING".to_string(),
        profile_materialization_status: "PENDING".to_string(),
        created_at: parse_time(&row.get::<_, String>(7).map_err(read)?)?,
    })
}

fn classify_idempotency(
    record: &crate::domain::idempotency::IdempotencyRecord,
    request_hash: &str,
) -> DbResult<BeginCreditMovementOutcome> {
    if record.request_hash != request_hash {
        return Ok(BeginCreditMovementOutcome::IdempotencyConflict);
    }
    match record.status {
        IdempotencyStatus::Completed => {
            let snapshot = record.response_snapshot.clone().ok_or_else(|| {
                DbError::Query("completed credit movement has no response".to_string())
            })?;
            let result = serde_json::from_value(snapshot).map_err(|error| {
                DbError::Query(format!("invalid credit movement replay: {error}"))
            })?;
            Ok(BeginCreditMovementOutcome::Replayed(result))
        }
        IdempotencyStatus::InProgress => Ok(BeginCreditMovementOutcome::IdempotencyInProgress),
        _ => Err(DbError::Query(
            "credit movement idempotency record has invalid state".to_string(),
        )),
    }
}

fn optional_string(
    connection: &oracle::Connection,
    sql: &str,
    binds: &[&dyn oracle::sql_type::ToSql],
) -> DbResult<Option<String>> {
    match connection.query_row_as(sql, binds) {
        Ok(value) => Ok(Some(value)),
        Err(error) if error.kind() == oracle::ErrorKind::NoDataFound => Ok(None),
        Err(error) => Err(query("failed to resolve provider credit state", error)),
    }
}

fn update_one(
    connection: &oracle::Connection,
    sql: &str,
    binds: &[&dyn oracle::sql_type::ToSql],
    conflict: &str,
) -> DbResult<()> {
    let statement = connection
        .execute(sql, binds)
        .map_err(|error| query("failed to update provider credit WAL", error))?;
    if statement.row_count().map_err(read)? != 1 {
        return Err(DbError::Conflict(conflict.to_string()));
    }
    Ok(())
}

fn raw(value: Uuid) -> Vec<u8> {
    uuid_to_raw16(value).to_vec()
}
fn row_uuid(row: &oracle::Row, index: usize) -> DbResult<Uuid> {
    let value: Vec<u8> = row.get(index).map_err(read)?;
    raw16_to_uuid(&value)
}
fn parse_time(value: &str) -> DbResult<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|error| DbError::Query(format!("invalid provider credit timestamp: {error}")))
}
fn read(error: oracle::Error) -> DbError {
    DbError::Query(format!("failed to map provider credit row: {error}"))
}
fn query(context: &str, error: oracle::Error) -> DbError {
    DbError::Query(format!("{context}: {error}"))
}
