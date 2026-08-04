use serde::{Deserialize, Serialize};
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
            provider_user::ExistingCardProvisioningIntent,
            types::{raw16_to_uuid, uuid_to_raw16},
        },
    },
    domain::{
        audit::{AuditAction, NewAuditLog},
        card_issuance::{CardIssuanceBatch, CardIssuanceResultRow},
        idempotency::IdempotencyStatus,
        user_card::{PolicyUsageAccountIds, deterministic_provider_user_account_id},
    },
};

#[derive(Debug, Clone)]
pub enum BeginIssuanceResultOutcome {
    Started,
    Resumed,
    Replayed(serde_json::Value),
    BatchNotFound,
    BatchNotReady,
    RowSetMismatch,
    IdempotencyConflict,
    IdempotencyInvalidState,
}

#[derive(Debug, Clone)]
pub struct IssuanceProviderAccountIntent {
    pub provider_user_id: Uuid,
    pub provider_id: Uuid,
    pub account_id: Uuid,
}

#[derive(Debug, Clone)]
pub struct IssuedCardProvisioningIntent {
    pub base: ExistingCardProvisioningIntent,
    pub issuance_request_id: Uuid,
    pub batch_id: Uuid,
    pub providers: Vec<IssuanceProviderAccountIntent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IssuedCardFinalizationSnapshot {
    pub issuance_request_id: Uuid,
    pub issuer_reference: Option<String>,
    pub produced_at: Option<chrono::DateTime<chrono::Utc>>,
    pub dispatched_at: Option<chrono::DateTime<chrono::Utc>>,
    pub tracking_reference: Option<String>,
}

impl IssuedCardFinalizationSnapshot {
    fn from_result(result: &CardIssuanceResultRow) -> Self {
        Self {
            issuance_request_id: result.issuance_request_id,
            issuer_reference: result.issuer_reference.clone(),
            produced_at: result.produced_at,
            dispatched_at: result.dispatched_at,
            tracking_reference: result.tracking_reference.clone(),
        }
    }

    pub fn into_result(self) -> CardIssuanceResultRow {
        CardIssuanceResultRow {
            issuance_request_id: self.issuance_request_id,
            status: crate::domain::card_issuance::CardIssuanceResultStatus::Issued,
            card_number: None,
            issuer_reference: self.issuer_reference,
            failure_code: None,
            failure_message: None,
            produced_at: self.produced_at,
            dispatched_at: self.dispatched_at,
            tracking_reference: self.tracking_reference,
        }
    }
}

#[derive(Debug, Clone)]
pub enum PrepareIssuedCardOutcome {
    Prepared(Box<IssuedCardProvisioningIntent>),
    AlreadyProcessed,
    RequestNotFound,
    InvalidPanRange,
    CardNumberConflict,
    ActiveCardConflict,
}

impl OracleRepository {
    #[tracing::instrument(skip(self, context, rows), fields(db.system="oracle", db.operation.name="card_issuance_results.begin", batch_id=%batch_id, row_count=rows.len()))]
    pub async fn begin_card_issuance_result_atomic(
        &self,
        context: MutationCommandContext,
        batch_id: Uuid,
        result_object_key: String,
        checksum: String,
        rows: Vec<CardIssuanceResultRow>,
    ) -> DbResult<BeginIssuanceResultOutcome> {
        let operation_type = context.operation_type.clone();
        let key = context.idempotency_key.as_str().to_string();
        let request_hash = context.request_hash.clone();
        self.pool.with_transaction("begin issuance result processing", move |connection| {
            let batch_status = match connection.query_row_as::<String>("SELECT status FROM card_issuance_batches WHERE card_issuance_batch_id=:1 FOR UPDATE", &[&raw(batch_id)]) {
                Ok(value) => value,
                Err(error) if error.kind() == oracle::ErrorKind::NoDataFound => return Ok(BeginIssuanceResultOutcome::BatchNotFound),
                Err(error) => return Err(query("failed to lock issuance batch", error)),
            };
            if let Some(existing) = fetch_idempotency_record(connection, &operation_type, &key)? {
                if existing.request_hash != request_hash { return Ok(BeginIssuanceResultOutcome::IdempotencyConflict); }
                return Ok(match existing.status {
                    IdempotencyStatus::Completed => existing.response_snapshot.clone().map(BeginIssuanceResultOutcome::Replayed).unwrap_or(BeginIssuanceResultOutcome::IdempotencyInvalidState),
                    IdempotencyStatus::InProgress if batch_status == "PROCESSING_RESULT" => BeginIssuanceResultOutcome::Resumed,
                    _ => BeginIssuanceResultOutcome::IdempotencyInvalidState,
                });
            }
            if batch_status != "READY" { return Ok(BeginIssuanceResultOutcome::BatchNotReady); }
            let expected = expected_request_ids(connection, batch_id)?;
            let mut supplied = rows.iter().map(|row| row.issuance_request_id).collect::<Vec<_>>();
            supplied.sort_unstable(); supplied.dedup();
            let mut expected_sorted = expected; expected_sorted.sort_unstable();
            if supplied != expected_sorted || supplied.len() != rows.len() { return Ok(BeginIssuanceResultOutcome::RowSetMismatch); }
            insert_idempotency_record(connection, context.new_idempotency_record())?;
            let durable_context = serde_json::to_string(&context.durable()).map_err(|error| DbError::Query(format!("failed to serialize issuance result context: {error}")))?;
            connection.execute("UPDATE card_issuance_batches SET status='PROCESSING_RESULT',result_object_key=:1,result_checksum_sha256=:2,result_command_context_json=:3,updated_by_subject=:4,updated_at=SYSTIMESTAMP WHERE card_issuance_batch_id=:5", &[&result_object_key, &checksum, &durable_context, &context.actor.subject, &raw(batch_id)]).map_err(|error| query("failed to start issuance result processing", error))?;
            Ok(BeginIssuanceResultOutcome::Started)
        }).await
    }

    #[tracing::instrument(skip(self, context, result), fields(db.system="oracle", db.operation.name="card_issuance_results.reject", batch_id=%batch_id, issuance_request_id=%result.issuance_request_id))]
    pub async fn reject_card_issuance_request_atomic(
        &self,
        context: &MutationCommandContext,
        batch_id: Uuid,
        result: &CardIssuanceResultRow,
    ) -> DbResult<bool> {
        let result = result.clone();
        let context = context.clone();
        self.pool.with_transaction("reject card issuance request", move |connection| {
            let request_raw = raw(result.issuance_request_id);
            let status = match connection.query_row_as::<String>("SELECT status FROM card_issuance_requests WHERE card_issuance_request_id=:1 AND batch_id=:2 FOR UPDATE", &[&request_raw, &raw(batch_id)]) {
                Ok(value) => value,
                Err(error) if error.kind() == oracle::ErrorKind::NoDataFound => return Ok(false),
                Err(error) => return Err(query("failed to lock rejected issuance request", error)),
            };
            if status == "REJECTED" { return Ok(true); }
            if status != "EXPORTED" { return Ok(false); }
            connection.execute("UPDATE card_issuance_requests SET status='REJECTED',failure_code=:1,safe_failure_message=:2,updated_by_subject=:3,updated_at=SYSTIMESTAMP WHERE card_issuance_request_id=:4", &[&result.failure_code, &result.failure_message, &context.actor.subject, &request_raw]).map_err(|error| query("failed to reject issuance request", error))?;
            connection.execute("UPDATE provider_users SET status='ISSUANCE_REJECTED',updated_by_subject=:1,updated_at=SYSTIMESTAMP WHERE provider_user_id IN (SELECT provider_user_id FROM card_issuance_request_providers WHERE card_issuance_request_id=:2)", &[&context.actor.subject, &request_raw]).map_err(|error| query("failed to reject pending provider users", error))?;
            connection.execute("UPDATE card_issuance_request_providers SET status='REJECTED',updated_at=SYSTIMESTAMP WHERE card_issuance_request_id=:1", &[&request_raw]).map_err(|error| query("failed to reject issuance provider rows", error))?;
            connection.execute("UPDATE card_issuance_batch_rows SET result_status='REJECTED',safe_result_code=:1,safe_result_message=:2,processed_at=SYSTIMESTAMP WHERE card_issuance_batch_id=:3 AND card_issuance_request_id=:4", &[&result.failure_code, &result.failure_message, &raw(batch_id), &request_raw]).map_err(|error| query("failed to finalize rejected batch row", error))?;
            insert_audit_log(connection, NewAuditLog { audit_log_id: Uuid::new_v4(), entity_type: "CARD_ISSUANCE_REQUEST".to_string(), entity_id: result.issuance_request_id, action_type: AuditAction::StateTransition, reason: Some("Bank rejected card issuance request".to_string()), old_values: Some(serde_json::json!({"status":"EXPORTED"})), new_values: Some(serde_json::json!({"status":"REJECTED","failure_code":result.failure_code})), context: context.audit_context() })?;
            Ok(true)
        }).await
    }

    #[tracing::instrument(skip(self, context, result), fields(db.system="oracle", db.operation.name="card_issuance_results.prepare_issued", batch_id=%batch_id, issuance_request_id=%result.issuance_request_id))]
    pub async fn prepare_issued_card_atomic(
        &self,
        context: &MutationCommandContext,
        batch_id: Uuid,
        result: &CardIssuanceResultRow,
    ) -> DbResult<PrepareIssuedCardOutcome> {
        let context = context.clone();
        let result = result.clone();
        self.pool.with_transaction("prepare issued card accounts", move |connection| {
            let request_raw = raw(result.issuance_request_id);
            let row = match connection.query_row("SELECT user_id,card_range_id,status FROM card_issuance_requests WHERE card_issuance_request_id=:1 AND batch_id=:2 FOR UPDATE", &[&request_raw, &raw(batch_id)]) {
                Ok(value) => value,
                Err(error) if error.kind() == oracle::ErrorKind::NoDataFound => return Ok(PrepareIssuedCardOutcome::RequestNotFound),
                Err(error) => return Err(query("failed to lock issued request", error)),
            };
            let user_raw: Vec<u8> = row.get(0).map_err(read)?; let range_raw: Vec<u8> = row.get(1).map_err(read)?; let status: String = row.get(2).map_err(read)?;
            if status == "ISSUED" { return Ok(PrepareIssuedCardOutcome::AlreadyProcessed); }
            if matches!(status.as_str(), "PROCESSING_RESULT" | "RECOVERY_REQUIRED") {
                return load_issued_card_intent(connection, batch_id, result.issuance_request_id)
                    .map(|intent| intent.map(|value| PrepareIssuedCardOutcome::Prepared(Box::new(value))).unwrap_or(PrepareIssuedCardOutcome::RequestNotFound));
            }
            if status != "EXPORTED" { return Ok(PrepareIssuedCardOutcome::RequestNotFound); }
            let user_id = raw16_to_uuid(&user_raw)?; let range_id = raw16_to_uuid(&range_raw)?;
            let card_number = result.card_number.as_deref().ok_or_else(|| DbError::Query("issued result has no card number".to_string()))?;
            let in_range = connection.query_row_as::<i64>("SELECT COUNT(*) FROM card_ranges WHERE card_range_id=:1 AND :2 BETWEEN start_card_number AND end_card_number", &[&range_raw, &card_number]).map_err(|error| query("failed to validate issued PAN range", error))?;
            if in_range != 1 { return Ok(PrepareIssuedCardOutcome::InvalidPanRange); }
            if connection.query_row_as::<i64>("SELECT COUNT(*) FROM cards WHERE card_number=:1", &[&card_number]).map_err(|error| query("failed to check issued PAN uniqueness", error))? != 0 { return Ok(PrepareIssuedCardOutcome::CardNumberConflict); }
            if connection.query_row_as::<i64>("SELECT COUNT(*) FROM cards WHERE user_id=:1 AND card_range_id=:2 AND status='ACTIVE'", &[&user_raw, &range_raw]).map_err(|error| query("failed to check active card uniqueness", error))? != 0 { return Ok(PrepareIssuedCardOutcome::ActiveCardConflict); }
            let card_id = Uuid::new_v4();
            connection.execute("INSERT INTO cards (card_id,card_number,user_id,card_range_id,status,metadata_json,created_by_subject,updated_by_subject) VALUES (:1,:2,:3,:4,'PROVISIONING','{}',:5,:5)", &[&raw(card_id), &card_number, &user_raw, &range_raw, &context.actor.subject]).map_err(|error| query("failed to stage issued card", error))?;
            let usage = PolicyUsageAccountIds::for_card(card_id);
            super::provider_user::ensure_usage_account_mapping(connection, card_id, &usage)?;
            let provider_rows = connection.query("SELECT p.provider_user_id,p.provider_id,p.enrollment_id,p.enrollment_order FROM card_issuance_request_providers p WHERE p.card_issuance_request_id=:1 AND p.status='PENDING' ORDER BY p.enrollment_order", &[&request_raw]).map_err(|error| query("failed to list pending issuance providers", error))?;
            let mut providers = Vec::new(); let mut enrollment_id = Uuid::nil();
            for provider_row in provider_rows {
                let provider_row = provider_row.map_err(|error| query("failed to read pending issuance provider", error))?;
                let provider_user_raw: Vec<u8> = provider_row.get(0).map_err(read)?; let provider_raw: Vec<u8> = provider_row.get(1).map_err(read)?; let enrollment_raw: Vec<u8> = provider_row.get(2).map_err(read)?; let priority: i64 = provider_row.get(3).map_err(read)?;
                let provider_user_id = raw16_to_uuid(&provider_user_raw)?; let provider_id = raw16_to_uuid(&provider_raw)?; enrollment_id = raw16_to_uuid(&enrollment_raw)?;
                let account_id = deterministic_provider_user_account_id(provider_id, user_id);
                connection.execute("INSERT INTO provider_user_accounts (provider_user_account_id,provider_user_id,provider_id,user_id,tigerbeetle_account_id,status) VALUES (:1,:2,:3,:4,:1,'PROVISIONING')", &[&raw(account_id), &provider_user_raw, &provider_raw, &user_raw]).map_err(|error| query("failed to stage issued provider-user account", error))?;
                connection.execute("INSERT INTO card_provider_funding_sources (card_funding_source_id,card_id,provider_id,provider_user_id,provider_user_account_id,priority,status,created_by_subject,updated_by_subject) VALUES (:1,:2,:3,:4,:5,:6,'PROVISIONING',:7,:7)", &[&raw(Uuid::new_v4()), &raw(card_id), &provider_raw, &provider_user_raw, &raw(account_id), &priority, &context.actor.subject]).map_err(|error| query("failed to stage issued card funding source", error))?;
                connection.execute("UPDATE provider_users SET status='PROVISIONING',updated_by_subject=:1,updated_at=SYSTIMESTAMP WHERE provider_user_id=:2 AND status='CARD_ISSUANCE_PENDING'", &[&context.actor.subject, &provider_user_raw]).map_err(|error| query("failed to stage issued provider user", error))?;
                connection.execute("UPDATE card_issuance_request_providers SET status='PROVISIONING',updated_at=SYSTIMESTAMP WHERE card_issuance_request_id=:1 AND provider_id=:2", &[&request_raw, &provider_raw]).map_err(|error| query("failed to stage issuance provider row", error))?;
                providers.push(IssuanceProviderAccountIntent { provider_user_id, provider_id, account_id });
            }
            if providers.is_empty() { return Err(DbError::Query("issued request has no pending providers".to_string())); }
            let operation_id = Uuid::new_v4();
            let wal = serde_json::json!({"command_context":context.durable(),"finalization":IssuedCardFinalizationSnapshot::from_result(&result),"issuance_request_id":result.issuance_request_id,"batch_id":batch_id,"card_id":card_id,"card_range_id":range_id,"provider_accounts":providers.iter().map(|p| serde_json::json!({"provider_user_id":p.provider_user_id,"provider_id":p.provider_id,"account_id":p.account_id})).collect::<Vec<_>>(),"policy_usage_account_ids":usage}).to_string();
            connection.execute("INSERT INTO operation_wal (operation_id,operation_type,aggregate_type,aggregate_id,status,request_json) VALUES (:1,'CARD_ISSUANCE_ACCOUNT_PROVISION','CARD_ISSUANCE_REQUEST',:2,'PENDING',:3)", &[&raw(operation_id), &request_raw, &wal]).map_err(|error| query("failed to create issued card WAL", error))?;
            connection.execute(
                "UPDATE card_issuance_requests SET status='PROCESSING_RESULT',updated_at=SYSTIMESTAMP WHERE card_issuance_request_id=:1 AND status='EXPORTED'",
                &[&request_raw],
            ).map_err(|error| query("failed to mark issuance request processing", error))?;
            Ok(PrepareIssuedCardOutcome::Prepared(Box::new(IssuedCardProvisioningIntent { base: ExistingCardProvisioningIntent { operation_id, enrollment_id, provider_user_id: providers[0].provider_user_id, provider_id: providers[0].provider_id, user_id, card_id, card_range_id: range_id, provider_user_account_id: providers[0].account_id, usage_account_ids: usage }, issuance_request_id: result.issuance_request_id, batch_id, providers })))
        }).await
    }

    #[tracing::instrument(skip(self, context, result), fields(db.system="oracle", db.operation.name="card_issuance_results.finalize_issued", batch_id=%intent.batch_id, issuance_request_id=%intent.issuance_request_id))]
    pub async fn finalize_issued_card_atomic(
        &self,
        context: &DurableMutationContext,
        intent: &IssuedCardProvisioningIntent,
        result: &CardIssuanceResultRow,
    ) -> DbResult<()> {
        let context = context.clone();
        let intent = intent.clone();
        let result = result.clone();
        let event_headers = context.event_headers();
        self.pool.with_transaction("finalize issued card", move |connection| {
            let request_raw=raw(intent.issuance_request_id); let card_raw=raw(intent.base.card_id);
            connection.execute("UPDATE cards SET status='ACTIVE',updated_by_subject=:1,updated_at=SYSTIMESTAMP WHERE card_id=:2 AND status IN ('PROVISIONING','RECOVERY_REQUIRED')", &[&context.audit.actor_subject,&card_raw]).map_err(|error|query("failed to activate issued card",error))?;
            connection.execute("UPDATE card_policy_usage_accounts SET status='ACTIVE',updated_at=SYSTIMESTAMP WHERE card_id=:1 AND status IN ('PROVISIONING','RECOVERY_REQUIRED')", &[&card_raw]).map_err(|error|query("failed to activate issued usage accounts",error))?;
            connection.execute("UPDATE provider_user_accounts SET status='ACTIVE',updated_at=SYSTIMESTAMP WHERE provider_user_id IN (SELECT provider_user_id FROM card_issuance_request_providers WHERE card_issuance_request_id=:1) AND status IN ('PROVISIONING','RECOVERY_REQUIRED')", &[&request_raw]).map_err(|error|query("failed to activate issued provider-user accounts",error))?;
            connection.execute("UPDATE provider_users SET status='ACTIVE',updated_by_subject=:1,updated_at=SYSTIMESTAMP WHERE provider_user_id IN (SELECT provider_user_id FROM card_issuance_request_providers WHERE card_issuance_request_id=:2) AND status IN ('PROVISIONING','RECOVERY_REQUIRED')", &[&context.audit.actor_subject,&request_raw]).map_err(|error|query("failed to activate issued provider users",error))?;
            connection.execute("UPDATE card_provider_funding_sources SET status='ACTIVE',updated_by_subject=:1,updated_at=SYSTIMESTAMP WHERE card_id=:2 AND status IN ('PROVISIONING','RECOVERY_REQUIRED')", &[&context.audit.actor_subject,&card_raw]).map_err(|error|query("failed to activate issued funding sources",error))?;
            connection.execute("UPDATE card_issuance_request_providers SET status='ACTIVE',updated_at=SYSTIMESTAMP WHERE card_issuance_request_id=:1 AND status IN ('PROVISIONING','RECOVERY_REQUIRED')", &[&request_raw]).map_err(|error|query("failed to activate issuance provider rows",error))?;
            let produced=result.produced_at.map(format_time); let dispatched=result.dispatched_at.map(format_time);
            connection.execute("UPDATE card_issuance_requests SET status='ISSUED',issuer_reference=:1,produced_at=TO_TIMESTAMP_TZ(:2,'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'),dispatched_at=TO_TIMESTAMP_TZ(:3,'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'),tracking_reference=:4,updated_by_subject=:5,updated_at=SYSTIMESTAMP WHERE card_issuance_request_id=:6 AND status IN ('PROCESSING_RESULT','RECOVERY_REQUIRED')", &[&result.issuer_reference,&produced,&dispatched,&result.tracking_reference,&context.audit.actor_subject,&request_raw]).map_err(|error|query("failed to finalize issued request",error))?;
            connection.execute("UPDATE card_issuance_batch_rows SET result_status='ISSUED',processed_at=SYSTIMESTAMP WHERE card_issuance_batch_id=:1 AND card_issuance_request_id=:2", &[&raw(intent.batch_id),&request_raw]).map_err(|error|query("failed to finalize issued batch row",error))?;
            let wal_response = serde_json::json!({"verified": true}).to_string();
            connection.execute("UPDATE operation_wal SET status='COMPLETED',response_json=:1,error_json=NULL,updated_at=SYSTIMESTAMP,completed_at=SYSTIMESTAMP WHERE operation_id=:2 AND status IN ('PENDING','EXTERNAL_IN_FLIGHT','EXTERNAL_VERIFIED','FAILED')", &[&wal_response, &raw(intent.base.operation_id)]).map_err(|error|query("failed to complete issued card WAL",error))?;
            super::provider_user::insert_card_projection_outbox(
                connection,
                intent.base.card_id,
                Uuid::new_v4(),
                crate::domain::user_card::CardProfileRefreshReason::CardCreated,
                &event_headers,
            )?;
            insert_audit_log(connection,NewAuditLog{audit_log_id:Uuid::new_v4(),entity_type:"CARD".to_string(),entity_id:intent.base.card_id,action_type:AuditAction::Insert,reason:Some("Bank completed card issuance".to_string()),old_values:None,new_values:Some(serde_json::json!({"card_id":intent.base.card_id,"card_range_id":intent.base.card_range_id,"status":"ACTIVE","provider_count":intent.providers.len()})),context:context.audit.clone()})?;
            Ok(())
        }).await
    }

    #[tracing::instrument(skip(self, context), fields(db.system="oracle", db.operation.name="card_issuance_results.complete", batch_id=%batch_id))]
    pub async fn complete_card_issuance_result_atomic(
        &self,
        context: &DurableMutationContext,
        batch_id: Uuid,
    ) -> DbResult<CardIssuanceBatch> {
        let context = context.clone();
        self.pool.with_transaction("complete issuance result",move|connection|{
            let issued=outcome_count(connection,batch_id,"ISSUED")?; let rejected=outcome_count(connection,batch_id,"REJECTED")?; let failed=outcome_count(connection,batch_id,"FAILED")?;
            let request_count=connection.query_row_as::<i64>("SELECT request_count FROM card_issuance_batches WHERE card_issuance_batch_id=:1 FOR UPDATE", &[&raw(batch_id)]).map_err(|error|query("failed to lock completing issuance batch",error))?;
            if issued+rejected+failed != request_count { return Err(DbError::Conflict("issuance result still has unprocessed rows".to_string())); }
            let status=if failed==0{"COMPLETED"}else{"PARTIALLY_COMPLETED"};
            connection.execute("UPDATE card_issuance_batches SET status=:1,issued_count=:2,rejected_count=:3,failed_count=:4,updated_by_subject=:5,updated_at=SYSTIMESTAMP,completed_at=SYSTIMESTAMP WHERE card_issuance_batch_id=:6 AND status='PROCESSING_RESULT'", &[&status,&issued,&rejected,&failed,&context.audit.actor_subject,&raw(batch_id)]).map_err(|error|query("failed to complete issuance batch",error))?;
            let batch=super::card_issuance::fetch_batch(connection,batch_id)?.ok_or_else(||DbError::Query("completed issuance batch disappeared".to_string()))?;
            complete_idempotency_record(connection,&context.operation_type,&context.idempotency_key,"card_issuance_batch",batch_id,serde_json::to_value(&batch).map_err(|error|DbError::Query(format!("failed to serialize completed issuance batch: {error}")))?)?;
            Ok(batch)
        }).await
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="card_issuance_results.fail_row", batch_id=%batch_id, issuance_request_id=%issuance_request_id))]
    pub async fn fail_card_issuance_result_row_atomic(
        &self,
        batch_id: Uuid,
        issuance_request_id: Uuid,
        safe_code: &'static str,
    ) -> DbResult<()> {
        self.pool.with_transaction("mark issuance result row outcome", move |connection| {
            let recoverable = safe_code == "TIGERBEETLE_PROVISIONING_UNCERTAIN";
            let row_status = if recoverable { "RECOVERY_REQUIRED" } else { "FAILED" };
            let request_raw = raw(issuance_request_id);
            connection.execute("UPDATE card_issuance_requests SET status='RECOVERY_REQUIRED',updated_at=SYSTIMESTAMP WHERE card_issuance_request_id=:1 AND status IN ('EXPORTED','PROCESSING_RESULT')", &[&request_raw]).map_err(|error|query("failed to mark issuance request for recovery",error))?;
            connection.execute("UPDATE provider_users SET status='RECOVERY_REQUIRED',updated_at=SYSTIMESTAMP WHERE provider_user_id IN (SELECT provider_user_id FROM card_issuance_request_providers WHERE card_issuance_request_id=:1) AND status='PROVISIONING'", &[&request_raw]).map_err(|error|query("failed to mark issuance provider users for recovery",error))?;
            connection.execute("UPDATE provider_user_accounts SET status='RECOVERY_REQUIRED',updated_at=SYSTIMESTAMP WHERE provider_user_id IN (SELECT provider_user_id FROM card_issuance_request_providers WHERE card_issuance_request_id=:1) AND status='PROVISIONING'", &[&request_raw]).map_err(|error|query("failed to mark issuance accounts for recovery",error))?;
            connection.execute("UPDATE card_provider_funding_sources SET status='RECOVERY_REQUIRED',updated_at=SYSTIMESTAMP WHERE provider_user_id IN (SELECT provider_user_id FROM card_issuance_request_providers WHERE card_issuance_request_id=:1) AND status='PROVISIONING'", &[&request_raw]).map_err(|error|query("failed to mark issuance funding sources for recovery",error))?;
            connection.execute("UPDATE cards SET status='RECOVERY_REQUIRED',updated_at=SYSTIMESTAMP WHERE card_id IN (SELECT fs.card_id FROM card_provider_funding_sources fs JOIN card_issuance_request_providers rp ON rp.provider_user_id=fs.provider_user_id WHERE rp.card_issuance_request_id=:1) AND status='PROVISIONING'", &[&request_raw]).map_err(|error|query("failed to mark issued card for recovery",error))?;
            connection.execute("UPDATE operation_wal SET status='FAILED',error_json=:1,attempt_count=attempt_count+1,updated_at=SYSTIMESTAMP WHERE aggregate_id=:2 AND operation_type='CARD_ISSUANCE_ACCOUNT_PROVISION' AND status<>'COMPLETED'", &[&serde_json::json!({"code":safe_code}).to_string(),&request_raw]).map_err(|error|query("failed to mark issuance WAL failed",error))?;
            connection.execute("UPDATE card_issuance_batch_rows SET result_status=:1,safe_result_code=:2,processed_at=SYSTIMESTAMP WHERE card_issuance_batch_id=:3 AND card_issuance_request_id=:4 AND (result_status IS NULL OR result_status='RECOVERY_REQUIRED')", &[&row_status,&safe_code,&raw(batch_id),&request_raw]).map_err(|error|query("failed to mark issuance batch row outcome",error))?;
            Ok(())
        }).await
    }
}

pub(crate) fn load_issued_card_intent(
    connection: &oracle::Connection,
    batch_id: Uuid,
    issuance_request_id: Uuid,
) -> DbResult<Option<IssuedCardProvisioningIntent>> {
    let request_raw = raw(issuance_request_id);
    let mut rows = connection.query(
        "SELECT w.operation_id,c.card_id,c.user_id,c.card_range_id,u.amount_daily_account_id,u.amount_weekly_account_id,u.amount_monthly_account_id,u.amount_yearly_account_id,u.count_daily_account_id,u.count_weekly_account_id,u.count_monthly_account_id,u.count_yearly_account_id FROM operation_wal w JOIN card_issuance_request_providers rp ON rp.card_issuance_request_id=w.aggregate_id JOIN card_provider_funding_sources fs ON fs.provider_user_id=rp.provider_user_id JOIN cards c ON c.card_id=fs.card_id JOIN card_policy_usage_accounts u ON u.card_id=c.card_id WHERE w.aggregate_id=:1 AND w.operation_type='CARD_ISSUANCE_ACCOUNT_PROVISION' AND w.status IN ('PENDING','EXTERNAL_IN_FLIGHT','EXTERNAL_VERIFIED','FAILED') FOR UPDATE",
        &[&request_raw],
    ).map_err(|error| query("failed to load issued-card recovery intent", error))?;
    let Some(row) = rows.next() else {
        return Ok(None);
    };
    let row = row.map_err(|error| query("failed to read issued-card recovery intent", error))?;
    let operation_id = row_uuid(&row, 0)?;
    let card_id = row_uuid(&row, 1)?;
    let user_id = row_uuid(&row, 2)?;
    let card_range_id = row_uuid(&row, 3)?;
    let usage = PolicyUsageAccountIds {
        amount_daily: row_uuid(&row, 4)?,
        amount_weekly: row_uuid(&row, 5)?,
        amount_monthly: row_uuid(&row, 6)?,
        amount_yearly: row_uuid(&row, 7)?,
        count_daily: row_uuid(&row, 8)?,
        count_weekly: row_uuid(&row, 9)?,
        count_monthly: row_uuid(&row, 10)?,
        count_yearly: row_uuid(&row, 11)?,
    };
    let provider_rows = connection.query(
        "SELECT rp.provider_user_id,rp.provider_id,rp.enrollment_id,pua.provider_user_account_id FROM card_issuance_request_providers rp JOIN provider_user_accounts pua ON pua.provider_user_id=rp.provider_user_id WHERE rp.card_issuance_request_id=:1 AND rp.status IN ('PROVISIONING','RECOVERY_REQUIRED') ORDER BY rp.enrollment_order",
        &[&request_raw],
    ).map_err(|error| query("failed to load issued-card recovery providers", error))?;
    let mut providers = Vec::new();
    let mut enrollment_id = Uuid::nil();
    for row in provider_rows {
        let row =
            row.map_err(|error| query("failed to read issued-card recovery provider", error))?;
        enrollment_id = row_uuid(&row, 2)?;
        providers.push(IssuanceProviderAccountIntent {
            provider_user_id: row_uuid(&row, 0)?,
            provider_id: row_uuid(&row, 1)?,
            account_id: row_uuid(&row, 3)?,
        });
    }
    let Some(first) = providers.first() else {
        return Ok(None);
    };
    Ok(Some(IssuedCardProvisioningIntent {
        base: ExistingCardProvisioningIntent {
            operation_id,
            enrollment_id,
            provider_user_id: first.provider_user_id,
            provider_id: first.provider_id,
            user_id,
            card_id,
            card_range_id,
            provider_user_account_id: first.account_id,
            usage_account_ids: usage,
        },
        issuance_request_id,
        batch_id,
        providers,
    }))
}

fn expected_request_ids(connection: &oracle::Connection, batch_id: Uuid) -> DbResult<Vec<Uuid>> {
    let rows=connection.query("SELECT card_issuance_request_id FROM card_issuance_batch_rows WHERE card_issuance_batch_id=:1", &[&raw(batch_id)]).map_err(|error|query("failed to list expected result rows",error))?;
    let mut values = Vec::new();
    for row in rows {
        let raw_id: Vec<u8> = row
            .map_err(|error| query("failed to read expected result row", error))?
            .get(0)
            .map_err(read)?;
        values.push(raw16_to_uuid(&raw_id)?);
    }
    Ok(values)
}
fn outcome_count(connection: &oracle::Connection, batch_id: Uuid, status: &str) -> DbResult<i64> {
    connection.query_row_as::<i64>("SELECT COUNT(*) FROM card_issuance_batch_rows WHERE card_issuance_batch_id=:1 AND result_status=:2", &[&raw(batch_id),&status]).map_err(|error|query("failed to count issuance outcomes",error))
}
fn raw(value: Uuid) -> Vec<u8> {
    uuid_to_raw16(value).to_vec()
}
fn row_uuid(row: &oracle::Row, index: usize) -> DbResult<Uuid> {
    let value: Vec<u8> = row.get(index).map_err(read)?;
    raw16_to_uuid(&value)
}
fn query(context: &str, error: oracle::Error) -> DbError {
    DbError::Query(format!("{context}: {error}"))
}
fn read(error: oracle::Error) -> DbError {
    DbError::Query(format!(
        "failed to read Oracle issuance-result row: {error}"
    ))
}
fn format_time(value: chrono::DateTime<chrono::Utc>) -> String {
    value.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}
