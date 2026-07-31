use chrono::{DateTime, Utc};
use oracle::Row;
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
            provider::fetch_provider,
            types::{raw16_to_uuid, uuid_to_raw16},
        },
    },
    domain::{
        audit::{AuditAction, NewAuditLog},
        idempotency::IdempotencyStatus,
        provider::{
            FieldUpdate, Provider, ProviderContact, ProviderContactCursor, ProviderContactListPage,
            ProviderContactListQuery, ProviderContactRecord, ProviderContactStatus,
            ProviderContactType, ProviderContactUpdate, ProviderIdentityUpdate,
        },
    },
};

#[derive(Debug, Clone, PartialEq)]
pub enum ProviderIdentityMutationOutcome {
    Applied(Box<Provider>),
    Replayed(serde_json::Value),
    ProviderNotFound,
    IdempotencyConflict,
    IdempotencyInProgress,
    IdempotencyInvalidState,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProviderContactMutationOutcome {
    Applied(Box<ProviderContactRecord>),
    Replayed(serde_json::Value),
    ProviderNotFound,
    ContactNotFound,
    ContractInvalid,
    InvalidState,
    IdempotencyConflict,
    IdempotencyInProgress,
    IdempotencyInvalidState,
}

impl OracleRepository {
    #[tracing::instrument(skip(self, context, update), fields(db.system="oracle", db.operation.name="providers.identity.update", provider_id=%provider_id))]
    pub async fn update_provider_identity_atomic(
        &self,
        context: MutationCommandContext,
        provider_id: Uuid,
        update: ProviderIdentityUpdate,
    ) -> DbResult<ProviderIdentityMutationOutcome> {
        self.pool
            .with_transaction("update provider identity", move |connection| {
                let Some(before) = lock_provider(connection, provider_id)? else {
                    return Ok(ProviderIdentityMutationOutcome::ProviderNotFound);
                };
                if let Some(outcome) = classify_identity_idempotency(connection, &context)? {
                    return Ok(outcome);
                }
                insert_idempotency_record(connection, context.new_idempotency_record())?;
                let legal_name = update
                    .legal_name
                    .clone()
                    .unwrap_or_else(|| before.legal_name.clone());
                let trade_name = update
                    .trade_name
                    .clone()
                    .unwrap_or_else(|| before.trade_name.clone());
                let tax_id = apply_field(before.tax_id.clone(), &update.tax_id);
                let registration_number = apply_field(
                    before.registration_number.clone(),
                    &update.registration_number,
                );
                let email_address =
                    apply_field(before.email_address.clone(), &update.email_address);
                let website_url = apply_field(before.website_url.clone(), &update.website_url);
                let mailing_address =
                    apply_field(before.mailing_address.clone(), &update.mailing_address);
                let metadata = update
                    .metadata
                    .clone()
                    .unwrap_or_else(|| before.metadata.clone())
                    .to_string();
                connection
                    .execute(
                        provider_identity_update_sql(),
                        &[
                            &legal_name,
                            &trade_name,
                            &tax_id,
                            &registration_number,
                            &email_address,
                            &website_url,
                            &mailing_address,
                            &metadata,
                            &context.actor.subject,
                            &raw(provider_id),
                        ],
                    )
                    .map_err(query_error)?;
                let after = fetch_provider(connection, provider_id)?;
                let snapshot = after.replay_snapshot();
                insert_audit_log(
                    connection,
                    NewAuditLog {
                        audit_log_id: Uuid::new_v4(),
                        entity_type: "PROVIDER".to_string(),
                        entity_id: provider_id,
                        action_type: AuditAction::Update,
                        reason: Some(update.reason),
                        old_values: Some(before.replay_snapshot()),
                        new_values: Some(snapshot.clone()),
                        context: context.audit_context(),
                    },
                )?;
                complete_idempotency_record(
                    connection,
                    &context.operation_type,
                    context.idempotency_key.as_str(),
                    "provider",
                    provider_id,
                    snapshot,
                )?;
                Ok(ProviderIdentityMutationOutcome::Applied(Box::new(after)))
            })
            .await
    }

    #[tracing::instrument(skip(self, context, contact), fields(db.system="oracle", db.operation.name="provider_contacts.create", provider_id=%provider_id))]
    pub async fn create_provider_contact_atomic(
        &self,
        context: MutationCommandContext,
        provider_id: Uuid,
        contact: ProviderContact,
        reason: String,
    ) -> DbResult<ProviderContactMutationOutcome> {
        self.pool
            .with_transaction("create provider contact", move |connection| {
                if lock_provider(connection, provider_id)?.is_none() {
                    return Ok(ProviderContactMutationOutcome::ProviderNotFound);
                }
                if let Some(outcome) = classify_contact_idempotency(connection, &context)? {
                    return Ok(outcome);
                }
                insert_idempotency_record(connection, context.new_idempotency_record())?;
                connection.execute(
                    "INSERT INTO provider_contacts (provider_contact_id,provider_id,contact_type,contact_name,email,phone,mobile,sms_enabled,metadata_json,status,created_by_subject,updated_by_subject) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,'ACTIVE',:10,:10)",
                    &[&raw(contact.provider_contact_id),&raw(provider_id),&contact.contact_type.as_db_value(),&contact.name,&contact.email,&contact.phone,&contact.mobile,&i32::from(contact.sms_enabled),&contact.metadata.to_string(),&context.actor.subject],
                ).map_err(query_error)?;
                let created = fetch_contact(connection, provider_id, contact.provider_contact_id)?
                    .expect("newly inserted provider contact exists");
                finish_contact_mutation(connection, &context, None, &created, AuditAction::Insert, reason)?;
                Ok(ProviderContactMutationOutcome::Applied(Box::new(created)))
            })
            .await
    }

    #[tracing::instrument(skip(self, context, update), fields(db.system="oracle", db.operation.name="provider_contacts.update", provider_id=%provider_id, provider_contact_id=%contact_id))]
    pub async fn update_provider_contact_atomic(
        &self,
        context: MutationCommandContext,
        provider_id: Uuid,
        contact_id: Uuid,
        update: ProviderContactUpdate,
    ) -> DbResult<ProviderContactMutationOutcome> {
        self.pool
            .with_transaction("update provider contact", move |connection| {
                if lock_provider(connection, provider_id)?.is_none() {
                    return Ok(ProviderContactMutationOutcome::ProviderNotFound);
                }
                if let Some(outcome) = classify_contact_idempotency(connection, &context)? {
                    return Ok(outcome);
                }
                let Some(before) = lock_contact(connection, provider_id, contact_id)? else {
                    return Ok(ProviderContactMutationOutcome::ContactNotFound);
                };
                let Ok(merged) = merge_contact(&before, &update) else {
                    return Ok(ProviderContactMutationOutcome::ContractInvalid);
                };
                insert_idempotency_record(connection, context.new_idempotency_record())?;
                connection.execute(
                    "UPDATE provider_contacts SET contact_type=:1,contact_name=:2,email=:3,phone=:4,mobile=:5,sms_enabled=:6,metadata_json=:7,updated_by_subject=:8,updated_at=SYSTIMESTAMP WHERE provider_contact_id=:9 AND provider_id=:10",
                    &[&merged.contact_type.as_db_value(),&merged.name,&merged.email,&merged.phone,&merged.mobile,&i32::from(merged.sms_enabled),&merged.metadata.to_string(),&context.actor.subject,&raw(contact_id),&raw(provider_id)],
                ).map_err(query_error)?;
                let after = fetch_contact(connection, provider_id, contact_id)?.expect("locked contact exists");
                finish_contact_mutation(connection, &context, Some(&before), &after, AuditAction::Update, update.reason)?;
                Ok(ProviderContactMutationOutcome::Applied(Box::new(after)))
            })
            .await
    }

    #[tracing::instrument(skip(self, context, reason), fields(db.system="oracle", db.operation.name="provider_contacts.transition", provider_id=%provider_id, provider_contact_id=%contact_id, target_status=target.as_db_value()))]
    pub async fn transition_provider_contact_atomic(
        &self,
        context: MutationCommandContext,
        provider_id: Uuid,
        contact_id: Uuid,
        target: ProviderContactStatus,
        reason: String,
    ) -> DbResult<ProviderContactMutationOutcome> {
        self.pool
            .with_transaction("transition provider contact", move |connection| {
                if lock_provider(connection, provider_id)?.is_none() {
                    return Ok(ProviderContactMutationOutcome::ProviderNotFound);
                }
                if let Some(outcome) = classify_contact_idempotency(connection, &context)? {
                    return Ok(outcome);
                }
                let Some(before) = lock_contact(connection, provider_id, contact_id)? else {
                    return Ok(ProviderContactMutationOutcome::ContactNotFound);
                };
                if before.status == target {
                    return Ok(ProviderContactMutationOutcome::InvalidState);
                }
                insert_idempotency_record(connection, context.new_idempotency_record())?;
                connection.execute(
                    "UPDATE provider_contacts SET status=:1,updated_by_subject=:2,updated_at=SYSTIMESTAMP WHERE provider_contact_id=:3 AND provider_id=:4",
                    &[&target.as_db_value(),&context.actor.subject,&raw(contact_id),&raw(provider_id)],
                ).map_err(query_error)?;
                let after = fetch_contact(connection, provider_id, contact_id)?.expect("locked contact exists");
                finish_contact_mutation(connection, &context, Some(&before), &after, AuditAction::StateTransition, reason)?;
                Ok(ProviderContactMutationOutcome::Applied(Box::new(after)))
            })
            .await
    }

    #[tracing::instrument(skip(self, query), fields(db.system="oracle", db.operation.name="provider_contacts.list", provider_id=%provider_id))]
    pub async fn list_provider_contacts(
        &self,
        provider_id: Uuid,
        query: ProviderContactListQuery,
    ) -> DbResult<Option<ProviderContactListPage>> {
        self.pool
            .with_connection(move |connection| {
                if !provider_exists(connection, provider_id)? {
                    return Ok(None);
                }
                let contact_type = query
                    .contact_type
                    .map(|value| value.as_db_value().to_string());
                let status = query.status.map(|value| value.as_db_value().to_string());
                let cursor_time = query
                    .cursor
                    .as_ref()
                    .map(|value| format_utc(value.created_at));
                let cursor_id = query
                    .cursor
                    .as_ref()
                    .map(|value| raw(value.provider_contact_id));
                let fetch_limit = i64::from(query.limit) + 1;
                let rows = connection
                    .query_named(
                        contact_list_sql(),
                        &[
                            ("provider_id", &raw(provider_id)),
                            ("contact_type", &contact_type),
                            ("status", &status),
                            ("cursor_time", &cursor_time),
                            ("cursor_id", &cursor_id),
                            ("fetch_limit", &fetch_limit),
                        ],
                    )
                    .map_err(query_error)?;
                let mut items = Vec::new();
                for row in rows {
                    items.push(map_contact_row(&row.map_err(query_error)?)?);
                }
                let has_next = items.len() > query.limit as usize;
                if has_next {
                    items.truncate(query.limit as usize);
                }
                let next_cursor = has_next.then(|| {
                    let last = items.last().expect("non-empty contact page");
                    ProviderContactCursor {
                        created_at: last.created_at,
                        provider_contact_id: last.provider_contact_id,
                    }
                });
                Ok(Some(ProviderContactListPage { items, next_cursor }))
            })
            .await
    }
}

fn finish_contact_mutation(
    connection: &oracle::Connection,
    context: &MutationCommandContext,
    before: Option<&ProviderContactRecord>,
    after: &ProviderContactRecord,
    action: AuditAction,
    reason: String,
) -> DbResult<()> {
    let snapshot = after.replay_snapshot();
    insert_audit_log(
        connection,
        NewAuditLog {
            audit_log_id: Uuid::new_v4(),
            entity_type: "PROVIDER_CONTACT".to_string(),
            entity_id: after.provider_contact_id,
            action_type: action,
            reason: Some(reason),
            old_values: before.map(ProviderContactRecord::replay_snapshot),
            new_values: Some(snapshot.clone()),
            context: context.audit_context(),
        },
    )?;
    complete_idempotency_record(
        connection,
        &context.operation_type,
        context.idempotency_key.as_str(),
        "provider_contact",
        after.provider_contact_id,
        snapshot,
    )
}

fn merge_contact(
    before: &ProviderContactRecord,
    update: &ProviderContactUpdate,
) -> Result<ProviderContact, &'static str> {
    let mut contact = ProviderContact {
        provider_contact_id: before.provider_contact_id,
        contact_type: update.contact_type.unwrap_or(before.contact_type),
        name: apply_field(before.name.clone(), &update.name),
        email: apply_field(before.email.clone(), &update.email),
        phone: apply_field(before.phone.clone(), &update.phone),
        mobile: apply_field(before.mobile.clone(), &update.mobile),
        sms_enabled: update.sms_enabled.unwrap_or(before.sms_enabled),
        metadata: update
            .metadata
            .clone()
            .unwrap_or_else(|| before.metadata.clone()),
    };
    contact.validate_and_normalize()?;
    Ok(contact)
}

fn apply_field<T: Clone>(current: Option<T>, update: &FieldUpdate<T>) -> Option<T> {
    match update {
        FieldUpdate::Unchanged => current,
        FieldUpdate::Set(value) => Some(value.clone()),
        FieldUpdate::Clear => None,
    }
}
fn classify_identity_idempotency(
    connection: &oracle::Connection,
    context: &MutationCommandContext,
) -> DbResult<Option<ProviderIdentityMutationOutcome>> {
    let Some(existing) = fetch_idempotency_record(
        connection,
        &context.operation_type,
        context.idempotency_key.as_str(),
    )?
    else {
        return Ok(None);
    };
    if existing.request_hash != context.request_hash {
        return Ok(Some(ProviderIdentityMutationOutcome::IdempotencyConflict));
    }
    Ok(Some(match existing.status {
        IdempotencyStatus::Completed => existing
            .response_snapshot
            .map(ProviderIdentityMutationOutcome::Replayed)
            .unwrap_or(ProviderIdentityMutationOutcome::IdempotencyInvalidState),
        IdempotencyStatus::InProgress => ProviderIdentityMutationOutcome::IdempotencyInProgress,
        _ => ProviderIdentityMutationOutcome::IdempotencyInvalidState,
    }))
}
fn classify_contact_idempotency(
    connection: &oracle::Connection,
    context: &MutationCommandContext,
) -> DbResult<Option<ProviderContactMutationOutcome>> {
    let Some(existing) = fetch_idempotency_record(
        connection,
        &context.operation_type,
        context.idempotency_key.as_str(),
    )?
    else {
        return Ok(None);
    };
    if existing.request_hash != context.request_hash {
        return Ok(Some(ProviderContactMutationOutcome::IdempotencyConflict));
    }
    Ok(Some(match existing.status {
        IdempotencyStatus::Completed => existing
            .response_snapshot
            .map(ProviderContactMutationOutcome::Replayed)
            .unwrap_or(ProviderContactMutationOutcome::IdempotencyInvalidState),
        IdempotencyStatus::InProgress => ProviderContactMutationOutcome::IdempotencyInProgress,
        _ => ProviderContactMutationOutcome::IdempotencyInvalidState,
    }))
}

fn lock_provider(connection: &oracle::Connection, provider_id: Uuid) -> DbResult<Option<Provider>> {
    match connection.query_row_as::<String>(
        "SELECT status FROM providers WHERE provider_id=:1 FOR UPDATE",
        &[&raw(provider_id)],
    ) {
        Ok(_) => fetch_provider(connection, provider_id).map(Some),
        Err(error) if error.kind() == oracle::ErrorKind::NoDataFound => Ok(None),
        Err(error) => Err(query_error(error)),
    }
}
fn provider_exists(connection: &oracle::Connection, provider_id: Uuid) -> DbResult<bool> {
    let count: i64 = connection
        .query_row_as(
            "SELECT COUNT(*) FROM providers WHERE provider_id=:1",
            &[&raw(provider_id)],
        )
        .map_err(query_error)?;
    Ok(count == 1)
}
fn lock_contact(
    connection: &oracle::Connection,
    provider_id: Uuid,
    contact_id: Uuid,
) -> DbResult<Option<ProviderContactRecord>> {
    let sql = format!("{} FOR UPDATE", contact_select_sql());
    fetch_optional_contact_sql(connection, &sql, provider_id, contact_id)
}
fn fetch_contact(
    connection: &oracle::Connection,
    provider_id: Uuid,
    contact_id: Uuid,
) -> DbResult<Option<ProviderContactRecord>> {
    fetch_optional_contact_sql(connection, contact_select_sql(), provider_id, contact_id)
}
fn fetch_optional_contact_sql(
    connection: &oracle::Connection,
    sql: &str,
    provider_id: Uuid,
    contact_id: Uuid,
) -> DbResult<Option<ProviderContactRecord>> {
    let mut rows = connection
        .query(sql, &[&raw(contact_id), &raw(provider_id)])
        .map_err(query_error)?;
    match rows.next() {
        Some(Ok(row)) => map_contact_row(&row).map(Some),
        Some(Err(error)) => Err(query_error(error)),
        None => Ok(None),
    }
}

fn map_contact_row(row: &Row) -> DbResult<ProviderContactRecord> {
    let id: Vec<u8> = row.get(0).map_err(read_error)?;
    let provider: Vec<u8> = row.get(1).map_err(read_error)?;
    let contact_type: String = row.get(2).map_err(read_error)?;
    let metadata: String = row.get(8).map_err(read_error)?;
    let status: String = row.get(9).map_err(read_error)?;
    let created: String = row.get(12).map_err(read_error)?;
    let updated: String = row.get(13).map_err(read_error)?;
    Ok(ProviderContactRecord {
        provider_contact_id: raw16_to_uuid(&id)?,
        provider_id: raw16_to_uuid(&provider)?,
        contact_type: ProviderContactType::from_db_value(&contact_type)
            .ok_or_else(|| DbError::Query("unknown provider contact type".to_string()))?,
        name: row.get(3).map_err(read_error)?,
        email: row.get(4).map_err(read_error)?,
        phone: row.get(5).map_err(read_error)?,
        mobile: row.get(6).map_err(read_error)?,
        sms_enabled: row.get::<_, i32>(7).map_err(read_error)? == 1,
        metadata: serde_json::from_str(&metadata).map_err(|error| {
            DbError::Query(format!("invalid provider contact metadata: {error}"))
        })?,
        status: ProviderContactStatus::from_db_value(&status)
            .ok_or_else(|| DbError::Query("unknown provider contact status".to_string()))?,
        created_by_subject: row.get(10).map_err(read_error)?,
        updated_by_subject: row.get(11).map_err(read_error)?,
        created_at: parse_utc(&created)?,
        updated_at: parse_utc(&updated)?,
    })
}

fn provider_identity_update_sql() -> &'static str {
    "UPDATE providers SET legal_name=:1,trade_name=:2,tax_id=:3,registration_number=:4,email_address=:5,website_url=:6,mailing_address=:7,metadata_json=:8,updated_by_subject=:9,updated_at=SYSTIMESTAMP WHERE provider_id=:10"
}
fn contact_select_sql() -> &'static str {
    "SELECT provider_contact_id,provider_id,contact_type,contact_name,email,phone,mobile,sms_enabled,JSON_SERIALIZE(metadata_json RETURNING CLOB),status,created_by_subject,updated_by_subject,TO_CHAR(SYS_EXTRACT_UTC(created_at),'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'),TO_CHAR(SYS_EXTRACT_UTC(updated_at),'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') FROM provider_contacts WHERE provider_contact_id=:1 AND provider_id=:2"
}
fn contact_list_sql() -> &'static str {
    r#"SELECT provider_contact_id,provider_id,contact_type,contact_name,email,phone,mobile,sms_enabled,JSON_SERIALIZE(metadata_json RETURNING CLOB),status,created_by_subject,updated_by_subject,TO_CHAR(SYS_EXTRACT_UTC(created_at),'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'),TO_CHAR(SYS_EXTRACT_UTC(updated_at),'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') FROM provider_contacts WHERE provider_id=:provider_id AND (:contact_type IS NULL OR contact_type=:contact_type) AND (:status IS NULL OR status=:status) AND (:cursor_time IS NULL OR SYS_EXTRACT_UTC(created_at)<TO_TIMESTAMP(:cursor_time,'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') OR (SYS_EXTRACT_UTC(created_at)=TO_TIMESTAMP(:cursor_time,'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AND provider_contact_id<:cursor_id)) ORDER BY created_at DESC,provider_contact_id DESC FETCH FIRST :fetch_limit ROWS ONLY"#
}
fn raw(value: Uuid) -> Vec<u8> {
    uuid_to_raw16(value).to_vec()
}
fn format_utc(value: DateTime<Utc>) -> String {
    value.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}
fn parse_utc(value: &str) -> DbResult<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)
        .map_err(|error| DbError::Query(format!("invalid provider contact timestamp: {error}")))?
        .with_timezone(&Utc))
}
fn query_error(error: oracle::Error) -> DbError {
    DbError::Query(format!(
        "provider identity/contact operation failed: {error}"
    ))
}
fn read_error(error: oracle::Error) -> DbError {
    DbError::Query(format!("failed to read provider contact row: {error}"))
}
