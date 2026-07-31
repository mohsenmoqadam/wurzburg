use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
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
            CardInstruction, CardProfileFundingSource, CardProfileLedgerAccounts,
            CardProfileProjection, CardResolution, NewProviderUserEnrollment,
            PolicyUsageAccountIds, ProviderUserCursor, ProviderUserPage, ProviderUserRecord,
            ProviderUserStatus, ProviderUserView, UserCardSummary, UserProviderSummary,
            deterministic_provider_user_account_id, mask_card_number,
        },
    },
    kafka::contract::{InternalEventEnvelope, InternalEventHeaders},
};

#[derive(Debug, Clone)]
pub struct ExistingCardProvisioningIntent {
    pub operation_id: Uuid,
    pub enrollment_id: Uuid,
    pub provider_user_id: Uuid,
    pub provider_id: Uuid,
    pub user_id: Uuid,
    pub card_id: Uuid,
    pub card_range_id: Uuid,
    pub provider_user_account_id: Uuid,
    pub usage_account_ids: PolicyUsageAccountIds,
}

#[derive(Debug, Clone)]
pub enum EnrollProviderUserPersistenceOutcome {
    ExistingCardPrepared(Box<ExistingCardProvisioningIntent>),
    IssuancePending(Box<ProviderUserView>),
    Replayed(serde_json::Value),
    ProviderNotFound,
    ProviderNotActive,
    OperationalProfileChanged,
    RangeNotReady,
    ActiveCardSelectionRequired,
    ExistingCardNotFound,
    ExistingCardOwnerMismatch,
    ExistingCardRangeMismatch,
    ExistingCardNotActive,
    ExistingRelationship,
    MultiProviderAttachmentDisabled,
    UserLimitReached,
    IdempotencyConflict,
    IdempotencyInProgress,
    IdempotencyInvalidState,
}

impl OracleRepository {
    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="users.cards.list", user_id=%user_id, provider_filter=provider_filter.is_some()))]
    pub async fn list_user_cards(
        &self,
        user_id: Uuid,
        provider_filter: Option<Uuid>,
    ) -> DbResult<Vec<UserCardSummary>> {
        self.pool.with_connection(move |connection| {
            let user_raw = raw(user_id);
            let provider_raw = provider_filter.map(raw);
            let sql = if provider_filter.is_some() {
                "SELECT DISTINCT c.card_id,c.card_number,c.card_range_id,c.status,c.state_version,c.materialized_version,TO_CHAR(SYS_EXTRACT_UTC(c.created_at),'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') FROM cards c JOIN card_provider_funding_sources fs ON fs.card_id=c.card_id WHERE c.user_id=:1 AND fs.provider_id=:2 ORDER BY c.created_at DESC,c.card_id DESC"
            } else {
                "SELECT c.card_id,c.card_number,c.card_range_id,c.status,c.state_version,c.materialized_version,TO_CHAR(SYS_EXTRACT_UTC(c.created_at),'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') FROM cards c WHERE c.user_id=:1 ORDER BY c.created_at DESC,c.card_id DESC"
            };
            let rows = match provider_raw.as_ref() {
                Some(provider_id) => connection.query(sql, &[&user_raw, provider_id]),
                None => connection.query(sql, &[&user_raw]),
            }.map_err(|error| query("failed to list user cards", error))?;
            let mut cards = Vec::new();
            for row in rows {
                let row = row.map_err(|error| query("failed to read user card", error))?;
                let card_id = row_uuid(&row, 0)?;
                let provider_rows = connection.query(
                    "SELECT provider_id FROM card_provider_funding_sources WHERE card_id=:1 AND status IN ('PROVISIONING','ACTIVE','SUSPENDED','RECOVERY_REQUIRED') ORDER BY priority NULLS LAST,provider_id",
                    &[&raw(card_id)],
                ).map_err(|error| query("failed to list card providers", error))?;
                let mut provider_ids = Vec::new();
                for provider in provider_rows {
                    provider_ids.push(row_uuid(
                        &provider.map_err(|error| query("failed to read card provider", error))?,
                        0,
                    )?);
                }
                let created: String = row.get(6).map_err(read)?;
                let card_number: String = row.get(1).map_err(read)?;
                cards.push(UserCardSummary {
                    card_id,
                    masked_card_number: mask_card_number(&card_number),
                    card_range_id: row_uuid(&row, 2)?,
                    status: row.get(3).map_err(read)?,
                    state_version: row.get(4).map_err(read)?,
                    materialized_version: row.get(5).map_err(read)?,
                    provider_ids,
                    created_at: parse_time(&created)?,
                });
            }
            Ok(cards)
        }).await
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="users.providers.list", user_id=%user_id, provider_filter=provider_filter.is_some()))]
    pub async fn list_user_providers(
        &self,
        user_id: Uuid,
        provider_filter: Option<Uuid>,
    ) -> DbResult<Vec<UserProviderSummary>> {
        self.pool.with_connection(move |connection| {
            let user_raw = raw(user_id);
            let provider_raw = provider_filter.map(raw);
            let base = "SELECT pu.provider_id,pu.provider_user_id,pu.status,pua.provider_user_account_id,c.card_id,c.card_number,c.card_range_id,TO_CHAR(SYS_EXTRACT_UTC(pu.created_at),'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') FROM provider_users pu LEFT JOIN provider_user_accounts pua ON pua.provider_user_id=pu.provider_user_id LEFT JOIN card_provider_funding_sources fs ON fs.provider_user_id=pu.provider_user_id AND fs.status IN ('PROVISIONING','ACTIVE','SUSPENDED','RECOVERY_REQUIRED') LEFT JOIN cards c ON c.card_id=fs.card_id";
            let sql = if provider_filter.is_some() {
                format!("{base} WHERE pu.user_id=:1 AND pu.provider_id=:2 ORDER BY pu.created_at DESC,pu.provider_user_id DESC")
            } else {
                format!("{base} WHERE pu.user_id=:1 ORDER BY pu.created_at DESC,pu.provider_user_id DESC")
            };
            let rows = match provider_raw.as_ref() {
                Some(provider_id) => connection.query(&sql, &[&user_raw, provider_id]),
                None => connection.query(&sql, &[&user_raw]),
            }.map_err(|error| query("failed to list user providers", error))?;
            let mut providers = Vec::new();
            for row in rows {
                let row = row.map_err(|error| query("failed to read user provider", error))?;
                let status: String = row.get(2).map_err(read)?;
                let account: Option<Vec<u8>> = row.get(3).map_err(read)?;
                let card: Option<Vec<u8>> = row.get(4).map_err(read)?;
                let card_number: Option<String> = row.get(5).map_err(read)?;
                let range: Option<Vec<u8>> = row.get(6).map_err(read)?;
                let created: String = row.get(7).map_err(read)?;
                providers.push(UserProviderSummary {
                    provider_id: row_uuid(&row, 0)?,
                    provider_user_id: row_uuid(&row, 1)?,
                    provider_user_status: ProviderUserStatus::from_db_value(&status)
                        .ok_or_else(|| DbError::Query("unknown provider-user status".to_string()))?,
                    provider_user_account_id: account.as_deref().map(raw16_to_uuid).transpose()?,
                    card_id: card.as_deref().map(raw16_to_uuid).transpose()?,
                    masked_card_number: card_number.as_deref().map(mask_card_number),
                    card_range_id: range.as_deref().map(raw16_to_uuid).transpose()?,
                    created_at: parse_time(&created)?,
                });
            }
            Ok(providers)
        }).await
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="provider_users.list", provider_id=%provider_id, limit))]
    pub async fn list_provider_users(
        &self,
        provider_id: Uuid,
        status: Option<ProviderUserStatus>,
        cursor: Option<ProviderUserCursor>,
        limit: u16,
    ) -> DbResult<ProviderUserPage> {
        self.pool.with_connection(move |connection| {
            let status = status.map(|value| value.as_db_value().to_string());
            let cursor_time = cursor.as_ref().map(|value| value.created_at.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string());
            let cursor_id = cursor.as_ref().map(|value| raw(value.provider_user_id));
            let fetch_limit = i64::from(limit) + 1;
            let rows = connection.query(
                "SELECT pu.provider_user_id,pu.enrollment_id,pu.provider_id,pu.user_id,u.national_id,u.first_name,u.last_name,pu.provider_customer_reference,pu.identity_mismatch,JSON_SERIALIZE(pu.mismatch_fields_json RETURNING CLOB),pu.status,c.card_id,c.card_number,c.card_range_id,pua.provider_user_account_id,TO_CHAR(SYS_EXTRACT_UTC(pu.created_at),'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'),TO_CHAR(SYS_EXTRACT_UTC(pu.updated_at),'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') FROM provider_users pu JOIN users u ON u.user_id=pu.user_id LEFT JOIN card_provider_funding_sources fs ON fs.provider_user_id=pu.provider_user_id AND fs.status IN ('PROVISIONING','ACTIVE','SUSPENDED','RECOVERY_REQUIRED') LEFT JOIN cards c ON c.card_id=fs.card_id LEFT JOIN provider_user_accounts pua ON pua.provider_user_id=pu.provider_user_id WHERE pu.provider_id=:1 AND (:2 IS NULL OR pu.status=:3) AND (:4 IS NULL OR pu.created_at<TO_TIMESTAMP_TZ(:5,'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') OR (pu.created_at=TO_TIMESTAMP_TZ(:6,'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AND pu.provider_user_id<:7)) ORDER BY pu.created_at DESC,pu.provider_user_id DESC FETCH FIRST :8 ROWS ONLY",
                &[&raw(provider_id), &status, &status, &cursor_time, &cursor_time, &cursor_time, &cursor_id, &fetch_limit],
            ).map_err(|error| query("failed to list provider users", error))?;
            let mut items = Vec::new();
            for row in rows { items.push(map_provider_user_record(&row.map_err(|error| query("failed to read provider-user list row", error))?)?); }
            let has_next = items.len() > usize::from(limit);
            if has_next { items.truncate(usize::from(limit)); }
            let next_cursor = has_next.then(|| { let item=items.last().expect("non-empty provider-user page"); ProviderUserCursor { created_at:item.created_at, provider_user_id:item.provider_user_id } });
            Ok(ProviderUserPage { items, next_cursor })
        }).await
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="provider_users.get", provider_id=%provider_id, user_id=%user_id))]
    pub async fn get_provider_user_record(
        &self,
        provider_id: Uuid,
        user_id: Uuid,
    ) -> DbResult<Option<ProviderUserRecord>> {
        self.pool.with_connection(move |connection| {
            match connection.query(
                "SELECT pu.provider_user_id,pu.enrollment_id,pu.provider_id,pu.user_id,u.national_id,u.first_name,u.last_name,pu.provider_customer_reference,pu.identity_mismatch,JSON_SERIALIZE(pu.mismatch_fields_json RETURNING CLOB),pu.status,c.card_id,c.card_number,c.card_range_id,pua.provider_user_account_id,TO_CHAR(SYS_EXTRACT_UTC(pu.created_at),'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'),TO_CHAR(SYS_EXTRACT_UTC(pu.updated_at),'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') FROM provider_users pu JOIN users u ON u.user_id=pu.user_id LEFT JOIN card_provider_funding_sources fs ON fs.provider_user_id=pu.provider_user_id AND fs.status IN ('PROVISIONING','ACTIVE','SUSPENDED','RECOVERY_REQUIRED') LEFT JOIN cards c ON c.card_id=fs.card_id LEFT JOIN provider_user_accounts pua ON pua.provider_user_id=pu.provider_user_id WHERE pu.provider_id=:1 AND pu.user_id=:2",
                &[&raw(provider_id), &raw(user_id)],
            ) {
                Ok(mut rows) => match rows.next() {
                    Some(Ok(row)) => map_provider_user_record(&row).map(Some),
                    Some(Err(error)) => Err(query("failed to read provider user", error)),
                    None => Ok(None),
                },
                Err(error) => Err(query("failed to get provider user", error)),
            }
        }).await
    }

    #[tracing::instrument(skip(self, context, enrollment), fields(db.system="oracle", db.operation.name="provider_users.enroll", provider_id=%provider_id, provider_operational_profile_id=%operational_profile_id))]
    pub async fn prepare_provider_user_enrollment_atomic(
        &self,
        context: MutationCommandContext,
        provider_id: Uuid,
        operational_profile_id: Uuid,
        max_total_users: Option<u64>,
        allow_multi_provider_attachment: bool,
        enrollment: NewProviderUserEnrollment,
    ) -> DbResult<EnrollProviderUserPersistenceOutcome> {
        let operation_type = context.operation_type.clone();
        let key = context.idempotency_key.as_str().to_string();
        let request_hash = context.request_hash.clone();
        self.pool
            .with_transaction("prepare provider-user enrollment", move |connection| {
                if let Some(existing) = fetch_idempotency_record(connection, &operation_type, &key)? {
                    return classify_idempotency(connection, &existing, &request_hash);
                }

                let provider_raw = raw(provider_id);
                let provider_status = match connection.query_row_as::<String>(
                    "SELECT status FROM providers WHERE provider_id=:1 FOR UPDATE",
                    &[&provider_raw],
                ) {
                    Ok(value) => value,
                    Err(error) if error.kind() == oracle::ErrorKind::NoDataFound => {
                        return Ok(EnrollProviderUserPersistenceOutcome::ProviderNotFound);
                    }
                    Err(error) => return Err(query("failed to lock provider", error)),
                };
                if provider_status != "ACTIVE" {
                    return Ok(EnrollProviderUserPersistenceOutcome::ProviderNotActive);
                }
                let profile_count = connection
                    .query_row_as::<i64>(
                        "SELECT COUNT(*) FROM provider_operational_profiles WHERE provider_operational_profile_id=:1 AND provider_id=:2 AND status='ACTIVE' AND effective_at<=SYSTIMESTAMP",
                        &[&raw(operational_profile_id), &provider_raw],
                    )
                    .map_err(|error| query("failed to verify provider operational profile", error))?;
                if profile_count != 1 {
                    return Ok(EnrollProviderUserPersistenceOutcome::OperationalProfileChanged);
                }

                lock_national_identity(connection, &enrollment.national_id)?;
                if let Some(existing) = fetch_idempotency_record(connection, &operation_type, &key)? {
                    return classify_idempotency(connection, &existing, &request_hash);
                }
                let (user_id, canonical_first_name, canonical_last_name) =
                    find_or_create_user(connection, &context, &enrollment)?;
                if let Some(existing) = find_relationship(connection, provider_id, user_id)? {
                    if existing.status != "ISSUANCE_REJECTED"
                        || existing.provider_customer_reference
                            != enrollment.provider_customer_reference
                        || !matches!(&enrollment.card_instruction, CardInstruction::IssueNew(_))
                    {
                        return Ok(EnrollProviderUserPersistenceOutcome::ExistingRelationship);
                    }
                    let Some(range) = resolve_provider_range(connection, provider_id)? else {
                        return Ok(EnrollProviderUserPersistenceOutcome::RangeNotReady);
                    };
                    if active_card_for_user_range(connection, user_id, range.card_range_id)?
                        .is_some()
                    {
                        return Ok(
                            EnrollProviderUserPersistenceOutcome::ActiveCardSelectionRequired,
                        );
                    }
                    let CardInstruction::IssueNew(delivery) = &enrollment.card_instruction else {
                        unreachable!("rejected enrollment retry requires ISSUE_NEW");
                    };
                    insert_idempotency_record(connection, context.new_idempotency_record())?;
                    let enrollment_id = Uuid::new_v4();
                    let mismatch_fields = identity_mismatch_fields(
                        &canonical_first_name,
                        &canonical_last_name,
                        &enrollment.first_name,
                        &enrollment.last_name,
                    );
                    let mismatch_json = serde_json::to_string(&mismatch_fields).map_err(|error| {
                        DbError::Query(format!("failed to serialize identity mismatch fields: {error}"))
                    })?;
                    connection.execute(
                        "UPDATE provider_users SET enrollment_id=:1,supplied_first_name=:2,supplied_last_name=:3,identity_mismatch=:4,mismatch_fields_json=:5,selection_reference=:6,status='CARD_ISSUANCE_PENDING',metadata_json=:7,updated_by_subject=:8,updated_at=SYSTIMESTAMP WHERE provider_user_id=:9 AND status='ISSUANCE_REJECTED'",
                        &[&raw(enrollment_id), &enrollment.first_name, &enrollment.last_name, &i32::from(!mismatch_fields.is_empty()), &mismatch_json, &enrollment.selection_reference, &enrollment.metadata.to_string(), &context.actor.subject, &raw(existing.provider_user_id)],
                    ).map_err(|error| query("failed to reopen rejected provider-user enrollment", error))?;
                    let (issuance_request_id, resolution) = find_or_create_issuance_request(
                        connection,
                        user_id,
                        range.card_range_id,
                        &enrollment,
                        delivery,
                        &context.actor.subject,
                    )?;
                    let enrollment_order = next_enrollment_order(connection, issuance_request_id)?;
                    connection.execute(
                        "INSERT INTO card_issuance_request_providers (card_issuance_request_id,provider_user_id,provider_id,enrollment_id,enrollment_order,status) VALUES (:1,:2,:3,:4,:5,'PENDING')",
                        &[&raw(issuance_request_id), &raw(existing.provider_user_id), &provider_raw, &raw(enrollment_id), &enrollment_order],
                    ).map_err(|error| query("failed to join retried provider enrollment", error))?;
                    let now = Utc::now();
                    let view = ProviderUserView {
                        enrollment_id,
                        provider_user_id: existing.provider_user_id,
                        provider_id,
                        user_id,
                        provider_customer_reference: enrollment.provider_customer_reference.clone(),
                        identity_mismatch: !mismatch_fields.is_empty(),
                        mismatch_fields,
                        status: ProviderUserStatus::CardIssuancePending,
                        card_resolution: resolution,
                        card_id: None,
                        masked_card_number: None,
                        card_range_id: range.card_range_id,
                        provider_user_account_id: None,
                        policy_usage_account_ids: None,
                        issuance_request_id: Some(issuance_request_id),
                        profile_materialization_status: "NOT_STARTED".to_string(),
                        created_at: now,
                        updated_at: now,
                    };
                    insert_enrollment_audit(connection, &context, &view)?;
                    complete_idempotency_record(
                        connection,
                        &context.operation_type,
                        context.idempotency_key.as_str(),
                        "provider_user",
                        existing.provider_user_id,
                        view.replay_snapshot(),
                    )?;
                    return Ok(EnrollProviderUserPersistenceOutcome::IssuancePending(
                        Box::new(view),
                    ));
                }
                if let Some(limit) = max_total_users {
                    let current = connection
                        .query_row_as::<i64>(
                            "SELECT COUNT(*) FROM provider_users WHERE provider_id=:1 AND status IN ('CARD_ISSUANCE_PENDING','PROVISIONING','ACTIVE','SUSPENDED','RECOVERY_REQUIRED')",
                            &[&provider_raw],
                        )
                        .map_err(|error| query("failed to count provider users", error))?;
                    if u64::try_from(current).unwrap_or(u64::MAX) >= limit {
                        return Ok(EnrollProviderUserPersistenceOutcome::UserLimitReached);
                    }
                }

                let Some(range) = resolve_provider_range(connection, provider_id)? else {
                    return Ok(EnrollProviderUserPersistenceOutcome::RangeNotReady);
                };
                let provider_user_id = Uuid::new_v4();
                let enrollment_id = Uuid::new_v4();
                let mismatch_fields = identity_mismatch_fields(
                    &canonical_first_name,
                    &canonical_last_name,
                    &enrollment.first_name,
                    &enrollment.last_name,
                );
                let identity_mismatch = !mismatch_fields.is_empty();
                let mismatch_json = serde_json::to_string(&mismatch_fields)
                    .map_err(|error| DbError::Query(format!("failed to serialize identity mismatch fields: {error}")))?;
                let metadata = enrollment.metadata.to_string();

                match &enrollment.card_instruction {
                    CardInstruction::IssueNew(delivery) => {
                        if active_card_for_user_range(connection, user_id, range.card_range_id)?.is_some() {
                            return Ok(EnrollProviderUserPersistenceOutcome::ActiveCardSelectionRequired);
                        }
                        insert_idempotency_record(connection, context.new_idempotency_record())?;
                        insert_provider_user(
                            connection,
                            provider_user_id,
                            enrollment_id,
                            provider_id,
                            user_id,
                            &enrollment,
                            identity_mismatch,
                            &mismatch_json,
                            &metadata,
                            "CARD_ISSUANCE_PENDING",
                            &context.actor.subject,
                        )?;
                        let (issuance_request_id, resolution) = find_or_create_issuance_request(
                            connection,
                            user_id,
                            range.card_range_id,
                            &enrollment,
                            delivery,
                            &context.actor.subject,
                        )?;
                        let enrollment_order = next_enrollment_order(connection, issuance_request_id)?;
                        connection.execute(
                            "INSERT INTO card_issuance_request_providers (card_issuance_request_id,provider_user_id,provider_id,enrollment_id,enrollment_order,status) VALUES (:1,:2,:3,:4,:5,'PENDING')",
                            &[&raw(issuance_request_id), &raw(provider_user_id), &provider_raw, &raw(enrollment_id), &enrollment_order],
                        ).map_err(|error| query("failed to join provider to issuance request", error))?;
                        let now = Utc::now();
                        let view = ProviderUserView {
                            enrollment_id,
                            provider_user_id,
                            provider_id,
                            user_id,
                            provider_customer_reference: enrollment.provider_customer_reference.clone(),
                            identity_mismatch,
                            mismatch_fields,
                            status: ProviderUserStatus::CardIssuancePending,
                            card_resolution: resolution,
                            card_id: None,
                            masked_card_number: None,
                            card_range_id: range.card_range_id,
                            provider_user_account_id: None,
                            policy_usage_account_ids: None,
                            issuance_request_id: Some(issuance_request_id),
                            profile_materialization_status: "NOT_APPLICABLE".to_string(),
                            created_at: now,
                            updated_at: now,
                        };
                        insert_enrollment_audit(connection, &context, &view)?;
                        complete_idempotency_record(
                            connection,
                            &operation_type,
                            &key,
                            "provider_user",
                            provider_user_id,
                            view.replay_snapshot(),
                        )?;
                        Ok(EnrollProviderUserPersistenceOutcome::IssuancePending(Box::new(view)))
                    }
                    CardInstruction::UseExisting { card_number } => {
                        let Some(card) = resolve_existing_card(connection, card_number)? else {
                            return Ok(EnrollProviderUserPersistenceOutcome::ExistingCardNotFound);
                        };
                        if card.user_id != user_id {
                            return Ok(EnrollProviderUserPersistenceOutcome::ExistingCardOwnerMismatch);
                        }
                        if card.card_range_id != range.card_range_id {
                            return Ok(EnrollProviderUserPersistenceOutcome::ExistingCardRangeMismatch);
                        }
                        if card.status != "ACTIVE" {
                            return Ok(EnrollProviderUserPersistenceOutcome::ExistingCardNotActive);
                        }
                        if range.funding_mode == "MULTI_PROVIDER" && !allow_multi_provider_attachment {
                            return Ok(EnrollProviderUserPersistenceOutcome::MultiProviderAttachmentDisabled);
                        }
                        insert_idempotency_record(connection, context.new_idempotency_record())?;
                        insert_provider_user(
                            connection,
                            provider_user_id,
                            enrollment_id,
                            provider_id,
                            user_id,
                            &enrollment,
                            identity_mismatch,
                            &mismatch_json,
                            &metadata,
                            "PROVISIONING",
                            &context.actor.subject,
                        )?;
                        let account_id = deterministic_provider_user_account_id(provider_id, user_id);
                        connection.execute(
                            "INSERT INTO provider_user_accounts (provider_user_account_id,provider_user_id,provider_id,user_id,tigerbeetle_account_id,status) VALUES (:1,:2,:3,:4,:5,'PROVISIONING')",
                            &[&raw(account_id), &raw(provider_user_id), &provider_raw, &raw(user_id), &raw(account_id)],
                        ).map_err(|error| query("failed to stage provider-user account", error))?;
                        let priority = next_card_priority(connection, card.card_id)?;
                        connection.execute(
                            "INSERT INTO card_provider_funding_sources (card_funding_source_id,card_id,provider_id,provider_user_id,provider_user_account_id,priority,status,created_by_subject,updated_by_subject) VALUES (:1,:2,:3,:4,:5,:6,'PROVISIONING',:7,:7)",
                            &[&raw(Uuid::new_v4()), &raw(card.card_id), &provider_raw, &raw(provider_user_id), &raw(account_id), &priority, &context.actor.subject],
                        ).map_err(|error| query("failed to stage card funding source", error))?;
                        let usage = PolicyUsageAccountIds::for_card(card.card_id);
                        ensure_usage_account_mapping(connection, card.card_id, &usage)?;
                        let operation_id = Uuid::new_v4();
                        let wal_request = serde_json::json!({
                            "provider_user_id": provider_user_id,
                            "provider_id": provider_id,
                            "user_id": user_id,
                            "card_id": card.card_id,
                            "card_range_id": range.card_range_id,
                            "provider_user_account_id": account_id,
                            "policy_usage_account_ids": usage,
                        }).to_string();
                        connection.execute(
                            "INSERT INTO operation_wal (operation_id,operation_type,aggregate_type,aggregate_id,status,deterministic_external_id,request_json) VALUES (:1,'PROVIDER_USER_ACCOUNT_PROVISION','PROVIDER_USER',:2,'PENDING',:3,:4)",
                            &[&raw(operation_id), &raw(provider_user_id), &raw(account_id), &wal_request],
                        ).map_err(|error| query("failed to create provider-user provisioning WAL", error))?;
                        connection.execute(
                            "UPDATE idempotency_records SET resource_type='provider_user',resource_id=:1,updated_at=SYSTIMESTAMP WHERE operation_type=:2 AND idempotency_key=:3 AND status='IN_PROGRESS'",
                            &[&raw(provider_user_id), &context.operation_type, &context.idempotency_key.as_str()],
                        ).map_err(|error| query("failed to bind provider-user recovery intent", error))?;
                        Ok(EnrollProviderUserPersistenceOutcome::ExistingCardPrepared(
                            Box::new(ExistingCardProvisioningIntent {
                                operation_id,
                                enrollment_id,
                                provider_user_id,
                                provider_id,
                                user_id,
                                card_id: card.card_id,
                                card_range_id: range.card_range_id,
                                provider_user_account_id: account_id,
                                usage_account_ids: usage,
                            }),
                        ))
                    }
                }
            })
            .await
    }

    #[tracing::instrument(skip(self, context), fields(db.system="oracle", db.operation.name="provider_users.finalize", provider_user_id=%intent.provider_user_id, operation_id=%intent.operation_id))]
    pub async fn finalize_existing_card_enrollment_atomic(
        &self,
        context: MutationCommandContext,
        intent: ExistingCardProvisioningIntent,
    ) -> DbResult<ProviderUserView> {
        let event_headers = InternalEventHeaders::from_current_span(
            context.request.correlation_id.clone(),
            context.request.request_id.to_string(),
        );
        self.pool.with_transaction("finalize existing-card enrollment", move |connection| {
            let provider_user_raw = raw(intent.provider_user_id);
            let status = connection.query_row_as::<String>(
                "SELECT status FROM provider_users WHERE provider_user_id=:1 FOR UPDATE",
                &[&provider_user_raw],
            ).map_err(|error| query("failed to lock staged provider user", error))?;
            if !matches!(status.as_str(), "PROVISIONING" | "RECOVERY_REQUIRED") {
                return Err(DbError::Conflict("provider-user provisioning state changed".to_string()));
            }
            connection.execute(
                "UPDATE provider_users SET status='ACTIVE',updated_at=SYSTIMESTAMP WHERE provider_user_id=:1 AND status IN ('PROVISIONING','RECOVERY_REQUIRED')",
                &[&provider_user_raw],
            ).map_err(|error| query("failed to activate provider user", error))?;
            connection.execute(
                "UPDATE provider_user_accounts SET status='ACTIVE',updated_at=SYSTIMESTAMP WHERE provider_user_id=:1 AND status IN ('PROVISIONING','RECOVERY_REQUIRED')",
                &[&provider_user_raw],
            ).map_err(|error| query("failed to activate provider-user account", error))?;
            connection.execute(
                "UPDATE card_provider_funding_sources SET status='ACTIVE',updated_at=SYSTIMESTAMP WHERE provider_user_id=:1 AND status IN ('PROVISIONING','RECOVERY_REQUIRED')",
                &[&provider_user_raw],
            ).map_err(|error| query("failed to activate card funding source", error))?;
            connection.execute(
                "UPDATE card_policy_usage_accounts SET status='ACTIVE',updated_at=SYSTIMESTAMP WHERE card_id=:1 AND status='PROVISIONING'",
                &[&raw(intent.card_id)],
            ).map_err(|error| query("failed to activate policy usage accounts", error))?;
            connection.execute(
                "UPDATE operation_wal SET status='COMPLETED',response_json=:1,error_json=NULL,updated_at=SYSTIMESTAMP,completed_at=SYSTIMESTAMP WHERE operation_id=:2 AND status IN ('PENDING','EXTERNAL_IN_FLIGHT','EXTERNAL_VERIFIED','FAILED')",
                &[&serde_json::json!({"verified": true}).to_string(), &raw(intent.operation_id)],
            ).map_err(|error| query("failed to complete provider-user WAL", error))?;
            let operation_id = Uuid::new_v4();
            insert_card_projection_outbox(
                connection,
                intent.card_id,
                operation_id,
                &event_headers,
            )?;
            let view = fetch_provider_user_view(connection, intent.provider_user_id, intent.enrollment_id, CardResolution::ExistingCardAttached, "PENDING")?;
            insert_enrollment_audit(connection, &context, &view)?;
            complete_idempotency_record(
                connection,
                &context.operation_type,
                context.idempotency_key.as_str(),
                "provider_user",
                intent.provider_user_id,
                view.replay_snapshot(),
            )?;
            Ok(view)
        }).await
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="provider_users.mark_recovery", provider_user_id=%provider_user_id, operation_id=%operation_id))]
    pub async fn mark_provider_user_provisioning_failed(
        &self,
        provider_user_id: Uuid,
        operation_id: Uuid,
        safe_error_code: &'static str,
    ) -> DbResult<()> {
        self.pool.with_transaction("mark provider-user provisioning recovery", move |connection| {
            connection.execute("UPDATE provider_users SET status='RECOVERY_REQUIRED',updated_at=SYSTIMESTAMP WHERE provider_user_id=:1 AND status='PROVISIONING'", &[&raw(provider_user_id)]).map_err(|error| query("failed to mark provider user for recovery", error))?;
            connection.execute("UPDATE provider_user_accounts SET status='RECOVERY_REQUIRED',updated_at=SYSTIMESTAMP WHERE provider_user_id=:1 AND status='PROVISIONING'", &[&raw(provider_user_id)]).map_err(|error| query("failed to mark provider-user account for recovery", error))?;
            connection.execute("UPDATE card_provider_funding_sources SET status='RECOVERY_REQUIRED',updated_at=SYSTIMESTAMP WHERE provider_user_id=:1 AND status='PROVISIONING'", &[&raw(provider_user_id)]).map_err(|error| query("failed to mark funding source for recovery", error))?;
            let error_json = serde_json::json!({"code": safe_error_code}).to_string();
            connection.execute("UPDATE operation_wal SET status='FAILED',error_json=:1,attempt_count=attempt_count+1,updated_at=SYSTIMESTAMP WHERE operation_id=:2 AND status<>'COMPLETED'", &[&error_json, &raw(operation_id)]).map_err(|error| query("failed to mark provider-user WAL failed", error))?;
            Ok(())
        }).await
    }
}

#[derive(Debug)]
struct ProviderRange {
    card_range_id: Uuid,
    funding_mode: String,
}

#[derive(Debug)]
struct ExistingCard {
    card_id: Uuid,
    user_id: Uuid,
    card_range_id: Uuid,
    status: String,
}

fn lock_national_identity(connection: &oracle::Connection, national_id: &str) -> DbResult<()> {
    let hash = format!("{:x}", Sha256::digest(national_id.as_bytes()));
    connection.execute(
        "MERGE INTO user_identity_allocation_locks target USING (SELECT :1 national_id_hash FROM dual) source ON (target.national_id_hash=source.national_id_hash) WHEN NOT MATCHED THEN INSERT (national_id_hash) VALUES (source.national_id_hash)",
        &[&hash],
    ).map_err(|error| query("failed to allocate national identity lock", error))?;
    connection.query_row_as::<String>(
        "SELECT national_id_hash FROM user_identity_allocation_locks WHERE national_id_hash=:1 FOR UPDATE",
        &[&hash],
    ).map_err(|error| query("failed to lock national identity", error))?;
    Ok(())
}

fn find_or_create_user(
    connection: &oracle::Connection,
    context: &MutationCommandContext,
    enrollment: &NewProviderUserEnrollment,
) -> DbResult<(Uuid, String, String)> {
    match connection.query_row(
        "SELECT user_id,first_name,last_name FROM users WHERE national_id=:1 FOR UPDATE",
        &[&enrollment.national_id],
    ) {
        Ok(row) => {
            let id: Vec<u8> = row.get(0).map_err(read)?;
            Ok((
                raw16_to_uuid(&id)?,
                row.get(1).map_err(read)?,
                row.get(2).map_err(read)?,
            ))
        }
        Err(error) if error.kind() == oracle::ErrorKind::NoDataFound => {
            let user_id = Uuid::new_v4();
            let metadata = serde_json::json!({}).to_string();
            connection.execute(
                "INSERT INTO users (user_id,national_id,first_name,last_name,metadata_json,status,created_by_subject,updated_by_subject) VALUES (:1,:2,:3,:4,:5,'ACTIVE',:6,:6)",
                &[&raw(user_id), &enrollment.national_id, &enrollment.first_name, &enrollment.last_name, &metadata, &context.actor.subject],
            ).map_err(|error| query("failed to create global user", error))?;
            Ok((
                user_id,
                enrollment.first_name.clone(),
                enrollment.last_name.clone(),
            ))
        }
        Err(error) => Err(query("failed to resolve global user", error)),
    }
}

struct ExistingProviderUserRelationship {
    provider_user_id: Uuid,
    provider_customer_reference: String,
    status: String,
}

fn find_relationship(
    connection: &oracle::Connection,
    provider_id: Uuid,
    user_id: Uuid,
) -> DbResult<Option<ExistingProviderUserRelationship>> {
    let mut rows = connection.query(
        "SELECT provider_user_id,provider_customer_reference,status FROM provider_users WHERE provider_id=:1 AND user_id=:2 FOR UPDATE",
        &[&raw(provider_id), &raw(user_id)],
    ).map_err(|error| query("failed to check provider-user relationship", error))?;
    match rows.next() {
        Some(Ok(row)) => Ok(Some(ExistingProviderUserRelationship {
            provider_user_id: row_uuid(&row, 0)?,
            provider_customer_reference: row.get(1).map_err(read)?,
            status: row.get(2).map_err(read)?,
        })),
        Some(Err(error)) => Err(query("failed to read provider-user relationship", error)),
        None => Ok(None),
    }
}

fn resolve_provider_range(
    connection: &oracle::Connection,
    provider_id: Uuid,
) -> DbResult<Option<ProviderRange>> {
    match connection.query_row(
        "SELECT cr.card_range_id,cr.funding_mode FROM card_range_providers crp JOIN card_ranges cr ON cr.card_range_id=crp.card_range_id WHERE crp.provider_id=:1 AND crp.status='ACTIVE' AND cr.status='ACTIVE' AND cr.issuance_enabled=1 FOR UPDATE OF crp.status",
        &[&raw(provider_id)],
    ) {
        Ok(row) => {
            let id: Vec<u8> = row.get(0).map_err(read)?;
            Ok(Some(ProviderRange { card_range_id: raw16_to_uuid(&id)?, funding_mode: row.get(1).map_err(read)? }))
        }
        Err(error) if error.kind() == oracle::ErrorKind::NoDataFound => Ok(None),
        Err(error) => Err(query("failed to resolve provider card range", error)),
    }
}

fn active_card_for_user_range(
    connection: &oracle::Connection,
    user_id: Uuid,
    range_id: Uuid,
) -> DbResult<Option<Uuid>> {
    match connection.query_row_as::<Vec<u8>>("SELECT card_id FROM cards WHERE user_id=:1 AND card_range_id=:2 AND status='ACTIVE' FOR UPDATE", &[&raw(user_id), &raw(range_id)]) {
        Ok(value) => raw16_to_uuid(&value).map(Some),
        Err(error) if error.kind() == oracle::ErrorKind::NoDataFound => Ok(None),
        Err(error) => Err(query("failed to resolve active card for range", error)),
    }
}

fn resolve_existing_card(
    connection: &oracle::Connection,
    card_number: &str,
) -> DbResult<Option<ExistingCard>> {
    match connection.query_row(
        "SELECT card_id,user_id,card_range_id,status FROM cards WHERE card_number=:1 FOR UPDATE",
        &[&card_number],
    ) {
        Ok(row) => {
            let card_id: Vec<u8> = row.get(0).map_err(read)?;
            let user_id: Vec<u8> = row.get(1).map_err(read)?;
            let range_id: Vec<u8> = row.get(2).map_err(read)?;
            Ok(Some(ExistingCard {
                card_id: raw16_to_uuid(&card_id)?,
                user_id: raw16_to_uuid(&user_id)?,
                card_range_id: raw16_to_uuid(&range_id)?,
                status: row.get(3).map_err(read)?,
            }))
        }
        Err(error) if error.kind() == oracle::ErrorKind::NoDataFound => Ok(None),
        Err(error) => Err(query("failed to resolve existing card", error)),
    }
}

#[allow(clippy::too_many_arguments)]
fn insert_provider_user(
    connection: &oracle::Connection,
    provider_user_id: Uuid,
    enrollment_id: Uuid,
    provider_id: Uuid,
    user_id: Uuid,
    enrollment: &NewProviderUserEnrollment,
    identity_mismatch: bool,
    mismatch_json: &str,
    metadata: &str,
    status: &str,
    actor: &str,
) -> DbResult<()> {
    connection.execute(
        "INSERT INTO provider_users (provider_user_id,enrollment_id,provider_id,user_id,provider_customer_reference,supplied_first_name,supplied_last_name,identity_mismatch,mismatch_fields_json,selection_reference,status,metadata_json,created_by_subject,updated_by_subject) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:13)",
        &[&raw(provider_user_id), &raw(enrollment_id), &raw(provider_id), &raw(user_id), &enrollment.provider_customer_reference, &enrollment.first_name, &enrollment.last_name, &i32::from(identity_mismatch), &mismatch_json, &enrollment.selection_reference, &status, &metadata, &actor],
    ).map_err(|error| query("failed to create provider-user relationship", error))?;
    Ok(())
}

fn find_or_create_issuance_request(
    connection: &oracle::Connection,
    user_id: Uuid,
    range_id: Uuid,
    enrollment: &NewProviderUserEnrollment,
    delivery: &crate::domain::user_card::NewCardDelivery,
    actor: &str,
) -> DbResult<(Uuid, CardResolution)> {
    match connection.query_row_as::<Vec<u8>>(
        "SELECT card_issuance_request_id FROM card_issuance_requests WHERE user_id=:1 AND card_range_id=:2 AND status IN ('PENDING_EXPORT','EXPORTED','PROCESSING_RESULT','RECOVERY_REQUIRED') FOR UPDATE",
        &[&raw(user_id), &raw(range_id)],
    ) {
        Ok(value) => Ok((raw16_to_uuid(&value)?, CardResolution::JoinedPendingIssuance)),
        Err(error) if error.kind() == oracle::ErrorKind::NoDataFound => {
            let request_id = Uuid::new_v4();
            let identity = serde_json::json!({
                "national_id": enrollment.national_id,
                "first_name": enrollment.first_name,
                "last_name": enrollment.last_name,
                "birth_date": delivery.birth_date,
            }).to_string();
            let delivery_json = serde_json::json!({
                "mobile": delivery.mobile,
                "delivery_province": delivery.delivery_province,
                "delivery_city": delivery.delivery_city,
                "delivery_address": delivery.delivery_address,
                "postal_code": delivery.postal_code,
            }).to_string();
            connection.execute(
                "INSERT INTO card_issuance_requests (card_issuance_request_id,user_id,card_range_id,status,identity_snapshot_json,delivery_snapshot_json,created_by_subject,updated_by_subject) VALUES (:1,:2,:3,'PENDING_EXPORT',:4,:5,:6,:6)",
                &[&raw(request_id), &raw(user_id), &raw(range_id), &identity, &delivery_json, &actor],
            ).map_err(|error| query("failed to create card issuance request", error))?;
            Ok((request_id, CardResolution::IssuanceRequested))
        }
        Err(error) => Err(query("failed to resolve open issuance request", error)),
    }
}

fn next_enrollment_order(connection: &oracle::Connection, request_id: Uuid) -> DbResult<i64> {
    connection.query_row_as::<i64>("SELECT NVL(MAX(enrollment_order),0)+1 FROM card_issuance_request_providers WHERE card_issuance_request_id=:1", &[&raw(request_id)])
        .map_err(|error| query("failed to allocate issuance enrollment order", error))
}

fn next_card_priority(connection: &oracle::Connection, card_id: Uuid) -> DbResult<i64> {
    connection.query_row_as::<i64>("SELECT NVL(MAX(priority),0)+1 FROM card_provider_funding_sources WHERE card_id=:1 AND status IN ('PROVISIONING','ACTIVE')", &[&raw(card_id)])
        .map_err(|error| query("failed to allocate card funding priority", error))
}

pub(crate) fn ensure_usage_account_mapping(
    connection: &oracle::Connection,
    card_id: Uuid,
    ids: &PolicyUsageAccountIds,
) -> DbResult<()> {
    connection.execute(
        "MERGE INTO card_policy_usage_accounts target USING (SELECT :1 card_id FROM dual) source ON (target.card_id=source.card_id) WHEN NOT MATCHED THEN INSERT (card_id,amount_daily_account_id,amount_weekly_account_id,amount_monthly_account_id,amount_yearly_account_id,count_daily_account_id,count_weekly_account_id,count_monthly_account_id,count_yearly_account_id,status) VALUES (source.card_id,:2,:3,:4,:5,:6,:7,:8,:9,'PROVISIONING')",
        &[&raw(card_id), &raw(ids.amount_daily), &raw(ids.amount_weekly), &raw(ids.amount_monthly), &raw(ids.amount_yearly), &raw(ids.count_daily), &raw(ids.count_weekly), &raw(ids.count_monthly), &raw(ids.count_yearly)],
    ).map_err(|error| query("failed to stage policy usage accounts", error))?;
    Ok(())
}

fn fetch_provider_user_view(
    connection: &oracle::Connection,
    provider_user_id: Uuid,
    enrollment_id: Uuid,
    resolution: CardResolution,
    materialization: &str,
) -> DbResult<ProviderUserView> {
    let row = connection.query_row(
        "SELECT pu.provider_id,pu.user_id,pu.provider_customer_reference,pu.identity_mismatch,JSON_SERIALIZE(pu.mismatch_fields_json RETURNING CLOB),pu.status,c.card_id,c.card_number,c.card_range_id,pua.provider_user_account_id,TO_CHAR(SYS_EXTRACT_UTC(pu.created_at),'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'),TO_CHAR(SYS_EXTRACT_UTC(pu.updated_at),'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') FROM provider_users pu JOIN card_provider_funding_sources fs ON fs.provider_user_id=pu.provider_user_id JOIN cards c ON c.card_id=fs.card_id JOIN provider_user_accounts pua ON pua.provider_user_id=pu.provider_user_id WHERE pu.provider_user_id=:1",
        &[&raw(provider_user_id)],
    ).map_err(|error| query("failed to read provider-user response", error))?;
    let provider: Vec<u8> = row.get(0).map_err(read)?;
    let user: Vec<u8> = row.get(1).map_err(read)?;
    let mismatch_json: String = row.get(4).map_err(read)?;
    let status: String = row.get(5).map_err(read)?;
    let card: Vec<u8> = row.get(6).map_err(read)?;
    let card_number: String = row.get(7).map_err(read)?;
    let range: Vec<u8> = row.get(8).map_err(read)?;
    let account: Vec<u8> = row.get(9).map_err(read)?;
    let created: String = row.get(10).map_err(read)?;
    let updated: String = row.get(11).map_err(read)?;
    let card_id = raw16_to_uuid(&card)?;
    Ok(ProviderUserView {
        enrollment_id,
        provider_user_id,
        provider_id: raw16_to_uuid(&provider)?,
        user_id: raw16_to_uuid(&user)?,
        provider_customer_reference: row.get(2).map_err(read)?,
        identity_mismatch: row.get::<_, i32>(3).map_err(read)? == 1,
        mismatch_fields: serde_json::from_str(&mismatch_json)
            .map_err(|error| DbError::Query(format!("invalid mismatch JSON: {error}")))?,
        status: ProviderUserStatus::from_db_value(&status)
            .ok_or_else(|| DbError::Query("unknown provider-user status".to_string()))?,
        card_resolution: resolution,
        card_id: Some(card_id),
        masked_card_number: Some(mask_card_number(&card_number)),
        card_range_id: raw16_to_uuid(&range)?,
        provider_user_account_id: Some(raw16_to_uuid(&account)?),
        policy_usage_account_ids: Some(PolicyUsageAccountIds::for_card(card_id)),
        issuance_request_id: None,
        profile_materialization_status: materialization.to_string(),
        created_at: parse_time(&created)?,
        updated_at: parse_time(&updated)?,
    })
}

pub(crate) fn insert_card_projection_outbox(
    connection: &oracle::Connection,
    card_id: Uuid,
    operation_id: Uuid,
    headers: &InternalEventHeaders,
) -> DbResult<()> {
    let event_id = Uuid::new_v4();
    let (partition_key, projection) = load_card_profile_projection(connection, card_id)?;
    let envelope = InternalEventEnvelope::new(
        event_id,
        "CARD_PROFILE_PUBLISH_REQUESTED",
        "CARD",
        card_id,
        operation_id,
        projection,
    );
    let payload = serde_json::to_string(&envelope)
        .map_err(|error| DbError::Query(format!("failed to serialize card projection: {error}")))?;
    let headers = serde_json::to_string(headers)
        .map_err(|error| DbError::Query(format!("failed to serialize event headers: {error}")))?;
    connection.execute(
        "INSERT INTO integration_outbox (outbox_event_id,operation_id,event_type,aggregate_type,aggregate_id,partition_key,payload_json,headers_json) VALUES (:1,:2,'CARD_PROFILE_PUBLISH_REQUESTED','CARD',:3,:4,:5,:6)",
        &[&raw(event_id), &raw(operation_id), &raw(card_id), &partition_key, &payload, &headers],
    ).map_err(|error| query("failed to enqueue card-profile publication", error))?;
    connection
        .execute(
            "UPDATE cards SET publication_operation_id=:1,updated_at=SYSTIMESTAMP WHERE card_id=:2",
            &[&raw(operation_id), &raw(card_id)],
        )
        .map_err(|error| query("failed to record card-profile publication", error))?;
    Ok(())
}

fn load_card_profile_projection(
    connection: &oracle::Connection,
    card_id: Uuid,
) -> DbResult<(String, CardProfileProjection)> {
    let card = connection
        .query_row(
            "SELECT c.card_number,c.user_id,c.card_range_id,r.funding_mode,c.state_version,u.amount_daily_account_id,u.amount_weekly_account_id,u.amount_monthly_account_id,u.amount_yearly_account_id,u.count_daily_account_id,u.count_weekly_account_id,u.count_monthly_account_id,u.count_yearly_account_id FROM cards c JOIN card_ranges r ON r.card_range_id=c.card_range_id JOIN card_policy_usage_accounts u ON u.card_id=c.card_id WHERE c.card_id=:1 AND c.status='ACTIVE' AND u.status='ACTIVE'",
            &[&raw(card_id)],
        )
        .map_err(|error| query("failed to load card projection base", error))?;
    let user_raw: Vec<u8> = card.get(1).map_err(read)?;
    let range_raw: Vec<u8> = card.get(2).map_err(read)?;
    let usage = PolicyUsageAccountIds {
        amount_daily: row_uuid(&card, 5)?,
        amount_weekly: row_uuid(&card, 6)?,
        amount_monthly: row_uuid(&card, 7)?,
        amount_yearly: row_uuid(&card, 8)?,
        count_daily: row_uuid(&card, 9)?,
        count_weekly: row_uuid(&card, 10)?,
        count_monthly: row_uuid(&card, 11)?,
        count_yearly: row_uuid(&card, 12)?,
    };
    let rows = connection.query(
        "SELECT fs.provider_id,fs.priority,fs.max_amount_rials,fs.provider_user_account_id,fee.tigerbeetle_account_id,cms.tigerbeetle_account_id,platform.tigerbeetle_account_id FROM card_provider_funding_sources fs JOIN provider_ledger_accounts fee ON fee.provider_id=fs.provider_id AND fee.account_category='PROVIDER_FEE' AND fee.status='ACTIVE' JOIN provider_ledger_accounts cms ON cms.provider_id=fs.provider_id AND cms.account_category='CMS_SETTLEMENT' AND cms.status='ACTIVE' JOIN provider_ledger_accounts platform ON platform.provider_id=fs.provider_id AND platform.account_category='PLATFORM_FEE' AND platform.status='ACTIVE' WHERE fs.card_id=:1 AND fs.status='ACTIVE' ORDER BY fs.priority",
        &[&raw(card_id)],
    ).map_err(|error| query("failed to load card funding projection", error))?;
    let mut funding_sources = Vec::new();
    for row in rows {
        let row = row.map_err(|error| query("failed to read card funding projection", error))?;
        let priority: i64 = row.get(1).map_err(read)?;
        let max_amount: Option<i64> = row.get(2).map_err(read)?;
        funding_sources.push(CardProfileFundingSource {
            provider_id: row_uuid(&row, 0)?,
            priority: u16::try_from(priority)
                .map_err(|_| DbError::Query("invalid card funding priority".to_string()))?,
            max_amount_rials: max_amount
                .map(u64::try_from)
                .transpose()
                .map_err(|_| DbError::Query("invalid card funding cap".to_string()))?,
            ledger_accounts: CardProfileLedgerAccounts {
                user_provider_account: row_uuid(&row, 3)?,
                provider_fee_account: row_uuid(&row, 4)?,
                cms_settlement_account: row_uuid(&row, 5)?,
                platform_fee_account: row_uuid(&row, 6)?,
            },
        });
    }
    if funding_sources.is_empty() {
        return Err(DbError::Conflict(
            "active card has no active funding source".to_string(),
        ));
    }
    Ok((
        card.get(0).map_err(read)?,
        CardProfileProjection {
            card_id,
            user_id: raw16_to_uuid(&user_raw)?,
            card_range_id: raw16_to_uuid(&range_raw)?,
            funding_mode: card.get(3).map_err(read)?,
            state_version: card.get(4).map_err(read)?,
            policy_usage_accounts: usage,
            funding_sources,
        },
    ))
}

fn row_uuid(row: &oracle::Row, index: usize) -> DbResult<Uuid> {
    let value: Vec<u8> = row.get(index).map_err(read)?;
    raw16_to_uuid(&value)
}

fn insert_enrollment_audit(
    connection: &oracle::Connection,
    context: &MutationCommandContext,
    view: &ProviderUserView,
) -> DbResult<()> {
    insert_audit_log(
        connection,
        NewAuditLog {
            audit_log_id: Uuid::new_v4(),
            entity_type: "PROVIDER_USER".to_string(),
            entity_id: view.provider_user_id,
            action_type: AuditAction::Insert,
            reason: Some("Provider asserted customer enrollment and card selection".to_string()),
            old_values: None,
            new_values: Some(view.replay_snapshot()),
            context: context.audit_context(),
        },
    )
}

fn identity_mismatch_fields(
    canonical_first: &str,
    canonical_last: &str,
    supplied_first: &str,
    supplied_last: &str,
) -> Vec<String> {
    let mut fields = Vec::new();
    if canonical_first != supplied_first {
        fields.push("first_name".to_string());
    }
    if canonical_last != supplied_last {
        fields.push("last_name".to_string());
    }
    fields
}

fn classify_idempotency(
    connection: &oracle::Connection,
    record: &crate::domain::idempotency::IdempotencyRecord,
    request_hash: &str,
) -> DbResult<EnrollProviderUserPersistenceOutcome> {
    if record.request_hash != request_hash {
        return Ok(EnrollProviderUserPersistenceOutcome::IdempotencyConflict);
    }
    Ok(match record.status {
        IdempotencyStatus::Completed => record
            .response_snapshot
            .clone()
            .map(EnrollProviderUserPersistenceOutcome::Replayed)
            .unwrap_or(EnrollProviderUserPersistenceOutcome::IdempotencyInvalidState),
        IdempotencyStatus::InProgress => match record.resource_id {
            Some(provider_user_id) => resume_provider_user_intent(connection, provider_user_id)?
                .map(Box::new)
                .map(EnrollProviderUserPersistenceOutcome::ExistingCardPrepared)
                .unwrap_or(EnrollProviderUserPersistenceOutcome::IdempotencyInProgress),
            None => EnrollProviderUserPersistenceOutcome::IdempotencyInProgress,
        },
        _ => EnrollProviderUserPersistenceOutcome::IdempotencyInvalidState,
    })
}

fn resume_provider_user_intent(
    connection: &oracle::Connection,
    provider_user_id: Uuid,
) -> DbResult<Option<ExistingCardProvisioningIntent>> {
    let mut rows = connection.query(
        "SELECT w.operation_id,pu.enrollment_id,pu.provider_id,pu.user_id,fs.card_id,c.card_range_id,pua.provider_user_account_id,u.amount_daily_account_id,u.amount_weekly_account_id,u.amount_monthly_account_id,u.amount_yearly_account_id,u.count_daily_account_id,u.count_weekly_account_id,u.count_monthly_account_id,u.count_yearly_account_id FROM provider_users pu JOIN provider_user_accounts pua ON pua.provider_user_id=pu.provider_user_id JOIN card_provider_funding_sources fs ON fs.provider_user_id=pu.provider_user_id JOIN cards c ON c.card_id=fs.card_id JOIN card_policy_usage_accounts u ON u.card_id=c.card_id JOIN operation_wal w ON w.aggregate_id=pu.provider_user_id AND w.operation_type='PROVIDER_USER_ACCOUNT_PROVISION' WHERE pu.provider_user_id=:1 AND pu.status IN ('PROVISIONING','RECOVERY_REQUIRED') AND w.status IN ('PENDING','EXTERNAL_IN_FLIGHT','EXTERNAL_VERIFIED','FAILED') FOR UPDATE",
        &[&raw(provider_user_id)],
    ).map_err(|error| query("failed to load provider-user recovery intent", error))?;
    let Some(row) = rows.next() else {
        return Ok(None);
    };
    let row = row.map_err(|error| query("failed to read provider-user recovery intent", error))?;
    Ok(Some(ExistingCardProvisioningIntent {
        operation_id: row_uuid(&row, 0)?,
        enrollment_id: row_uuid(&row, 1)?,
        provider_user_id,
        provider_id: row_uuid(&row, 2)?,
        user_id: row_uuid(&row, 3)?,
        card_id: row_uuid(&row, 4)?,
        card_range_id: row_uuid(&row, 5)?,
        provider_user_account_id: row_uuid(&row, 6)?,
        usage_account_ids: PolicyUsageAccountIds {
            amount_daily: row_uuid(&row, 7)?,
            amount_weekly: row_uuid(&row, 8)?,
            amount_monthly: row_uuid(&row, 9)?,
            amount_yearly: row_uuid(&row, 10)?,
            count_daily: row_uuid(&row, 11)?,
            count_weekly: row_uuid(&row, 12)?,
            count_monthly: row_uuid(&row, 13)?,
            count_yearly: row_uuid(&row, 14)?,
        },
    }))
}

fn raw(value: Uuid) -> Vec<u8> {
    uuid_to_raw16(value).to_vec()
}
fn query(context: &str, error: oracle::Error) -> DbError {
    DbError::Query(format!("{context}: {error}"))
}
fn read(error: oracle::Error) -> DbError {
    DbError::Query(format!("failed to read Oracle provider-user row: {error}"))
}
fn parse_time(value: &str) -> DbResult<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)
        .map_err(|error| DbError::Query(format!("invalid Oracle timestamp: {error}")))?
        .with_timezone(&Utc))
}

fn map_provider_user_record(row: &oracle::Row) -> DbResult<ProviderUserRecord> {
    let provider_user: Vec<u8> = row.get(0).map_err(read)?;
    let enrollment: Vec<u8> = row.get(1).map_err(read)?;
    let provider: Vec<u8> = row.get(2).map_err(read)?;
    let user: Vec<u8> = row.get(3).map_err(read)?;
    let mismatch: String = row.get(9).map_err(read)?;
    let status: String = row.get(10).map_err(read)?;
    let card_id: Option<Vec<u8>> = row.get(11).map_err(read)?;
    let card_number: Option<String> = row.get(12).map_err(read)?;
    let range_id: Option<Vec<u8>> = row.get(13).map_err(read)?;
    let account_id: Option<Vec<u8>> = row.get(14).map_err(read)?;
    let created: String = row.get(15).map_err(read)?;
    let updated: String = row.get(16).map_err(read)?;
    Ok(ProviderUserRecord {
        provider_user_id: raw16_to_uuid(&provider_user)?,
        enrollment_id: raw16_to_uuid(&enrollment)?,
        provider_id: raw16_to_uuid(&provider)?,
        user_id: raw16_to_uuid(&user)?,
        national_id: row.get(4).map_err(read)?,
        first_name: row.get(5).map_err(read)?,
        last_name: row.get(6).map_err(read)?,
        provider_customer_reference: row.get(7).map_err(read)?,
        identity_mismatch: row.get::<_, i32>(8).map_err(read)? == 1,
        mismatch_fields: serde_json::from_str(&mismatch).map_err(|error| {
            DbError::Query(format!("invalid provider-user mismatch JSON: {error}"))
        })?,
        status: ProviderUserStatus::from_db_value(&status)
            .ok_or_else(|| DbError::Query("unknown provider-user status".to_string()))?,
        card_id: card_id.as_deref().map(raw16_to_uuid).transpose()?,
        masked_card_number: card_number.as_deref().map(mask_card_number),
        card_range_id: range_id.as_deref().map(raw16_to_uuid).transpose()?,
        provider_user_account_id: account_id.as_deref().map(raw16_to_uuid).transpose()?,
        created_at: parse_time(&created)?,
        updated_at: parse_time(&updated)?,
    })
}
