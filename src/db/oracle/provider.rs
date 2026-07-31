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
            types::{raw16_to_uuid, uuid_to_raw16},
        },
    },
    domain::{
        audit::{AuditAction, NewAuditLog, TrustedAuditContext},
        idempotency::IdempotencyStatus,
        provider::{
            NewProvider, Provider, ProviderAccountCategory, ProviderKafkaProvisioningStatus,
            ProviderListCursor, ProviderListItem, ProviderListPage, ProviderListQuery,
            ProviderStatus,
        },
    },
};

#[derive(Debug, Clone, PartialEq)]
pub enum CreateProviderPersistenceOutcome {
    Created(Box<Provider>),
    Replayed(serde_json::Value),
    IdempotencyConflict,
    IdempotencyInProgress,
    IdempotencyInvalidState,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProviderLedgerAccountMapping {
    pub category: ProviderAccountCategory,
    pub tigerbeetle_account_id: Uuid,
    pub status: String,
}

impl OracleRepository {
    #[tracing::instrument(skip(self, context, provider), fields(db.system="oracle", db.operation.name="providers.create"))]
    pub async fn create_provider_atomic(
        &self,
        context: MutationCommandContext,
        provider: NewProvider,
    ) -> DbResult<CreateProviderPersistenceOutcome> {
        let operation_type = context.operation_type.clone();
        let idempotency_key = context.idempotency_key.as_str().to_string();
        let request_hash = context.request_hash.clone();

        self.pool
            .with_transaction("atomic provider creation", move |connection| {
                if let Some(existing) =
                    fetch_idempotency_record(connection, &operation_type, &idempotency_key)?
                {
                    return classify_idempotency(&existing, &request_hash);
                }

                insert_idempotency_record(connection, context.new_idempotency_record())?;
                insert_provider(connection, &provider, &context.actor.subject)?;
                insert_contacts(connection, &provider, &context.actor.subject)?;
                insert_operational_profile(connection, &provider, &context.actor.subject)?;
                insert_ledger_mappings(connection, provider.provider_id)?;
                insert_kafka_access(connection, &provider)?;
                insert_provider_event_subscriptions(
                    connection,
                    provider.provider_id,
                    &context.actor.subject,
                )?;
                insert_provisioning_jobs(
                    connection,
                    provider.provider_id,
                    provider.kafka_access.is_some(),
                )?;

                let created = fetch_provider(connection, provider.provider_id)?;
                let snapshot = created.replay_snapshot();
                insert_audit_log(
                    connection,
                    NewAuditLog {
                        audit_log_id: Uuid::new_v4(),
                        entity_type: "PROVIDER".to_string(),
                        entity_id: provider.provider_id,
                        action_type: AuditAction::Insert,
                        reason: Some(
                            "provider created and core provisioning requested".to_string(),
                        ),
                        old_values: None,
                        new_values: Some(snapshot.clone()),
                        context: context.audit_context(),
                    },
                )?;
                complete_idempotency_record(
                    connection,
                    &operation_type,
                    &idempotency_key,
                    "provider",
                    provider.provider_id,
                    snapshot,
                )?;

                Ok(CreateProviderPersistenceOutcome::Created(Box::new(created)))
            })
            .await
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="providers.get", provider_id=%provider_id))]
    pub async fn get_provider(&self, provider_id: Uuid) -> DbResult<Option<Provider>> {
        self.pool
            .with_connection(move |connection| {
                let mut rows = connection
                    .query(
                        provider_select_sql(),
                        &[&uuid_to_raw16(provider_id).to_vec()],
                    )
                    .map_err(|error| DbError::Query(format!("failed to get provider: {error}")))?;
                match rows.next() {
                    Some(Ok(row)) => {
                        let kafka_status = provider_kafka_status(connection, provider_id)?;
                        map_provider_row(&row, kafka_status).map(Some)
                    }
                    Some(Err(error)) => Err(DbError::Query(format!(
                        "failed to read provider row: {error}"
                    ))),
                    None => Ok(None),
                }
            })
            .await
    }

    #[tracing::instrument(skip(self, query), fields(db.system="oracle", db.operation.name="providers.list"))]
    pub async fn list_providers(&self, query: ProviderListQuery) -> DbResult<ProviderListPage> {
        self.pool
            .with_connection(move |connection| {
                let status = query.status.map(|value| value.as_db_value().to_string());
                let cursor_created_at = query
                    .cursor
                    .as_ref()
                    .map(|cursor| format_oracle_utc(cursor.created_at));
                let cursor_id = query
                    .cursor
                    .as_ref()
                    .map(|cursor| uuid_to_raw16(cursor.provider_id).to_vec());
                let fetch_limit = i64::from(query.limit) + 1;
                let page_limit = query.limit as usize;
                let bind_params: &[(&str, &dyn oracle::sql_type::ToSql)] = &[
                    ("status", &status),
                    ("tax_id", &query.tax_id),
                    ("cursor_created_at", &cursor_created_at),
                    ("cursor_id", &cursor_id),
                    ("fetch_limit", &fetch_limit),
                ];
                let rows = connection
                    .query_named(provider_list_sql(), bind_params)
                    .map_err(|error| {
                        DbError::Query(format!("failed to list providers: {error}"))
                    })?;
                let mut items = Vec::new();
                for row in rows {
                    let row = row.map_err(|error| {
                        DbError::Query(format!("failed to read listed provider row: {error}"))
                    })?;
                    items.push(map_provider_list_row(&row)?);
                }
                let has_next_page = items.len() > page_limit;
                if has_next_page {
                    items.truncate(page_limit);
                }
                let next_cursor = has_next_page.then(|| {
                    let item = items.last().expect("non-empty paginated provider page");
                    ProviderListCursor {
                        created_at: item.created_at,
                        provider_id: item.provider_id,
                    }
                });
                Ok(ProviderListPage { items, next_cursor })
            })
            .await
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="provider_ledger_accounts.list", provider_id=%provider_id))]
    pub async fn get_provider_ledger_mappings(
        &self,
        provider_id: Uuid,
    ) -> DbResult<Vec<ProviderLedgerAccountMapping>> {
        self.pool
            .with_connection(move |connection| {
                let rows = connection
                    .query(
                        "SELECT account_category,tigerbeetle_account_id,status FROM provider_ledger_accounts WHERE provider_id=:1 ORDER BY account_category",
                        &[&uuid_to_raw16(provider_id).to_vec()],
                    )
                    .map_err(|error| DbError::Query(format!("failed to list provider ledger mappings: {error}")))?;
                rows.map(|row| {
                    let row = row.map_err(|error| DbError::Query(format!("failed to read provider ledger mapping: {error}")))?;
                    let category: String = row.get(0).map_err(read_error)?;
                    let account_id: Vec<u8> = row.get(1).map_err(read_error)?;
                    Ok(ProviderLedgerAccountMapping {
                        category: ProviderAccountCategory::from_db_value(&category).ok_or_else(|| DbError::Query("unknown provider account category".to_string()))?,
                        tigerbeetle_account_id: raw16_to_uuid(&account_id)?,
                        status: row.get(2).map_err(read_error)?,
                    })
                }).collect()
            })
            .await
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="providers.mark_ready", provider_id=%provider_id))]
    pub async fn mark_provider_ready(&self, provider_id: Uuid) -> DbResult<Provider> {
        self.pool
            .with_transaction("finalize provider core provisioning", move |connection| {
                let provider_id_raw = uuid_to_raw16(provider_id).to_vec();
                let previous = fetch_provider(connection, provider_id)?;
                connection.execute(
                    "UPDATE provider_ledger_accounts SET status='ACTIVE',updated_at=SYSTIMESTAMP WHERE provider_id=:1 AND status='PROVISIONING'",
                    &[&provider_id_raw],
                ).map_err(|error| DbError::Query(format!("failed to activate provider ledger mappings: {error}")))?;
                connection.execute(
                    "UPDATE provider_provisioning_jobs SET status='SUCCEEDED',completed_at=SYSTIMESTAMP,updated_at=SYSTIMESTAMP,error_code=NULL,error_message=NULL WHERE provider_id=:1 AND job_type='TIGERBEETLE_PROVISION'",
                    &[&provider_id_raw],
                ).map_err(|error| DbError::Query(format!("failed to complete provider provisioning job: {error}")))?;
                let updated = connection.execute(
                    "UPDATE providers SET status='READY',updated_at=SYSTIMESTAMP WHERE provider_id=:1 AND status='PENDING_PROVISIONING'",
                    &[&provider_id_raw],
                ).map_err(|error| DbError::Query(format!("failed to mark provider ready: {error}")))?;
                let provider = fetch_provider(connection, provider_id)?;
                if updated.row_count().map_err(read_error)? != 1
                    && provider.status != ProviderStatus::Ready
                {
                    return Err(DbError::Conflict("provider is not pending core provisioning".to_string()));
                }
                let snapshot = provider.replay_snapshot().to_string();
                connection.execute(
                    "UPDATE idempotency_records SET response_snapshot=:1,updated_at=SYSTIMESTAMP WHERE resource_type='provider' AND resource_id=:2 AND status='COMPLETED'",
                    &[&snapshot, &provider_id_raw],
                ).map_err(|error| DbError::Query(format!("failed to refresh provider idempotency snapshot: {error}")))?;
                if previous.status != ProviderStatus::Ready {
                    insert_audit_log(
                        connection,
                        NewAuditLog {
                            audit_log_id: Uuid::new_v4(),
                            entity_type: "PROVIDER".to_string(),
                            entity_id: provider_id,
                            action_type: AuditAction::StateTransition,
                            reason: Some("provider core provisioning completed".to_string()),
                            old_values: Some(previous.replay_snapshot()),
                            new_values: Some(provider.replay_snapshot()),
                            context: provisioning_audit_context(provider_id, "ready"),
                        },
                    )?;
                }
                Ok(provider)
            })
            .await
    }
}

pub(crate) fn provisioning_audit_context(
    provider_id: Uuid,
    transition: &str,
) -> TrustedAuditContext {
    TrustedAuditContext {
        actor_subject: "wurzburg-provider-provisioning".to_string(),
        actor_client_id: Some("wurzburg-provider-provisioning".to_string()),
        actor_provider_id: None,
        actor_user_id: None,
        actor_issuer: Some("wurzburg-internal".to_string()),
        source_ip: None,
        correlation_id: format!(
            "provider-provisioning-{}-{transition}",
            provider_id.simple()
        ),
        request_id: Uuid::new_v4().to_string(),
    }
}

fn insert_provider(
    connection: &oracle::Connection,
    provider: &NewProvider,
    actor_subject: &str,
) -> DbResult<()> {
    connection.execute(
        "INSERT INTO providers (provider_id,legal_name,trade_name,tax_id,registration_number,email_address,website_url,mailing_address,status,metadata_json,created_by_subject,updated_by_subject) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,'PENDING_PROVISIONING',:9,:10,:10)",
        &[&uuid_to_raw16(provider.provider_id).to_vec(), &provider.legal_name, &provider.trade_name, &provider.tax_id, &provider.registration_number, &provider.email_address, &provider.website_url, &provider.mailing_address, &provider.metadata.to_string(), &actor_subject],
    ).map_err(|error| DbError::Query(format!("failed to insert provider: {error}")))?;
    Ok(())
}

fn insert_contacts(
    connection: &oracle::Connection,
    provider: &NewProvider,
    actor_subject: &str,
) -> DbResult<()> {
    for contact in &provider.contacts {
        connection.execute(
            "INSERT INTO provider_contacts (provider_contact_id,provider_id,contact_type,contact_name,email,phone,mobile,sms_enabled,metadata_json,status,created_by_subject,updated_by_subject) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,'ACTIVE',:10,:10)",
            &[&uuid_to_raw16(contact.provider_contact_id).to_vec(), &uuid_to_raw16(provider.provider_id).to_vec(), &contact.contact_type.as_db_value(), &contact.name, &contact.email, &contact.phone, &contact.mobile, &i32::from(contact.sms_enabled), &contact.metadata.to_string(), &actor_subject],
        ).map_err(|error| DbError::Query(format!("failed to insert provider contact: {error}")))?;
    }
    Ok(())
}

fn insert_operational_profile(
    connection: &oracle::Connection,
    provider: &NewProvider,
    actor_subject: &str,
) -> DbResult<()> {
    let profile_id = Uuid::new_v4();
    let effective_at = provider.operational_profile.effective_at.to_rfc3339();
    let profile =
        serde_json::to_string(&provider.operational_profile.controls).map_err(|error| {
            DbError::Query(format!("failed to serialize provider profile: {error}"))
        })?;
    connection
        .execute(
            operational_profile_insert_sql(),
            &[
                &uuid_to_raw16(profile_id).to_vec(),
                &uuid_to_raw16(provider.provider_id).to_vec(),
                &effective_at,
                &effective_at,
                &profile,
                &actor_subject,
                &actor_subject,
                &"initial provider operational profile",
                &effective_at,
            ],
        )
        .map_err(|error| {
            DbError::Query(format!(
                "failed to insert provider operational profile: {error}"
            ))
        })?;
    Ok(())
}

fn operational_profile_insert_sql() -> &'static str {
    "INSERT INTO provider_operational_profiles (provider_operational_profile_id,provider_id,status,version,effective_at,profile_json,created_by_subject,updated_by_subject,change_reason,activated_at) VALUES (:1,:2,CASE WHEN TO_TIMESTAMP_TZ(:3,'YYYY-MM-DD\"T\"HH24:MI:SS.FFTZH:TZM')<=SYSTIMESTAMP THEN 'ACTIVE' ELSE 'SCHEDULED' END,1,TO_TIMESTAMP_TZ(:4,'YYYY-MM-DD\"T\"HH24:MI:SS.FFTZH:TZM'),:5,:6,:7,:8,CASE WHEN TO_TIMESTAMP_TZ(:9,'YYYY-MM-DD\"T\"HH24:MI:SS.FFTZH:TZM')<=SYSTIMESTAMP THEN SYSTIMESTAMP ELSE NULL END)"
}

fn insert_ledger_mappings(connection: &oracle::Connection, provider_id: Uuid) -> DbResult<()> {
    for category in ProviderAccountCategory::ALL {
        let account_id = category.deterministic_account_id(provider_id);
        connection.execute(
            "INSERT INTO provider_ledger_accounts (provider_ledger_account_id,provider_id,account_category,tigerbeetle_account_id,status) VALUES (:1,:2,:3,:4,'PROVISIONING')",
            &[&uuid_to_raw16(Uuid::new_v4()).to_vec(), &uuid_to_raw16(provider_id).to_vec(), &category.as_db_value(), &uuid_to_raw16(account_id).to_vec()],
        ).map_err(|error| DbError::Query(format!("failed to insert provider ledger mapping: {error}")))?;
    }
    Ok(())
}

fn insert_kafka_access(connection: &oracle::Connection, provider: &NewProvider) -> DbResult<()> {
    let Some(access) = &provider.kafka_access else {
        return Ok(());
    };
    let brokers = serde_json::to_string(&access.bootstrap_servers)
        .map_err(|error| DbError::Query(format!("failed to serialize Kafka brokers: {error}")))?;
    let credential_version = i64::try_from(access.credential_version).map_err(|_| {
        DbError::Query("Kafka credential version exceeds Oracle NUMBER".to_string())
    })?;
    connection.execute(
        "INSERT INTO provider_kafka_access (provider_kafka_access_id,provider_id,topic_name,username,consumer_group,security_protocol,sasl_mechanism,bootstrap_servers_json,credential_status) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,'PROVISIONING')",
        &[&uuid_to_raw16(access.provider_kafka_access_id).to_vec(), &uuid_to_raw16(provider.provider_id).to_vec(), &access.topic_name, &access.username, &access.consumer_group, &access.security_protocol, &access.sasl_mechanism, &brokers],
    ).map_err(|error| DbError::Query(format!("failed to insert provider Kafka access: {error}")))?;
    connection.execute(
        "INSERT INTO provider_kafka_credentials (provider_kafka_credential_id,provider_kafka_access_id,provider_id,credential_version,password_ciphertext,encryption_key_version,status) VALUES (:1,:2,:3,:4,:5,:6,'CANDIDATE')",
        &[&uuid_to_raw16(access.provider_kafka_credential_id).to_vec(), &uuid_to_raw16(access.provider_kafka_access_id).to_vec(), &uuid_to_raw16(provider.provider_id).to_vec(), &credential_version, &access.password_ciphertext, &access.encryption_key_version],
    ).map_err(|error| DbError::Query(format!("failed to insert Provider Kafka credential: {error}")))?;
    Ok(())
}

fn insert_provider_event_subscriptions(
    connection: &oracle::Connection,
    provider_id: Uuid,
    actor_subject: &str,
) -> DbResult<()> {
    for event_type in crate::domain::provider_event::ProviderEventType::ALL {
        connection.execute(
            "INSERT INTO provider_event_subscriptions (provider_event_subscription_id,provider_id,event_type,enabled,version,updated_by_subject,reason) VALUES (:1,:2,:3,0,1,:4,'disabled by default during provider onboarding')",
            &[&uuid_to_raw16(Uuid::new_v4()).to_vec(), &uuid_to_raw16(provider_id).to_vec(), &event_type.as_str(), &actor_subject],
        ).map_err(|error| DbError::Query(format!("failed to seed Provider event subscriptions: {error}")))?;
    }
    Ok(())
}

fn insert_provisioning_jobs(
    connection: &oracle::Connection,
    provider_id: Uuid,
    kafka_enabled: bool,
) -> DbResult<()> {
    let provider_id_simple = provider_id.simple().to_string();
    let mut jobs = vec![("TIGERBEETLE_PROVISION", serde_json::json!({}))];
    if kafka_enabled {
        jobs.push((
            "KAFKA_PROVISION",
            serde_json::json!({
                "topic_name": format!("provider.events.{provider_id_simple}"),
                "username": format!("provider_user_{provider_id_simple}"),
                "consumer_group": format!("provider_group_{provider_id_simple}")
            }),
        ));
    }
    for (job_type, request) in jobs {
        connection.execute(
            "INSERT INTO provider_provisioning_jobs (provider_provisioning_job_id,provider_id,job_type,status,request_json,result_json) VALUES (:1,:2,:3,'PENDING',:4,'{}')",
            &[&uuid_to_raw16(Uuid::new_v4()).to_vec(), &uuid_to_raw16(provider_id).to_vec(), &job_type, &request.to_string()],
        ).map_err(|error| DbError::Query(format!("failed to insert provider provisioning job: {error}")))?;
    }
    Ok(())
}

pub(crate) fn fetch_provider(
    connection: &oracle::Connection,
    provider_id: Uuid,
) -> DbResult<Provider> {
    let row = connection
        .query_row(
            provider_select_sql(),
            &[&uuid_to_raw16(provider_id).to_vec()],
        )
        .map_err(|error| DbError::Query(format!("failed to fetch provider: {error}")))?;
    let kafka_status = provider_kafka_status(connection, provider_id)?;
    map_provider_row(&row, kafka_status)
}

pub(crate) fn fetch_provider_for_command(
    connection: &oracle::Connection,
    provider_id: Uuid,
) -> DbResult<Provider> {
    fetch_provider(connection, provider_id)
}

fn provider_select_sql() -> &'static str {
    "SELECT provider_id,legal_name,trade_name,tax_id,registration_number,email_address,website_url,mailing_address,status,JSON_SERIALIZE(metadata_json RETURNING CLOB),TO_CHAR(SYS_EXTRACT_UTC(created_at),'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'),TO_CHAR(SYS_EXTRACT_UTC(updated_at),'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') FROM providers WHERE provider_id=:1"
}

fn provider_list_sql() -> &'static str {
    r#"
    SELECT provider_id, legal_name, trade_name, tax_id, status,
           TO_CHAR(SYS_EXTRACT_UTC(created_at),'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'),
           TO_CHAR(SYS_EXTRACT_UTC(updated_at),'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')
    FROM providers
    WHERE (:status IS NULL OR status = :status)
      AND (:tax_id IS NULL OR tax_id = :tax_id)
      AND (
          :cursor_created_at IS NULL
          OR SYS_EXTRACT_UTC(created_at) < TO_TIMESTAMP(:cursor_created_at, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')
          OR (
              SYS_EXTRACT_UTC(created_at) = TO_TIMESTAMP(:cursor_created_at, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')
              AND provider_id < :cursor_id
          )
      )
    ORDER BY created_at DESC, provider_id DESC
    FETCH FIRST :fetch_limit ROWS ONLY
    "#
}

fn map_provider_list_row(row: &Row) -> DbResult<ProviderListItem> {
    let provider_id: Vec<u8> = row.get(0).map_err(read_error)?;
    let status: String = row.get(4).map_err(read_error)?;
    let created_at: String = row.get(5).map_err(read_error)?;
    let updated_at: String = row.get(6).map_err(read_error)?;
    Ok(ProviderListItem {
        provider_id: raw16_to_uuid(&provider_id)?,
        legal_name: row.get(1).map_err(read_error)?,
        trade_name: row.get(2).map_err(read_error)?,
        tax_id: row.get(3).map_err(read_error)?,
        status: ProviderStatus::from_db_value(&status)
            .ok_or_else(|| DbError::Query("unknown provider status".to_string()))?,
        created_at: parse_utc(&created_at)?,
        updated_at: parse_utc(&updated_at)?,
    })
}

fn provider_kafka_status(connection: &oracle::Connection, provider_id: Uuid) -> DbResult<String> {
    let provider_id = uuid_to_raw16(provider_id).to_vec();
    let pending = connection.query_row_as::<i64>(
        "SELECT COUNT(*) FROM provider_provisioning_jobs WHERE provider_id=:1 AND job_type IN ('KAFKA_PROVISION','KAFKA_ROTATE','KAFKA_SUSPEND','KAFKA_RESUME') AND status IN ('PENDING','RUNNING')",
        &[&provider_id],
    ).map_err(|error| DbError::Query(format!("failed to count pending Provider Kafka jobs: {error}")))?;
    if pending > 0 {
        return Ok("PENDING".to_string());
    }
    match connection.query_row_as::<String>(
        "SELECT credential_status FROM provider_kafka_access WHERE provider_id=:1",
        &[&provider_id],
    ) {
        Ok(status) if status == "FAILED" => Ok("FAILED".to_string()),
        Ok(_) => Ok("SUCCEEDED".to_string()),
        Err(error) if error.kind() == oracle::ErrorKind::NoDataFound => Ok("DISABLED".to_string()),
        Err(error) => Err(DbError::Query(format!(
            "failed to read Provider Kafka provisioning status: {error}"
        ))),
    }
}

fn map_provider_row(row: &Row, kafka_status: String) -> DbResult<Provider> {
    let provider_id: Vec<u8> = row.get(0).map_err(read_error)?;
    let status: String = row.get(8).map_err(read_error)?;
    let metadata: String = row.get(9).map_err(read_error)?;
    let created_at: String = row.get(10).map_err(read_error)?;
    let updated_at: String = row.get(11).map_err(read_error)?;
    Ok(Provider {
        provider_id: raw16_to_uuid(&provider_id)?,
        legal_name: row.get(1).map_err(read_error)?,
        trade_name: row.get(2).map_err(read_error)?,
        tax_id: row.get(3).map_err(read_error)?,
        registration_number: row.get(4).map_err(read_error)?,
        email_address: row.get(5).map_err(read_error)?,
        website_url: row.get(6).map_err(read_error)?,
        mailing_address: row.get(7).map_err(read_error)?,
        status: ProviderStatus::from_db_value(&status)
            .ok_or_else(|| DbError::Query("unknown provider status".to_string()))?,
        metadata: serde_json::from_str(&metadata)
            .map_err(|error| DbError::Query(format!("invalid provider metadata: {error}")))?,
        created_at: parse_utc(&created_at)?,
        updated_at: parse_utc(&updated_at)?,
        kafka_provisioning_status: match kafka_status.as_str() {
            "DISABLED" => ProviderKafkaProvisioningStatus::Disabled,
            "PENDING" => ProviderKafkaProvisioningStatus::Pending,
            "SUCCEEDED" => ProviderKafkaProvisioningStatus::Succeeded,
            "FAILED" => ProviderKafkaProvisioningStatus::Failed,
            _ => {
                return Err(DbError::Query(
                    "unknown Kafka provisioning status".to_string(),
                ));
            }
        },
    })
}

fn classify_idempotency(
    existing: &crate::domain::idempotency::IdempotencyRecord,
    request_hash: &str,
) -> DbResult<CreateProviderPersistenceOutcome> {
    if existing.request_hash != request_hash {
        return Ok(CreateProviderPersistenceOutcome::IdempotencyConflict);
    }
    Ok(match existing.status {
        IdempotencyStatus::Completed => existing
            .response_snapshot
            .clone()
            .map(CreateProviderPersistenceOutcome::Replayed)
            .unwrap_or(CreateProviderPersistenceOutcome::IdempotencyInvalidState),
        IdempotencyStatus::InProgress => CreateProviderPersistenceOutcome::IdempotencyInProgress,
        IdempotencyStatus::Failed | IdempotencyStatus::Conflict => {
            CreateProviderPersistenceOutcome::IdempotencyInvalidState
        }
    })
}

fn parse_utc(value: &str) -> DbResult<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)
        .map_err(|error| DbError::Query(format!("invalid provider timestamp: {error}")))?
        .with_timezone(&Utc))
}

fn format_oracle_utc(value: DateTime<Utc>) -> String {
    value.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

fn read_error(error: oracle::Error) -> DbError {
    DbError::Query(format!("failed to read Oracle provider row: {error}"))
}

#[cfg(test)]
mod tests {
    use super::{operational_profile_insert_sql, provider_list_sql};

    #[test]
    fn provider_list_uses_bound_filters_and_descending_keyset_order() {
        let sql = provider_list_sql();
        assert!(sql.contains(":status IS NULL"));
        assert!(sql.contains(":tax_id IS NULL"));
        assert!(sql.contains("provider_id < :cursor_id"));
        assert!(sql.contains("ORDER BY created_at DESC, provider_id DESC"));
        assert!(sql.contains("FETCH FIRST :fetch_limit ROWS ONLY"));
    }

    #[test]
    fn initial_operational_profile_uses_distinct_oracle_bind_positions() {
        let sql = operational_profile_insert_sql();
        for position in 1..=9 {
            assert_eq!(sql.matches(&format!(":{position}")).count(), 1);
        }
    }
}
