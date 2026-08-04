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
            provider_operational_profile::{lock_provider, promote_due_for_provider},
            types::uuid_to_raw16,
        },
    },
    domain::{
        audit::{AuditAction, NewAuditLog},
        idempotency::IdempotencyStatus,
        provider_event::ProviderEventType,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderEventSubscriptionRecord {
    pub event_type: ProviderEventType,
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderEventSubscriptionSet {
    pub provider_id: Uuid,
    pub version: u64,
    pub global_delivery_enabled: bool,
    pub provider_delivery_enabled: bool,
    pub provider_status: String,
    pub credential_status: Option<String>,
    pub subscriptions: Vec<ProviderEventSubscriptionRecord>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProviderEventSubscriptionUpdateOutcome {
    Applied(serde_json::Value),
    Replayed(serde_json::Value),
    ProviderNotFound,
    VersionConflict,
    IdempotencyConflict,
    IdempotencyInProgress,
    IdempotencyInvalidState,
}

impl OracleRepository {
    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="provider_event_subscriptions.get", provider_id=%provider_id))]
    pub async fn get_provider_event_subscriptions(
        &self,
        provider_id: Uuid,
    ) -> DbResult<Option<ProviderEventSubscriptionSet>> {
        self.pool
            .with_transaction("resolve Provider event subscriptions", move |connection| {
                if lock_provider(connection, provider_id)?.is_none() {
                    return Ok(None);
                }
                promote_due_for_provider(connection, provider_id)?;
                fetch_subscription_set(connection, provider_id)
            })
            .await
    }

    #[tracing::instrument(skip(self, context, subscriptions, reason), fields(db.system="oracle", db.operation.name="provider_event_subscriptions.replace", provider_id=%provider_id))]
    pub async fn replace_provider_event_subscriptions_atomic(
        &self,
        context: MutationCommandContext,
        provider_id: Uuid,
        expected_version: u64,
        subscriptions: Vec<ProviderEventSubscriptionRecord>,
        reason: String,
    ) -> DbResult<ProviderEventSubscriptionUpdateOutcome> {
        let operation_type = context.operation_type.clone();
        let idempotency_key = context.idempotency_key.as_str().to_string();
        let request_hash = context.request_hash.clone();
        self.pool
            .with_transaction("replace Provider event subscriptions", move |connection| {
                if let Some(existing) =
                    fetch_idempotency_record(connection, &operation_type, &idempotency_key)?
                {
                    return classify_idempotency(&existing, &request_hash);
                }
                if lock_provider(connection, provider_id)?.is_none() {
                    return Ok(ProviderEventSubscriptionUpdateOutcome::ProviderNotFound);
                }
                promote_due_for_provider(connection, provider_id)?;
                let Some(previous) = fetch_subscription_set(connection, provider_id)? else {
                    return Ok(ProviderEventSubscriptionUpdateOutcome::ProviderNotFound);
                };
                let rows = connection
                    .query(
                        "SELECT provider_event_subscription_id FROM provider_event_subscriptions WHERE provider_id=:1 FOR UPDATE",
                        &[&uuid_to_raw16(provider_id).to_vec()],
                    )
                    .map_err(query_error)?;
                for row in rows {
                    let row = row.map_err(query_error)?;
                    let _: Vec<u8> = row.get(0).map_err(read_error)?;
                }
                if previous.version != expected_version {
                    return Ok(ProviderEventSubscriptionUpdateOutcome::VersionConflict);
                }
                insert_idempotency_record(connection, context.new_idempotency_record())?;
                let next_version = expected_version.checked_add(1).ok_or_else(|| {
                    DbError::Query("Provider event subscription version overflow".to_string())
                })?;
                let next_version_db = i64::try_from(next_version).map_err(|_| {
                    DbError::Query(
                        "Provider event subscription version exceeds Oracle NUMBER".to_string(),
                    )
                })?;
                for subscription in &subscriptions {
                    let updated = connection.execute(
                        "UPDATE provider_event_subscriptions SET enabled=:1,version=:2,updated_by_subject=:3,reason=:4,updated_at=SYSTIMESTAMP WHERE provider_id=:5 AND event_type=:6",
                        &[&i32::from(subscription.enabled), &next_version_db, &context.actor.subject, &reason, &uuid_to_raw16(provider_id).to_vec(), &subscription.event_type.as_str()],
                    ).map_err(query_error)?;
                    if updated.row_count().map_err(read_error)? != 1 {
                        return Err(DbError::Query(
                            "Provider event subscription catalog row is missing".to_string(),
                        ));
                    }
                }
                let current = fetch_subscription_set(connection, provider_id)?.ok_or_else(|| {
                    DbError::Query("Provider event subscription set disappeared".to_string())
                })?;
                let snapshot = subscription_snapshot(&current);
                insert_audit_log(connection, NewAuditLog {
                    audit_log_id: Uuid::new_v4(),
                    entity_type: "PROVIDER_EVENT_SUBSCRIPTIONS".to_string(),
                    entity_id: provider_id,
                    action_type: AuditAction::Update,
                    reason: Some(reason),
                    old_values: Some(subscription_snapshot(&previous)),
                    new_values: Some(snapshot.clone()),
                    context: context.audit_context(),
                })?;
                complete_idempotency_record(
                    connection,
                    &operation_type,
                    &idempotency_key,
                    "provider_event_subscriptions",
                    provider_id,
                    snapshot.clone(),
                )?;
                Ok(ProviderEventSubscriptionUpdateOutcome::Applied(snapshot))
            })
            .await
    }
}

fn fetch_subscription_set(
    connection: &oracle::Connection,
    provider_id: Uuid,
) -> DbResult<Option<ProviderEventSubscriptionSet>> {
    let provider = match connection.query_row(
        "SELECT status FROM providers WHERE provider_id=:1",
        &[&uuid_to_raw16(provider_id).to_vec()],
    ) {
        Ok(row) => row,
        Err(error) if error.kind() == oracle::ErrorKind::NoDataFound => return Ok(None),
        Err(error) => return Err(query_error(error)),
    };
    let provider_status: String = provider.get(0).map_err(read_error)?;
    let credential_status = match connection.query_row_as::<String>(
        "SELECT credential_status FROM provider_kafka_access WHERE provider_id=:1",
        &[&uuid_to_raw16(provider_id).to_vec()],
    ) {
        Ok(status) => Some(status),
        Err(error) if error.kind() == oracle::ErrorKind::NoDataFound => None,
        Err(error) => return Err(query_error(error)),
    };
    let global_json = connection
        .query_row_as::<String>(
            "SELECT JSON_SERIALIZE(value_json RETURNING CLOB) FROM business_config WHERE config_key='provider_event_delivery.global_enabled' AND status='ACTIVE' AND (effective_at IS NULL OR effective_at<=SYSTIMESTAMP)",
            &[],
        )
        .map_err(query_error)?;
    let global_value: serde_json::Value = serde_json::from_str(&global_json)
        .map_err(|_| DbError::Query("invalid provider event global gate JSON".to_string()))?;
    let global_delivery_enabled = global_value
        .get("enabled")
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| DbError::Query("provider event global gate is invalid".to_string()))?;
    let provider_delivery_enabled = match connection.query_row_as::<String>(
        "SELECT JSON_SERIALIZE(profile_json RETURNING CLOB) FROM provider_operational_profiles WHERE provider_id=:1 AND status='ACTIVE' AND effective_at<=SYSTIMESTAMP ORDER BY version DESC FETCH FIRST 1 ROWS ONLY",
        &[&uuid_to_raw16(provider_id).to_vec()],
    ) {
        Ok(profile) => serde_json::from_str::<serde_json::Value>(&profile)
            .ok()
            .and_then(|value| value.get("event_delivery_enabled").and_then(serde_json::Value::as_bool))
            .unwrap_or(false),
        Err(error) if error.kind() == oracle::ErrorKind::NoDataFound => false,
        Err(error) => return Err(query_error(error)),
    };
    let rows = connection
        .query(
            "SELECT event_type,enabled,version FROM provider_event_subscriptions WHERE provider_id=:1 ORDER BY event_type",
            &[&uuid_to_raw16(provider_id).to_vec()],
        )
        .map_err(query_error)?;
    let mut subscriptions = Vec::new();
    let mut version = 0_u64;
    for row in rows {
        let row = row.map_err(query_error)?;
        let event_type: String = row.get(0).map_err(read_error)?;
        let enabled: i32 = row.get(1).map_err(read_error)?;
        let row_version: i64 = row.get(2).map_err(read_error)?;
        version = u64::try_from(row_version).map_err(|_| {
            DbError::Query("invalid Provider event subscription version".to_string())
        })?;
        subscriptions.push(ProviderEventSubscriptionRecord {
            event_type: ProviderEventType::parse_name(&event_type).ok_or_else(|| {
                DbError::Query("unknown Provider event subscription type".to_string())
            })?,
            enabled: enabled == 1,
        });
    }
    if subscriptions.len() != ProviderEventType::ALL.len() {
        return Err(DbError::Query(
            "Provider event subscription catalog is incomplete".to_string(),
        ));
    }
    Ok(Some(ProviderEventSubscriptionSet {
        provider_id,
        version,
        global_delivery_enabled,
        provider_delivery_enabled,
        provider_status,
        credential_status,
        subscriptions,
    }))
}

/// Resolves and snapshots every delivery gate in the same Oracle transaction
/// that creates a provider-facing outbox row. The snapshot explains why an
/// event was published or suppressed without tracking provider consumption.
pub(crate) fn provider_event_delivery_gate(
    connection: &oracle::Connection,
    provider_id: Uuid,
    event_type: ProviderEventType,
) -> DbResult<(bool, serde_json::Value)> {
    let set = fetch_subscription_set(connection, provider_id)?.ok_or_else(|| {
        DbError::Query("provider disappeared while resolving event delivery".to_string())
    })?;
    let subscription = set
        .subscriptions
        .iter()
        .find(|value| value.event_type == event_type)
        .ok_or_else(|| DbError::Query("provider event subscription is missing".to_string()))?;
    let blocked = blocked_by(&set, subscription.enabled);
    let enabled = blocked.is_empty();
    Ok((
        enabled,
        serde_json::json!({
            "global_delivery_enabled": set.global_delivery_enabled,
            "provider_delivery_enabled": set.provider_delivery_enabled,
            "provider_active": set.provider_status == "ACTIVE",
            "kafka_credential_active": set.credential_status.as_deref() == Some("ACTIVE"),
            "subscription_enabled": subscription.enabled,
            "subscription_version": set.version,
            "blocked_by": blocked,
        }),
    ))
}

fn subscription_snapshot(value: &ProviderEventSubscriptionSet) -> serde_json::Value {
    let credential_active = value.credential_status.as_deref() == Some("ACTIVE");
    let provider_active = value.provider_status == "ACTIVE";
    serde_json::json!({
        "provider_id": value.provider_id,
        "version": value.version,
        "global_delivery_enabled": value.global_delivery_enabled,
        "provider_delivery_enabled": value.provider_delivery_enabled,
        "credential_status": value.credential_status,
        "subscriptions": value.subscriptions.iter().map(|item| serde_json::json!({
            "event_type": item.event_type.as_str(),
            "schema_versions": [1],
            "configured_enabled": item.enabled,
            "effective_enabled": item.enabled
                && value.global_delivery_enabled
                && value.provider_delivery_enabled
                && provider_active
                && credential_active,
            "blocked_by": blocked_by(value, item.enabled)
        })).collect::<Vec<_>>()
    })
}

fn blocked_by(value: &ProviderEventSubscriptionSet, configured_enabled: bool) -> Vec<&'static str> {
    let mut reasons = Vec::new();
    if !value.global_delivery_enabled {
        reasons.push("GLOBAL_DELIVERY_DISABLED");
    }
    if !value.provider_delivery_enabled {
        reasons.push("PROVIDER_DELIVERY_DISABLED");
    }
    if value.provider_status != "ACTIVE" {
        reasons.push("PROVIDER_NOT_ACTIVE");
    }
    if value.credential_status.as_deref() != Some("ACTIVE") {
        reasons.push("KAFKA_CREDENTIAL_NOT_ACTIVE");
    }
    if !configured_enabled {
        reasons.push("EVENT_DISABLED");
    }
    reasons
}

fn classify_idempotency(
    existing: &crate::domain::idempotency::IdempotencyRecord,
    request_hash: &str,
) -> DbResult<ProviderEventSubscriptionUpdateOutcome> {
    if existing.request_hash != request_hash {
        return Ok(ProviderEventSubscriptionUpdateOutcome::IdempotencyConflict);
    }
    Ok(match existing.status {
        IdempotencyStatus::Completed => existing
            .response_snapshot
            .clone()
            .map(ProviderEventSubscriptionUpdateOutcome::Replayed)
            .unwrap_or(ProviderEventSubscriptionUpdateOutcome::IdempotencyInvalidState),
        IdempotencyStatus::InProgress => {
            ProviderEventSubscriptionUpdateOutcome::IdempotencyInProgress
        }
        IdempotencyStatus::Failed | IdempotencyStatus::Conflict => {
            ProviderEventSubscriptionUpdateOutcome::IdempotencyInvalidState
        }
    })
}

fn query_error(error: oracle::Error) -> DbError {
    DbError::Query(format!(
        "Provider event subscription Oracle operation failed: {error}"
    ))
}

fn read_error(error: oracle::Error) -> DbError {
    DbError::Query(format!(
        "failed to read Provider event subscription row: {error}"
    ))
}
