use std::{collections::HashMap, net::IpAddr, sync::Mutex};

use async_trait::async_trait;
use chrono::Utc;
use uuid::Uuid;
use wurzburg::{
    api::{
        auth::{TrustedActor, VerifiedActorClaims},
        command::{IdempotencyStart, IdempotencyStarter, MutationCommandContext},
        idempotency::IdempotencyKey,
        request_context::{BackendToken, TrustedRequestContext},
    },
    config::BackendTokenTransport,
    db::{error::DbResult, traits::IdempotencyRepository},
    domain::idempotency::{IdempotencyRecord, IdempotencyStatus, NewIdempotencyRecord},
};

#[derive(Default)]
struct MemoryIdempotencyRepository {
    records: Mutex<HashMap<(String, String), IdempotencyRecord>>,
}

#[async_trait]
impl IdempotencyRepository for MemoryIdempotencyRepository {
    async fn get_idempotency_record(
        &self,
        operation_type: &str,
        idempotency_key: &str,
    ) -> DbResult<Option<IdempotencyRecord>> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .get(&(operation_type.to_string(), idempotency_key.to_string()))
            .cloned())
    }

    async fn create_idempotency_record(&self, record: NewIdempotencyRecord) -> DbResult<()> {
        let now = Utc::now();
        self.records.lock().unwrap().insert(
            (
                record.operation_type.clone(),
                record.idempotency_key.clone(),
            ),
            IdempotencyRecord {
                idempotency_record_id: record.idempotency_record_id,
                operation_type: record.operation_type,
                idempotency_key: record.idempotency_key,
                request_hash: record.request_hash,
                status: IdempotencyStatus::InProgress,
                resource_type: None,
                resource_id: None,
                response_snapshot: None,
                error_snapshot: None,
                created_by_subject: record.created_by_subject,
                created_by_client_id: record.created_by_client_id,
                actor_provider_id: record.actor_provider_id,
                actor_user_id: record.actor_user_id,
                correlation_id: record.correlation_id,
                request_id: record.request_id,
                created_at: now,
                updated_at: now,
                completed_at: None,
            },
        );
        Ok(())
    }

    async fn complete_idempotency_record(
        &self,
        _operation_type: &str,
        _idempotency_key: &str,
        _resource_type: &str,
        _resource_id: Uuid,
        _response_snapshot: serde_json::Value,
    ) -> DbResult<()> {
        Ok(())
    }
}

fn command_context() -> MutationCommandContext {
    let provider_id = Uuid::new_v4();
    MutationCommandContext {
        operation_type: "card_ranges.create".to_string(),
        actor: TrustedActor::from_verified_claims(VerifiedActorClaims {
            issuer: "https://wso2.example.test".to_string(),
            subject: "admin@example.test".to_string(),
            client_id: "admin-ui".to_string(),
            roles: vec!["wurzburg_platform_admin".to_string()],
            scopes: vec!["platform.card_ranges:write".to_string()],
            provider_id: Some(provider_id),
            user_id: None,
        })
        .unwrap(),
        request: TrustedRequestContext {
            correlation_id: "corr-001".to_string(),
            request_id: Uuid::parse_str("018f9e64-1b5f-7cc1-a3cf-2a519179f801").unwrap(),
            client_ip: "203.0.113.10".parse::<IpAddr>().unwrap(),
            gateway_id: "gw-prod-a".to_string(),
            backend_token: BackendToken::from_verified_transport(
                BackendTokenTransport::AuthorizationBearer,
                "secret.jwt.value",
            )
            .unwrap(),
        },
        idempotency_key: IdempotencyKey::from_validated("idem-001").unwrap(),
        request_hash: "hash-001".to_string(),
    }
}

#[tokio::test]
async fn starts_new_idempotent_mutation_when_record_is_absent() {
    let repository = MemoryIdempotencyRepository::default();
    let context = command_context();

    let start = repository
        .start_idempotent_mutation(&context)
        .await
        .expect("new command should execute");

    assert_eq!(start, IdempotencyStart::Execute);
    assert!(
        repository
            .get_idempotency_record("card_ranges.create", "idem-001")
            .await
            .unwrap()
            .is_some()
    );
}

#[test]
fn maps_command_context_to_audit_context() {
    let context = command_context();
    let audit_context = context.audit_context();

    assert_eq!(audit_context.actor_subject, "admin@example.test");
    assert_eq!(audit_context.actor_client_id.as_deref(), Some("admin-ui"));
    assert_eq!(audit_context.source_ip.unwrap().to_string(), "203.0.113.10");
    assert_eq!(audit_context.correlation_id, "corr-001");
}
