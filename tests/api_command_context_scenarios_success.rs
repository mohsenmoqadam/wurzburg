use std::net::IpAddr;

use uuid::Uuid;
use wurzburg::{
    api::{
        auth::{TrustedActor, VerifiedActorClaims},
        command::MutationCommandContext,
        idempotency::IdempotencyKey,
        request_context::{BackendToken, TrustedRequestContext},
    },
    config::BackendTokenTransport,
};

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

#[test]
fn builds_idempotency_record_from_trusted_command_context() {
    let context = command_context();
    let record = context.new_idempotency_record();

    assert_eq!(record.operation_type, "card_ranges.create");
    assert_eq!(record.idempotency_key, "idem-001");
    assert_eq!(record.request_hash, "hash-001");
    assert_eq!(record.created_by_subject, "admin@example.test");
    assert_eq!(record.created_by_client_id.as_deref(), Some("admin-ui"));
    assert_eq!(record.correlation_id, "corr-001");
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
