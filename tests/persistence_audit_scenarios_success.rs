use std::net::IpAddr;

use uuid::Uuid;
use wurzburg::domain::audit::{AuditAction, NewAuditLog, TrustedAuditContext};

#[test]
fn builds_audit_log_with_trusted_actor_and_network_context() {
    let provider_id = Uuid::new_v4();
    let audit_log = NewAuditLog {
        audit_log_id: Uuid::new_v4(),
        entity_type: "CARD_RANGE".to_string(),
        entity_id: Uuid::new_v4(),
        action_type: AuditAction::StateTransition,
        reason: Some("activation approved".to_string()),
        old_values: Some(serde_json::json!({ "status": "DRAFT" })),
        new_values: Some(serde_json::json!({ "status": "ACTIVE" })),
        context: TrustedAuditContext {
            actor_subject: "admin@example.test".to_string(),
            actor_client_id: Some("admin-ui".to_string()),
            actor_provider_id: Some(provider_id),
            actor_user_id: None,
            actor_issuer: Some("https://wso2.example.test".to_string()),
            source_ip: Some("203.0.113.10".parse::<IpAddr>().unwrap()),
            correlation_id: "corr-001".to_string(),
            request_id: "018f9e64-1b5f-7cc1-a3cf-2a519179f801".to_string(),
        },
    };

    assert_eq!(audit_log.action_type.as_db_value(), "STATE_TRANSITION");
    assert_eq!(audit_log.context.actor_provider_id, Some(provider_id));
    assert_eq!(audit_log.new_values.unwrap()["status"], "ACTIVE");
}
