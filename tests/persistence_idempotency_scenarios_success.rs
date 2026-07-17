use uuid::Uuid;
use wurzburg::domain::idempotency::{IdempotencyStatus, NewIdempotencyRecord};

#[test]
fn builds_idempotency_record_with_request_identity_context() {
    let provider_id = Uuid::new_v4();
    let record = NewIdempotencyRecord {
        idempotency_record_id: Uuid::new_v4(),
        operation_type: "card_ranges.create".to_string(),
        idempotency_key: "idem-001".to_string(),
        request_hash: "abc123".to_string(),
        created_by_subject: "admin@example.test".to_string(),
        created_by_client_id: Some("admin-ui".to_string()),
        actor_provider_id: Some(provider_id),
        actor_user_id: None,
        correlation_id: "corr-001".to_string(),
        request_id: "018f9e64-1b5f-7cc1-a3cf-2a519179f801".to_string(),
    };

    assert_eq!(record.operation_type, "card_ranges.create");
    assert_eq!(record.actor_provider_id, Some(provider_id));
    assert_eq!(record.correlation_id, "corr-001");
}

#[test]
fn maps_completed_status_to_oracle_value() {
    assert_eq!(IdempotencyStatus::Completed.as_db_value(), "COMPLETED");
    assert_eq!(
        IdempotencyStatus::from_db_value("COMPLETED"),
        Some(IdempotencyStatus::Completed)
    );
}
