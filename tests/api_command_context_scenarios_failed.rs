use chrono::Utc;
use uuid::Uuid;
use wurzburg::{
    api::command::{IdempotencyStart, classify_existing_idempotency_record},
    domain::idempotency::{IdempotencyRecord, IdempotencyStatus},
};

fn existing_record(
    status: IdempotencyStatus,
    response_snapshot: Option<serde_json::Value>,
) -> IdempotencyRecord {
    let now = Utc::now();
    IdempotencyRecord {
        idempotency_record_id: Uuid::new_v4(),
        operation_type: "card_ranges.create".to_string(),
        idempotency_key: "idem-001".to_string(),
        request_hash: "hash-001".to_string(),
        status,
        resource_type: Some("card_range".to_string()),
        resource_id: Some(Uuid::new_v4()),
        response_snapshot,
        error_snapshot: None,
        created_by_subject: "admin@example.test".to_string(),
        created_by_client_id: Some("admin-ui".to_string()),
        actor_provider_id: None,
        actor_user_id: None,
        correlation_id: "corr-001".to_string(),
        request_id: "018f9e64-1b5f-7cc1-a3cf-2a519179f801".to_string(),
        created_at: now,
        updated_at: now,
        completed_at: None,
    }
}

#[test]
fn replays_completed_record_with_matching_hash() {
    let record = existing_record(
        IdempotencyStatus::Completed,
        Some(serde_json::json!({ "card_range_id": "range-001" })),
    );

    let start = classify_existing_idempotency_record(&record, "hash-001")
        .expect("completed record should replay");

    assert_eq!(
        start,
        IdempotencyStart::Replay(serde_json::json!({ "card_range_id": "range-001" }))
    );
}

#[test]
fn rejects_same_key_with_different_request_hash() {
    let record = existing_record(IdempotencyStatus::Completed, Some(serde_json::json!({})));

    let error = classify_existing_idempotency_record(&record, "hash-002")
        .expect_err("different hash should conflict");

    assert_eq!(error.body().error.code, "IDEMPOTENCY_KEY_CONFLICT");
}

#[test]
fn rejects_in_progress_duplicate_request() {
    let record = existing_record(IdempotencyStatus::InProgress, None);

    let error = classify_existing_idempotency_record(&record, "hash-001")
        .expect_err("in-progress duplicate should conflict");

    assert_eq!(error.body().error.code, "IDEMPOTENCY_IN_PROGRESS");
}
