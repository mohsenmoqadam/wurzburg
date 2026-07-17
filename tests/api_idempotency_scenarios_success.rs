use axum::http::{HeaderMap, HeaderValue, Method};
use wurzburg::api::idempotency::{
    canonical_request_hash, mutating_method_requires_idempotency, require_idempotency_key,
};

#[test]
fn accepts_visible_ascii_idempotency_key() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "Idempotency-Key",
        HeaderValue::from_static("provider-onboarding-0001"),
    );

    let key = require_idempotency_key(&headers).expect("key should be accepted");

    assert_eq!(key.as_str(), "provider-onboarding-0001");
}

#[test]
fn classifies_only_business_mutations_as_requiring_idempotency() {
    assert!(mutating_method_requires_idempotency(&Method::POST));
    assert!(mutating_method_requires_idempotency(&Method::PUT));
    assert!(mutating_method_requires_idempotency(&Method::PATCH));
    assert!(mutating_method_requires_idempotency(&Method::DELETE));

    assert!(!mutating_method_requires_idempotency(&Method::GET));
    assert!(!mutating_method_requires_idempotency(&Method::HEAD));
}

#[test]
fn canonical_request_hash_changes_when_body_changes() {
    let first = canonical_request_hash(&Method::POST, "/api/v1/card-ranges", br#"{"a":1}"#);
    let second = canonical_request_hash(&Method::POST, "/api/v1/card-ranges", br#"{"a":2}"#);

    assert_ne!(first, second);
}
