use axum::http::{HeaderMap, HeaderValue};
use wurzburg::api::{idempotency::require_idempotency_key, result_codes::WurzburgResultCode};

#[test]
fn rejects_missing_idempotency_key() {
    let headers = HeaderMap::new();

    let error = require_idempotency_key(&headers).expect_err("missing key should fail");

    assert_eq!(error.body().error.code, "MISSING_IDEMPOTENCY_KEY");
    assert_eq!(
        error.body().error.rs_code,
        WurzburgResultCode::MissingIdempotencyKey.parts().0
    );
}

#[test]
fn rejects_empty_idempotency_key() {
    let mut headers = HeaderMap::new();
    headers.insert("Idempotency-Key", HeaderValue::from_static(""));

    let error = require_idempotency_key(&headers).expect_err("empty key should fail");

    assert_eq!(error.body().error.code, "INVALID_IDEMPOTENCY_KEY");
}

#[test]
fn rejects_idempotency_key_with_spaces() {
    let mut headers = HeaderMap::new();
    headers.insert("Idempotency-Key", HeaderValue::from_static("not accepted"));

    let error = require_idempotency_key(&headers).expect_err("space should fail");

    assert_eq!(error.body().error.code, "INVALID_IDEMPOTENCY_KEY");
}

#[test]
fn rejects_idempotency_key_longer_than_gateway_contract() {
    let mut headers = HeaderMap::new();
    let value = "a".repeat(256);
    headers.insert("Idempotency-Key", HeaderValue::from_str(&value).unwrap());

    let error = require_idempotency_key(&headers).expect_err("oversized key should fail");

    assert_eq!(error.body().error.code, "INVALID_IDEMPOTENCY_KEY");
}
