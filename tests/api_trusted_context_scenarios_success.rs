use axum::http::{HeaderMap, HeaderValue};
use wurzburg::{
    api::request_context::extract_trusted_request_context, config::BackendTokenTransport,
};

fn base_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("X-Correlation-Id", HeaderValue::from_static("corr-001"));
    headers.insert(
        "X-Request-Id",
        HeaderValue::from_static("018f9e64-1b5f-7cc1-a3cf-2a519179f801"),
    );
    headers.insert("X-WSO2-Client-IP", HeaderValue::from_static("203.0.113.10"));
    headers.insert("X-WSO2-Gateway-Id", HeaderValue::from_static("gw-prod-a"));
    headers
}

#[test]
fn extracts_context_with_authorization_bearer_transport() {
    let mut headers = base_headers();
    headers.insert(
        "Authorization",
        HeaderValue::from_static("Bearer secret.jwt.value"),
    );

    let context =
        extract_trusted_request_context(&headers, BackendTokenTransport::AuthorizationBearer)
            .expect("trusted context should parse");

    assert_eq!(context.correlation_id, "corr-001");
    assert_eq!(context.client_ip.to_string(), "203.0.113.10");
    assert_eq!(context.gateway_id, "gw-prod-a");
    assert_eq!(
        context.backend_token.expose_for_signature_validation(),
        "secret.jwt.value"
    );
}

#[test]
fn extracts_context_with_x_jwt_assertion_transport() {
    let mut headers = base_headers();
    headers.insert("X-JWT-Assertion", HeaderValue::from_static("assertion.jwt"));

    let context = extract_trusted_request_context(&headers, BackendTokenTransport::XJwtAssertion)
        .expect("trusted context should parse");

    assert_eq!(
        context.backend_token.redacted_transport_only(),
        BackendTokenTransport::XJwtAssertion
    );
}

#[test]
fn redacts_backend_token_from_debug_output() {
    let mut headers = base_headers();
    headers.insert(
        "Authorization",
        HeaderValue::from_static("Bearer secret.jwt.value"),
    );

    let context =
        extract_trusted_request_context(&headers, BackendTokenTransport::AuthorizationBearer)
            .expect("trusted context should parse");

    let debug = format!("{context:?}");

    assert!(!debug.contains("secret.jwt.value"));
    assert!(debug.contains("<redacted>"));
}
