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
    headers.insert(
        "Authorization",
        HeaderValue::from_static("Bearer secret.jwt.value"),
    );
    headers
}

#[test]
fn rejects_missing_trusted_request_header() {
    let mut headers = base_headers();
    headers.remove("X-Correlation-Id");

    let error =
        extract_trusted_request_context(&headers, BackendTokenTransport::AuthorizationBearer)
            .expect_err("missing trusted header should fail");

    assert_eq!(error.body().error.code, "MISSING_TRUSTED_REQUEST_HEADER");
}

#[test]
fn rejects_invalid_correlation_id() {
    let mut headers = base_headers();
    headers.insert(
        "X-Correlation-Id",
        HeaderValue::from_static("bad correlation"),
    );

    let error =
        extract_trusted_request_context(&headers, BackendTokenTransport::AuthorizationBearer)
            .expect_err("invalid correlation should fail");

    assert_eq!(error.body().error.code, "INVALID_TRUSTED_REQUEST_HEADER");
}

#[test]
fn rejects_invalid_request_id() {
    let mut headers = base_headers();
    headers.insert("X-Request-Id", HeaderValue::from_static("not-a-uuid"));

    let error =
        extract_trusted_request_context(&headers, BackendTokenTransport::AuthorizationBearer)
            .expect_err("invalid request id should fail");

    assert_eq!(error.body().error.code, "INVALID_TRUSTED_REQUEST_HEADER");
}

#[test]
fn rejects_invalid_client_ip() {
    let mut headers = base_headers();
    headers.insert(
        "X-WSO2-Client-IP",
        HeaderValue::from_static("203.0.113.10:443"),
    );

    let error =
        extract_trusted_request_context(&headers, BackendTokenTransport::AuthorizationBearer)
            .expect_err("invalid client ip should fail");

    assert_eq!(error.body().error.code, "INVALID_TRUSTED_REQUEST_HEADER");
}

#[test]
fn rejects_ambiguous_backend_token_transports() {
    let mut headers = base_headers();
    headers.insert("X-JWT-Assertion", HeaderValue::from_static("assertion.jwt"));

    let error =
        extract_trusted_request_context(&headers, BackendTokenTransport::AuthorizationBearer)
            .expect_err("ambiguous token transport should fail");

    assert_eq!(error.body().error.code, "AMBIGUOUS_BACKEND_TOKEN");
}

#[test]
fn rejects_backend_token_sent_through_wrong_transport() {
    let headers = base_headers();

    let error = extract_trusted_request_context(&headers, BackendTokenTransport::XJwtAssertion)
        .expect_err("wrong token transport should fail");

    assert_eq!(error.body().error.code, "INVALID_BACKEND_TOKEN_TRANSPORT");
}
