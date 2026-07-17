use axum::{
    Router,
    body::Body,
    http::{HeaderMap, HeaderValue, Request, StatusCode},
    middleware,
    routing::get,
};
use tower::ServiceExt;
use wurzburg::{
    config::BackendTokenTransport,
    telemetry::http::{
        RESULT_CODE_HEADER, RESULT_SYMBOL_HEADER, ensure_result_headers,
        summarize_response_for_trace, trace_http_request,
    },
};

#[test]
fn adds_success_result_headers_for_plain_success_response() {
    let mut headers = HeaderMap::new();

    ensure_result_headers(&mut headers, StatusCode::OK);

    assert_eq!(headers.get(&RESULT_CODE_HEADER).unwrap(), "0");
    assert_eq!(headers.get(&RESULT_SYMBOL_HEADER).unwrap(), "SUCCESS");
}

#[test]
fn keeps_existing_result_headers_from_api_error_response() {
    let mut headers = HeaderMap::new();
    headers.insert(&RESULT_CODE_HEADER, HeaderValue::from_static("6201"));
    headers.insert(
        &RESULT_SYMBOL_HEADER,
        HeaderValue::from_static("MISSING_TRUSTED_REQUEST_HEADER"),
    );

    ensure_result_headers(&mut headers, StatusCode::BAD_REQUEST);

    assert_eq!(headers.get(&RESULT_CODE_HEADER).unwrap(), "6201");
    assert_eq!(
        headers.get(&RESULT_SYMBOL_HEADER).unwrap(),
        "MISSING_TRUSTED_REQUEST_HEADER"
    );
}

#[tokio::test]
async fn router_tracing_middleware_adds_success_result_headers() {
    let app = Router::new().route("/probe", get(|| async { "ok" })).layer(
        middleware::from_fn_with_state(
            BackendTokenTransport::AuthorizationBearer,
            trace_http_request,
        ),
    );

    let response = app
        .oneshot(
            Request::builder()
                .uri("/probe")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("request should complete");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get(&RESULT_CODE_HEADER).unwrap(), "0");
    assert_eq!(
        response.headers().get(&RESULT_SYMBOL_HEADER).unwrap(),
        "SUCCESS"
    );
}

#[test]
fn trace_summary_uses_safe_contract_headers_only() {
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
    ensure_result_headers(&mut headers, StatusCode::OK);

    let summary = summarize_response_for_trace("GET", "/probe", StatusCode::OK, &headers);
    let debug = format!("{summary:?}");

    assert_eq!(summary.correlation_id.as_deref(), Some("corr-001"));
    assert_eq!(summary.result_symbol, "SUCCESS");
    assert!(!debug.contains("secret.jwt.value"));
    assert!(!debug.contains("Bearer"));
}
