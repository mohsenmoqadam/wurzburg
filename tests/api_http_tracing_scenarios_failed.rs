use axum::http::{HeaderMap, StatusCode};
use wurzburg::telemetry::http::{
    RESULT_CODE_HEADER, RESULT_SYMBOL_HEADER, ensure_result_headers, response_result_code,
    response_result_symbol,
};

#[test]
fn defaults_unclassified_failure_to_system_error_for_trace_visibility() {
    let mut headers = HeaderMap::new();

    ensure_result_headers(&mut headers, StatusCode::INTERNAL_SERVER_ERROR);

    assert_eq!(headers.get(&RESULT_CODE_HEADER).unwrap(), "5000");
    assert_eq!(headers.get(&RESULT_SYMBOL_HEADER).unwrap(), "SYSTEM_ERROR");
}

#[test]
fn falls_back_to_system_error_when_result_header_is_malformed() {
    let mut headers = HeaderMap::new();
    headers.insert(&RESULT_CODE_HEADER, "not-a-number".parse().unwrap());

    assert_eq!(response_result_code(&headers), 5000);
    assert_eq!(response_result_symbol(&headers), "SYSTEM_ERROR");
}
