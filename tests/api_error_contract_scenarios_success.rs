use wurzburg::api::{error::ApiError, result_codes::WurzburgResultCode};

#[test]
fn renders_structured_error_envelope() {
    let error = ApiError::with_details(
        WurzburgResultCode::MissingTrustedRequestHeader,
        serde_json::json!({ "header": "x-correlation-id" }),
    );
    let body = error.body();

    assert_eq!(body.error.rs_code, 6201);
    assert_eq!(body.error.code, "MISSING_TRUSTED_REQUEST_HEADER");
    assert_eq!(
        body.error.details,
        serde_json::json!({ "header": "x-correlation-id" })
    );
}
