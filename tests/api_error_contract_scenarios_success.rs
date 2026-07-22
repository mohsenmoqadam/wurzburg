use axum::{body::to_bytes, http::StatusCode};
use serde::Serialize;
use wurzburg::db::error::DbError;
use wurzburg::{
    api::{error::ApiError, response::success_response, result_codes::WurzburgResultCode},
    telemetry::http::{RESULT_CODE_HEADER, RESULT_SYMBOL_HEADER},
};

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

#[test]
fn hides_oracle_query_details_from_public_error_contract() {
    let error = ApiError::from_database(DbError::Query(
        "ORA-00942: internal table and SQL details".to_string(),
    ));
    let body = error.body();

    assert_eq!(body.error.rs_code, 5000);
    assert_eq!(body.error.code, "SYSTEM_ERROR");
    assert_eq!(body.error.message, "An internal system error occurred");
    assert!(!body.error.message.contains("ORA-"));
}

#[test]
fn maps_oracle_connectivity_failure_without_exposing_driver_details() {
    let error = ApiError::from_database(DbError::Connection(
        "ORA-12541: listener endpoint details".to_string(),
    ));
    let body = error.body();

    assert_eq!(body.error.rs_code, 6300);
    assert_eq!(body.error.code, "DATABASE_UNAVAILABLE");
    assert_eq!(body.error.message, "Oracle database is unavailable");
    assert!(!body.error.message.contains("ORA-"));
}

#[tokio::test]
async fn renders_success_response_with_central_result_headers() {
    let response = success_response(
        StatusCode::CREATED,
        serde_json::json!({ "card_range_id": "018f9e64-1b5f-7cc1-a3cf-2a519179f801" }),
    )
    .expect("success response should serialize");

    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(response.headers().get(&RESULT_CODE_HEADER).unwrap(), "0");
    assert_eq!(
        response.headers().get(&RESULT_SYMBOL_HEADER).unwrap(),
        "SUCCESS"
    );

    let body = to_bytes(response.into_body(), 1024)
        .await
        .expect("body should be readable");
    let value: serde_json::Value =
        serde_json::from_slice(&body).expect("body should be valid JSON");

    assert_eq!(
        value,
        serde_json::json!({ "card_range_id": "018f9e64-1b5f-7cc1-a3cf-2a519179f801" })
    );
}

#[test]
fn maps_success_response_serialization_failure_before_success_headers_exist() {
    #[derive(Debug)]
    struct FailingBody;

    impl Serialize for FailingBody {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom(
                "intentional serialization failure",
            ))
        }
    }

    let error = success_response(StatusCode::OK, FailingBody)
        .expect_err("serialization failure should become ApiError");
    let body = error.body();

    assert_eq!(error.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body.error.rs_code, 5001);
    assert_eq!(body.error.code, "SERIALIZATION_ERROR");
    assert_eq!(body.error.message, "Response serialization failed");
}
