use wurzburg::api::{error::ApiError, result_codes::WurzburgResultCode};
use wurzburg::db::error::DbError;

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
