mod support;

use std::{
    env,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::net::TcpListener;
use uuid::Uuid;
use wurzburg::{
    api::router::build_app_router, config::Settings, db::oracle::prepare_oracle_schema,
    state::AppState,
};

#[tokio::test]
async fn creates_card_range_through_running_wurzburg_http_instance() {
    if env::var("RUN_FULL_INTEGRATION_TESTS").ok().as_deref() != Some("1") {
        return;
    }

    let settings = Settings::new().expect("full integration settings should load");
    prepare_oracle_schema(&settings.database, &settings.migrations)
        .await
        .expect("Oracle schema should be prepared before Wurzburg starts");
    let state = Arc::new(
        AppState::new(settings)
            .await
            .expect("Wurzburg application state should start"),
    );
    let app = build_app_router(state);
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let address = listener
        .local_addr()
        .expect("listener address should exist");

    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("Wurzburg HTTP instance should serve");
    });
    tokio::task::yield_now().await;

    let (start_card_number, end_card_number) = unique_card_range();
    let body = serde_json::json!({
        "start_card_number": start_card_number,
        "end_card_number": end_card_number,
        "funding_mode": "SINGLE_PROVIDER",
        "withdrawal_limit_authority": "PLATFORM",
        "limit_calendar": {
            "timezone": "Asia/Tehran",
            "week_starts_on": "SATURDAY",
            "window_mode": "CALENDAR"
        },
        "issuance_enabled": true,
        "cms_operation_mode": "FULL",
        "metadata": {
            "test_case": "api_card_range_http_integration_scenarios_success"
        }
    })
    .to_string();

    let response = reqwest::Client::new()
        .post(format!("http://{address}/api/v1/card-ranges"))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("Idempotency-Key", Uuid::new_v4().to_string())
        .header("X-Correlation-Id", "card-range-http-integration")
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.10")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await
        .expect("HTTP request should complete");

    let status = response.status();
    let result_symbol = response
        .headers()
        .get("x-wurzburg-result-symbol")
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);
    let response_body = response
        .text()
        .await
        .expect("HTTP response body should read");

    assert!(
        status == reqwest::StatusCode::CREATED,
        "unexpected HTTP response: status={status}, body={response_body}"
    );
    assert_eq!(result_symbol.as_deref(), Some("SUCCESS"));
    assert!(response_body.contains("\"start_card_number\""));
}

fn unique_card_range() -> (String, String) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after Unix epoch")
        .as_nanos();
    let suffix = 1_000_000_000_u128 + (now % 8_999_999_000_u128);
    (
        format!("621986{suffix:010}"),
        format!("621986{:010}", suffix + 9),
    )
}
