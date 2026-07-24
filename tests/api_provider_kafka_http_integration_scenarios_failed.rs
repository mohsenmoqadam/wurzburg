mod support;

use std::{env, sync::Arc};

use tokio::net::TcpListener;
use uuid::Uuid;
use wurzburg::{
    api::router::build_app_router, config::Settings, db::oracle::prepare_oracle_schema,
    state::AppState,
};

/// Scenario goal: prove Provider Kafka secrets and lifecycle commands fail
/// closed while asynchronous broker provisioning is incomplete.
///
/// Oracle facts: provider creation commits encrypted candidate credential and
/// PENDING provisioning job records. No Provider Kafka worker is started.
/// HTTP facts: requests traverse the real router with signed WSO2 JWTs.
/// Final proof: pending secrets remain unavailable, an invalid rotation is
/// rejected, status exposes only safe job facts, and provider scope isolation
/// prevents cross-provider inspection.
#[tokio::test]
async fn rejects_pending_invalid_and_cross_provider_kafka_access() {
    if env::var("RUN_PROVIDER_KAFKA_INTEGRATION_TESTS")
        .ok()
        .as_deref()
        != Some("1")
    {
        return;
    }
    support::init_test_tracing();
    let mut settings = Settings::new().expect("integration settings should load");
    settings.provider_kafka_access.enabled = true;
    prepare_oracle_schema(&settings.database, &settings.migrations)
        .await
        .expect("Oracle schema should be prepared before scenarios run");
    settings.provider_core_provisioning.enabled = false;
    let state = Arc::new(AppState::new(settings).await.unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, build_app_router(state))
            .await
            .unwrap()
    });
    tokio::task::yield_now().await;
    let client = reqwest::Client::new();

    let created = request(
        &client,
        reqwest::Method::POST,
        address,
        "/api/v1/providers",
        &support::signed_platform_admin_jwt(),
        Some(Uuid::new_v4()),
        Some(&provider_body()),
    )
    .await;
    assert_eq!(created.0, reqwest::StatusCode::CREATED, "{}", created.1);
    let created_json: serde_json::Value = serde_json::from_str(&created.1).unwrap();
    let provider_id = Uuid::parse_str(created_json["provider_id"].as_str().unwrap()).unwrap();

    let pending_secret = request(
        &client,
        reqwest::Method::GET,
        address,
        &format!("/api/v1/providers/{provider_id}/kafka/credentials"),
        &support::signed_platform_admin_jwt(),
        None,
        None,
    )
    .await;
    assert_eq!(pending_secret.0, reqwest::StatusCode::CONFLICT);
    assert_eq!(
        error_code(&pending_secret.1),
        "PROVIDER_KAFKA_CREDENTIAL_NOT_AVAILABLE"
    );
    assert!(!pending_secret.1.contains("password_ciphertext"));

    let status = request(
        &client,
        reqwest::Method::GET,
        address,
        &format!("/api/v1/providers/{provider_id}/kafka/status"),
        &support::signed_platform_admin_jwt(),
        None,
        None,
    )
    .await;
    assert_eq!(status.0, reqwest::StatusCode::OK, "{}", status.1);
    let status_json: serde_json::Value = serde_json::from_str(&status.1).unwrap();
    assert_eq!(status_json["access_status"], "PROVISIONING");
    assert_eq!(status_json["candidate_credential_version"], 1);
    assert_eq!(status_json["latest_operation"]["status"], "PENDING");
    assert!(!status.1.contains("password"));

    let invalid_rotation = request(
        &client,
        reqwest::Method::POST,
        address,
        &format!("/api/v1/providers/{provider_id}/kafka/rotate-credentials"),
        &support::signed_platform_admin_jwt(),
        Some(Uuid::new_v4()),
        Some(r#"{"reason":"rotation while initial provisioning is pending"}"#),
    )
    .await;
    assert_eq!(invalid_rotation.0, reqwest::StatusCode::CONFLICT);
    assert_eq!(
        error_code(&invalid_rotation.1),
        "PROVIDER_KAFKA_INVALID_TRANSITION"
    );

    let cross_provider = request(
        &client,
        reqwest::Method::GET,
        address,
        &format!("/api/v1/providers/{provider_id}/kafka/status"),
        &support::signed_provider_admin_jwt(Uuid::new_v4()),
        None,
        None,
    )
    .await;
    assert_eq!(cross_provider.0, reqwest::StatusCode::FORBIDDEN);
    assert_eq!(error_code(&cross_provider.1), "PROVIDER_SCOPE_MISMATCH");
}

async fn request(
    client: &reqwest::Client,
    method: reqwest::Method,
    address: std::net::SocketAddr,
    path: &str,
    jwt: &str,
    idempotency_key: Option<Uuid>,
    body: Option<&str>,
) -> (reqwest::StatusCode, String) {
    let mut request = client
        .request(method, format!("http://{address}{path}"))
        .bearer_auth(jwt)
        .header("X-Correlation-Id", "provider-kafka-negative-scenario")
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.33")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test");
    if let Some(key) = idempotency_key {
        request = request.header("Idempotency-Key", key.to_string());
    }
    if let Some(body) = body {
        request = request
            .header("Content-Type", "application/json")
            .body(body.to_string());
    }
    let response = request.send().await.unwrap();
    let status = response.status();
    (status, response.text().await.unwrap())
}

fn error_code(body: &str) -> String {
    let value: serde_json::Value = serde_json::from_str(body).unwrap();
    value["error"]["code"].as_str().unwrap().to_string()
}

fn provider_body() -> String {
    serde_json::json!({"legal_name":"Provider Kafka Negative Scenario","trade_name":"Provider Kafka Negative","metadata":{},"contacts":[],"operational_profile":{"effective_at":"2026-01-01T00:00:00Z","profile":{"timezone":"Asia/Tehran","user_onboarding":{"enabled":true,"active_windows":[],"max_total_users":null},"credit_grant":{"enabled":true,"mode":"FixedLimit","limit_amount_rials":1000000},"credit_return":{"enabled":true},"card_operations":{"new_assignment_enabled":true,"same_pan_reprint_enabled":true,"new_pan_replacement_enabled":true,"attach_existing_multi_provider_card_enabled":true},"event_delivery":{"enabled":true,"disabled_reason":null}}}}).to_string()
}
