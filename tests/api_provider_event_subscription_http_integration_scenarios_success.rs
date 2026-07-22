mod support;

use std::{env, sync::Arc};

use tokio::net::TcpListener;
use uuid::Uuid;
use wurzburg::{
    api::router::build_app_router, config::Settings, db::oracle::prepare_oracle_schema,
    state::AppState,
};

/// Scenario goal: prove the public Provider event catalog and complete-set
/// subscription replacement through a real Wurzburg HTTP/WSO2 boundary.
///
/// Oracle facts: one Provider is created with all nine catalog rows disabled,
/// then activated while Kafka access remains disabled.
/// Kafka facts: no broker operation is needed; the missing active credential is
/// deliberately one of the effective-delivery gates.
/// Final proof: the catalog is allowlisted, replacement is versioned and
/// idempotent, and enabled events remain effectively blocked until Kafka access
/// is active.
#[tokio::test]
async fn replaces_and_replays_complete_provider_event_subscription_set() {
    if env::var("RUN_FULL_INTEGRATION_TESTS").ok().as_deref() != Some("1") {
        return;
    }
    support::init_test_tracing();
    let mut settings = Settings::new().expect("integration settings should load");
    settings.provider_kafka.enabled = false;
    prepare_oracle_schema(&settings.database, &settings.migrations)
        .await
        .expect("Oracle schema should be prepared before scenarios run");
    let address = start_server(settings).await;
    let client = reqwest::Client::new();

    let provider = post(
        &client,
        address,
        "/api/v1/providers",
        &Uuid::new_v4().to_string(),
        &provider_body(),
    )
    .await;
    assert_eq!(provider.0, reqwest::StatusCode::CREATED, "{}", provider.1);
    let provider_id = Uuid::parse_str(
        serde_json::from_str::<serde_json::Value>(&provider.1).unwrap()["provider_id"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    let activation = post(
        &client,
        address,
        &format!("/api/v1/providers/{provider_id}/activate"),
        &Uuid::new_v4().to_string(),
        &serde_json::json!({"reason":"event subscription scenario activation"}).to_string(),
    )
    .await;
    assert_eq!(activation.0, reqwest::StatusCode::OK, "{}", activation.1);

    let catalog = get(&client, address, "/api/v1/admin/provider-event-types").await;
    assert_eq!(catalog.0, reqwest::StatusCode::OK, "{}", catalog.1);
    let catalog_json: serde_json::Value = serde_json::from_str(&catalog.1).unwrap();
    assert_eq!(catalog_json["data"].as_array().unwrap().len(), 9);
    assert!(!catalog.1.contains("INTERNAL"));

    let before = get(
        &client,
        address,
        &format!("/api/v1/admin/providers/{provider_id}/event-subscriptions"),
    )
    .await;
    assert_eq!(before.0, reqwest::StatusCode::OK, "{}", before.1);
    let before_json: serde_json::Value = serde_json::from_str(&before.1).unwrap();
    assert_eq!(before_json["version"], 1);
    assert!(
        before_json["subscriptions"]
            .as_array()
            .unwrap()
            .iter()
            .all(|entry| entry["configured_enabled"] == false)
    );

    let key = Uuid::new_v4().to_string();
    let replacement = complete_subscription_body(1, true);
    let applied = put(
        &client,
        address,
        &format!("/api/v1/admin/providers/{provider_id}/event-subscriptions"),
        &key,
        &replacement,
    )
    .await;
    assert_eq!(applied.0, reqwest::StatusCode::OK, "{}", applied.1);
    let applied_json: serde_json::Value = serde_json::from_str(&applied.1).unwrap();
    assert_eq!(applied_json["version"], 2);
    let credit_granted = applied_json["subscriptions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["event_type"] == "CREDIT_GRANTED")
        .unwrap();
    assert_eq!(credit_granted["configured_enabled"], true);
    assert_eq!(credit_granted["effective_enabled"], false);
    assert!(
        credit_granted["blocked_by"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value == "KAFKA_CREDENTIAL_NOT_ACTIVE")
    );

    let replay = put(
        &client,
        address,
        &format!("/api/v1/admin/providers/{provider_id}/event-subscriptions"),
        &key,
        &replacement,
    )
    .await;
    assert_eq!(replay.0, reqwest::StatusCode::OK, "{}", replay.1);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&replay.1).unwrap(),
        applied_json
    );
}

async fn start_server(mut settings: Settings) -> std::net::SocketAddr {
    settings.provider_provisioning.enabled = false;
    let state = Arc::new(AppState::new(settings).await.unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, build_app_router(state))
            .await
            .unwrap()
    });
    tokio::task::yield_now().await;
    address
}

async fn post(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    path: &str,
    key: &str,
    body: &str,
) -> (reqwest::StatusCode, String) {
    request(client.post(format!("http://{address}{path}")), key, body).await
}

async fn put(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    path: &str,
    key: &str,
    body: &str,
) -> (reqwest::StatusCode, String) {
    request(client.put(format!("http://{address}{path}")), key, body).await
}

async fn request(
    builder: reqwest::RequestBuilder,
    key: &str,
    body: &str,
) -> (reqwest::StatusCode, String) {
    let response = builder
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("Idempotency-Key", key)
        .header("X-Correlation-Id", "provider-event-subscription-success")
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.30")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .header("Content-Type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    let status = response.status();
    (status, response.text().await.unwrap())
}

async fn get(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    path: &str,
) -> (reqwest::StatusCode, String) {
    let response = client
        .get(format!("http://{address}{path}"))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("X-Correlation-Id", "provider-event-subscription-read")
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.30")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .send()
        .await
        .unwrap();
    let status = response.status();
    (status, response.text().await.unwrap())
}

fn complete_subscription_body(expected_version: u64, enable_credit: bool) -> String {
    let event_types = [
        "PROVIDER_STATUS_CHANGED",
        "USER_ONBOARDED",
        "CARD_ASSIGNED",
        "CARD_REPLACED",
        "CREDIT_GRANTED",
        "CREDIT_RETURNED",
        "WITHDRAWAL_CONFIRMED",
        "WITHDRAWAL_ROLLED_BACK",
        "FEE_CHARGED",
    ];
    serde_json::json!({
        "expected_version": expected_version,
        "subscriptions": event_types.into_iter().map(|event_type| serde_json::json!({"event_type":event_type,"enabled":enable_credit && event_type=="CREDIT_GRANTED"})).collect::<Vec<_>>(),
        "reason": "enable only the verified credit event contract"
    }).to_string()
}

fn provider_body() -> String {
    serde_json::json!({
        "legal_name":"Provider Event Subscription Scenario",
        "trade_name":"Provider Event Scenario",
        "metadata":{},"contacts":[],
        "operational_profile":{"effective_at":"2026-01-01T00:00:00Z","profile":{
            "timezone":"Asia/Tehran","user_onboarding":{"enabled":true,"active_windows":[],"max_total_users":null},
            "credit_grant":{"enabled":true,"mode":"FixedLimit","limit_amount_rials":1000000},
            "credit_return":{"enabled":true},"card_operations":{"new_assignment_enabled":true,"same_pan_reprint_enabled":true,"new_pan_replacement_enabled":true,"attach_existing_multi_provider_card_enabled":true},
            "event_delivery":{"enabled":true,"disabled_reason":null}
        }}
    }).to_string()
}
