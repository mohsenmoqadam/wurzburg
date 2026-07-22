mod support;

use std::{env, sync::Arc};

use tokio::net::TcpListener;
use uuid::Uuid;
use wurzburg::{
    api::router::build_app_router, config::Settings, db::oracle::prepare_oracle_schema,
    state::AppState,
};

/// Scenario goal: prove Provider event delivery configuration fails closed at
/// the real HTTP/WSO2 boundary.
///
/// Oracle facts: two Providers have the complete disabled version-1 catalog.
/// Final proof: incomplete replacement, stale optimistic version, reused
/// idempotency key with changed content, and cross-provider reads are rejected
/// without changing the active subscription set.
#[tokio::test]
async fn rejects_invalid_concurrent_and_cross_provider_subscription_commands() {
    if env::var("RUN_FULL_INTEGRATION_TESTS").ok().as_deref() != Some("1") {
        return;
    }
    let mut settings = Settings::new().expect("integration settings should load");
    settings.provider_kafka.enabled = false;
    prepare_oracle_schema(&settings.database, &settings.migrations)
        .await
        .expect("Oracle schema should be prepared before scenarios run");
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
    let client = reqwest::Client::new();
    let first = create_provider(&client, address, "first").await;
    let second = create_provider(&client, address, "second").await;

    let path = format!("/api/v1/admin/providers/{first}/event-subscriptions");
    let incomplete = serde_json::json!({
        "expected_version":1,
        "subscriptions":[{"event_type":"CREDIT_GRANTED","enabled":true}],
        "reason":"incomplete catalog must fail"
    })
    .to_string();
    let invalid = put(
        &client,
        address,
        &path,
        &Uuid::new_v4().to_string(),
        &incomplete,
    )
    .await;
    assert_eq!(invalid.0, reqwest::StatusCode::BAD_REQUEST, "{}", invalid.1);
    assert!(invalid.1.contains("PROVIDER_EVENT_SUBSCRIPTION_INVALID"));

    let key = Uuid::new_v4().to_string();
    let complete = complete_body(1, "CREDIT_GRANTED");
    let applied = put(&client, address, &path, &key, &complete).await;
    assert_eq!(applied.0, reqwest::StatusCode::OK, "{}", applied.1);

    let changed_same_key = complete_body(1, "CREDIT_RETURNED");
    let conflict = put(&client, address, &path, &key, &changed_same_key).await;
    assert_eq!(conflict.0, reqwest::StatusCode::CONFLICT, "{}", conflict.1);
    assert!(conflict.1.contains("IDEMPOTENCY_KEY_CONFLICT"));

    let stale = put(
        &client,
        address,
        &path,
        &Uuid::new_v4().to_string(),
        &complete_body(1, "CREDIT_RETURNED"),
    )
    .await;
    assert_eq!(stale.0, reqwest::StatusCode::CONFLICT, "{}", stale.1);
    assert!(
        stale
            .1
            .contains("PROVIDER_EVENT_SUBSCRIPTION_VERSION_CONFLICT")
    );

    let scoped = client
        .get(format!(
            "http://{address}/api/v1/providers/{second}/event-subscriptions"
        ))
        .bearer_auth(support::signed_provider_admin_jwt(first))
        .header("X-Correlation-Id", "provider-event-cross-provider")
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.31")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .send()
        .await
        .unwrap();
    let scoped_status = scoped.status();
    let scoped_body = scoped.text().await.unwrap();
    assert_eq!(
        scoped_status,
        reqwest::StatusCode::FORBIDDEN,
        "{scoped_body}"
    );
    assert!(scoped_body.contains("PROVIDER_SCOPE_MISMATCH"));
}

async fn create_provider(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    marker: &str,
) -> Uuid {
    let response = client
        .post(format!("http://{address}/api/v1/providers"))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("Idempotency-Key", Uuid::new_v4().to_string())
        .header("X-Correlation-Id", format!("provider-events-{marker}"))
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.31")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .header("Content-Type", "application/json")
        .body(provider_body(marker))
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert!(
        status == reqwest::StatusCode::CREATED || status == reqwest::StatusCode::ACCEPTED,
        "{body}"
    );
    Uuid::parse_str(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["provider_id"]
            .as_str()
            .unwrap(),
    )
    .unwrap()
}

async fn put(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    path: &str,
    key: &str,
    body: &str,
) -> (reqwest::StatusCode, String) {
    let response = client
        .put(format!("http://{address}{path}"))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("Idempotency-Key", key)
        .header("X-Correlation-Id", "provider-event-subscription-failure")
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.31")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .header("Content-Type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    let status = response.status();
    (status, response.text().await.unwrap())
}

fn complete_body(version: u64, enabled: &str) -> String {
    let types = [
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
    serde_json::json!({"expected_version":version,"subscriptions":types.into_iter().map(|event_type|serde_json::json!({"event_type":event_type,"enabled":event_type==enabled})).collect::<Vec<_>>(),"reason":"controlled delivery update"}).to_string()
}

fn provider_body(marker: &str) -> String {
    serde_json::json!({"legal_name":format!("Provider Event Failure {marker}"),"trade_name":"Provider Event Failure","metadata":{},"contacts":[],"operational_profile":{"effective_at":"2026-01-01T00:00:00Z","profile":{"timezone":"Asia/Tehran","user_onboarding":{"enabled":true,"active_windows":[],"max_total_users":null},"credit_grant":{"enabled":true,"mode":"FixedLimit","limit_amount_rials":1000000},"credit_return":{"enabled":true},"card_operations":{"new_assignment_enabled":true,"same_pan_reprint_enabled":true,"new_pan_replacement_enabled":true,"attach_existing_multi_provider_card_enabled":true},"event_delivery":{"enabled":true,"disabled_reason":null}}}}).to_string()
}
