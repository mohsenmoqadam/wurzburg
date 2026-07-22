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

/// Scenario goal: create a Provider through the real HTTP/WSO2 boundary and
/// prove the Oracle command, audit, idempotency replay, and all four live
/// TigerBeetle account contracts. Kafka provisioning remains independently
/// pending and is never represented as successful by this API.
#[tokio::test]
async fn creates_and_replays_provider_with_four_verified_accounts() {
    if env::var("RUN_FULL_INTEGRATION_TESTS").ok().as_deref() != Some("1") {
        return;
    }

    let settings = Settings::new().expect("integration settings should load");
    prepare_oracle_schema(&settings.database, &settings.migrations)
        .await
        .expect("Oracle schema should be prepared");
    let state = Arc::new(
        AppState::new(settings)
            .await
            .expect("Wurzburg state should start"),
    );
    let repository = state.db.clone();
    let tb_client = state.tb_client.clone();
    let app = build_app_router(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    tokio::task::yield_now().await;

    let idempotency_key = Uuid::new_v4().to_string();
    let body = provider_body("provider-create-success");
    let client = reqwest::Client::new();
    let (status, response_body) = post_provider(
        &client,
        address,
        &idempotency_key,
        "provider-create-success",
        &body,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::CREATED, "{response_body}");
    let created: serde_json::Value = serde_json::from_str(&response_body).unwrap();
    assert_eq!(created["status"], "READY");
    assert_eq!(created["core_provisioning_status"], "SUCCEEDED");
    assert_eq!(created["kafka_provisioning_status"], "PENDING");
    let provider_id = Uuid::parse_str(created["provider_id"].as_str().unwrap()).unwrap();

    let (activate_status, activate_body) = post_json(
        &client,
        address,
        &format!("/api/v1/providers/{provider_id}/activate"),
        &Uuid::new_v4().to_string(),
        "provider-activate",
        &serde_json::json!({"reason": "provider passed core provisioning"}).to_string(),
    )
    .await;
    assert_eq!(activate_status, reqwest::StatusCode::OK, "{activate_body}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&activate_body).unwrap()["status"],
        "ACTIVE"
    );

    // Card facts: a DRAFT range and policy are created through their public
    // APIs. The first active Provider assignment must freeze CPOL and emit both
    // CPOL and CRCTL commands in the same Oracle transaction.
    let card_range_id = create_range_and_policy(&client, address).await;
    let assignment_body = serde_json::json!({
        "card_range_id": card_range_id,
        "reason": "initial provider eligibility"
    })
    .to_string();
    let assignment_response = client
        .put(format!(
            "http://{address}/api/v1/providers/{provider_id}/card-range"
        ))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("Idempotency-Key", Uuid::new_v4().to_string())
        .header("X-Correlation-Id", "provider-range-assign")
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.20")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .header("Content-Type", "application/json")
        .body(assignment_body)
        .send()
        .await
        .unwrap();
    let assignment_status = assignment_response.status();
    let assignment_text = assignment_response.text().await.unwrap();
    assert_eq!(
        assignment_status,
        reqwest::StatusCode::ACCEPTED,
        "{assignment_text}"
    );
    let assignment: serde_json::Value = serde_json::from_str(&assignment_text).unwrap();
    assert!(assignment["policy_operation_id"].is_string());
    assert!(assignment["range_control_operation_id"].is_string());

    let mappings = repository
        .get_provider_ledger_mappings(provider_id)
        .await
        .expect("provider mappings should load");
    assert_eq!(mappings.len(), 4);
    for mapping in mappings {
        let accounts = tb_client
            .lookup_account(mapping.tigerbeetle_account_id.as_u128())
            .await
            .expect("TigerBeetle account lookup should succeed");
        assert_eq!(accounts.len(), 1);
        assert_eq!(accounts[0].user_data_128, provider_id.as_u128());
    }

    let (replay_status, replay_body) = post_provider(
        &client,
        address,
        &idempotency_key,
        "provider-create-replay",
        &body,
    )
    .await;
    assert_eq!(replay_status, reqwest::StatusCode::OK, "{replay_body}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&replay_body).unwrap(),
        created
    );

    let changed = provider_body("different-request");
    let (conflict_status, conflict_body) = post_provider(
        &client,
        address,
        &idempotency_key,
        "provider-create-conflict",
        &changed,
    )
    .await;
    assert_eq!(conflict_status, reqwest::StatusCode::CONFLICT);
    assert!(conflict_body.contains("IDEMPOTENCY_KEY_CONFLICT"));
}

async fn create_range_and_policy(client: &reqwest::Client, address: std::net::SocketAddr) -> Uuid {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        % 9_000_000_000;
    let start = format!("621987{:010}", 1_000_000_000_u128 + suffix);
    let end_value = start.parse::<u64>().unwrap() + 9;
    let range_body = serde_json::json!({
        "start_card_number": start,
        "end_card_number": format!("{end_value:016}"),
        "funding_mode": "SINGLE_PROVIDER",
        "withdrawal_limit_authority": "PLATFORM",
        "limit_calendar": {"timezone":"Asia/Tehran","week_starts_on":"SATURDAY","window_mode":"CALENDAR"},
        "issuance_enabled": true,
        "cms_operation_mode": "FULL",
        "metadata": {}
    }).to_string();
    let (status, body) = post_json(
        client,
        address,
        "/api/v1/card-ranges",
        &Uuid::new_v4().to_string(),
        "provider-range-create",
        &range_body,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::CREATED, "{body}");
    let range_id = Uuid::parse_str(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["card_range_id"]
            .as_str()
            .unwrap(),
    )
    .unwrap();

    let policy_body = serde_json::json!({
        "reason": "initial provider range policy",
        "withdrawal_limits": {
            "per_transaction_min_amount": null,
            "per_transaction_max_amount": null,
            "daily": {"max_amount": null,"max_count": null},
            "weekly": null,"monthly": null,"yearly": null
        }
    })
    .to_string();
    let response = client
        .put(format!(
            "http://{address}/api/v1/card-ranges/{range_id}/policy"
        ))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("Idempotency-Key", Uuid::new_v4().to_string())
        .header("X-Correlation-Id", "provider-range-policy")
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.20")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .header("Content-Type", "application/json")
        .body(policy_body)
        .send()
        .await
        .unwrap();
    let policy_status = response.status();
    let policy_text = response.text().await.unwrap();
    assert_eq!(policy_status, reqwest::StatusCode::CREATED, "{policy_text}");
    range_id
}

async fn post_json(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    path: &str,
    key: &str,
    correlation: &str,
    body: &str,
) -> (reqwest::StatusCode, String) {
    let response = client
        .post(format!("http://{address}{path}"))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("Idempotency-Key", key)
        .header("X-Correlation-Id", correlation)
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.20")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .header("Content-Type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    let status = response.status();
    let text = response.text().await.unwrap();
    (status, text)
}

fn provider_body(marker: &str) -> String {
    serde_json::json!({
        "legal_name": format!("Integration Legal Provider {marker}"),
        "trade_name": "Integration Provider",
        "tax_id": null,
        "registration_number": null,
        "email_address": null,
        "website_url": null,
        "mailing_address": null,
        "metadata": { "marker": marker },
        "contacts": [],
        "operational_profile": {
            "effective_at": "2026-01-01T00:00:00Z",
            "profile": {
                "timezone": "Asia/Tehran",
                "user_onboarding": { "enabled": true, "active_windows": [], "max_total_users": null },
                "credit_grant": { "enabled": true, "mode": "FixedLimit", "limit_amount_rials": 1000000000 },
                "credit_return": { "enabled": true },
                "card_operations": {
                    "new_assignment_enabled": true,
                    "same_pan_reprint_enabled": true,
                    "new_pan_replacement_enabled": true,
                    "attach_existing_multi_provider_card_enabled": true
                },
                "event_delivery": { "enabled": true, "disabled_reason": null }
            }
        }
    }).to_string()
}

async fn post_provider(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    idempotency_key: &str,
    correlation_id: &str,
    body: &str,
) -> (reqwest::StatusCode, String) {
    let response = client
        .post(format!("http://{address}/api/v1/providers"))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("Idempotency-Key", idempotency_key)
        .header("X-Correlation-Id", correlation_id)
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.20")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .header("Content-Type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .expect("provider HTTP request should complete");
    let status = response.status();
    let body = response
        .text()
        .await
        .expect("provider response should read");
    (status, body)
}
