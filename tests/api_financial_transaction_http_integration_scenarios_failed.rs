mod support;

use std::{env, sync::Arc};

use tokio::net::TcpListener;
use uuid::Uuid;
use wurzburg::{
    api::router::build_app_router, config::Settings, db::oracle::prepare_oracle_schema,
    state::AppState,
};

/// Scenario goal:
/// Prove that malformed filters and cross-tenant transaction reads fail at the
/// real HTTP/WSO2 boundary before any Oracle data can leak.
///
/// Oracle facts: an empty production schema is sufficient because every case
/// must be rejected by authorization or contract validation before row access.
/// TigerBeetle facts: none; transaction history never derives balances.
/// Final proof: provider, cardholder, admin, cursor, and time-window violations
/// return centralized safe errors without internal Oracle details.
#[tokio::test]
async fn rejects_unsafe_financial_transaction_queries_through_running_server() {
    if env::var("RUN_FULL_INTEGRATION_TESTS").ok().as_deref() != Some("1") {
        return;
    }

    let mut settings = Settings::new().expect("full integration settings should load");
    settings.migrations.force_recreate = true;
    prepare_oracle_schema(&settings.database, &settings.migrations)
        .await
        .expect("Oracle schema should be rebuilt from the final baseline");
    settings.migrations.force_recreate = false;
    let state = Arc::new(AppState::new(settings).await.unwrap());
    let address = start_server(state).await;
    let client = reqwest::Client::new();
    let provider_id = Uuid::new_v4();
    let user_id = Uuid::new_v4();

    let cross_provider = get(
        &client,
        address,
        &format!("/api/v1/providers/{}/transactions", Uuid::new_v4()),
        &support::signed_provider_admin_jwt(provider_id),
    )
    .await;
    assert_eq!(cross_provider.0, reqwest::StatusCode::FORBIDDEN);
    assert!(cross_provider.1.contains("PROVIDER_SCOPE_MISMATCH"));

    let cross_cardholder = get(
        &client,
        address,
        &format!("/api/v1/users/{}/transactions", Uuid::new_v4()),
        &support::signed_cardholder_jwt(user_id),
    )
    .await;
    assert_eq!(cross_cardholder.0, reqwest::StatusCode::FORBIDDEN);
    assert!(cross_cardholder.1.contains("CARDHOLDER_SCOPE_MISMATCH"));

    let provider_on_admin = get(
        &client,
        address,
        "/api/v1/admin/transactions",
        &support::signed_provider_admin_jwt(provider_id),
    )
    .await;
    assert_eq!(provider_on_admin.0, reqwest::StatusCode::FORBIDDEN);
    assert!(provider_on_admin.1.contains("MISSING_REQUIRED_SCOPE"));

    for path in [
        "/api/v1/admin/transactions?page_size=0",
        "/api/v1/admin/transactions?transaction_type=UNKNOWN",
        "/api/v1/admin/transactions?page_token=not-hex",
        "/api/v1/admin/transactions?occurred_from=2026-08-02T00%3A00%3A00Z&occurred_to=2026-08-01T00%3A00%3A00Z",
        "/api/v1/admin/transactions?card_number=0111000000000001",
    ] {
        let response = get(
            &client,
            address,
            path,
            &support::signed_platform_admin_jwt(),
        )
        .await;
        assert_eq!(response.0, reqwest::StatusCode::BAD_REQUEST, "{path}");
        assert!(response.1.contains("INVALID_TRANSACTION_QUERY"), "{path}");
        assert!(!response.1.contains("ORA-"), "{path}");
    }
}

async fn get(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    path: &str,
    jwt: &str,
) -> (reqwest::StatusCode, String) {
    let response = client
        .get(format!("http://{address}{path}"))
        .bearer_auth(jwt)
        .header("X-Correlation-Id", Uuid::new_v4().to_string())
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.20")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    (status, body)
}

async fn start_server(state: Arc<AppState>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, build_app_router(state))
            .await
            .unwrap();
    });
    address
}
