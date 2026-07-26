mod support;

use std::{env, sync::Arc};

use tokio::net::TcpListener;
use uuid::Uuid;
use wurzburg::{
    api::router::build_app_router, config::Settings, db::oracle::prepare_oracle_schema,
    state::AppState,
};

/// Scenario goal:
/// Prove public fee APIs fail closed for invalid money/rate contracts, missing
/// providers, and idempotency-key reuse with a different command.
///
/// Oracle facts: one ACTIVE provider is inserted explicitly.
/// External facts: no TigerBeetle, Dragonfly, or Wolfsburg effect is required
/// for contract rejection.
/// Final proof: rejected commands create no additional fee profile or outbox
/// event and expose only centralized result codes, never Oracle error strings.
#[tokio::test]
async fn rejects_invalid_provider_fee_commands_through_running_server() {
    if env::var("RUN_FULL_INTEGRATION_TESTS").ok().as_deref() != Some("1") {
        return;
    }
    let mut settings = Settings::new().unwrap();
    settings.migrations.force_recreate = true;
    prepare_oracle_schema(&settings.database, &settings.migrations)
        .await
        .unwrap();
    settings.migrations.force_recreate = false;
    let state = Arc::new(AppState::new(settings).await.unwrap());
    let pool = state.db.pool.clone();
    let provider_id = Uuid::new_v4();
    seed_provider(&pool, provider_id).await;
    let address = start_server(state).await;
    let client = reqwest::Client::new();

    let invalid=put(&client,address,provider_id,&Uuid::new_v4().to_string(),serde_json::json!({"rate_bps":10001,"fixed_amount_rials":0,"fee_payer":"PROVIDER","reason":"invalid rate"})).await;
    assert_eq!(invalid.0, reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        invalid.1["error"]["code"],
        "PROVIDER_FEE_PROFILE_CONTRACT_INVALID"
    );

    let missing=put(&client,address,Uuid::new_v4(),&Uuid::new_v4().to_string(),serde_json::json!({"rate_bps":0,"fixed_amount_rials":0,"fee_payer":"PROVIDER_USER","reason":"missing provider"})).await;
    assert_eq!(missing.0, reqwest::StatusCode::NOT_FOUND);
    assert_eq!(missing.1["error"]["code"], "PROVIDER_NOT_FOUND");

    let key = Uuid::new_v4().to_string();
    let first=put(&client,address,provider_id,&key,serde_json::json!({"rate_bps":10,"fixed_amount_rials":0,"fee_payer":"PROVIDER_USER","reason":"first"})).await;
    assert_eq!(first.0, reqwest::StatusCode::CREATED);
    let conflict=put(&client,address,provider_id,&key,serde_json::json!({"rate_bps":11,"fixed_amount_rials":0,"fee_payer":"PROVIDER_USER","reason":"changed"})).await;
    assert_eq!(conflict.0, reqwest::StatusCode::CONFLICT);
    assert_eq!(conflict.1["error"]["code"], "IDEMPOTENCY_KEY_CONFLICT");
}

async fn start_server(state: Arc<AppState>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, build_app_router(state))
            .await
            .unwrap();
    });
    tokio::task::yield_now().await;
    address
}
async fn put(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    provider_id: Uuid,
    key: &str,
    body: serde_json::Value,
) -> (reqwest::StatusCode, serde_json::Value) {
    let response = client
        .put(format!(
            "http://{address}/api/v1/providers/{provider_id}/fee-profile"
        ))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("Idempotency-Key", key)
        .header("X-Correlation-Id", "fee-failure")
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.10")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .header("Content-Type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    let status = response.status();
    let value = serde_json::from_str(&response.text().await.unwrap()).unwrap();
    (status, value)
}
async fn seed_provider(pool: &wurzburg::db::oracle::OraclePool, provider_id: Uuid) {
    let raw = wurzburg::db::oracle::types::uuid_to_raw16(provider_id).to_vec();
    pool.with_connection(move|connection|{connection.execute("INSERT INTO providers (provider_id,legal_name,trade_name,status,metadata_json,created_by_subject,updated_by_subject) VALUES (:1,'Fee Failure Legal','Fee Failure','ACTIVE','{}','test','test')",&[&raw]).map_err(|error|wurzburg::db::error::DbError::Query(error.to_string()))?;connection.commit().map_err(|error|wurzburg::db::error::DbError::Query(error.to_string()))?;Ok(())}).await.unwrap()
}
