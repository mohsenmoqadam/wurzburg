mod support;

use std::{env, sync::Arc};

use chrono::Utc;
use tokio::net::TcpListener;
use uuid::Uuid;
use wurzburg::{
    api::router::build_app_router,
    config::Settings,
    db::oracle::{OraclePool, ProviderFeeReceiptPersistenceOutcome, prepare_oracle_schema},
    domain::provider_fee::ProviderFeeMaterializationReceipt,
    state::AppState,
};

/// Scenario goal:
/// Prove the provider fee lifecycle through a real Wurzburg HTTP server and
/// production Oracle persistence: editable draft, idempotent replay,
/// attachment-triggered publication, receipt-gated activation, and history.
///
/// Oracle facts: one ACTIVE provider and one card-range relationship are made
/// explicit in this test. No hidden fixture creates fee state.
/// External facts: the Wolfsburg receipt is applied through the concrete inbox
/// repository path; no Dragonfly shortcut is added to production code.
/// Final proof: only a matching receipt makes a fee profile ACTIVE, while a
/// frozen draft rejects edits and its outbox payload preserves the fee payer.
#[tokio::test]
async fn manages_provider_fee_lifecycle_through_running_server_and_oracle() {
    if env::var("RUN_FULL_INTEGRATION_TESTS").ok().as_deref() != Some("1") {
        return;
    }
    let mut settings = Settings::new().expect("integration settings should load");
    settings.migrations.force_recreate = true;
    prepare_oracle_schema(&settings.database, &settings.migrations)
        .await
        .expect("Oracle schema should be rebuilt from the final baseline");
    settings.migrations.force_recreate = false;
    let state = Arc::new(AppState::new(settings).await.unwrap());
    let repository = state.db.clone();
    let provider_id = Uuid::new_v4();
    seed_provider(&repository.pool, provider_id).await;
    let address = start_server(state).await;
    let client = reqwest::Client::new();

    let key = Uuid::new_v4().to_string();
    let body = fee_body(125, 5_000, "PROVIDER_USER", "initial fee draft");
    let (status, draft) = put_fee(&client, address, provider_id, &key, &body).await;
    assert_eq!(status, reqwest::StatusCode::CREATED, "{draft}");
    assert_eq!(draft["profile"]["status"], "DRAFT");
    assert!(draft["operation_id"].is_null());
    let profile_id = Uuid::parse_str(
        draft["profile"]["provider_fee_profile_id"]
            .as_str()
            .unwrap(),
    )
    .unwrap();

    let (replay_status, replay) = put_fee(&client, address, provider_id, &key, &body).await;
    assert_eq!(replay_status, reqwest::StatusCode::OK);
    assert_eq!(replay, draft);

    let current = get_fee(&client, address, provider_id).await;
    assert_eq!(current.0, reqwest::StatusCode::NOT_FOUND);
    let history = get_json(
        &client,
        address,
        format!("/api/v1/providers/{provider_id}/fee-profiles"),
    )
    .await;
    assert_eq!(history.0, reqwest::StatusCode::OK);
    assert_eq!(
        history.1["items"][0]["provider_fee_profile_id"],
        profile_id.to_string()
    );

    seed_range_relationship(&repository.pool, provider_id).await;
    let publish_body = fee_body(150, 7_500, "PROVIDER", "publish attached provider fee");
    let (publish_status, pending) = put_fee(
        &client,
        address,
        provider_id,
        &Uuid::new_v4().to_string(),
        &publish_body,
    )
    .await;
    assert_eq!(publish_status, reqwest::StatusCode::ACCEPTED, "{pending}");
    assert_eq!(pending["disposition"], "PUBLICATION_PENDING");
    let operation_id = Uuid::parse_str(pending["operation_id"].as_str().unwrap()).unwrap();

    let frozen = put_fee(
        &client,
        address,
        provider_id,
        &Uuid::new_v4().to_string(),
        &fee_body(200, 0, "PROVIDER", "unsafe edit"),
    )
    .await;
    assert_eq!(frozen.0, reqwest::StatusCode::CONFLICT);
    assert_eq!(
        frozen.1["error"]["code"],
        "PROVIDER_FEE_PROFILE_DRAFT_FROZEN"
    );

    let receipt = ProviderFeeMaterializationReceipt {
        receipt_event_id: Uuid::new_v4(),
        operation_id,
        provider_id,
        provider_fee_profile_id: profile_id,
        materialized_version: 1,
        runtime_key: format!("FEE:{provider_id}"),
        materialized_at: Utc::now(),
    };
    assert!(matches!(
        repository
            .apply_provider_fee_materialization_receipt(receipt.clone())
            .await
            .unwrap(),
        ProviderFeeReceiptPersistenceOutcome::Activated(_)
    ));
    assert_eq!(
        repository
            .apply_provider_fee_materialization_receipt(receipt)
            .await
            .unwrap(),
        ProviderFeeReceiptPersistenceOutcome::Replayed
    );

    let active = get_fee(&client, address, provider_id).await;
    assert_eq!(active.0, reqwest::StatusCode::OK, "{}", active.1);
    assert_eq!(active.1["status"], "ACTIVE");
    assert_eq!(active.1["fee_policy"]["fee_payer"], "PROVIDER");
    assert_fee_outbox(&repository.pool, operation_id).await;
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

fn fee_body(rate: u32, fixed: u64, payer: &str, reason: &str) -> String {
    serde_json::json!({"rate_bps":rate,"fixed_amount_rials":fixed,"fee_payer":payer,"reason":reason}).to_string()
}

async fn put_fee(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    provider_id: Uuid,
    key: &str,
    body: &str,
) -> (reqwest::StatusCode, serde_json::Value) {
    let response = client
        .put(format!(
            "http://{address}/api/v1/providers/{provider_id}/fee-profile"
        ))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("Idempotency-Key", key)
        .header("X-Correlation-Id", "fee-scenario")
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

async fn get_fee(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    provider_id: Uuid,
) -> (reqwest::StatusCode, serde_json::Value) {
    get_json(
        client,
        address,
        format!("/api/v1/providers/{provider_id}/fee-profile"),
    )
    .await
}

async fn get_json(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    path: String,
) -> (reqwest::StatusCode, serde_json::Value) {
    let response = client
        .get(format!("http://{address}{path}"))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("X-Correlation-Id", "fee-read")
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.10")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .send()
        .await
        .unwrap();
    let status = response.status();
    let value = serde_json::from_str(&response.text().await.unwrap()).unwrap();
    (status, value)
}

async fn seed_provider(pool: &OraclePool, provider_id: Uuid) {
    let raw = wurzburg::db::oracle::types::uuid_to_raw16(provider_id).to_vec();
    pool.with_connection(move|connection|{connection.execute("INSERT INTO providers (provider_id,legal_name,trade_name,status,metadata_json,created_by_subject,updated_by_subject) VALUES (:1,'Fee Test Legal','Fee Test','ACTIVE','{}','test','test')",&[&raw]).map_err(|error|wurzburg::db::error::DbError::Query(error.to_string()))?;connection.commit().map_err(|error|wurzburg::db::error::DbError::Query(error.to_string()))?;Ok(())}).await.unwrap()
}

async fn seed_range_relationship(pool: &OraclePool, provider_id: Uuid) {
    let provider = wurzburg::db::oracle::types::uuid_to_raw16(provider_id).to_vec();
    let range_id = wurzburg::db::oracle::types::uuid_to_raw16(Uuid::new_v4()).to_vec();
    pool.with_connection(move|connection|{connection.execute("INSERT INTO card_ranges (card_range_id,start_card_number,end_card_number,funding_mode,withdrawal_limit_authority,limit_calendar_json,status,created_by_subject,updated_by_subject) VALUES (:1,'8111000000000000','8111000000000099','SINGLE_PROVIDER','PLATFORM','{\"timezone\":\"Asia/Tehran\",\"week_starts_on\":\"SATURDAY\",\"window_mode\":\"CALENDAR\"}','DRAFT','test','test')",&[&range_id]).map_err(|error|wurzburg::db::error::DbError::Query(error.to_string()))?;connection.execute("INSERT INTO card_range_providers (card_range_id,provider_id,status,created_by_subject,updated_by_subject) VALUES (:1,:2,'ACTIVE','test','test')",&[&range_id,&provider]).map_err(|error|wurzburg::db::error::DbError::Query(error.to_string()))?;connection.commit().map_err(|error|wurzburg::db::error::DbError::Query(error.to_string()))?;Ok(())}).await.unwrap()
}

async fn assert_fee_outbox(pool: &OraclePool, operation_id: Uuid) {
    let raw = wurzburg::db::oracle::types::uuid_to_raw16(operation_id).to_vec();
    pool.with_connection(move|connection|{let payload:String=connection.query_row_as("SELECT JSON_SERIALIZE(payload_json RETURNING CLOB) FROM integration_outbox WHERE operation_id=:1 AND event_type='PROVIDER_FEE_PROFILE_PUBLISH_REQUESTED'",&[&raw]).map_err(|error|wurzburg::db::error::DbError::Query(error.to_string()))?;let value:serde_json::Value=serde_json::from_str(&payload).unwrap();assert_eq!(value["payload"]["fee_policy"]["fee_payer"],"PROVIDER");Ok(())}).await.unwrap()
}
