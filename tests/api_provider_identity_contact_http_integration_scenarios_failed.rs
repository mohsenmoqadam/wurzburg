mod support;

use std::{env, sync::Arc};

use tokio::net::TcpListener;
use uuid::Uuid;
use wurzburg::{
    api::router::build_app_router,
    config::Settings,
    db::oracle::{OraclePool, prepare_oracle_schema},
    state::AppState,
};

/// Scenario goal: prove invalid identity/contact commands, cross-provider
/// access, conflicting replays, invalid lifecycle transitions, and Oracle
/// failures fail closed through the production HTTP boundary.
/// Final proof: forced audit failure leaves provider state and idempotency rows
/// unchanged, while the client receives only the centralized error contract.
#[tokio::test]
async fn rejects_and_rolls_back_provider_identity_and_contact_failures() {
    if env::var("RUN_FULL_INTEGRATION_TESTS").ok().as_deref() != Some("1") {
        return;
    }
    support::init_test_tracing();
    let mut settings = Settings::new().unwrap();
    settings.migrations.force_recreate = true;
    prepare_oracle_schema(&settings.database, &settings.migrations)
        .await
        .unwrap();
    settings.migrations.force_recreate = false;
    let state = Arc::new(AppState::new(settings).await.unwrap());
    let pool = state.db.pool.clone();
    let provider_id = Uuid::new_v4();
    let other_provider = Uuid::new_v4();
    seed_provider(&pool, provider_id).await;
    seed_provider(&pool, other_provider).await;
    let contact_id = seed_contact(&pool, provider_id).await;
    let address = start_server(state).await;
    let client = reqwest::Client::new();

    let invalid_identity = send(
        &client,
        reqwest::Method::PATCH,
        address,
        format!("/api/v1/providers/{provider_id}"),
        Some(&Uuid::new_v4().to_string()),
        serde_json::json!({"legal_name":null,"reason":"invalid clear"}),
        "invalid-identity",
    )
    .await;
    assert_eq!(invalid_identity.0, reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        invalid_identity.1["error"]["code"],
        "INVALID_PROVIDER_IDENTITY_CONTRACT"
    );
    let invalid_contact = send(
        &client,
        reqwest::Method::PATCH,
        address,
        format!("/api/v1/providers/{provider_id}/contacts/{contact_id}"),
        Some(&Uuid::new_v4().to_string()),
        serde_json::json!({"mobile":null,"reason":"would break SMS route"}),
        "invalid-contact",
    )
    .await;
    assert_eq!(invalid_contact.0, reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        invalid_contact.1["error"]["code"],
        "INVALID_PROVIDER_CONTACT_CONTRACT"
    );
    let cross_provider = send(
        &client,
        reqwest::Method::PATCH,
        address,
        format!("/api/v1/providers/{other_provider}/contacts/{contact_id}"),
        Some(&Uuid::new_v4().to_string()),
        serde_json::json!({"phone":"02111111111","reason":"cross provider attempt"}),
        "cross-provider-contact",
    )
    .await;
    assert_eq!(cross_provider.0, reqwest::StatusCode::NOT_FOUND);
    assert_eq!(
        cross_provider.1["error"]["code"],
        "PROVIDER_CONTACT_NOT_FOUND"
    );

    let key = Uuid::new_v4().to_string();
    let first = send(
        &client,
        reqwest::Method::PATCH,
        address,
        format!("/api/v1/providers/{provider_id}"),
        Some(&key),
        serde_json::json!({"trade_name":"First Brand","reason":"first identity command"}),
        "identity-conflict",
    )
    .await;
    assert_eq!(first.0, reqwest::StatusCode::OK);
    let conflict = send(
        &client,
        reqwest::Method::PATCH,
        address,
        format!("/api/v1/providers/{provider_id}"),
        Some(&key),
        serde_json::json!({"trade_name":"Second Brand","reason":"changed identity command"}),
        "identity-conflict",
    )
    .await;
    assert_eq!(conflict.0, reqwest::StatusCode::CONFLICT);
    assert_eq!(conflict.1["error"]["code"], "IDEMPOTENCY_KEY_CONFLICT");

    let suspended = send(
        &client,
        reqwest::Method::POST,
        address,
        format!("/api/v1/providers/{provider_id}/contacts/{contact_id}/suspend"),
        Some(&Uuid::new_v4().to_string()),
        serde_json::json!({"reason":"suspend once"}),
        "contact-state",
    )
    .await;
    assert_eq!(suspended.0, reqwest::StatusCode::OK);
    let duplicate = send(
        &client,
        reqwest::Method::POST,
        address,
        format!("/api/v1/providers/{provider_id}/contacts/{contact_id}/suspend"),
        Some(&Uuid::new_v4().to_string()),
        serde_json::json!({"reason":"suspend twice"}),
        "contact-state",
    )
    .await;
    assert_eq!(duplicate.0, reqwest::StatusCode::CONFLICT);
    assert_eq!(
        duplicate.1["error"]["code"],
        "PROVIDER_CONTACT_INVALID_STATE"
    );

    install_audit_failure_trigger(&pool).await;
    let rollback_key = Uuid::new_v4().to_string();
    let before = provider_trade_name(&pool, provider_id).await;
    let failed = send(
        &client,
        reqwest::Method::PATCH,
        address,
        format!("/api/v1/providers/{provider_id}"),
        Some(&rollback_key),
        serde_json::json!({"trade_name":"Must Roll Back","reason":"forced audit rollback"}),
        "forced-provider-identity-audit-rollback",
    )
    .await;
    assert_eq!(failed.0, reqwest::StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(failed.1["error"]["code"], "SYSTEM_ERROR");
    assert!(
        !failed
            .1
            .to_string()
            .contains("forced provider identity audit failure")
    );
    assert_eq!(provider_trade_name(&pool, provider_id).await, before);
    assert_eq!(count_idempotency(&pool, &rollback_key).await, 0);
    drop_audit_failure_trigger(&pool).await;
}

async fn send(
    client: &reqwest::Client,
    method: reqwest::Method,
    address: std::net::SocketAddr,
    path: String,
    key: Option<&str>,
    body: serde_json::Value,
    correlation: &str,
) -> (reqwest::StatusCode, serde_json::Value) {
    let mut request = client
        .request(method, format!("http://{address}{path}"))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("X-Correlation-Id", correlation)
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.21")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .header("Content-Type", "application/json")
        .body(body.to_string());
    if let Some(key) = key {
        request = request.header("Idempotency-Key", key);
    }
    let response = request.send().await.unwrap();
    let status = response.status();
    let value = serde_json::from_str(&response.text().await.unwrap()).unwrap();
    (status, value)
}
async fn start_server(state: Arc<AppState>) -> std::net::SocketAddr {
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
async fn seed_provider(pool: &OraclePool, id: Uuid) {
    let raw = id.as_bytes().to_vec();
    pool.with_connection(move|connection|{connection.execute("INSERT INTO providers (provider_id,legal_name,trade_name,status,metadata_json,created_by_subject,updated_by_subject) VALUES (:1,'Contact Failure Legal','Contact Failure','ACTIVE','{}','seed','seed')",&[&raw]).map_err(err)?;connection.commit().map_err(err)?;Ok(())}).await.unwrap()
}
async fn seed_contact(pool: &OraclePool, provider_id: Uuid) -> Uuid {
    let id = Uuid::new_v4();
    let raw_id = id.as_bytes().to_vec();
    let provider = provider_id.as_bytes().to_vec();
    pool.with_connection(move|connection|{connection.execute("INSERT INTO provider_contacts (provider_contact_id,provider_id,contact_type,contact_name,mobile,sms_enabled,metadata_json,status,created_by_subject,updated_by_subject) VALUES (:1,:2,'NOTIFICATION','Seed Contact','09120000000',1,'{}','ACTIVE','seed','seed')",&[&raw_id,&provider]).map_err(err)?;connection.commit().map_err(err)?;Ok(())}).await.unwrap();
    id
}
async fn provider_trade_name(pool: &OraclePool, id: Uuid) -> String {
    let raw = id.as_bytes().to_vec();
    pool.with_connection(move |connection| {
        connection
            .query_row_as(
                "SELECT trade_name FROM providers WHERE provider_id=:1",
                &[&raw],
            )
            .map_err(err)
    })
    .await
    .unwrap()
}
async fn count_idempotency(pool: &OraclePool, key: &str) -> i64 {
    let key = key.to_string();
    pool.with_connection(move |connection| {
        connection
            .query_row_as(
                "SELECT COUNT(*) FROM idempotency_records WHERE idempotency_key=:1",
                &[&key],
            )
            .map_err(err)
    })
    .await
    .unwrap()
}
async fn install_audit_failure_trigger(pool: &OraclePool) {
    pool.with_connection(|connection|{connection.execute("CREATE OR REPLACE TRIGGER test_fail_provider_identity_audit BEFORE INSERT ON audit_logs FOR EACH ROW WHEN (NEW.correlation_id = 'forced-provider-identity-audit-rollback') BEGIN RAISE_APPLICATION_ERROR(-20041, 'forced provider identity audit failure'); END",&[]).map_err(err)?;Ok(())}).await.unwrap()
}
async fn drop_audit_failure_trigger(pool: &OraclePool) {
    pool.with_connection(|connection| {
        connection
            .execute("DROP TRIGGER test_fail_provider_identity_audit", &[])
            .map_err(err)?;
        Ok(())
    })
    .await
    .unwrap()
}
fn err(error: oracle::Error) -> wurzburg::db::error::DbError {
    wurzburg::db::error::DbError::Query(error.to_string())
}
