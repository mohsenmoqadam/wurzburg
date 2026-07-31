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

/// Scenario goal: prove provider identity and contact lifecycle contracts
/// through a real Wurzburg server, signed WSO2 JWT, and Oracle transactions.
///
/// Oracle facts: one Provider is seeded without contacts. The scenario updates
/// identity, creates and edits contacts, pages/filter lists, and performs
/// suspend/reactivate transitions. Final proof: exact replay is stable, one
/// concurrent idempotent create commits, and audit snapshots redact PII.
#[tokio::test]
async fn manages_provider_identity_and_contacts_through_running_server() {
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
    seed_provider(&pool, provider_id).await;
    let address = start_server(state).await;
    let client = reqwest::Client::new();

    let identity_key = Uuid::new_v4().to_string();
    let identity_body = serde_json::json!({
        "trade_name":"Updated Provider Brand",
        "tax_id":null,
        "registration_number":"REG-200",
        "metadata":{"segment":"enterprise"},
        "reason":"verified registered provider details"
    });
    let identity = request(
        &client,
        reqwest::Method::PATCH,
        address,
        format!("/api/v1/providers/{provider_id}"),
        Some(&identity_key),
        Some(identity_body.clone()),
        "identity-update",
    )
    .await;
    assert_eq!(identity.0, reqwest::StatusCode::OK, "{}", identity.1);
    assert_eq!(identity.1["trade_name"], "Updated Provider Brand");
    assert!(identity.1["tax_id"].is_null());
    let replay = request(
        &client,
        reqwest::Method::PATCH,
        address,
        format!("/api/v1/providers/{provider_id}"),
        Some(&identity_key),
        Some(identity_body),
        "identity-update",
    )
    .await;
    assert_eq!(replay.1, identity.1);

    let contact_key = Uuid::new_v4().to_string();
    let contact_body = serde_json::json!({
        "contact_type":"NOTIFICATION","name":"Operations Contact",
        "email":"notify@example.test","mobile":"09120000000",
        "sms_enabled":true,"metadata":{},"reason":"notification route created"
    });
    let (left, right) = tokio::join!(
        request(
            &client,
            reqwest::Method::POST,
            address,
            format!("/api/v1/providers/{provider_id}/contacts"),
            Some(&contact_key),
            Some(contact_body.clone()),
            "contact-create"
        ),
        request(
            &client,
            reqwest::Method::POST,
            address,
            format!("/api/v1/providers/{provider_id}/contacts"),
            Some(&contact_key),
            Some(contact_body),
            "contact-create"
        )
    );
    assert!(
        [reqwest::StatusCode::CREATED, reqwest::StatusCode::OK].contains(&left.0),
        "{}",
        left.1
    );
    assert!(
        [reqwest::StatusCode::CREATED, reqwest::StatusCode::OK].contains(&right.0),
        "{}",
        right.1
    );
    assert_eq!(left.1, right.1);
    let contact_id = Uuid::parse_str(left.1["provider_contact_id"].as_str().unwrap()).unwrap();
    assert_eq!(count_contacts(&pool, provider_id).await, 1);

    let updated = request(
        &client,
        reqwest::Method::PATCH,
        address,
        format!("/api/v1/providers/{provider_id}/contacts/{contact_id}"),
        Some(&Uuid::new_v4().to_string()),
        Some(serde_json::json!({
            "email":null,"mobile":"09121111111","reason":"notification route refreshed"
        })),
        "contact-update",
    )
    .await;
    assert_eq!(updated.0, reqwest::StatusCode::OK, "{}", updated.1);
    assert!(updated.1["email"].is_null());
    assert_eq!(updated.1["mobile"], "09121111111");

    let suspended = transition(&client, address, provider_id, contact_id, "suspend").await;
    assert_eq!(suspended.1["status"], "SUSPENDED");
    let filtered = request(
        &client,
        reqwest::Method::GET,
        address,
        format!("/api/v1/providers/{provider_id}/contacts?status=SUSPENDED&page_size=1"),
        None,
        None,
        "contact-list",
    )
    .await;
    assert_eq!(filtered.0, reqwest::StatusCode::OK, "{}", filtered.1);
    assert_eq!(filtered.1["data"].as_array().unwrap().len(), 1);
    let reactivated = transition(&client, address, provider_id, contact_id, "reactivate").await;
    assert_eq!(reactivated.1["status"], "ACTIVE");

    assert_redacted_audit(&pool, provider_id, contact_id).await;
}

async fn transition(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    provider_id: Uuid,
    contact_id: Uuid,
    action: &str,
) -> (reqwest::StatusCode, serde_json::Value) {
    request(
        client,
        reqwest::Method::POST,
        address,
        format!("/api/v1/providers/{provider_id}/contacts/{contact_id}/{action}"),
        Some(&Uuid::new_v4().to_string()),
        Some(serde_json::json!({"reason":format!("contact {action} requested")})),
        "contact-transition",
    )
    .await
}

async fn request(
    client: &reqwest::Client,
    method: reqwest::Method,
    address: std::net::SocketAddr,
    path: String,
    key: Option<&str>,
    body: Option<serde_json::Value>,
    correlation: &str,
) -> (reqwest::StatusCode, serde_json::Value) {
    let mut request = client
        .request(method, format!("http://{address}{path}"))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("X-Correlation-Id", correlation)
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.20")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test");
    if let Some(key) = key {
        request = request.header("Idempotency-Key", key);
    }
    if let Some(body) = body {
        request = request
            .header("Content-Type", "application/json")
            .body(body.to_string());
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
async fn seed_provider(pool: &OraclePool, provider_id: Uuid) {
    let provider = provider_id.as_bytes().to_vec();
    pool.with_connection(move|connection|{connection.execute("INSERT INTO providers (provider_id,legal_name,trade_name,tax_id,status,metadata_json,created_by_subject,updated_by_subject) VALUES (:1,'Identity Legal','Identity Brand','123456','ACTIVE','{}','seed','seed')",&[&provider]).map_err(err)?;connection.commit().map_err(err)?;Ok(())}).await.unwrap()
}
async fn count_contacts(pool: &OraclePool, provider_id: Uuid) -> i64 {
    let provider = provider_id.as_bytes().to_vec();
    pool.with_connection(move |connection| {
        connection
            .query_row_as(
                "SELECT COUNT(*) FROM provider_contacts WHERE provider_id=:1",
                &[&provider],
            )
            .map_err(err)
    })
    .await
    .unwrap()
}
async fn assert_redacted_audit(pool: &OraclePool, provider_id: Uuid, contact_id: Uuid) {
    let provider = provider_id.as_bytes().to_vec();
    let contact = contact_id.as_bytes().to_vec();
    pool.with_connection(move|connection|{let provider_snapshot:String=connection.query_row_as("SELECT JSON_SERIALIZE(new_values RETURNING CLOB) FROM audit_logs WHERE entity_type='PROVIDER' AND entity_id=:1 ORDER BY created_at DESC FETCH FIRST 1 ROW ONLY",&[&provider]).map_err(err)?;let contact_snapshot:String=connection.query_row_as("SELECT JSON_SERIALIZE(new_values RETURNING CLOB) FROM audit_logs WHERE entity_type='PROVIDER_CONTACT' AND entity_id=:1 ORDER BY created_at DESC FETCH FIRST 1 ROW ONLY",&[&contact]).map_err(err)?;assert!(provider_snapshot.contains("[REDACTED]"));assert!(!provider_snapshot.contains("Updated Provider Brand"));assert!(contact_snapshot.contains("[REDACTED]"));assert!(!contact_snapshot.contains("09121111111"));Ok(())}).await.unwrap()
}
fn err(error: oracle::Error) -> wurzburg::db::error::DbError {
    wurzburg::db::error::DbError::Query(error.to_string())
}
