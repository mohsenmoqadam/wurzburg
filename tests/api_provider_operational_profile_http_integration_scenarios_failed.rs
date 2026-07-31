mod support;

use std::{env, sync::Arc};

use chrono::{Duration, SecondsFormat, Utc};
use tokio::net::TcpListener;
use uuid::Uuid;
use wurzburg::{
    api::router::build_app_router,
    config::Settings,
    db::oracle::{OraclePool, prepare_oracle_schema},
    state::AppState,
};

/// Scenario goal: prove invalid contracts, inactive providers, idempotency-key
/// conflicts, and terminal audit failures fail closed through the real API.
/// Final proof: a forced Oracle audit error rolls back profile, supersession,
/// and idempotency state, while the client receives only a centralized error.
#[tokio::test]
async fn rejects_and_rolls_back_provider_operational_profile_failures() {
    if env::var("RUN_FULL_INTEGRATION_TESTS").ok().as_deref() != Some("1") {
        return;
    }
    let mut settings = Settings::new().unwrap();
    settings.migrations.force_recreate = true;
    prepare_oracle_schema(&settings.database, &settings.migrations)
        .await
        .unwrap();
    settings.migrations.force_recreate = false;
    settings.provider_operational_profile_scheduler.enabled = false;
    let state = Arc::new(AppState::new(settings).await.unwrap());
    let pool = state.db.pool.clone();
    let provider_id = Uuid::new_v4();
    seed_provider(&pool, provider_id, "ACTIVE").await;
    let inactive_id = Uuid::new_v4();
    seed_provider(&pool, inactive_id, "INACTIVE").await;
    let address = start_server(state).await;
    let client = reqwest::Client::new();

    let invalid = post(
        &client,
        address,
        provider_id,
        &Uuid::new_v4().to_string(),
        body("Invalid/Timezone", 100, "invalid timezone"),
        "profile-invalid",
    )
    .await;
    assert_eq!(invalid.0, reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        invalid.1["error"]["code"],
        "PROVIDER_OPERATIONAL_PROFILE_CONTRACT_INVALID"
    );
    let missing = post(
        &client,
        address,
        Uuid::new_v4(),
        &Uuid::new_v4().to_string(),
        body("Asia/Tehran", 100, "missing provider"),
        "profile-missing",
    )
    .await;
    assert_eq!(missing.0, reqwest::StatusCode::NOT_FOUND);
    let inactive = post(
        &client,
        address,
        inactive_id,
        &Uuid::new_v4().to_string(),
        body("Asia/Tehran", 100, "inactive provider"),
        "profile-inactive",
    )
    .await;
    assert_eq!(inactive.0, reqwest::StatusCode::CONFLICT);
    assert_eq!(
        inactive.1["error"]["code"],
        "PROVIDER_OPERATIONAL_PROFILE_INVALID_STATE"
    );

    let key = Uuid::new_v4().to_string();
    let first = post(
        &client,
        address,
        provider_id,
        &key,
        body("Asia/Tehran", 200, "first command"),
        "profile-first",
    )
    .await;
    assert_eq!(first.0, reqwest::StatusCode::CREATED);
    let conflict = post(
        &client,
        address,
        provider_id,
        &key,
        body("Asia/Tehran", 300, "changed command"),
        "profile-conflict",
    )
    .await;
    assert_eq!(conflict.0, reqwest::StatusCode::CONFLICT);
    assert_eq!(conflict.1["error"]["code"], "IDEMPOTENCY_KEY_CONFLICT");

    install_audit_failure_trigger(&pool).await;
    let rollback_key = Uuid::new_v4().to_string();
    let before = count_profiles(&pool, provider_id).await;
    let failed = post(
        &client,
        address,
        provider_id,
        &rollback_key,
        body("Asia/Tehran", 400, "forced rollback"),
        "forced-operational-profile-audit-rollback",
    )
    .await;
    assert_eq!(failed.0, reqwest::StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(failed.1["error"]["code"], "SYSTEM_ERROR");
    assert!(
        !failed
            .1
            .to_string()
            .contains("forced operational profile audit failure")
    );
    assert_eq!(count_profiles(&pool, provider_id).await, before);
    assert_eq!(count_idempotency(&pool, &rollback_key).await, 0);
    drop_audit_failure_trigger(&pool).await;
}

fn body(timezone: &str, limit: u64, reason: &str) -> serde_json::Value {
    serde_json::json!({"effective_at":(Utc::now()-Duration::seconds(1)).to_rfc3339_opts(SecondsFormat::Millis,true),"reason":reason,"profile":{"timezone":timezone,"user_onboarding":{"enabled":true,"active_windows":[],"max_total_users":null},"credit_grant":{"enabled":true,"mode":"FixedLimit","limit_amount_rials":limit},"credit_return":{"enabled":true},"card_operations":{"new_assignment_enabled":true,"same_pan_reprint_enabled":true,"new_pan_replacement_enabled":true,"attach_existing_multi_provider_card_enabled":true},"event_delivery":{"enabled":true,"disabled_reason":null}}})
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
async fn post(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    provider_id: Uuid,
    key: &str,
    body: serde_json::Value,
    correlation: &str,
) -> (reqwest::StatusCode, serde_json::Value) {
    let response = client
        .post(format!(
            "http://{address}/api/v1/providers/{provider_id}/operational-profiles"
        ))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("Idempotency-Key", key)
        .header("X-Correlation-Id", correlation)
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
async fn seed_provider(pool: &OraclePool, provider_id: Uuid, status: &str) {
    let provider = provider_id.as_bytes().to_vec();
    let profile = Uuid::new_v4().as_bytes().to_vec();
    let status = status.to_string();
    let controls=serde_json::json!({"timezone":"Asia/Tehran","user_onboarding_enabled":true,"active_windows":[],"max_total_users":null,"credit_grant_enabled":true,"credit_grant_mode":"FIXED_LIMIT","credit_grant_limit_amount_rials":100,"credit_return_enabled":true,"new_assignment_enabled":true,"same_pan_reprint_enabled":true,"new_pan_replacement_enabled":true,"attach_existing_multi_provider_card_enabled":true,"event_delivery_enabled":true,"event_delivery_disabled_reason":null}).to_string();
    pool.with_connection(move|c|{c.execute("INSERT INTO providers (provider_id,legal_name,trade_name,status,metadata_json,created_by_subject,updated_by_subject) VALUES (:1,'Failure Legal','Failure',:2,'{}','test','test')",&[&provider,&status]).map_err(err)?;c.execute("INSERT INTO provider_operational_profiles (provider_operational_profile_id,provider_id,status,version,effective_at,profile_json,created_by_subject,updated_by_subject,change_reason,activated_at) VALUES (:1,:2,'ACTIVE',1,SYSTIMESTAMP-INTERVAL '1' DAY,:3,'test','test','initial',SYSTIMESTAMP-INTERVAL '1' DAY)",&[&profile,&provider,&controls]).map_err(err)?;c.commit().map_err(err)?;Ok(())}).await.unwrap()
}
async fn install_audit_failure_trigger(pool: &OraclePool) {
    pool.with_connection(|c|{c.execute("CREATE OR REPLACE TRIGGER test_fail_operational_profile_audit BEFORE INSERT ON audit_logs FOR EACH ROW WHEN (NEW.correlation_id = 'forced-operational-profile-audit-rollback') BEGIN RAISE_APPLICATION_ERROR(-20031, 'forced operational profile audit failure'); END",&[]).map_err(err)?;Ok(())}).await.unwrap()
}
async fn drop_audit_failure_trigger(pool: &OraclePool) {
    pool.with_connection(|c| {
        c.execute("DROP TRIGGER test_fail_operational_profile_audit", &[])
            .map_err(err)?;
        Ok(())
    })
    .await
    .unwrap()
}
async fn count_profiles(pool: &OraclePool, provider_id: Uuid) -> i64 {
    let provider = provider_id.as_bytes().to_vec();
    pool.with_connection(move |c| {
        c.query_row_as(
            "SELECT COUNT(*) FROM provider_operational_profiles WHERE provider_id=:1",
            &[&provider],
        )
        .map_err(err)
    })
    .await
    .unwrap()
}
async fn count_idempotency(pool: &OraclePool, key: &str) -> i64 {
    let key = key.to_string();
    pool.with_connection(move|c|c.query_row_as("SELECT COUNT(*) FROM idempotency_records WHERE operation_type='provider_operational_profiles.set' AND idempotency_key=:1",&[&key]).map_err(err)).await.unwrap()
}
fn err(error: oracle::Error) -> wurzburg::db::error::DbError {
    wurzburg::db::error::DbError::Query(error.to_string())
}
