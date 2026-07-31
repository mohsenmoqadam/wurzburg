mod support;

use std::{env, sync::Arc, time::Duration};

use chrono::{Duration as ChronoDuration, SecondsFormat, Utc};
use tokio::net::TcpListener;
use uuid::Uuid;
use wurzburg::{
    api::router::build_app_router,
    config::{ProviderOperationalProfileSchedulerConfig, Settings},
    db::oracle::{OraclePool, prepare_oracle_schema},
    services::provider_operational_profile::start_provider_operational_profile_scheduler,
    state::AppState,
};

/// Scenario goal:
/// Prove immediate activation, future scheduling, replacement, cancellation,
/// due-time promotion, history, and idempotent replay through a real server.
///
/// Oracle facts: one ACTIVE provider and one initial ACTIVE operational profile
/// are inserted explicitly. No Kafka, Dragonfly, or TigerBeetle state is used.
/// Final proof: Oracle keeps one ACTIVE and at most one SCHEDULED profile while
/// every lifecycle transition and idempotency result commits atomically.
#[tokio::test]
async fn manages_provider_operational_profiles_through_running_server_and_oracle() {
    if env::var("RUN_FULL_INTEGRATION_TESTS").ok().as_deref() != Some("1") {
        return;
    }
    let mut settings = Settings::new().expect("integration settings should load");
    settings.migrations.force_recreate = true;
    prepare_oracle_schema(&settings.database, &settings.migrations)
        .await
        .expect("Oracle schema should be rebuilt from the final baseline");
    settings.migrations.force_recreate = false;
    settings.provider_operational_profile_scheduler.enabled = false;
    let state = Arc::new(AppState::new(settings).await.unwrap());
    let pool = state.db.pool.clone();
    let provider_id = Uuid::new_v4();
    seed_provider_with_profile(&pool, provider_id).await;
    let address = start_server(state).await;
    let client = reqwest::Client::new();

    let immediate_key = Uuid::new_v4().to_string();
    let immediate_body = profile_body(
        Utc::now() - ChronoDuration::seconds(1),
        2_000_000,
        "activate stricter controls now",
    );
    let immediate = post_profile(
        &client,
        address,
        provider_id,
        &immediate_key,
        &immediate_body,
        "operational-profile-immediate",
    )
    .await;
    assert_eq!(immediate.0, reqwest::StatusCode::CREATED, "{}", immediate.1);
    assert_eq!(immediate.1["disposition"], "ACTIVATED");
    assert_eq!(immediate.1["profile"]["status"], "ACTIVE");
    assert_eq!(immediate.1["profile"]["version"], 2);

    let replay = post_profile(
        &client,
        address,
        provider_id,
        &immediate_key,
        &immediate_body,
        "operational-profile-immediate",
    )
    .await;
    assert_eq!(replay.0, reqwest::StatusCode::OK);
    assert_eq!(replay.1, immediate.1);

    let first_future = post_profile(
        &client,
        address,
        provider_id,
        &Uuid::new_v4().to_string(),
        &profile_body(
            Utc::now() + ChronoDuration::hours(2),
            3_000_000,
            "first future schedule",
        ),
        "operational-profile-future-one",
    )
    .await;
    assert_eq!(first_future.1["disposition"], "SCHEDULED");
    let first_future_id = first_future.1["profile"]["provider_operational_profile_id"]
        .as_str()
        .unwrap();

    let replacement = post_profile(
        &client,
        address,
        provider_id,
        &Uuid::new_v4().to_string(),
        &profile_body(
            Utc::now() + ChronoDuration::hours(3),
            4_000_000,
            "replace future schedule",
        ),
        "operational-profile-future-two",
    )
    .await;
    assert_eq!(replacement.1["profile"]["status"], "SCHEDULED");
    let replacement_id = Uuid::parse_str(
        replacement.1["profile"]["provider_operational_profile_id"]
            .as_str()
            .unwrap(),
    )
    .unwrap();

    let history = get_json(
        &client,
        address,
        format!("/api/v1/providers/{provider_id}/operational-profiles?limit=10"),
    )
    .await;
    assert_eq!(history.0, reqwest::StatusCode::OK);
    let replaced = history.1["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|profile| {
            profile["provider_operational_profile_id"].as_str() == Some(first_future_id)
        })
        .unwrap();
    assert_eq!(replaced["status"], "CANCELLED");

    let cancelled = post_json(
        &client,
        address,
        format!("/api/v1/providers/{provider_id}/operational-profiles/{replacement_id}/cancel"),
        &Uuid::new_v4().to_string(),
        serde_json::json!({"reason":"future controls are no longer required"}),
        "operational-profile-cancel",
    )
    .await;
    assert_eq!(cancelled.0, reqwest::StatusCode::OK, "{}", cancelled.1);
    assert_eq!(cancelled.1["status"], "CANCELLED");

    let concurrent_key_a = Uuid::new_v4().to_string();
    let concurrent_key_b = Uuid::new_v4().to_string();
    let concurrent_body_a = profile_body(
        Utc::now() + ChronoDuration::hours(4),
        4_100_000,
        "concurrent schedule A",
    );
    let concurrent_body_b = profile_body(
        Utc::now() + ChronoDuration::hours(5),
        4_200_000,
        "concurrent schedule B",
    );
    let (concurrent_a, concurrent_b) = tokio::join!(
        post_profile(
            &client,
            address,
            provider_id,
            &concurrent_key_a,
            &concurrent_body_a,
            "operational-profile-concurrent-a"
        ),
        post_profile(
            &client,
            address,
            provider_id,
            &concurrent_key_b,
            &concurrent_body_b,
            "operational-profile-concurrent-b"
        )
    );
    assert_eq!(concurrent_a.0, reqwest::StatusCode::CREATED);
    assert_eq!(concurrent_b.0, reqwest::StatusCode::CREATED);
    assert_eq!(count_status(&pool, provider_id, "SCHEDULED").await, 1);

    let due = post_profile(
        &client,
        address,
        provider_id,
        &Uuid::new_v4().to_string(),
        &profile_body(
            Utc::now() + ChronoDuration::seconds(2),
            5_000_000,
            "promote through command path safety net",
        ),
        "operational-profile-due",
    )
    .await;
    assert_eq!(due.1["profile"]["status"], "SCHEDULED");
    tokio::time::sleep(Duration::from_millis(2_200)).await;
    let current = get_json(
        &client,
        address,
        format!("/api/v1/providers/{provider_id}/operational-profile"),
    )
    .await;
    assert_eq!(current.0, reqwest::StatusCode::OK, "{}", current.1);
    assert_eq!(current.1["status"], "ACTIVE");
    assert_eq!(
        current.1["profile"]["credit_grant"]["limit_amount_rials"],
        5_000_000
    );

    let scheduler_due = post_profile(
        &client,
        address,
        provider_id,
        &Uuid::new_v4().to_string(),
        &profile_body(
            Utc::now() + ChronoDuration::seconds(2),
            6_000_000,
            "promote through scheduler",
        ),
        "operational-profile-scheduler",
    )
    .await;
    assert_eq!(scheduler_due.1["profile"]["status"], "SCHEDULED");
    let scheduler = start_provider_operational_profile_scheduler(
        repository_from_pool(&pool),
        ProviderOperationalProfileSchedulerConfig {
            enabled: true,
            batch_size: 10,
            poll_interval_ms: 50,
        },
    )
    .expect("scheduler should start");
    tokio::time::sleep(Duration::from_millis(2_200)).await;
    scheduler.shutdown().await;
    let scheduler_current = get_json(
        &client,
        address,
        format!("/api/v1/providers/{provider_id}/operational-profile"),
    )
    .await;
    assert_eq!(
        scheduler_current.1["profile"]["credit_grant"]["limit_amount_rials"],
        6_000_000
    );
    assert_profile_cardinality_and_audit(&pool, provider_id).await;
}

fn repository_from_pool(pool: &OraclePool) -> Arc<wurzburg::db::oracle::OracleRepository> {
    Arc::new(wurzburg::db::oracle::OracleRepository::new(pool.clone()))
}

fn profile_body(
    effective_at: chrono::DateTime<Utc>,
    limit: u64,
    reason: &str,
) -> serde_json::Value {
    serde_json::json!({
        "effective_at": effective_at.to_rfc3339_opts(SecondsFormat::Millis, true),
        "reason": reason,
        "profile": {
            "timezone":"Asia/Tehran",
            "user_onboarding":{"enabled":true,"active_windows":[],"max_total_users":1000},
            "credit_grant":{"enabled":true,"mode":"FixedLimit","limit_amount_rials":limit},
            "credit_return":{"enabled":true},
            "card_operations":{"new_assignment_enabled":true,"same_pan_reprint_enabled":true,"new_pan_replacement_enabled":true,"attach_existing_multi_provider_card_enabled":true},
            "event_delivery":{"enabled":true,"disabled_reason":null}
        }
    })
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

async fn post_profile(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    provider_id: Uuid,
    key: &str,
    body: &serde_json::Value,
    correlation: &str,
) -> (reqwest::StatusCode, serde_json::Value) {
    post_json(
        client,
        address,
        format!("/api/v1/providers/{provider_id}/operational-profiles"),
        key,
        body.clone(),
        correlation,
    )
    .await
}

async fn post_json(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    path: String,
    key: &str,
    body: serde_json::Value,
    correlation: &str,
) -> (reqwest::StatusCode, serde_json::Value) {
    let response = client
        .post(format!("http://{address}{path}"))
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

async fn get_json(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    path: String,
) -> (reqwest::StatusCode, serde_json::Value) {
    let response = client
        .get(format!("http://{address}{path}"))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("X-Correlation-Id", "operational-profile-read")
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

async fn seed_provider_with_profile(pool: &OraclePool, provider_id: Uuid) {
    let provider = provider_id.as_bytes().to_vec();
    let profile = Uuid::new_v4().as_bytes().to_vec();
    let controls=serde_json::json!({"timezone":"Asia/Tehran","user_onboarding_enabled":true,"active_windows":[],"max_total_users":1000,"credit_grant_enabled":true,"credit_grant_mode":"FIXED_LIMIT","credit_grant_limit_amount_rials":1000000,"credit_return_enabled":true,"new_assignment_enabled":true,"same_pan_reprint_enabled":true,"new_pan_replacement_enabled":true,"attach_existing_multi_provider_card_enabled":true,"event_delivery_enabled":true,"event_delivery_disabled_reason":null}).to_string();
    pool.with_connection(move|connection|{connection.execute("INSERT INTO providers (provider_id,legal_name,trade_name,status,metadata_json,created_by_subject,updated_by_subject) VALUES (:1,'Operational Profile Legal','Operational Profile','ACTIVE','{}','test','test')",&[&provider]).map_err(|e|wurzburg::db::error::DbError::Query(e.to_string()))?;connection.execute("INSERT INTO provider_operational_profiles (provider_operational_profile_id,provider_id,status,version,effective_at,profile_json,created_by_subject,updated_by_subject,change_reason,activated_at) VALUES (:1,:2,'ACTIVE',1,SYSTIMESTAMP-INTERVAL '1' DAY,:3,'test','test','initial profile',SYSTIMESTAMP-INTERVAL '1' DAY)",&[&profile,&provider,&controls]).map_err(|e|wurzburg::db::error::DbError::Query(e.to_string()))?;connection.commit().map_err(|e|wurzburg::db::error::DbError::Query(e.to_string()))?;Ok(())}).await.unwrap();
}

async fn assert_profile_cardinality_and_audit(pool: &OraclePool, provider_id: Uuid) {
    let provider = provider_id.as_bytes().to_vec();
    pool.with_connection(move|connection|{let active:i64=connection.query_row_as("SELECT COUNT(*) FROM provider_operational_profiles WHERE provider_id=:1 AND status='ACTIVE'",&[&provider]).map_err(|e|wurzburg::db::error::DbError::Query(e.to_string()))?;let scheduled:i64=connection.query_row_as("SELECT COUNT(*) FROM provider_operational_profiles WHERE provider_id=:1 AND status='SCHEDULED'",&[&provider]).map_err(|e|wurzburg::db::error::DbError::Query(e.to_string()))?;let audits:i64=connection.query_row_as("SELECT COUNT(*) FROM audit_logs WHERE entity_type='PROVIDER_OPERATIONAL_PROFILE'",&[]).map_err(|e|wurzburg::db::error::DbError::Query(e.to_string()))?;assert_eq!(active,1);assert_eq!(scheduled,0);assert!(audits>=6);Ok(())}).await.unwrap();
}

async fn count_status(pool: &OraclePool, provider_id: Uuid, status: &str) -> i64 {
    let provider = provider_id.as_bytes().to_vec();
    let status = status.to_string();
    pool.with_connection(move |connection| {
        connection
            .query_row_as(
                "SELECT COUNT(*) FROM provider_operational_profiles WHERE provider_id=:1 AND status=:2",
                &[&provider, &status],
            )
            .map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))
    })
    .await
    .unwrap()
}
