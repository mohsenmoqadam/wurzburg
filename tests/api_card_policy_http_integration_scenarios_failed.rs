mod support;

use std::{env, sync::Arc, time::SystemTime};

use chrono::Utc;
use tokio::net::TcpListener;
use uuid::Uuid;
use wurzburg::{
    api::router::build_app_router,
    config::Settings,
    db::oracle::{OraclePool, PolicyReceiptPersistenceOutcome, prepare_oracle_schema},
    domain::card_policy::PolicyMaterializationReceipt,
    state::AppState,
};

/// Scenario goal:
/// Prove policy contract rejection, idempotency conflict, frozen-draft safety,
/// receipt mismatch handling, and full Oracle rollback through a real server.
///
/// Oracle facts: ranges and provider eligibility are created explicitly; a
/// trigger forces the final audit insert to fail after policy/outbox writes.
/// External effects: no Dragonfly write is simulated for mismatched receipts.
/// Final proof: invalid or ambiguous commands never become ACTIVE, mismatches
/// remain durable inbox evidence, and failed transactions leave no partial row.
#[tokio::test]
async fn rejects_unsafe_policy_scenarios_through_running_wurzburg_and_oracle() {
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
    let repository = state.db.clone();
    let address = start_server(state).await;
    let client = reqwest::Client::new();

    let card_range_id = create_range(&client, address, "PLATFORM").await;
    let invalid_bounds = serde_json::json!({
        "reason": "invalid transaction bounds",
        "withdrawal_limits": {
            "per_transaction_min_amount": 200,
            "per_transaction_max_amount": 100,
            "daily": null,
            "weekly": null,
            "monthly": null,
            "yearly": null
        }
    })
    .to_string();
    let (status, body) = put_policy(
        &client,
        address,
        card_range_id,
        &Uuid::new_v4().to_string(),
        "policy-invalid-bounds",
        &invalid_bounds,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST, "{body}");
    assert!(body.contains("CARD_POLICY_CONTRACT_INVALID"));
    assert_eq!(count_policies(&repository.pool, card_range_id).await, 0);

    let cms_range_id = create_range(&client, address, "CMS").await;
    let (status, body) = put_policy(
        &client,
        address,
        cms_range_id,
        &Uuid::new_v4().to_string(),
        "policy-cms-with-platform-limits",
        &valid_policy("CMS must reject these limits"),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST, "{body}");
    assert!(body.contains("CARD_POLICY_CONTRACT_INVALID"));

    let idempotency_key = Uuid::new_v4().to_string();
    let original = valid_policy("idempotency original");
    let (status, _) = put_policy(
        &client,
        address,
        card_range_id,
        &idempotency_key,
        "policy-idempotency-original",
        &original,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::CREATED);
    let (status, body) = put_policy(
        &client,
        address,
        card_range_id,
        &idempotency_key,
        "policy-idempotency-conflict",
        &valid_policy("different payload"),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::CONFLICT, "{body}");
    assert!(body.contains("IDEMPOTENCY_KEY_CONFLICT"));

    attach_active_provider(&repository.pool, card_range_id).await;
    let (status, pending_body) = put_policy(
        &client,
        address,
        card_range_id,
        &Uuid::new_v4().to_string(),
        "policy-freeze",
        &valid_policy("freeze draft for publication"),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::ACCEPTED, "{pending_body}");
    let pending: serde_json::Value = serde_json::from_str(&pending_body).unwrap();
    let operation_id = uuid_field(&pending, "operation_id");
    let policy_id = uuid_field(&pending["profile"], "card_policy_profile_id");
    let version = pending["profile"]["version"].as_i64().unwrap();

    let (status, body) = put_policy(
        &client,
        address,
        card_range_id,
        &Uuid::new_v4().to_string(),
        "policy-frozen-edit",
        &valid_policy("unsafe edit while publication is pending"),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::CONFLICT, "{body}");
    assert!(body.contains("POLICY_DRAFT_FROZEN"));

    let mismatched_receipt = PolicyMaterializationReceipt {
        receipt_event_id: Uuid::new_v4(),
        operation_id,
        card_range_id,
        card_policy_profile_id: policy_id,
        materialized_version: version,
        runtime_key: format!("CPOL:MultiProvider:{card_range_id}"),
        materialized_at: Utc::now(),
    };
    assert_eq!(
        repository
            .apply_policy_materialization_receipt(mismatched_receipt.clone())
            .await
            .expect("mismatch is a committed business outcome"),
        PolicyReceiptPersistenceOutcome::Mismatch
    );
    assert_eq!(policy_status(&repository.pool, policy_id).await, "DRAFT");
    assert_eq!(
        failed_inbox_count(&repository.pool, mismatched_receipt.receipt_event_id).await,
        1
    );
    assert_eq!(
        repository
            .apply_policy_materialization_receipt(mismatched_receipt)
            .await
            .expect("replayed mismatch should remain deterministic"),
        PolicyReceiptPersistenceOutcome::Mismatch
    );

    // The policy, outbox, audit, and idempotency completion share one Oracle
    // transaction. Failing the final audit insert must roll all of them back.
    let rollback_range_id = create_range(&client, address, "PLATFORM").await;
    attach_active_provider(&repository.pool, rollback_range_id).await;
    install_audit_failure_trigger(&repository.pool).await;
    let rollback_key = Uuid::new_v4().to_string();
    let (status, body) = put_policy(
        &client,
        address,
        rollback_range_id,
        &rollback_key,
        "forced-policy-audit-rollback",
        &valid_policy("must rollback atomically"),
    )
    .await;
    remove_audit_failure_trigger(&repository.pool).await;

    assert_eq!(status, reqwest::StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert!(body.contains("SYSTEM_ERROR"));
    assert!(!body.contains("ORA-"));
    assert!(!body.contains("forced policy audit failure"));
    assert_eq!(count_policies(&repository.pool, rollback_range_id).await, 0);
    assert_eq!(count_outbox(&repository.pool, rollback_range_id).await, 0);
    assert_eq!(count_idempotency(&repository.pool, &rollback_key).await, 0);
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

async fn create_range(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    authority: &str,
) -> Uuid {
    let suffix = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        % 8_000_000_000;
    let start = format!("8{:015}", 100_000_000_000_000_u128 + suffix);
    let end = format!("{:016}", start.parse::<u64>().unwrap() + 99);
    let calendar = (authority == "PLATFORM").then(|| {
        serde_json::json!({
            "timezone": "Asia/Tehran",
            "week_starts_on": "SATURDAY",
            "window_mode": "CALENDAR"
        })
    });
    let body = serde_json::json!({
        "start_card_number": start,
        "end_card_number": end,
        "funding_mode": "SINGLE_PROVIDER",
        "withdrawal_limit_authority": authority,
        "limit_calendar": calendar,
        "issuance_enabled": true,
        "cms_operation_mode": "FULL",
        "metadata": {}
    });
    let response = client
        .post(format!("http://{address}/api/v1/card-ranges"))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("Idempotency-Key", Uuid::new_v4().to_string())
        .header("X-Correlation-Id", format!("policy-{authority}-range"))
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.10")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .header("Content-Type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert_eq!(status, reqwest::StatusCode::CREATED, "{body}");
    let value: serde_json::Value = serde_json::from_str(&body).unwrap();
    uuid_field(&value, "card_range_id")
}

fn valid_policy(reason: &str) -> String {
    serde_json::json!({
        "reason": reason,
        "withdrawal_limits": {
            "per_transaction_min_amount": null,
            "per_transaction_max_amount": 5_000_000,
            "daily": { "max_amount": 10_000_000, "max_count": null },
            "weekly": null,
            "monthly": null,
            "yearly": null
        }
    })
    .to_string()
}

async fn put_policy(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    card_range_id: Uuid,
    key: &str,
    correlation_id: &str,
    body: &str,
) -> (reqwest::StatusCode, String) {
    let response = client
        .put(format!(
            "http://{address}/api/v1/card-ranges/{card_range_id}/policy"
        ))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("Idempotency-Key", key)
        .header("X-Correlation-Id", correlation_id)
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.10")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .header("Content-Type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    (status, body)
}

async fn attach_active_provider(pool: &OraclePool, card_range_id: Uuid) {
    let provider_raw = Uuid::new_v4().as_bytes().to_vec();
    let range_raw = card_range_id.as_bytes().to_vec();
    pool.with_connection(move |connection| {
        connection.execute(
            "INSERT INTO providers (provider_id, legal_name, trade_name, status, created_by_subject, updated_by_subject) VALUES (:1, :2, :3, 'ACTIVE', :4, :4)",
            &[&provider_raw, &"Failure Provider", &"Failure", &"test-suite"],
        ).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        connection.execute(
            "INSERT INTO card_range_providers (card_range_id, provider_id, status, created_by_subject, updated_by_subject) VALUES (:1, :2, 'ACTIVE', :3, :3)",
            &[&range_raw, &provider_raw, &"test-suite"],
        ).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        connection.commit().map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        Ok(())
    }).await.unwrap();
}

async fn install_audit_failure_trigger(pool: &OraclePool) {
    pool.with_connection(|connection| {
        connection
            .execute(
                r#"
            CREATE OR REPLACE TRIGGER test_fail_card_policy_audit
            BEFORE INSERT ON audit_logs
            FOR EACH ROW
            WHEN (NEW.correlation_id = 'forced-policy-audit-rollback')
            BEGIN
                RAISE_APPLICATION_ERROR(-20002, 'forced policy audit failure');
            END;
            "#,
                &[],
            )
            .map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        Ok(())
    })
    .await
    .unwrap();
}

async fn remove_audit_failure_trigger(pool: &OraclePool) {
    pool.with_connection(|connection| {
        connection
            .execute("DROP TRIGGER test_fail_card_policy_audit", &[])
            .map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        Ok(())
    })
    .await
    .unwrap();
}

async fn count_policies(pool: &OraclePool, range_id: Uuid) -> i64 {
    count_raw(
        pool,
        "card_policy_profiles",
        "card_range_id",
        range_id.as_bytes().to_vec(),
    )
    .await
}

async fn count_outbox(pool: &OraclePool, range_id: Uuid) -> i64 {
    count_raw(
        pool,
        "integration_outbox",
        "aggregate_id",
        range_id.as_bytes().to_vec(),
    )
    .await
}

async fn count_raw(pool: &OraclePool, table: &str, column: &str, raw: Vec<u8>) -> i64 {
    let sql = format!("SELECT COUNT(*) FROM {table} WHERE {column} = :1");
    pool.with_connection(move |connection| {
        connection
            .query_row_as::<i64>(&sql, &[&raw])
            .map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))
    })
    .await
    .unwrap()
}

async fn count_idempotency(pool: &OraclePool, key: &str) -> i64 {
    let key = key.to_string();
    pool.with_connection(move |connection| {
        connection
            .query_row_as::<i64>(
                "SELECT COUNT(*) FROM idempotency_records WHERE idempotency_key = :1",
                &[&key],
            )
            .map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))
    })
    .await
    .unwrap()
}

async fn failed_inbox_count(pool: &OraclePool, event_id: Uuid) -> i64 {
    let raw = event_id.as_bytes().to_vec();
    pool.with_connection(move |connection| {
        connection.query_row_as::<i64>(
            "SELECT COUNT(*) FROM integration_inbox WHERE source_event_id = :1 AND status = 'FAILED'",
            &[&raw],
        ).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))
    }).await.unwrap()
}

async fn policy_status(pool: &OraclePool, policy_id: Uuid) -> String {
    let raw = policy_id.as_bytes().to_vec();
    pool.with_connection(move |connection| {
        connection
            .query_row_as::<String>(
                "SELECT status FROM card_policy_profiles WHERE card_policy_profile_id = :1",
                &[&raw],
            )
            .map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))
    })
    .await
    .unwrap()
}

fn uuid_field(value: &serde_json::Value, field: &str) -> Uuid {
    Uuid::parse_str(value[field].as_str().expect("UUID field should exist")).unwrap()
}
