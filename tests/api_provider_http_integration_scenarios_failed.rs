mod support;

use std::{env, sync::Arc};

use tokio::net::TcpListener;
use uuid::Uuid;
use wurzburg::{
    api::router::build_app_router,
    config::Settings,
    db::{
        error::DbError,
        oracle::{OraclePool, prepare_oracle_schema},
    },
    domain::provider::ProviderAccountCategory,
    state::AppState,
};

/// Scenario goal:
/// Prove that Provider validation and audit failures cannot leave partial
/// Oracle facts, and that non-operational Providers cannot be activated or
/// assigned to a range through the real HTTP/WSO2 boundary.
///
/// Oracle facts: one pending Provider is seeded with four PROVISIONING account
/// mappings and a leased core-provisioning job. A trigger deliberately rejects
/// selected audit writes.
/// TigerBeetle facts: no account command is reached by rejected create calls.
/// Final proof: failed HTTP commands leave no Provider/idempotency residue;
/// stale workers cannot mutate a lease; and a failed terminal audit rolls the
/// Provider, mappings, job, and idempotency snapshot back together.
#[tokio::test]
async fn rejects_and_rolls_back_provider_failure_paths() {
    if env::var("RUN_FULL_INTEGRATION_TESTS").ok().as_deref() != Some("1") {
        return;
    }

    let mut settings = Settings::new().expect("full integration settings should load");
    settings.migrations.force_recreate = true;
    settings.provider_core_provisioning.enabled = false;
    prepare_oracle_schema(&settings.database, &settings.migrations)
        .await
        .expect("Oracle schema should be rebuilt from the final baseline");
    settings.migrations.force_recreate = false;
    let state = Arc::new(AppState::new(settings).await.unwrap());
    let repository = state.db.clone();
    let address = start_server(state).await;
    let client = reqwest::Client::new();

    let (status, body) = get_json(
        &client,
        address,
        "/api/v1/providers?status=NOT_A_PROVIDER_STATUS",
        "provider-list-invalid-status",
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST, "{body}");
    assert!(body.contains("INVALID_PROVIDER_FILTER"));

    let (status, body) = get_json(
        &client,
        address,
        "/api/v1/admin/audit-logs?created_from=2026-07-25T00%3A00%3A00Z&created_to=2026-07-24T00%3A00%3A00Z",
        "audit-list-invalid-range",
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST, "{body}");
    assert!(body.contains("INVALID_AUDIT_LOG_FILTER"));

    let response = client
        .get(format!("http://{address}/api/v1/admin/audit-logs"))
        .bearer_auth(support::signed_platform_admin_without_audit_scope())
        .header("X-Correlation-Id", "audit-list-missing-scope")
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.20")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert_eq!(status, reqwest::StatusCode::FORBIDDEN, "{body}");
    assert!(body.contains("MISSING_REQUIRED_SCOPE"));

    let initial_provider_count = count_all(&repository.pool, "providers").await;
    let invalid_key = Uuid::new_v4().to_string();
    let (status, body) = post_provider(
        &client,
        address,
        &invalid_key,
        "provider-invalid-contract",
        &provider_body(""),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST, "{body}");
    assert!(body.contains("INVALID_PROVIDER_CONTRACT"));
    assert_eq!(
        count_all(&repository.pool, "providers").await,
        initial_provider_count
    );
    assert_eq!(count_idempotency(&repository.pool, &invalid_key).await, 0);

    install_create_audit_failure_trigger(&repository.pool).await;
    let rollback_key = Uuid::new_v4().to_string();
    let (status, body) = post_provider(
        &client,
        address,
        &rollback_key,
        "forced-provider-create-audit-rollback",
        &provider_body("Provider whose command must roll back"),
    )
    .await;
    remove_trigger(&repository.pool, "test_fail_provider_create_audit").await;
    assert_eq!(status, reqwest::StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert!(body.contains("SYSTEM_ERROR"));
    assert!(!body.contains("ORA-"));
    assert!(!body.contains("forced provider create audit failure"));
    assert_eq!(
        count_all(&repository.pool, "providers").await,
        initial_provider_count
    );
    assert_eq!(
        count_all(&repository.pool, "provider_ledger_accounts").await,
        0
    );
    assert_eq!(
        count_all(&repository.pool, "provider_provisioning_jobs").await,
        0
    );
    assert_eq!(count_idempotency(&repository.pool, &rollback_key).await, 0);

    let provider_id = Uuid::new_v4();
    let job_id = Uuid::new_v4();
    let provisioning_key = Uuid::new_v4().to_string();
    seed_pending_provider(&repository.pool, provider_id, job_id, &provisioning_key).await;
    let ready_without_accounts_id = Uuid::new_v4();
    seed_ready_provider_without_accounts(&repository.pool, ready_without_accounts_id).await;

    let (status, body) = get_json(
        &client,
        address,
        &format!("/api/v1/providers/{ready_without_accounts_id}/ledger"),
        "provider-ledger-incomplete-mapping",
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert!(body.contains("PROVIDER_LEDGER_UNAVAILABLE"));

    let (status, body) = post_json(
        &client,
        address,
        &format!("/api/v1/providers/{ready_without_accounts_id}/activate"),
        &Uuid::new_v4().to_string(),
        "provider-activate-before-ready",
        &serde_json::json!({"reason":"activation must wait for verified accounts"}).to_string(),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::CONFLICT, "{body}");
    assert!(body.contains("PROVIDER_PROVISIONING_PENDING"));

    let assignment_response = client
        .put(format!(
            "http://{address}/api/v1/providers/{provider_id}/card-range"
        ))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("Idempotency-Key", Uuid::new_v4().to_string())
        .header("X-Correlation-Id", "pending-provider-range-assignment")
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.21")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .header("Content-Type", "application/json")
        .body(
            serde_json::json!({
                "card_range_id": Uuid::new_v4(),
                "reason": "pending provider must be rejected first"
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    let assignment_status = assignment_response.status();
    let assignment_body = assignment_response.text().await.unwrap();
    assert_eq!(assignment_status, reqwest::StatusCode::CONFLICT);
    assert!(assignment_body.contains("PROVIDER_NOT_ACTIVE"));

    let claimed = repository
        .claim_provider_core_jobs("lease-owner".to_string(), 1, 60_000)
        .await
        .expect("first worker should claim the provisioning job");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].job_id, job_id);
    assert!(
        repository
            .claim_provider_core_jobs("other-worker".to_string(), 1, 60_000)
            .await
            .expect("second claim should be a valid empty result")
            .is_empty()
    );
    assert!(matches!(
        repository
            .retry_or_fail_provider_core_job(
                job_id,
                provider_id,
                "other-worker".to_string(),
                0,
                true,
                "DEPENDENCY_FAILURE",
            )
            .await,
        Err(DbError::Conflict(_))
    ));

    install_terminal_audit_failure_trigger(&repository.pool, provider_id).await;
    assert!(
        repository
            .retry_or_fail_provider_core_job(
                job_id,
                provider_id,
                "lease-owner".to_string(),
                0,
                true,
                "DEPENDENCY_FAILURE",
            )
            .await
            .is_err()
    );
    remove_trigger(&repository.pool, "test_fail_provider_terminal_audit").await;
    assert_eq!(
        provider_status(&repository.pool, provider_id).await,
        "PENDING_PROVISIONING"
    );
    assert_eq!(job_status(&repository.pool, job_id).await, "RUNNING");
    assert_eq!(
        account_status_count(&repository.pool, provider_id, "PROVISIONING").await,
        4
    );
    assert_eq!(
        idempotency_core_status(&repository.pool, &provisioning_key).await,
        "PENDING"
    );

    repository
        .retry_or_fail_provider_core_job(
            job_id,
            provider_id,
            "lease-owner".to_string(),
            0,
            true,
            "DEPENDENCY_FAILURE",
        )
        .await
        .expect("lease owner should atomically finalize terminal failure");
    assert_eq!(
        provider_status(&repository.pool, provider_id).await,
        "FAILED"
    );
    assert_eq!(job_status(&repository.pool, job_id).await, "FAILED");
    assert_eq!(
        account_status_count(&repository.pool, provider_id, "FAILED_PROVISIONING").await,
        4
    );
    assert_eq!(
        idempotency_core_status(&repository.pool, &provisioning_key).await,
        "FAILED"
    );
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

async fn post_provider(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    key: &str,
    correlation: &str,
    body: &str,
) -> (reqwest::StatusCode, String) {
    post_json(client, address, "/api/v1/providers", key, correlation, body).await
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
        .header("X-WSO2-Client-IP", "198.51.100.21")
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

async fn get_json(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    path: &str,
    correlation: &str,
) -> (reqwest::StatusCode, String) {
    let response = client
        .get(format!("http://{address}{path}"))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("X-Correlation-Id", correlation)
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.20")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .send()
        .await
        .unwrap();
    let status = response.status();
    let text = response.text().await.unwrap();
    (status, text)
}

fn provider_body(legal_name: &str) -> String {
    serde_json::json!({
        "legal_name": legal_name,
        "trade_name": "Provider failure scenario",
        "metadata": {},
        "contacts": [],
        "operational_profile": {
            "effective_at": "2026-01-01T00:00:00Z",
            "profile": {
                "timezone": "Asia/Tehran",
                "user_onboarding": {"enabled": true,"active_windows": [],"max_total_users": null},
                "credit_grant": {"enabled": true,"mode": "FixedLimit","limit_amount_rials": 1000},
                "credit_return": {"enabled": true},
                "card_operations": {
                    "new_assignment_enabled": true,
                    "same_pan_reprint_enabled": true,
                    "new_pan_replacement_enabled": true,
                    "attach_existing_multi_provider_card_enabled": true
                },
                "event_delivery": {"enabled": true,"disabled_reason": null}
            }
        }
    })
    .to_string()
}

async fn seed_pending_provider(
    pool: &OraclePool,
    provider_id: Uuid,
    job_id: Uuid,
    idempotency_key: &str,
) {
    let idempotency_key = idempotency_key.to_string();
    pool.with_transaction("seed pending provider scenario", move |connection| {
        let provider_raw = provider_id.as_bytes().to_vec();
        connection.execute(
            "INSERT INTO providers (provider_id,legal_name,trade_name,status,metadata_json,created_by_subject,updated_by_subject) VALUES (:1,'Pending Legal Provider','Pending Provider','PENDING_PROVISIONING','{}','integration-test','integration-test')",
            &[&provider_raw],
        ).map_err(db_error)?;
        for category in ProviderAccountCategory::ALL {
            connection.execute(
                "INSERT INTO provider_ledger_accounts (provider_ledger_account_id,provider_id,account_category,tigerbeetle_account_id,status) VALUES (:1,:2,:3,:4,'PROVISIONING')",
                &[&Uuid::new_v4().as_bytes().to_vec(), &provider_raw, &category.as_db_value(), &category.deterministic_account_id(provider_id).as_bytes().to_vec()],
            ).map_err(db_error)?;
        }
        connection.execute(
            "INSERT INTO provider_provisioning_jobs (provider_provisioning_job_id,provider_id,job_type,status,request_json,result_json) VALUES (:1,:2,'TIGERBEETLE_PROVISION','PENDING','{}','{}')",
            &[&job_id.as_bytes().to_vec(), &provider_raw],
        ).map_err(db_error)?;
        connection.execute(
            "INSERT INTO idempotency_records (idempotency_record_id,operation_type,idempotency_key,request_hash,status,resource_type,resource_id,response_snapshot,created_by_subject,correlation_id,request_id,completed_at) VALUES (:1,'provider.create',:2,'seed-hash','COMPLETED','provider',:3,:4,'integration-test','provider-seed','provider-seed',SYSTIMESTAMP)",
            &[&Uuid::new_v4().as_bytes().to_vec(), &idempotency_key, &provider_raw, &serde_json::json!({"provider_id":provider_id,"core_provisioning_status":"PENDING"}).to_string()],
        ).map_err(db_error)?;
        Ok(())
    }).await.expect("pending provider facts should seed");
}

async fn seed_ready_provider_without_accounts(pool: &OraclePool, provider_id: Uuid) {
    pool.with_connection(move |connection| {
        connection
            .execute(
                "INSERT INTO providers (provider_id,legal_name,trade_name,status,metadata_json,created_by_subject,updated_by_subject) VALUES (:1,'Incomplete Ready Provider','Incomplete Ready','READY','{}','integration-test','integration-test')",
                &[&provider_id.as_bytes().to_vec()],
            )
            .map_err(db_error)?;
        connection.commit().map_err(db_error)?;
        Ok(())
    })
    .await
    .expect("incomplete ready provider should seed");
}

async fn install_create_audit_failure_trigger(pool: &OraclePool) {
    install_trigger(
        pool,
        "CREATE OR REPLACE TRIGGER test_fail_provider_create_audit BEFORE INSERT ON audit_logs FOR EACH ROW WHEN (NEW.correlation_id = 'forced-provider-create-audit-rollback') BEGIN RAISE_APPLICATION_ERROR(-20011, 'forced provider create audit failure'); END;",
    ).await;
}

async fn install_terminal_audit_failure_trigger(pool: &OraclePool, provider_id: Uuid) {
    install_trigger(
        pool,
        &format!(
            "CREATE OR REPLACE TRIGGER test_fail_provider_terminal_audit BEFORE INSERT ON audit_logs FOR EACH ROW WHEN (NEW.correlation_id = 'provider-provisioning-{}-failed') BEGIN RAISE_APPLICATION_ERROR(-20012, 'forced provider terminal audit failure'); END;",
            provider_id.simple()
        ),
    ).await;
}

async fn install_trigger(pool: &OraclePool, sql: &str) {
    let sql = sql.to_string();
    pool.with_connection(move |connection| {
        connection.execute(&sql, &[]).map_err(db_error)?;
        Ok(())
    })
    .await
    .expect("test failure trigger should install");
}

async fn remove_trigger(pool: &OraclePool, trigger: &str) {
    let sql = format!("DROP TRIGGER {trigger}");
    pool.with_connection(move |connection| {
        connection.execute(&sql, &[]).map_err(db_error)?;
        Ok(())
    })
    .await
    .expect("test failure trigger should be removed");
}

async fn count_all(pool: &OraclePool, table: &str) -> i64 {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    query_i64(pool, sql, Vec::new()).await
}

async fn count_idempotency(pool: &OraclePool, key: &str) -> i64 {
    query_i64(
        pool,
        "SELECT COUNT(*) FROM idempotency_records WHERE idempotency_key=:1".to_string(),
        vec![key.to_string()],
    )
    .await
}

async fn account_status_count(pool: &OraclePool, provider_id: Uuid, status: &str) -> i64 {
    let status = status.to_string();
    pool.with_connection(move |connection| {
        connection
            .query_row_as(
                "SELECT COUNT(*) FROM provider_ledger_accounts WHERE provider_id=:1 AND status=:2",
                &[&provider_id.as_bytes().to_vec(), &status],
            )
            .map_err(db_error)
    })
    .await
    .unwrap()
}

async fn provider_status(pool: &OraclePool, provider_id: Uuid) -> String {
    query_string_by_raw(
        pool,
        "SELECT status FROM providers WHERE provider_id=:1",
        provider_id,
    )
    .await
}

async fn job_status(pool: &OraclePool, job_id: Uuid) -> String {
    query_string_by_raw(
        pool,
        "SELECT status FROM provider_provisioning_jobs WHERE provider_provisioning_job_id=:1",
        job_id,
    )
    .await
}

async fn idempotency_core_status(pool: &OraclePool, key: &str) -> String {
    let key = key.to_string();
    pool.with_connection(move |connection| {
        connection.query_row_as(
            "SELECT JSON_VALUE(response_snapshot,'$.core_provisioning_status') FROM idempotency_records WHERE idempotency_key=:1",
            &[&key],
        ).map_err(db_error)
    }).await.unwrap()
}

async fn query_string_by_raw(pool: &OraclePool, sql: &str, id: Uuid) -> String {
    let sql = sql.to_string();
    pool.with_connection(move |connection| {
        connection
            .query_row_as(&sql, &[&id.as_bytes().to_vec()])
            .map_err(db_error)
    })
    .await
    .unwrap()
}

async fn query_i64(pool: &OraclePool, sql: String, values: Vec<String>) -> i64 {
    pool.with_connection(move |connection| {
        let binds: Vec<&dyn oracle::sql_type::ToSql> = values
            .iter()
            .map(|value| value as &dyn oracle::sql_type::ToSql)
            .collect();
        connection.query_row_as(&sql, &binds).map_err(db_error)
    })
    .await
    .unwrap()
}

fn db_error(error: oracle::Error) -> DbError {
    DbError::Query(error.to_string())
}
