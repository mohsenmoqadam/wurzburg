mod support;

use std::{
    env,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::net::TcpListener;
use uuid::Uuid;
use wurzburg::{
    api::router::build_app_router, config::Settings, db::oracle::OraclePool,
    db::oracle::prepare_oracle_schema, state::AppState,
};

#[tokio::test]
async fn creates_card_range_through_running_wurzburg_http_instance() {
    if env::var("RUN_FULL_INTEGRATION_TESTS").ok().as_deref() != Some("1") {
        return;
    }

    let settings = Settings::new().expect("full integration settings should load");
    prepare_oracle_schema(&settings.database, &settings.migrations)
        .await
        .expect("Oracle schema should be prepared before Wurzburg starts");
    let state = Arc::new(
        AppState::new(settings)
            .await
            .expect("Wurzburg application state should start"),
    );
    let repository = state.db.clone();
    let app = build_app_router(state);
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let address = listener
        .local_addr()
        .expect("listener address should exist");

    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("Wurzburg HTTP instance should serve");
    });
    tokio::task::yield_now().await;

    let (start_card_number, end_card_number) = unique_card_range();
    let body = create_body(&start_card_number, &end_card_number, "initial-create");
    let idempotency_key = Uuid::new_v4().to_string();

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{address}/api/v1/card-ranges"))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("Idempotency-Key", &idempotency_key)
        .header("X-Correlation-Id", "card-range-http-integration")
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.10")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .header("Content-Type", "application/json")
        .body(body.clone())
        .send()
        .await
        .expect("HTTP request should complete");

    let status = response.status();
    let result_symbol = response
        .headers()
        .get("x-wurzburg-result-symbol")
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);
    let response_body = response
        .text()
        .await
        .expect("HTTP response body should read");

    assert!(
        status == reqwest::StatusCode::CREATED,
        "unexpected HTTP response: status={status}, body={response_body}"
    );
    assert_eq!(result_symbol.as_deref(), Some("SUCCESS"));
    assert!(response_body.contains("\"start_card_number\""));
    let created: serde_json::Value =
        serde_json::from_str(&response_body).expect("create response should be JSON");
    let card_range_id = created["card_range_id"]
        .as_str()
        .expect("created card range ID should be present");

    let get_response = client
        .get(format!(
            "http://{address}/api/v1/card-ranges/{card_range_id}"
        ))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("X-Correlation-Id", "card-range-http-integration-get")
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.10")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .send()
        .await
        .expect("HTTP get request should complete");
    let get_status = get_response.status();
    let get_body = get_response
        .text()
        .await
        .expect("HTTP get response body should read");

    assert!(
        get_status == reqwest::StatusCode::OK,
        "unexpected get HTTP response: status={get_status}, body={get_body}"
    );
    assert!(get_body.contains(card_range_id));

    let list_response = client
        .get(format!(
            "http://{address}/api/v1/card-ranges?status=DRAFT&funding_mode=SINGLE_PROVIDER&withdrawal_limit_authority=PLATFORM&limit=10"
        ))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("X-Correlation-Id", "card-range-http-integration-list")
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.10")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .send()
        .await
        .expect("HTTP list request should complete");
    let list_status = list_response.status();
    let list_body = list_response
        .text()
        .await
        .expect("HTTP list response body should read");

    assert!(
        list_status == reqwest::StatusCode::OK,
        "unexpected list HTTP response: status={list_status}, body={list_body}"
    );
    assert!(list_body.contains(card_range_id));

    // Scenario: a completed request is replayed from Oracle without creating a
    // second range or audit record. The same key with a different body conflicts.
    let (replay_status, replay_body) = post_card_range(
        &client,
        address,
        &idempotency_key,
        "card-range-http-integration-replay",
        &body,
    )
    .await;
    assert_eq!(replay_status, reqwest::StatusCode::OK, "{replay_body}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&replay_body).unwrap()["card_range_id"],
        card_range_id
    );
    assert_eq!(
        count_where(
            &repository.pool,
            "card_ranges",
            "start_card_number",
            &start_card_number,
        )
        .await,
        1
    );
    assert_eq!(
        count_where(
            &repository.pool,
            "audit_logs",
            "correlation_id",
            "card-range-http-integration",
        )
        .await,
        1
    );
    assert_eq!(
        count_where(
            &repository.pool,
            "idempotency_records",
            "idempotency_key",
            &idempotency_key,
        )
        .await,
        1
    );

    let conflicting_body = create_body(&start_card_number, &end_card_number, "changed-body");
    let (conflict_status, conflict_body) = post_card_range(
        &client,
        address,
        &idempotency_key,
        "card-range-http-integration-conflict",
        &conflicting_body,
    )
    .await;
    assert_eq!(conflict_status, reqwest::StatusCode::CONFLICT);
    assert!(conflict_body.contains("IDEMPOTENCY_KEY_CONFLICT"));

    // Scenario: concurrent requests with the same command identity converge on
    // one committed mutation and one replay of the exact response snapshot.
    let (concurrent_start, concurrent_end) = unique_card_range();
    let concurrent_body = create_body(&concurrent_start, &concurrent_end, "same-key-race");
    let concurrent_key = Uuid::new_v4().to_string();
    let first = post_card_range(
        &client,
        address,
        &concurrent_key,
        "card-range-concurrent-same-key-a",
        &concurrent_body,
    );
    let second = post_card_range(
        &client,
        address,
        &concurrent_key,
        "card-range-concurrent-same-key-b",
        &concurrent_body,
    );
    let ((first_status, first_body), (second_status, second_body)) = tokio::join!(first, second);
    let statuses = [first_status, second_status];
    assert!(
        statuses.contains(&reqwest::StatusCode::CREATED),
        "{first_body} {second_body}"
    );
    assert!(
        statuses.contains(&reqwest::StatusCode::OK),
        "{first_body} {second_body}"
    );
    let first_id =
        serde_json::from_str::<serde_json::Value>(&first_body).unwrap()["card_range_id"].clone();
    let second_id =
        serde_json::from_str::<serde_json::Value>(&second_body).unwrap()["card_range_id"].clone();
    assert_eq!(first_id, second_id);
    assert_eq!(
        count_where(
            &repository.pool,
            "card_ranges",
            "start_card_number",
            &concurrent_start,
        )
        .await,
        1
    );

    // Scenario: different commands racing to create the same inclusive range
    // are serialized. Exactly one commits and the other receives overlap.
    let (overlap_start, overlap_end) = unique_card_range();
    let overlap_body = create_body(&overlap_start, &overlap_end, "overlap-race");
    let first_overlap_key = Uuid::new_v4().to_string();
    let second_overlap_key = Uuid::new_v4().to_string();
    let first = post_card_range(
        &client,
        address,
        &first_overlap_key,
        "card-range-concurrent-overlap-a",
        &overlap_body,
    );
    let second = post_card_range(
        &client,
        address,
        &second_overlap_key,
        "card-range-concurrent-overlap-b",
        &overlap_body,
    );
    let ((first_status, first_body), (second_status, second_body)) = tokio::join!(first, second);
    let statuses = [first_status, second_status];
    assert!(
        statuses.contains(&reqwest::StatusCode::CREATED),
        "{first_body} {second_body}"
    );
    assert!(
        statuses.contains(&reqwest::StatusCode::CONFLICT),
        "{first_body} {second_body}"
    );
    assert_eq!(
        count_where(
            &repository.pool,
            "card_ranges",
            "start_card_number",
            &overlap_start,
        )
        .await,
        1
    );

    // Scenario: Oracle fails after the range insert while writing its audit
    // evidence. The transaction must leave no range, idempotency, or audit row,
    // and the public response must not expose Oracle diagnostics.
    install_audit_failure_trigger(&repository.pool).await;
    let (rollback_start, rollback_end) = unique_card_range();
    let rollback_body = create_body(&rollback_start, &rollback_end, "forced-rollback");
    let rollback_key = Uuid::new_v4().to_string();
    let (rollback_status, rollback_response) = post_card_range(
        &client,
        address,
        &rollback_key,
        "forced-card-range-audit-rollback",
        &rollback_body,
    )
    .await;
    remove_audit_failure_trigger(&repository.pool).await;

    assert_eq!(rollback_status, reqwest::StatusCode::INTERNAL_SERVER_ERROR);
    assert!(rollback_response.contains("SYSTEM_ERROR"));
    assert!(!rollback_response.contains("ORA-"));
    assert!(!rollback_response.contains("forced audit failure"));
    assert_eq!(
        count_where(
            &repository.pool,
            "card_ranges",
            "start_card_number",
            &rollback_start,
        )
        .await,
        0
    );
    assert_eq!(
        count_where(
            &repository.pool,
            "idempotency_records",
            "idempotency_key",
            &rollback_key,
        )
        .await,
        0
    );
    assert_eq!(
        count_where(
            &repository.pool,
            "audit_logs",
            "correlation_id",
            "forced-card-range-audit-rollback",
        )
        .await,
        0
    );
}

fn create_body(start: &str, end: &str, test_case: &str) -> String {
    serde_json::json!({
        "start_card_number": start,
        "end_card_number": end,
        "funding_mode": "SINGLE_PROVIDER",
        "withdrawal_limit_authority": "PLATFORM",
        "limit_calendar": {
            "timezone": "Asia/Tehran",
            "week_starts_on": "SATURDAY",
            "window_mode": "CALENDAR"
        },
        "issuance_enabled": true,
        "cms_operation_mode": "FULL",
        "metadata": { "test_case": test_case }
    })
    .to_string()
}

async fn post_card_range(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    idempotency_key: &str,
    correlation_id: &str,
    body: &str,
) -> (reqwest::StatusCode, String) {
    let response = client
        .post(format!("http://{address}/api/v1/card-ranges"))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("Idempotency-Key", idempotency_key)
        .header("X-Correlation-Id", correlation_id)
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.10")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .header("Content-Type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .expect("card range HTTP request should complete");
    let status = response.status();
    let body = response.text().await.expect("response body should read");
    (status, body)
}

async fn count_where(pool: &OraclePool, table: &str, column: &str, value: &str) -> i64 {
    let sql = format!("SELECT COUNT(*) FROM {table} WHERE {column} = :1");
    let value = value.to_string();
    pool.with_connection(move |connection| {
        connection
            .query_row_as::<i64>(&sql, &[&value])
            .map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))
    })
    .await
    .expect("verification query should succeed")
}

async fn install_audit_failure_trigger(pool: &OraclePool) {
    pool.with_connection(|connection| {
        connection
            .execute(
                r#"
                CREATE OR REPLACE TRIGGER test_fail_card_range_audit
                BEFORE INSERT ON audit_logs
                FOR EACH ROW
                WHEN (NEW.correlation_id = 'forced-card-range-audit-rollback')
                BEGIN
                    RAISE_APPLICATION_ERROR(-20001, 'forced audit failure');
                END;
                "#,
                &[],
            )
            .map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        Ok(())
    })
    .await
    .expect("audit failure trigger should install");
}

async fn remove_audit_failure_trigger(pool: &OraclePool) {
    pool.with_connection(|connection| {
        connection
            .execute("DROP TRIGGER test_fail_card_range_audit", &[])
            .map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        Ok(())
    })
    .await
    .expect("audit failure trigger should be removed");
}

fn unique_card_range() -> (String, String) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after Unix epoch")
        .as_nanos();
    let suffix = 1_000_000_000_u128 + (now % 8_999_999_000_u128);
    (
        format!("621986{suffix:010}"),
        format!("621986{:010}", suffix + 9),
    )
}
