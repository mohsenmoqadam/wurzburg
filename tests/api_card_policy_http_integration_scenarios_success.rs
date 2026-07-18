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
/// Prove the complete policy lifecycle through a real Wurzburg HTTP server and
/// the production Oracle repository: editable draft, durable publication
/// request, receipt-gated activation, replacement, and idempotent replay.
///
/// Oracle facts: a range is created through HTTP; a provider attachment is
/// inserted only when the scenario reaches the publication boundary.
/// External facts: Wolfsburg receipts are delivered through the same concrete
/// Oracle consumer operation that the future Kafka worker will invoke.
/// Final proof: only a matching receipt changes ACTIVE/SUPERSEDED state, while
/// null amount/count settings survive every API, outbox, and Oracle boundary.
#[tokio::test]
async fn manages_policy_lifecycle_through_running_wurzburg_and_oracle() {
    if env::var("RUN_FULL_INTEGRATION_TESTS").ok().as_deref() != Some("1") {
        return;
    }

    let mut settings = Settings::new().expect("full integration settings should load");
    settings.migrations.force_recreate = true;
    prepare_oracle_schema(&settings.database, &settings.migrations)
        .await
        .expect("Oracle schema should be rebuilt from the final baseline");
    settings.migrations.force_recreate = false;

    let state = Arc::new(
        AppState::new(settings)
            .await
            .expect("Wurzburg application state should start"),
    );
    let repository = state.db.clone();
    let address = start_server(state).await;
    let client = reqwest::Client::new();
    let card_range_id = create_platform_range(&client, address).await;

    // With no active provider, policy configuration remains an editable DRAFT
    // and no outbox event exists. Null metrics retain their exact meaning.
    let first_body = policy_body(1_000_000, None, "initial policy draft");
    let first_key = Uuid::new_v4().to_string();
    let (status, first) = put_policy(
        &client,
        address,
        card_range_id,
        &first_key,
        "policy-draft-create",
        &first_body,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::CREATED, "{first}");
    assert_eq!(first["disposition"], "CREATED");
    assert_eq!(first["profile"]["status"], "DRAFT");
    assert!(first["operation_id"].is_null());
    assert!(first["profile"]["withdrawal_limits"]["daily"]["max_count"].is_null());
    let first_policy_id = uuid_field(&first["profile"], "card_policy_profile_id");

    let (active_status, active_body) = get_current_policy(&client, address, card_range_id).await;
    assert_eq!(active_status, reqwest::StatusCode::NOT_FOUND);
    assert!(active_body.contains("CARD_POLICY_NOT_FOUND"));

    let edited_body = policy_body(1_500_000, Some(20), "edit unfrozen draft");
    let (status, edited) = put_policy(
        &client,
        address,
        card_range_id,
        &Uuid::new_v4().to_string(),
        "policy-draft-edit",
        &edited_body,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK, "{edited}");
    assert_eq!(edited["disposition"], "UPDATED");
    assert_eq!(
        uuid_field(&edited["profile"], "card_policy_profile_id"),
        first_policy_id
    );

    // The same key and canonical body replays the completed response. A replay
    // cannot create a second profile, audit row, or publication operation.
    let replay_key = Uuid::new_v4().to_string();
    let (created_status, created_snapshot) = put_policy(
        &client,
        address,
        card_range_id,
        &replay_key,
        "policy-idempotency-original",
        &edited_body,
    )
    .await;
    assert_eq!(
        created_status,
        reqwest::StatusCode::OK,
        "{created_snapshot}"
    );
    let (replay_status, replay_snapshot) = put_policy(
        &client,
        address,
        card_range_id,
        &replay_key,
        "policy-idempotency-replay",
        &edited_body,
    )
    .await;
    assert_eq!(replay_status, reqwest::StatusCode::OK);
    assert_eq!(replay_snapshot, created_snapshot);

    attach_active_provider(&repository.pool, card_range_id).await;
    let publication_body = policy_body(2_000_000, None, "publish first policy");
    let (status, pending) = put_policy(
        &client,
        address,
        card_range_id,
        &Uuid::new_v4().to_string(),
        "policy-first-publication",
        &publication_body,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::ACCEPTED, "{pending}");
    assert_eq!(pending["disposition"], "PUBLICATION_PENDING");
    let operation_id = uuid_field(&pending, "operation_id");
    let policy_id = uuid_field(&pending["profile"], "card_policy_profile_id");
    let version = pending["profile"]["version"].as_i64().unwrap();
    assert_outbox_contract(&repository.pool, operation_id).await;

    let runtime_key = format!("CPOL:SingleProvider:{card_range_id}");
    let receipt = PolicyMaterializationReceipt {
        receipt_event_id: Uuid::new_v4(),
        operation_id,
        card_range_id,
        card_policy_profile_id: policy_id,
        materialized_version: version,
        runtime_key: runtime_key.clone(),
        materialized_at: Utc::now(),
    };
    let activation = repository
        .apply_policy_materialization_receipt(receipt.clone())
        .await
        .expect("matching receipt should process");
    assert!(matches!(
        activation,
        PolicyReceiptPersistenceOutcome::Activated(_)
    ));
    assert_eq!(
        repository
            .apply_policy_materialization_receipt(receipt)
            .await
            .expect("duplicate receipt should replay"),
        PolicyReceiptPersistenceOutcome::Replayed
    );

    let (active_status, active_body) = get_current_policy(&client, address, card_range_id).await;
    assert_eq!(active_status, reqwest::StatusCode::OK, "{active_body}");
    let active: serde_json::Value = serde_json::from_str(&active_body).unwrap();
    assert_eq!(active["status"], "ACTIVE");
    assert_eq!(active["card_policy_profile_id"], policy_id.to_string());

    // A replacement freezes immediately because the range already has an
    // active provider, but the previous ACTIVE policy remains operational.
    let replacement_body = policy_body(3_000_000, Some(30), "replace active policy");
    let (status, replacement) = put_policy(
        &client,
        address,
        card_range_id,
        &Uuid::new_v4().to_string(),
        "policy-replacement",
        &replacement_body,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::ACCEPTED, "{replacement}");
    let replacement_id = uuid_field(&replacement["profile"], "card_policy_profile_id");
    let replacement_operation = uuid_field(&replacement, "operation_id");
    let replacement_version = replacement["profile"]["version"].as_i64().unwrap();
    assert!(replacement_version > version);

    let (_, still_active_body) = get_current_policy(&client, address, card_range_id).await;
    let still_active: serde_json::Value = serde_json::from_str(&still_active_body).unwrap();
    assert_eq!(
        still_active["card_policy_profile_id"],
        policy_id.to_string()
    );

    let replacement_receipt = PolicyMaterializationReceipt {
        receipt_event_id: Uuid::new_v4(),
        operation_id: replacement_operation,
        card_range_id,
        card_policy_profile_id: replacement_id,
        materialized_version: replacement_version,
        runtime_key,
        materialized_at: Utc::now(),
    };
    repository
        .apply_policy_materialization_receipt(replacement_receipt)
        .await
        .expect("replacement receipt should process");

    let (_, final_body) = get_current_policy(&client, address, card_range_id).await;
    let final_policy: serde_json::Value = serde_json::from_str(&final_body).unwrap();
    assert_eq!(
        final_policy["card_policy_profile_id"],
        replacement_id.to_string()
    );
    assert_eq!(final_policy["status"], "ACTIVE");
    assert_eq!(
        policy_status(&repository.pool, policy_id).await,
        "SUPERSEDED"
    );

    // Two real HTTP requests racing with one idempotency identity converge on
    // one Oracle profile. One request creates it and the waiter replays it.
    let concurrent_range_id = create_platform_range(&client, address).await;
    let concurrent_key = Uuid::new_v4().to_string();
    let concurrent_body = policy_body(4_000_000, None, "concurrent policy create");
    let first = put_policy(
        &client,
        address,
        concurrent_range_id,
        &concurrent_key,
        "policy-concurrent-a",
        &concurrent_body,
    );
    let second = put_policy(
        &client,
        address,
        concurrent_range_id,
        &concurrent_key,
        "policy-concurrent-b",
        &concurrent_body,
    );
    let ((first_status, first_body), (second_status, second_body)) = tokio::join!(first, second);
    assert!(
        [first_status, second_status].contains(&reqwest::StatusCode::CREATED),
        "{first_body} {second_body}"
    );
    assert!(
        [first_status, second_status].contains(&reqwest::StatusCode::OK),
        "{first_body} {second_body}"
    );
    assert_eq!(
        uuid_field(&first_body["profile"], "card_policy_profile_id"),
        uuid_field(&second_body["profile"], "card_policy_profile_id")
    );
    assert_eq!(
        count_policies(&repository.pool, concurrent_range_id).await,
        1
    );
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

async fn create_platform_range(client: &reqwest::Client, address: std::net::SocketAddr) -> Uuid {
    let suffix = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        % 8_000_000_000;
    let start = format!("7{:015}", 100_000_000_000_000_u128 + suffix);
    let end_number = start.parse::<u64>().unwrap() + 99;
    let body = serde_json::json!({
        "start_card_number": start,
        "end_card_number": format!("{end_number:016}"),
        "funding_mode": "SINGLE_PROVIDER",
        "withdrawal_limit_authority": "PLATFORM",
        "limit_calendar": {
            "timezone": "Asia/Tehran",
            "week_starts_on": "SATURDAY",
            "window_mode": "CALENDAR"
        },
        "issuance_enabled": true,
        "cms_operation_mode": "FULL",
        "metadata": {}
    });
    let response = client
        .post(format!("http://{address}/api/v1/card-ranges"))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("Idempotency-Key", Uuid::new_v4().to_string())
        .header("X-Correlation-Id", "policy-range-create")
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.10")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .header("Content-Type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    let status = response.status();
    let response_body = response.text().await.unwrap();
    let value: serde_json::Value = serde_json::from_str(&response_body).unwrap();
    assert_eq!(status, reqwest::StatusCode::CREATED, "{value}");
    uuid_field(&value, "card_range_id")
}

fn policy_body(max_amount: u64, max_count: Option<u32>, reason: &str) -> String {
    serde_json::json!({
        "reason": reason,
        "withdrawal_limits": {
            "per_transaction_min_amount": null,
            "per_transaction_max_amount": null,
            "daily": { "max_amount": max_amount, "max_count": max_count },
            "weekly": null,
            "monthly": { "max_amount": null, "max_count": null },
            "yearly": null
        }
    })
    .to_string()
}

async fn put_policy(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    card_range_id: Uuid,
    idempotency_key: &str,
    correlation_id: &str,
    body: &str,
) -> (reqwest::StatusCode, serde_json::Value) {
    let response = client
        .put(format!(
            "http://{address}/api/v1/card-ranges/{card_range_id}/policy"
        ))
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
        .unwrap();
    let status = response.status();
    let text = response.text().await.unwrap();
    let value = serde_json::from_str(&text).unwrap_or_else(|_| serde_json::json!({ "raw": text }));
    (status, value)
}

async fn get_current_policy(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    card_range_id: Uuid,
) -> (reqwest::StatusCode, String) {
    let response = client
        .get(format!(
            "http://{address}/api/v1/card-ranges/{card_range_id}/policy"
        ))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("X-Correlation-Id", "policy-current-read")
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.10")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    (status, body)
}

async fn attach_active_provider(pool: &OraclePool, card_range_id: Uuid) {
    let provider_id = Uuid::new_v4();
    let provider_raw = provider_id.as_bytes().to_vec();
    let range_raw = card_range_id.as_bytes().to_vec();
    pool.with_connection(move |connection| {
        connection.execute(
            "INSERT INTO providers (provider_id, legal_name, trade_name, status, created_by_subject, updated_by_subject) VALUES (:1, :2, :3, 'ACTIVE', :4, :4)",
            &[&provider_raw, &"Integration Provider", &"Integration", &"test-suite"],
        ).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        connection.execute(
            "INSERT INTO card_range_providers (card_range_id, provider_id, status, created_by_subject, updated_by_subject) VALUES (:1, :2, 'ACTIVE', :3, :3)",
            &[&range_raw, &provider_raw, &"test-suite"],
        ).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        connection.commit().map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        Ok(())
    }).await.unwrap();
}

async fn assert_outbox_contract(pool: &OraclePool, operation_id: Uuid) {
    let operation_raw = operation_id.as_bytes().to_vec();
    pool.with_connection(move |connection| {
        let payload = connection.query_row_as::<String>(
            "SELECT JSON_SERIALIZE(payload_json RETURNING CLOB) FROM integration_outbox WHERE operation_id = :1",
            &[&operation_raw],
        ).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(payload["event_type"], "CARD_POLICY_PROFILE_PUBLISH_REQUESTED");
        assert_eq!(payload["payload"]["calendar"]["timezone"], "Asia/Tehran");
        assert!(payload["payload"]["withdrawal_limits"]["daily"]["max_count"].is_null());
        Ok(())
    }).await.unwrap();
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

async fn count_policies(pool: &OraclePool, card_range_id: Uuid) -> i64 {
    let raw = card_range_id.as_bytes().to_vec();
    pool.with_connection(move |connection| {
        connection
            .query_row_as::<i64>(
                "SELECT COUNT(*) FROM card_policy_profiles WHERE card_range_id = :1",
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
