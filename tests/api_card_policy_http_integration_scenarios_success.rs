mod support;

use std::{env, sync::Arc, time::SystemTime};

use chrono::Utc;
use tokio::net::TcpListener;
use uuid::Uuid;
use wurzburg::{
    api::router::build_app_router,
    config::Settings,
    db::oracle::{
        OraclePool, PolicyReceiptPersistenceOutcome, RangeControlReceiptOutcome,
        prepare_oracle_schema,
    },
    domain::card_policy::PolicyMaterializationReceipt,
    messaging::contract::RuntimeMaterializationReceipt,
    state::AppState,
};

/// Scenario goal:
/// Prove the complete policy lifecycle through a real Wurzburg HTTP server and
/// the production Oracle repository: editable draft, durable publication
/// request, receipt-gated activation, replacement, and idempotent replay.
///
/// Oracle facts: a range is created through HTTP; an active provider with its
/// required active fee profile is attached only at the publication boundary.
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
    assert_eq!(first["limit_calendar"]["week_starts_on"], "SATURDAY");
    assert_eq!(first["limit_calendar"]["window_mode"], "CALENDAR");
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
    assert_eq!(
        replay_snapshot["limit_calendar"]["week_starts_on"],
        "SATURDAY"
    );
    assert_eq!(replay_snapshot["limit_calendar"]["window_mode"], "CALENDAR");

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

    // Range activation is a real asynchronous runtime-control command. Oracle
    // state, audit, idempotency, and CRCTL outbox evidence commit together;
    // only Wolfsburg's matching receipt advances the materialized version.
    let (activate_status, activated) = mutate_range(
        &client,
        address,
        card_range_id,
        "activate",
        serde_json::json!({"reason":"activate fully provisioned range"}),
    )
    .await;
    assert_eq!(
        activate_status,
        reqwest::StatusCode::ACCEPTED,
        "{activated}"
    );
    assert_eq!(activated["card_range"]["status"], "ACTIVE");
    let control_operation = uuid_field(&activated, "operation_id");
    let control_version = activated["card_range"]["operational_version"]
        .as_i64()
        .unwrap();

    let (pending_status, pending_operation) =
        get_operation(&client, address, control_operation).await;
    assert_eq!(pending_status, reqwest::StatusCode::OK);
    assert_eq!(
        pending_operation["event_type"],
        "CARD_RANGE_CONTROL_PUBLISH_REQUESTED"
    );
    assert_eq!(pending_operation["status"], "PENDING");

    let control_receipt = RuntimeMaterializationReceipt {
        receipt_event_id: Uuid::new_v4(),
        operation_id: control_operation,
        profile_type: "CRCTL".to_string(),
        aggregate_id: card_range_id,
        profile_id: None,
        materialized_version: control_version,
        runtime_key: format!("CRCTL:{card_range_id}"),
        materialized_at: Utc::now(),
    };
    let mismatched_control_receipt = RuntimeMaterializationReceipt {
        receipt_event_id: Uuid::new_v4(),
        runtime_key: format!("CRCTL:wrong-{card_range_id}"),
        ..control_receipt.clone()
    };
    assert_eq!(
        repository
            .apply_range_control_receipt(mismatched_control_receipt.clone())
            .await
            .unwrap(),
        RangeControlReceiptOutcome::Mismatch
    );
    assert_eq!(
        repository
            .apply_range_control_receipt(mismatched_control_receipt)
            .await
            .unwrap(),
        RangeControlReceiptOutcome::Mismatch
    );
    assert_eq!(
        repository
            .apply_range_control_receipt(control_receipt.clone())
            .await
            .unwrap(),
        RangeControlReceiptOutcome::Materialized
    );
    assert_eq!(
        repository
            .apply_range_control_receipt(control_receipt)
            .await
            .unwrap(),
        RangeControlReceiptOutcome::Replayed
    );
    let (_, materialized_operation) = get_operation(&client, address, control_operation).await;
    assert_eq!(materialized_operation["status"], "MATERIALIZED");

    // A semantic no-op is not a new business command: it creates no version,
    // outbox event, audit row, or idempotency completion.
    let (no_op_status, no_op_body) = mutate_range(
        &client,
        address,
        card_range_id,
        "operational-controls",
        serde_json::json!({
            "issuance_enabled": true,
            "cms_operation_mode": "FULL",
            "reason": "must not manufacture a runtime version"
        }),
    )
    .await;
    assert_eq!(
        no_op_status,
        reqwest::StatusCode::BAD_REQUEST,
        "{no_op_body}"
    );
    assert_eq!(no_op_body["error"]["rs_code"], 6400);

    // Structural edits are frozen after activation, while operational controls
    // remain versioned asynchronous commands.
    let patch_response = client.patch(format!("http://{address}/api/v1/card-ranges/{card_range_id}"))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("Idempotency-Key", Uuid::new_v4().to_string())
        .header("X-Correlation-Id", "active-range-immutable")
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.10")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .header("Content-Type", "application/json")
        .body(serde_json::json!({"start_card_number":"7111111111111111","end_card_number":"7111111111111199","funding_mode":"SINGLE_PROVIDER","withdrawal_limit_authority":"PLATFORM","limit_calendar":{"timezone":"Asia/Tehran","week_starts_on":"SATURDAY","window_mode":"CALENDAR"},"issuance_enabled":true,"cms_operation_mode":"FULL","metadata":{},"reason":"forbidden structural edit"}).to_string())
        .send().await.unwrap();
    assert_eq!(patch_response.status(), reqwest::StatusCode::CONFLICT);
    assert!(
        patch_response
            .text()
            .await
            .unwrap()
            .contains("CARD_RANGE_IMMUTABLE_FIELD")
    );

    let (control_status, controls) = mutate_range(
        &client, address, card_range_id, "operational-controls",
        serde_json::json!({"issuance_enabled":false,"cms_operation_mode":"BALANCE_ONLY","reason":"temporary operational restriction"}),
    ).await;
    assert_eq!(control_status, reqwest::StatusCode::ACCEPTED, "{controls}");
    assert_eq!(controls["card_range"]["issuance_enabled"], false);
    assert_eq!(controls["card_range"]["cms_operation_mode"], "BALANCE_ONLY");

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

    // Policy history is a real Oracle keyset query. Prove both the initial
    // page (no cursor bind) and the next page selected by before_version.
    let (history_status, history) = list_policies(&client, address, card_range_id, None, 1).await;
    assert_eq!(history_status, reqwest::StatusCode::OK, "{history}");
    assert_eq!(history["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        history["items"][0]["card_policy_profile_id"],
        replacement_id.to_string()
    );

    let (previous_status, previous) = list_policies(
        &client,
        address,
        card_range_id,
        Some(replacement_version),
        1,
    )
    .await;
    assert_eq!(previous_status, reqwest::StatusCode::OK, "{previous}");
    assert_eq!(previous["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        previous["items"][0]["card_policy_profile_id"],
        policy_id.to_string()
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

    // The real Oracle relay lease is recoverable and preserves typed event
    // identity across retries. Broker I/O is intentionally outside this DB
    // transaction and is covered by the separately gated Kafka smoke test.
    let relay_worker = format!("integration-relay-{}", Uuid::new_v4());
    let claimed = repository
        .claim_outbox_batch(relay_worker.clone(), 50, 45_000)
        .await
        .expect("outbox batch should lease");
    assert!(!claimed.is_empty());
    assert!(claimed.iter().all(|event| event.schema_version == 1));
    let retried_event_id = claimed[0].event_id;
    repository
        .reschedule_outbox_event(retried_event_id, relay_worker.clone(), false, 0)
        .await
        .expect("delivery uncertainty should reschedule");
    let reclaimed = repository
        .claim_outbox_batch(relay_worker.clone(), 50, 45_000)
        .await
        .expect("rescheduled event should be claimable");
    let retried = reclaimed
        .iter()
        .find(|event| event.event_id == retried_event_id)
        .expect("same immutable event should be retried");
    assert!(retried.attempt_count >= 2);
    repository
        .mark_outbox_published(retried_event_id, relay_worker)
        .await
        .expect("broker acknowledgement should finalize publication");

    // A malformed Kafka record stores only deterministic transport evidence;
    // replaying the same topic/partition/offset remains idempotent.
    repository
        .record_kafka_poison_message(
            "receipt.test".to_string(),
            0,
            42,
            Some("00".repeat(32)),
            "RECEIPT_ENVELOPE_INVALID",
        )
        .await
        .unwrap();
    repository
        .record_kafka_poison_message(
            "receipt.test".to_string(),
            0,
            42,
            Some("00".repeat(32)),
            "RECEIPT_ENVELOPE_INVALID",
        )
        .await
        .unwrap();
    assert_eq!(
        count_poison(&repository.pool, "receipt.test", 0, 42).await,
        1
    );
}

async fn count_poison(pool: &OraclePool, topic: &str, partition: i32, offset: i64) -> i64 {
    let topic = topic.to_string();
    pool.with_connection(move |connection| connection.query_row_as::<i64>("SELECT COUNT(*) FROM kafka_poison_messages WHERE topic_name=:1 AND partition_id=:2 AND message_offset=:3", &[&topic, &partition, &offset]).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))).await.unwrap()
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

async fn list_policies(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    card_range_id: Uuid,
    before_version: Option<i64>,
    limit: u16,
) -> (reqwest::StatusCode, serde_json::Value) {
    let mut url =
        format!("http://{address}/api/v1/card-ranges/{card_range_id}/policies?limit={limit}");
    if let Some(before_version) = before_version {
        url.push_str(&format!("&before_version={before_version}"));
    }
    let response = client
        .get(url)
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("X-Correlation-Id", "policy-history-read")
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.10")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .send()
        .await
        .unwrap();
    let status = response.status();
    let text = response.text().await.unwrap();
    let body = serde_json::from_str(&text).unwrap_or_else(|_| serde_json::json!({ "raw": text }));
    (status, body)
}

async fn mutate_range(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    card_range_id: Uuid,
    action: &str,
    body: serde_json::Value,
) -> (reqwest::StatusCode, serde_json::Value) {
    let url = format!("http://{address}/api/v1/card-ranges/{card_range_id}/{action}");
    let request = if action == "operational-controls" {
        client.put(url)
    } else {
        client.post(url)
    };
    let response = request
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("Idempotency-Key", Uuid::new_v4().to_string())
        .header("X-Correlation-Id", format!("range-{action}"))
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
    (status, serde_json::from_str(&text).unwrap())
}

async fn get_operation(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    operation_id: Uuid,
) -> (reqwest::StatusCode, serde_json::Value) {
    let response = client
        .get(format!("http://{address}/api/v1/operations/{operation_id}"))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("X-Correlation-Id", "operation-status")
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.10")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = serde_json::from_str(&response.text().await.unwrap()).unwrap();
    (status, body)
}

async fn attach_active_provider(pool: &OraclePool, card_range_id: Uuid) {
    let provider_id = Uuid::new_v4();
    let fee_profile_id = Uuid::new_v4();
    let fee_operation_id = Uuid::new_v4();
    let fee_event_id = Uuid::new_v4();
    let provider_raw = provider_id.as_bytes().to_vec();
    let fee_profile_raw = fee_profile_id.as_bytes().to_vec();
    let fee_operation_raw = fee_operation_id.as_bytes().to_vec();
    let fee_event_raw = fee_event_id.as_bytes().to_vec();
    let range_raw = card_range_id.as_bytes().to_vec();
    pool.with_connection(move |connection| {
        connection.execute(
            "INSERT INTO providers (provider_id, legal_name, trade_name, status, created_by_subject, updated_by_subject) VALUES (:1, :2, :3, 'ACTIVE', :4, :4)",
            &[&provider_raw, &"Integration Provider", &"Integration", &"test-suite"],
        ).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        connection.execute(
            "INSERT INTO integration_operations (operation_id,operation_type,aggregate_type,aggregate_id,status,event_count,published_event_count) VALUES (:1,'PROVIDER_FEE_PROFILE_PUBLISH','PROVIDER',:2,'PUBLISHED',1,1)",
            &[&fee_operation_raw, &provider_raw],
        ).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        connection.execute(
            "INSERT INTO integration_outbox (outbox_event_id, operation_id, event_type, aggregate_type, aggregate_id, partition_key, payload_json, status, published_at) VALUES (:1, :2, 'PROVIDER_FEE_PROFILE_PUBLISH_REQUESTED', 'PROVIDER', :3, :4, '{}', 'PUBLISHED', SYSTIMESTAMP)",
            &[&fee_event_raw, &fee_operation_raw, &provider_raw, &provider_id.to_string()],
        ).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        connection.execute(
            "INSERT INTO provider_fee_profiles (provider_fee_profile_id, provider_id, rate_bps, fixed_amount_rials, fee_payer, status, version, publication_operation_id, created_by_subject, updated_by_subject, change_reason, activated_at) VALUES (:1, :2, 0, 0, 'PROVIDER_USER', 'ACTIVE', 1, :3, :4, :5, :6, SYSTIMESTAMP)",
            &[&fee_profile_raw, &provider_raw, &fee_operation_raw, &"test-suite", &"test-suite", &"cross-domain policy scenario prerequisite"],
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
