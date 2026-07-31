mod support;

use std::{
    env,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::net::TcpListener;
use uuid::Uuid;
use wurzburg::{
    api::router::build_app_router,
    config::Settings,
    db::oracle::prepare_oracle_schema,
    domain::{
        card_policy::PolicyMaterializationReceipt, provider_fee::ProviderFeeMaterializationReceipt,
    },
    kafka::contract::RuntimeMaterializationReceipt,
    object_storage::initialize_bucket,
    state::AppState,
};

/// Scenario goal: create a Provider through the real HTTP/WSO2 boundary and
/// prove the Oracle command, audit, idempotency replay, and all four live
/// TigerBeetle account contracts. Kafka provisioning may still be pending in
/// the create response and is covered by its dedicated real-broker scenario.
#[tokio::test]
async fn creates_and_replays_provider_with_four_verified_accounts() {
    if env::var("RUN_FULL_INTEGRATION_TESTS").ok().as_deref() != Some("1") {
        return;
    }
    support::init_test_tracing();

    let settings = Settings::new().expect("integration settings should load");
    initialize_bucket(&settings.object_storage)
        .await
        .expect("MinIO card-issuance bucket should be ready");
    prepare_oracle_schema(&settings.database, &settings.migrations)
        .await
        .expect("Oracle schema should be prepared");
    let state = Arc::new(
        AppState::new(settings)
            .await
            .expect("Wurzburg state should start"),
    );
    let repository = state.db.clone();
    let tb_client = state.tb_client.clone();
    let app = build_app_router(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    tokio::task::yield_now().await;

    let idempotency_key = Uuid::new_v4().to_string();
    let body = provider_body("provider-create-success");
    let client = reqwest::Client::new();
    let (status, response_body) = post_provider(
        &client,
        address,
        &idempotency_key,
        "provider-create-success",
        &body,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::CREATED, "{response_body}");
    let created: serde_json::Value = serde_json::from_str(&response_body).unwrap();
    assert_eq!(created["status"], "READY");
    assert_eq!(created["core_provisioning_status"], "SUCCEEDED");
    assert!(matches!(
        created["kafka_provisioning_status"].as_str(),
        Some("PENDING" | "SUCCEEDED")
    ));
    let provider_id = Uuid::parse_str(created["provider_id"].as_str().unwrap()).unwrap();

    let (activate_status, activate_body) = post_json(
        &client,
        address,
        &format!("/api/v1/providers/{provider_id}/activate"),
        &Uuid::new_v4().to_string(),
        "provider-activate",
        &serde_json::json!({"reason": "provider passed core provisioning"}).to_string(),
    )
    .await;
    assert_eq!(activate_status, reqwest::StatusCode::OK, "{activate_body}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&activate_body).unwrap()["status"],
        "ACTIVE"
    );

    // Provider collection reads are Oracle-only and use opaque keyset tokens.
    // They remain available independently of TigerBeetle read health.
    let (list_status, list_body) = get_json(
        &client,
        address,
        "/api/v1/providers?status=ACTIVE&page_size=200",
        "provider-list",
    )
    .await;
    assert_eq!(list_status, reqwest::StatusCode::OK, "{list_body}");
    let listed: serde_json::Value = serde_json::from_str(&list_body).unwrap();
    assert!(
        listed["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["provider_id"] == provider_id.to_string())
    );

    // Financial values are fetched live from TigerBeetle in one batch. Oracle
    // contributes account identity/category mappings only.
    let (ledger_status, ledger_body) = get_json(
        &client,
        address,
        &format!("/api/v1/providers/{provider_id}/ledger"),
        "provider-ledger-read",
    )
    .await;
    assert_eq!(ledger_status, reqwest::StatusCode::OK, "{ledger_body}");
    let ledger: serde_json::Value = serde_json::from_str(&ledger_body).unwrap();
    assert_eq!(ledger["currency"], "IRR");
    assert_eq!(ledger["accounts"].as_array().unwrap().len(), 4);
    for account in ledger["accounts"].as_array().unwrap() {
        assert_eq!(account["posted_balance"], "0");
        assert_eq!(account["effective_balance"], "0");
        assert!(account["tigerbeetle_account_id"].is_string());
    }

    // One Provider has multiple immutable audit entries. A page size of one
    // proves keyset pagination and filter-bound token generation.
    let (audit_status, audit_body) = get_json(
        &client,
        address,
        &format!(
            "/api/v1/admin/audit-logs?entity_type=PROVIDER&entity_id={provider_id}&page_size=1"
        ),
        "provider-audit-list",
    )
    .await;
    assert_eq!(audit_status, reqwest::StatusCode::OK, "{audit_body}");
    let audit: serde_json::Value = serde_json::from_str(&audit_body).unwrap();
    assert_eq!(audit["data"].as_array().unwrap().len(), 1);
    let next_page_token = audit["next_page_token"].as_str().unwrap();
    let (next_status, next_body) = get_json(
        &client,
        address,
        &format!(
            "/api/v1/admin/audit-logs?entity_type=PROVIDER&entity_id={provider_id}&page_size=1&page_token={next_page_token}"
        ),
        "provider-audit-list-next-page",
    )
    .await;
    assert_eq!(next_status, reqwest::StatusCode::OK, "{next_body}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&next_body).unwrap()["data"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    // Card facts: a DRAFT range and policy are created through their public
    // APIs. The first active Provider assignment must freeze CPOL and emit both
    // CPOL and CRCTL commands in the same Oracle transaction.
    let fee_response = client
        .put(format!(
            "http://{address}/api/v1/providers/{provider_id}/fee-profile"
        ))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("Idempotency-Key", Uuid::new_v4().to_string())
        .header("X-Correlation-Id", "provider-fee-draft")
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.20")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .header("Content-Type", "application/json")
        .body(
            serde_json::json!({
                "rate_bps": 100,
                "fixed_amount_rials": 0,
                "fee_payer": "PROVIDER_USER",
                "reason": "initial provider fee profile"
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    let fee_status = fee_response.status();
    let fee_text = fee_response.text().await.unwrap();
    assert_eq!(fee_status, reqwest::StatusCode::CREATED, "{fee_text}");
    let fee_profile_id = Uuid::parse_str(
        serde_json::from_str::<serde_json::Value>(&fee_text).unwrap()["profile"]
            ["provider_fee_profile_id"]
            .as_str()
            .unwrap(),
    )
    .unwrap();

    let (card_range_id, card_policy_profile_id, issued_pan) =
        create_range_and_policy(&client, address).await;
    let assignment_body = serde_json::json!({
        "card_range_id": card_range_id,
        "reason": "initial provider eligibility"
    })
    .to_string();
    let assignment_response = client
        .put(format!(
            "http://{address}/api/v1/providers/{provider_id}/card-range"
        ))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("Idempotency-Key", Uuid::new_v4().to_string())
        .header("X-Correlation-Id", "provider-range-assign")
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.20")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .header("Content-Type", "application/json")
        .body(assignment_body)
        .send()
        .await
        .unwrap();
    let assignment_status = assignment_response.status();
    let assignment_text = assignment_response.text().await.unwrap();
    assert_eq!(
        assignment_status,
        reqwest::StatusCode::ACCEPTED,
        "{assignment_text}"
    );
    let assignment: serde_json::Value = serde_json::from_str(&assignment_text).unwrap();
    let policy_operation_id = value_uuid(&assignment, "policy_operation_id");
    let fee_operation_id = value_uuid(&assignment, "fee_operation_id");
    let range_operation_id = value_uuid(&assignment, "range_control_operation_id");
    let (range_status, range_body) = get_json(
        &client,
        address,
        &format!("/api/v1/card-ranges/{card_range_id}"),
        "provider-range-after-assignment",
    )
    .await;
    assert_eq!(range_status, reqwest::StatusCode::OK, "{range_body}");
    let range_operational_version = serde_json::from_str::<serde_json::Value>(&range_body).unwrap()
        ["operational_version"]
        .as_i64()
        .unwrap();

    repository
        .apply_policy_materialization_receipt(PolicyMaterializationReceipt {
            receipt_event_id: Uuid::new_v4(),
            operation_id: policy_operation_id,
            card_range_id,
            card_policy_profile_id,
            materialized_version: 1,
            runtime_key: format!("CPOL:SingleProvider:{card_range_id}"),
            materialized_at: chrono::Utc::now(),
        })
        .await
        .expect("policy receipt should activate the policy");
    repository
        .apply_provider_fee_materialization_receipt(ProviderFeeMaterializationReceipt {
            receipt_event_id: Uuid::new_v4(),
            operation_id: fee_operation_id,
            provider_id,
            provider_fee_profile_id: fee_profile_id,
            materialized_version: 1,
            runtime_key: format!("FEE:{provider_id}"),
            materialized_at: chrono::Utc::now(),
        })
        .await
        .expect("fee receipt should activate the fee profile");
    repository
        .apply_range_control_receipt(RuntimeMaterializationReceipt {
            receipt_event_id: Uuid::new_v4(),
            operation_id: range_operation_id,
            profile_type: "CRCTL".to_string(),
            aggregate_id: card_range_id,
            profile_id: None,
            materialized_version: range_operational_version,
            runtime_key: format!("CRCTL:{card_range_id}"),
            materialized_at: chrono::Utc::now(),
        })
        .await
        .expect("range receipt should finalize provider eligibility");

    let (range_activate_status, range_activate_body) = post_json(
        &client,
        address,
        &format!("/api/v1/card-ranges/{card_range_id}/activate"),
        &Uuid::new_v4().to_string(),
        "provider-range-activate",
        &serde_json::json!({"reason":"issuance integration scenario"}).to_string(),
    )
    .await;
    assert_eq!(
        range_activate_status,
        reqwest::StatusCode::ACCEPTED,
        "{range_activate_body}"
    );

    let enrollment_key = Uuid::new_v4().to_string();
    let enrollment_body = serde_json::json!({
        "national_id":"0013547852",
        "first_name":"Integration",
        "last_name":"Cardholder",
        "provider_customer_reference":format!("customer-{provider_id}"),
        "selection_reference":format!("selection-{provider_id}"),
        "card_instruction":{
            "type":"ISSUE_NEW",
            "birth_date":"1990-01-01",
            "mobile":"09120000000",
            "delivery_province":"Tehran",
            "delivery_city":"Tehran",
            "delivery_address":"Integration delivery address",
            "postal_code":"1234567890"
        },
        "metadata":{}
    })
    .to_string();
    let (enroll_status, enroll_body) = post_json(
        &client,
        address,
        &format!("/api/v1/providers/{provider_id}/users"),
        &enrollment_key,
        "provider-user-enroll",
        &enrollment_body,
    )
    .await;
    assert_eq!(
        enroll_status,
        reqwest::StatusCode::ACCEPTED,
        "{enroll_body}"
    );
    let enrolled: serde_json::Value = serde_json::from_str(&enroll_body).unwrap();
    let user_id = value_uuid(&enrolled, "user_id");
    let issuance_request_id = value_uuid(&enrolled, "issuance_request_id");

    let (batch_status, batch_body) = post_json(
        &client,
        address,
        "/api/v1/admin/card-issuance-batches",
        &Uuid::new_v4().to_string(),
        "issuance-batch-create",
        &serde_json::json!({"batch_size":50}).to_string(),
    )
    .await;
    assert_eq!(batch_status, reqwest::StatusCode::CREATED, "{batch_body}");
    let batch: serde_json::Value = serde_json::from_str(&batch_body).unwrap();
    let batch_id = value_uuid(&batch, "batch_id");

    let duplicate_result_csv = format!(
        "issuance_request_id,status,card_number,issuer_reference,failure_code,failure_message,produced_at,dispatched_at,tracking_reference\n{issuance_request_id},REJECTED,,,BANK_REJECTED,,,,\n{issuance_request_id},REJECTED,,,BANK_REJECTED,,,,\n"
    );
    let duplicate_response = upload_result_file(
        &client,
        address,
        batch_id,
        &Uuid::new_v4().to_string(),
        "issuance-result-duplicate-row",
        duplicate_result_csv,
    )
    .await;
    assert_eq!(
        duplicate_response.0,
        reqwest::StatusCode::BAD_REQUEST,
        "{}",
        duplicate_response.1
    );
    assert!(
        duplicate_response
            .1
            .contains("CARD_ISSUANCE_RESULT_CONTRACT_INVALID")
    );

    let result_csv = format!(
        "issuance_request_id,status,card_number,issuer_reference,failure_code,failure_message,produced_at,dispatched_at,tracking_reference\n{issuance_request_id},ISSUED,{issued_pan},issuer-1,,,,,tracking-1\n"
    );
    let (result_status, result_body) = upload_result_file(
        &client,
        address,
        batch_id,
        &Uuid::new_v4().to_string(),
        "issuance-result-upload",
        result_csv,
    )
    .await;
    assert_eq!(result_status, reqwest::StatusCode::OK, "{result_body}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&result_body).unwrap()["issued_count"],
        1
    );

    let (cards_status, cards_body) = get_json(
        &client,
        address,
        &format!("/api/v1/users/{user_id}/cards"),
        "user-card-list",
    )
    .await;
    assert_eq!(cards_status, reqwest::StatusCode::OK, "{cards_body}");
    let cards: serde_json::Value = serde_json::from_str(&cards_body).unwrap();
    assert_eq!(cards.as_array().unwrap().len(), 1);
    assert_eq!(cards[0]["provider_ids"][0], provider_id.to_string());

    let card_id = Uuid::parse_str(cards[0]["card_id"].as_str().unwrap()).unwrap();
    let usage = wurzburg::domain::user_card::PolicyUsageAccountIds::for_card(card_id);
    for id in usage.ordered() {
        assert_eq!(
            tb_client.lookup_account(id.as_u128()).await.unwrap().len(),
            1
        );
    }

    let (provider_user_status, provider_user_body) = get_json(
        &client,
        address,
        &format!("/api/v1/providers/{provider_id}/users/{user_id}"),
        "provider-user-read-after-issuance",
    )
    .await;
    assert_eq!(
        provider_user_status,
        reqwest::StatusCode::OK,
        "{provider_user_body}"
    );
    let provider_user: serde_json::Value = serde_json::from_str(&provider_user_body).unwrap();
    assert_eq!(provider_user["status"], "ACTIVE");
    let provider_user_account_id = value_uuid(&provider_user, "provider_user_account_id");
    let provider_user_accounts = tb_client
        .lookup_account(provider_user_account_id.as_u128())
        .await
        .expect("provider-user TigerBeetle lookup should succeed");
    assert_eq!(provider_user_accounts.len(), 1);

    let projection = load_card_projection_payload(&repository.pool, card_id).await;
    assert_eq!(projection["payload"]["card_id"], card_id.to_string());
    assert_eq!(
        projection["payload"]["funding_sources"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        projection["payload"]["funding_sources"][0]["ledger_accounts"]["user_provider_account"],
        provider_user_account_id.to_string()
    );
    let projection_text = projection.to_string();
    assert!(!projection_text.contains(&issued_pan));
    assert!(!projection_text.contains("0013547852"));
    assert!(!projection_text.contains("Integration"));

    let mappings = repository
        .get_provider_ledger_mappings(provider_id)
        .await
        .expect("provider mappings should load");
    assert_eq!(mappings.len(), 4);
    for mapping in mappings {
        let accounts = tb_client
            .lookup_account(mapping.tigerbeetle_account_id.as_u128())
            .await
            .expect("TigerBeetle account lookup should succeed");
        assert_eq!(accounts.len(), 1);
        assert_eq!(accounts[0].user_data_128, provider_id.as_u128());
    }

    let (replay_status, replay_body) = post_provider(
        &client,
        address,
        &idempotency_key,
        "provider-create-replay",
        &body,
    )
    .await;
    assert_eq!(replay_status, reqwest::StatusCode::OK, "{replay_body}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&replay_body).unwrap(),
        created
    );

    let changed = provider_body("different-request");
    let (conflict_status, conflict_body) = post_provider(
        &client,
        address,
        &idempotency_key,
        "provider-create-conflict",
        &changed,
    )
    .await;
    assert_eq!(conflict_status, reqwest::StatusCode::CONFLICT);
    assert!(conflict_body.contains("IDEMPOTENCY_KEY_CONFLICT"));
}

async fn create_range_and_policy(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
) -> (Uuid, Uuid, String) {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        % 9_000_000_000;
    let start = format!("621987{:010}", 1_000_000_000_u128 + suffix);
    let end_value = start.parse::<u64>().unwrap() + 9;
    let range_body = serde_json::json!({
        "start_card_number": start,
        "end_card_number": format!("{end_value:016}"),
        "funding_mode": "SINGLE_PROVIDER",
        "withdrawal_limit_authority": "PLATFORM",
        "limit_calendar": {"timezone":"Asia/Tehran","week_starts_on":"SATURDAY","window_mode":"CALENDAR"},
        "issuance_enabled": true,
        "cms_operation_mode": "FULL",
        "metadata": {}
    }).to_string();
    let (status, body) = post_json(
        client,
        address,
        "/api/v1/card-ranges",
        &Uuid::new_v4().to_string(),
        "provider-range-create",
        &range_body,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::CREATED, "{body}");
    let range_id = Uuid::parse_str(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["card_range_id"]
            .as_str()
            .unwrap(),
    )
    .unwrap();

    let policy_body = serde_json::json!({
        "reason": "initial provider range policy",
        "withdrawal_limits": {
            "per_transaction_min_amount": null,
            "per_transaction_max_amount": null,
            "daily": {"max_amount": null,"max_count": null},
            "weekly": null,"monthly": null,"yearly": null
        }
    })
    .to_string();
    let response = client
        .put(format!(
            "http://{address}/api/v1/card-ranges/{range_id}/policy"
        ))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("Idempotency-Key", Uuid::new_v4().to_string())
        .header("X-Correlation-Id", "provider-range-policy")
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.20")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .header("Content-Type", "application/json")
        .body(policy_body)
        .send()
        .await
        .unwrap();
    let policy_status = response.status();
    let policy_text = response.text().await.unwrap();
    assert_eq!(policy_status, reqwest::StatusCode::CREATED, "{policy_text}");
    let policy_id = value_uuid(
        &serde_json::from_str::<serde_json::Value>(&policy_text).unwrap()["profile"],
        "card_policy_profile_id",
    );
    (range_id, policy_id, start)
}

fn value_uuid(value: &serde_json::Value, field: &str) -> Uuid {
    Uuid::parse_str(value[field].as_str().expect("UUID field should exist")).unwrap()
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
        .header("X-WSO2-Client-IP", "198.51.100.20")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .header("Content-Type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    let status = response.status();
    let text = response.text().await.unwrap();
    (status, text)
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
        .expect("read HTTP request should complete");
    let status = response.status();
    let text = response.text().await.expect("read response should read");
    (status, text)
}

async fn upload_result_file(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    batch_id: Uuid,
    idempotency_key: &str,
    correlation_id: &str,
    body: String,
) -> (reqwest::StatusCode, String) {
    let response = client
        .post(format!(
            "http://{address}/api/v1/admin/card-issuance-batches/{batch_id}/result-file"
        ))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("Idempotency-Key", idempotency_key)
        .header("X-Correlation-Id", correlation_id)
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.20")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .header("Content-Type", "text/csv")
        .body(body)
        .send()
        .await
        .expect("issuance result upload should complete");
    let status = response.status();
    let body = response.text().await.expect("issuance result should read");
    (status, body)
}

async fn load_card_projection_payload(
    pool: &wurzburg::db::oracle::OraclePool,
    card_id: Uuid,
) -> serde_json::Value {
    let card_raw = wurzburg::db::oracle::types::uuid_to_raw16(card_id).to_vec();
    pool.with_connection(move |connection| {
        let payload: String = connection
            .query_row_as(
                "SELECT JSON_SERIALIZE(payload_json RETURNING CLOB) FROM integration_outbox WHERE aggregate_id=:1 AND event_type='CARD_PROFILE_PUBLISH_REQUESTED' ORDER BY created_at DESC FETCH FIRST 1 ROW ONLY",
                &[&card_raw],
            )
            .map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        serde_json::from_str(&payload)
            .map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))
    })
    .await
    .expect("card projection outbox payload should load")
}

fn provider_body(marker: &str) -> String {
    serde_json::json!({
        "legal_name": format!("Integration Legal Provider {marker}"),
        "trade_name": "Integration Provider",
        "tax_id": null,
        "registration_number": null,
        "email_address": null,
        "website_url": null,
        "mailing_address": null,
        "metadata": { "marker": marker },
        "contacts": [],
        "operational_profile": {
            "effective_at": "2026-01-01T00:00:00Z",
            "profile": {
                "timezone": "Asia/Tehran",
                "user_onboarding": { "enabled": true, "active_windows": [], "max_total_users": null },
                "credit_grant": { "enabled": true, "mode": "FixedLimit", "limit_amount_rials": 1000000000 },
                "credit_return": { "enabled": true },
                "card_operations": {
                    "new_assignment_enabled": true,
                    "same_pan_reprint_enabled": true,
                    "new_pan_replacement_enabled": true,
                    "attach_existing_multi_provider_card_enabled": true
                },
                "event_delivery": { "enabled": true, "disabled_reason": null }
            }
        }
    }).to_string()
}

async fn post_provider(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    idempotency_key: &str,
    correlation_id: &str,
    body: &str,
) -> (reqwest::StatusCode, String) {
    let response = client
        .post(format!("http://{address}/api/v1/providers"))
        .bearer_auth(support::signed_platform_admin_jwt())
        .header("Idempotency-Key", idempotency_key)
        .header("X-Correlation-Id", correlation_id)
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.20")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .header("Content-Type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .expect("provider HTTP request should complete");
    let status = response.status();
    let body = response
        .text()
        .await
        .expect("provider response should read");
    (status, body)
}
