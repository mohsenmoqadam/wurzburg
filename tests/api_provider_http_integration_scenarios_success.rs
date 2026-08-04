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
    messaging::contract::RuntimeMaterializationReceipt,
    object_storage::initialize_bucket,
    services::{
        provider_credit::ProviderCreditService, provider_user::ProviderUserService,
        wal_recovery::start_wal_recovery_worker,
    },
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
    let ledger_client = state.ledger_client.clone();
    let card_profile_locks = state.card_profile_locks.clone();
    let provider_credit_service = ProviderCreditService::new(
        repository.clone(),
        ledger_client.clone(),
        state.config.tigerbeetle.clone(),
        card_profile_locks.clone(),
        state.provider_credit_locks.clone(),
    );
    let dragonfly = state.redis.clone();
    let app = build_app_router(state.clone());
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
    let national_id = unique_valid_national_id();
    let enrollment_body = serde_json::json!({
        "national_id":national_id.clone(),
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
            ledger_client
                .lookup_account(id.as_u128())
                .await
                .unwrap()
                .len(),
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
    let provider_user_id = value_uuid(&provider_user, "provider_user_id");
    let provider_user_account_id = value_uuid(&provider_user, "provider_user_account_id");
    let provider_user_accounts = ledger_client
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
    assert!(!projection_text.contains(&national_id));
    assert!(!projection_text.contains("Integration"));

    // Wolfsburg materializes the initial card profile before a cardholder or
    // platform operator may replace its funding order.
    let initial_operation_id = value_uuid(&projection, "operation_id");
    repository
        .apply_card_profile_receipt(RuntimeMaterializationReceipt {
            receipt_event_id: Uuid::new_v4(),
            operation_id: initial_operation_id,
            profile_type: "CP".to_string(),
            aggregate_id: card_id,
            profile_id: None,
            materialized_version: 1,
            runtime_key: format!("CP:{issued_pan}"),
            materialized_at: chrono::Utc::now(),
        })
        .await
        .expect("initial card receipt should complete");

    // Credit movement uses the production HTTP, WSO2, Dragonfly, Oracle WAL,
    // TigerBeetle, and outbox boundaries. Wolfsburg is emulated only for the
    // final materialization receipt because it is a separate service.
    let provider_token = support::signed_provider_admin_jwt(provider_id);
    let cardholder_token = support::signed_cardholder_jwt(user_id);
    let mut dragonfly_connection = dragonfly.get().await.unwrap();
    redis::cmd("SET")
        .arg(format!("CP:{issued_pan}"))
        .arg("stale-before-credit-grant")
        .query_async::<String>(&mut dragonfly_connection)
        .await
        .unwrap();
    drop(dragonfly_connection);

    let grant_key = Uuid::new_v4().to_string();
    let grant_reference = format!("grant-{provider_id}");
    let grant_body = serde_json::json!({
        "user_id": user_id,
        "card_number": issued_pan,
        "amount_rials": 250_000,
        "provider_reference": grant_reference,
        "reason": "integration credit grant",
        "metadata": {"channel":"integration-test"}
    })
    .to_string();
    let (grant_status, grant_text) = post_json_with_token(
        &client,
        address,
        &provider_token,
        &format!("/api/v1/providers/{provider_id}/credits/grant"),
        &grant_key,
        "provider-credit-grant",
        &grant_body,
    )
    .await;
    assert_eq!(grant_status, reqwest::StatusCode::ACCEPTED, "{grant_text}");
    let grant: serde_json::Value = serde_json::from_str(&grant_text).unwrap();
    assert_eq!(grant["movement_type"], "GRANT");
    assert_eq!(grant["amount_rials"], 250_000);
    assert_eq!(grant["command_status"], "APPLIED");
    assert_eq!(grant["profile_materialization_status"], "PENDING");
    let grant_operation_id = value_uuid(&grant, "operation_id");
    let mut dragonfly_connection = dragonfly.get().await.unwrap();
    let stale_cp: Option<String> = redis::cmd("GET")
        .arg(format!("CP:{issued_pan}"))
        .query_async(&mut dragonfly_connection)
        .await
        .unwrap();
    assert!(stale_cp.is_none(), "credit grant must invalidate stale CP");
    drop(dragonfly_connection);

    let (provider_balance_status, provider_balance_text) = get_json_with_token(
        &client,
        address,
        &provider_token,
        &format!("/api/v1/providers/{provider_id}/users/{user_id}/credit?card_number={issued_pan}"),
        "provider-credit-balance",
    )
    .await;
    assert_eq!(
        provider_balance_status,
        reqwest::StatusCode::OK,
        "{provider_balance_text}"
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&provider_balance_text).unwrap()["observed_remaining_amount_rials"],
        250_000
    );

    let (grant_replay_status, grant_replay_text) = post_json_with_token(
        &client,
        address,
        &provider_token,
        &format!("/api/v1/providers/{provider_id}/credits/grant"),
        &grant_key,
        "provider-credit-grant-replay",
        &grant_body,
    )
    .await;
    assert_eq!(
        grant_replay_status,
        reqwest::StatusCode::OK,
        "{grant_replay_text}"
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&grant_replay_text).unwrap(),
        grant
    );

    repository
        .apply_card_profile_receipt(RuntimeMaterializationReceipt {
            receipt_event_id: Uuid::new_v4(),
            operation_id: grant_operation_id,
            profile_type: "CP".to_string(),
            aggregate_id: card_id,
            profile_id: None,
            materialized_version: 2,
            runtime_key: format!("CP:{issued_pan}"),
            materialized_at: chrono::Utc::now(),
        })
        .await
        .expect("credit grant CP receipt should complete the WAL");
    assert!(
        card_profile_locks
            .release_materialized(&issued_pan, grant_operation_id)
            .await
            .unwrap()
    );

    let duplicate_reference_key = Uuid::new_v4().to_string();
    let duplicate_reference_body = serde_json::json!({
        "user_id": user_id,
        "card_number": issued_pan,
        "amount_rials": 1,
        "provider_reference": grant_reference,
        "reason": "duplicate provider reference",
        "metadata": {}
    })
    .to_string();
    let (duplicate_status, duplicate_text) = post_json_with_token(
        &client,
        address,
        &provider_token,
        &format!("/api/v1/providers/{provider_id}/credits/grant"),
        &duplicate_reference_key,
        "provider-credit-duplicate-reference",
        &duplicate_reference_body,
    )
    .await;
    assert_eq!(
        duplicate_status,
        reqwest::StatusCode::CONFLICT,
        "{duplicate_text}"
    );
    assert!(duplicate_text.contains("PROVIDER_CREDIT_REFERENCE_CONFLICT"));
    assert_eq!(
        count_idempotency_key(&repository.pool, &duplicate_reference_key).await,
        0,
        "provider-reference conflict must roll back the provisional idempotency row"
    );

    let stale_return_body = serde_json::json!({
        "expected_remaining_amount_rials": 249_999,
        "reason": "stale cardholder view"
    })
    .to_string();
    let (stale_return_status, stale_return_text) = post_json_with_token(
        &client,
        address,
        &cardholder_token,
        &format!("/api/v1/cards/{issued_pan}/providers/{provider_id}/credit/return"),
        &Uuid::new_v4().to_string(),
        "cardholder-credit-return-stale",
        &stale_return_body,
    )
    .await;
    assert_eq!(
        stale_return_status,
        reqwest::StatusCode::CONFLICT,
        "{stale_return_text}"
    );
    assert!(stale_return_text.contains("PROVIDER_CREDIT_BALANCE_CHANGED"));

    let full_return_body = serde_json::json!({
        "expected_remaining_amount_rials": 250_000,
        "reason": "cardholder returns all remaining provider credit"
    })
    .to_string();
    let (return_status, return_text) = post_json_with_token(
        &client,
        address,
        &cardholder_token,
        &format!("/api/v1/cards/{issued_pan}/providers/{provider_id}/credit/return"),
        &Uuid::new_v4().to_string(),
        "cardholder-credit-return-full",
        &full_return_body,
    )
    .await;
    assert_eq!(
        return_status,
        reqwest::StatusCode::ACCEPTED,
        "{return_text}"
    );
    let returned: serde_json::Value = serde_json::from_str(&return_text).unwrap();
    assert_eq!(returned["movement_type"], "RETURN_FULL_BALANCE");
    assert_eq!(returned["amount_rials"], 250_000);
    let return_operation_id = value_uuid(&returned, "operation_id");
    repository
        .apply_card_profile_receipt(RuntimeMaterializationReceipt {
            receipt_event_id: Uuid::new_v4(),
            operation_id: return_operation_id,
            profile_type: "CP".to_string(),
            aggregate_id: card_id,
            profile_id: None,
            materialized_version: 3,
            runtime_key: format!("CP:{issued_pan}"),
            materialized_at: chrono::Utc::now(),
        })
        .await
        .expect("credit return CP receipt should complete the WAL");
    assert!(
        card_profile_locks
            .release_materialized(&issued_pan, return_operation_id)
            .await
            .unwrap()
    );

    let (cardholder_balance_status, cardholder_balance_text) = get_json_with_token(
        &client,
        address,
        &cardholder_token,
        &format!("/api/v1/cards/{issued_pan}/providers/{provider_id}/credit"),
        "cardholder-credit-balance-after-return",
    )
    .await;
    assert_eq!(
        cardholder_balance_status,
        reqwest::StatusCode::OK,
        "{cardholder_balance_text}"
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&cardholder_balance_text).unwrap()["observed_remaining_amount_rials"],
        0
    );

    let issuance_wal_operation_id = stage_issuance_recovery(
        &repository.pool,
        batch_id,
        issuance_request_id,
        card_id,
        provider_user_id,
    )
    .await;
    let mut recovery_config = state.config.wal_recovery.clone();
    recovery_config.poll_interval_ms = 25;
    recovery_config.stale_after_ms = 60_000;
    let recovery = start_wal_recovery_worker(
        repository.clone(),
        ProviderUserService::new(
            repository.clone(),
            ledger_client.clone(),
            state.config.tigerbeetle.clone(),
        ),
        provider_credit_service.clone(),
        recovery_config,
    )
    .expect("WAL recovery worker should start");
    wait_for_issuance_recovery(&repository.pool, issuance_wal_operation_id, batch_id).await;
    recovery.shutdown().await;
    let recovered_projection = load_card_projection_payload(&repository.pool, card_id).await;
    assert_eq!(
        recovered_projection["payload"]["refresh_reason"],
        "CARD_CREATED"
    );
    let recovered_operation_id = value_uuid(&recovered_projection, "operation_id");
    assert_ne!(recovered_operation_id, initial_operation_id);
    assert!(
        card_profile_locks
            .ensure_pending(&issued_pan, recovered_operation_id)
            .await
            .unwrap()
    );
    repository
        .apply_card_profile_receipt(RuntimeMaterializationReceipt {
            receipt_event_id: Uuid::new_v4(),
            operation_id: recovered_operation_id,
            profile_type: "CP".to_string(),
            aggregate_id: card_id,
            profile_id: None,
            materialized_version: 3,
            runtime_key: format!("CP:{issued_pan}"),
            materialized_at: chrono::Utc::now(),
        })
        .await
        .expect("recovered issuance CP receipt should complete");
    assert!(
        card_profile_locks
            .release_materialized(&issued_pan, recovered_operation_id)
            .await
            .unwrap()
    );

    let provider_user_recovery_operation = stage_provider_user_recovery(
        &repository.pool,
        provider_user_id,
        provider_id,
        user_id,
        card_id,
        card_range_id,
        provider_user_account_id,
    )
    .await;
    let mut recovery_config = state.config.wal_recovery.clone();
    recovery_config.poll_interval_ms = 25;
    recovery_config.stale_after_ms = 60_000;
    let recovery = start_wal_recovery_worker(
        repository.clone(),
        ProviderUserService::new(
            repository.clone(),
            ledger_client.clone(),
            state.config.tigerbeetle.clone(),
        ),
        provider_credit_service.clone(),
        recovery_config,
    )
    .expect("provider-user WAL recovery worker should start");
    wait_for_provider_user_recovery(
        &repository.pool,
        provider_user_recovery_operation,
        provider_user_id,
    )
    .await;
    recovery.shutdown().await;
    let provider_user_projection = load_card_projection_payload(&repository.pool, card_id).await;
    assert_eq!(
        provider_user_projection["payload"]["refresh_reason"],
        "FUNDING_SOURCE_CHANGED"
    );
    let provider_user_projection_operation = value_uuid(&provider_user_projection, "operation_id");
    assert!(
        card_profile_locks
            .ensure_pending(&issued_pan, provider_user_projection_operation)
            .await
            .unwrap()
    );
    repository
        .apply_card_profile_receipt(RuntimeMaterializationReceipt {
            receipt_event_id: Uuid::new_v4(),
            operation_id: provider_user_projection_operation,
            profile_type: "CP".to_string(),
            aggregate_id: card_id,
            profile_id: None,
            materialized_version: 3,
            runtime_key: format!("CP:{issued_pan}"),
            materialized_at: chrono::Utc::now(),
        })
        .await
        .expect("provider-user recovery CP receipt should complete");
    assert!(
        card_profile_locks
            .release_materialized(&issued_pan, provider_user_projection_operation)
            .await
            .unwrap()
    );
    let mut dragonfly_connection = dragonfly.get().await.unwrap();
    redis::cmd("SET")
        .arg(format!("CP:{issued_pan}"))
        .arg("stale-profile")
        .query_async::<String>(&mut dragonfly_connection)
        .await
        .unwrap();
    drop(dragonfly_connection);

    let funding_order_key = Uuid::new_v4().to_string();
    let cardholder_token = support::signed_cardholder_jwt(user_id);
    let funding_order_body = serde_json::json!({
        "expected_card_state_version": 3,
        "sources": [{"provider_id":provider_id,"max_amount_rials":5000000}],
        "reason":"integration cardholder preference"
    })
    .to_string();
    let competing_operation_id = Uuid::new_v4();
    let competing_lock = match card_profile_locks
        .acquire(&issued_pan, competing_operation_id)
        .await
        .unwrap()
    {
        wurzburg::runtime_profiles::CardProfileLockOutcome::Acquired(lock) => lock,
        wurzburg::runtime_profiles::CardProfileLockOutcome::Busy => {
            panic!("integration card should not already be locked")
        }
    };
    let (locked_status, locked_body) = put_json(
        &client,
        address,
        &cardholder_token,
        &format!("/api/v1/cards/{issued_pan}/funding-order"),
        &funding_order_key,
        "card-funding-order-locked",
        &funding_order_body,
    )
    .await;
    assert_eq!(
        locked_status,
        reqwest::StatusCode::CONFLICT,
        "{locked_body}"
    );
    assert!(locked_body.contains("CARD_PROFILE_LOCKED"));
    assert!(card_profile_locks.release(&competing_lock).await.unwrap());
    let (funding_status, funding_body) = put_json(
        &client,
        address,
        &cardholder_token,
        &format!("/api/v1/cards/{issued_pan}/funding-order"),
        &funding_order_key,
        "card-funding-order",
        &funding_order_body,
    )
    .await;
    assert_eq!(
        funding_status,
        reqwest::StatusCode::ACCEPTED,
        "{funding_body}"
    );
    let funding: serde_json::Value = serde_json::from_str(&funding_body).unwrap();
    assert_eq!(funding["state_version"], 4);
    assert_eq!(funding["sources"][0]["priority"], 1);
    assert_eq!(funding["sources"][0]["max_amount_rials"], 5_000_000);
    let funding_operation_id = value_uuid(&funding, "operation_id");
    let refreshed = load_card_projection_payload(&repository.pool, card_id).await;
    assert_eq!(refreshed["event_type"], "CARD_PROFILE_REFRESH_REQUESTED");
    assert_eq!(
        refreshed["payload"]["refresh_reason"],
        "FUNDING_ORDER_CHANGED"
    );
    assert_eq!(refreshed["payload"]["state_version"], 4);
    let mut dragonfly_connection = dragonfly.get().await.unwrap();
    let stale_cp: Option<String> = redis::cmd("GET")
        .arg(format!("CP:{issued_pan}"))
        .query_async(&mut dragonfly_connection)
        .await
        .unwrap();
    assert!(
        stale_cp.is_none(),
        "funding-order mutation must invalidate stale CP"
    );
    redis::cmd("SET")
        .arg(format!("CP:{issued_pan}"))
        .arg("replacement-profile")
        .query_async::<String>(&mut dragonfly_connection)
        .await
        .unwrap();
    drop(dragonfly_connection);
    assert!(
        card_profile_locks
            .ensure_pending(&issued_pan, funding_operation_id)
            .await
            .unwrap()
    );
    let mut dragonfly_connection = dragonfly.get().await.unwrap();
    let recovered_cp: Option<String> = redis::cmd("GET")
        .arg(format!("CP:{issued_pan}"))
        .query_async(&mut dragonfly_connection)
        .await
        .unwrap();
    assert!(
        recovered_cp.is_none(),
        "coordination must remove a stale CP that reappears while publication is pending"
    );
    redis::cmd("SET")
        .arg(format!("CP:{issued_pan}"))
        .arg("replacement-profile")
        .query_async::<String>(&mut dragonfly_connection)
        .await
        .unwrap();
    drop(dragonfly_connection);
    repository
        .apply_card_profile_receipt(RuntimeMaterializationReceipt {
            receipt_event_id: Uuid::new_v4(),
            operation_id: funding_operation_id,
            profile_type: "CP".to_string(),
            aggregate_id: card_id,
            profile_id: None,
            materialized_version: 4,
            runtime_key: format!("CP:{issued_pan}"),
            materialized_at: chrono::Utc::now(),
        })
        .await
        .expect("funding-order CP receipt should complete");
    assert!(
        card_profile_locks
            .release_materialized(&issued_pan, funding_operation_id)
            .await
            .unwrap()
    );

    let wrong_cardholder_token = support::signed_cardholder_jwt(Uuid::new_v4());
    let wrong_owner_body=serde_json::json!({"expected_card_state_version":4,"sources":[{"provider_id":provider_id,"max_amount_rials":null}],"reason":"wrong cardholder"}).to_string();
    let (wrong_owner_status, wrong_owner_response) = put_json(
        &client,
        address,
        &wrong_cardholder_token,
        &format!("/api/v1/cards/{issued_pan}/funding-order"),
        &Uuid::new_v4().to_string(),
        "card-funding-order-wrong-owner",
        &wrong_owner_body,
    )
    .await;
    assert_eq!(
        wrong_owner_status,
        reqwest::StatusCode::FORBIDDEN,
        "{wrong_owner_response}"
    );
    assert!(wrong_owner_response.contains("CARDHOLDER_SCOPE_MISMATCH"));

    let stale_order_body=serde_json::json!({"expected_card_state_version":1,"sources":[{"provider_id":provider_id,"max_amount_rials":null}],"reason":"stale card view"}).to_string();
    let (stale_status, stale_body) = put_json(
        &client,
        address,
        &cardholder_token,
        &format!("/api/v1/cards/{issued_pan}/funding-order"),
        &Uuid::new_v4().to_string(),
        "card-funding-order-stale",
        &stale_order_body,
    )
    .await;
    assert_eq!(stale_status, reqwest::StatusCode::CONFLICT, "{stale_body}");
    assert!(stale_body.contains("CARD_STATE_VERSION_CONFLICT"));

    let (funding_replay_status, funding_replay_body) = put_json(
        &client,
        address,
        &cardholder_token,
        &format!("/api/v1/cards/{issued_pan}/funding-order"),
        &funding_order_key,
        "card-funding-order-replay",
        &funding_order_body,
    )
    .await;
    assert_eq!(
        funding_replay_status,
        reqwest::StatusCode::OK,
        "{funding_replay_body}"
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&funding_replay_body).unwrap(),
        funding
    );
    let changed_funding_body=serde_json::json!({"expected_card_state_version":4,"sources":[{"provider_id":provider_id,"max_amount_rials":null}],"reason":"changed request"}).to_string();
    let (funding_conflict_status, funding_conflict_body) = put_json(
        &client,
        address,
        &cardholder_token,
        &format!("/api/v1/cards/{issued_pan}/funding-order"),
        &funding_order_key,
        "card-funding-order-conflict",
        &changed_funding_body,
    )
    .await;
    assert_eq!(
        funding_conflict_status,
        reqwest::StatusCode::CONFLICT,
        "{funding_conflict_body}"
    );
    assert!(funding_conflict_body.contains("IDEMPOTENCY_KEY_CONFLICT"));

    let (suspend_status, suspend_body) = post_json(
        &client,
        address,
        &format!("/api/v1/providers/{provider_id}/suspend"),
        &Uuid::new_v4().to_string(),
        "provider-suspend-with-card",
        &serde_json::json!({"reason":"integration provider funding suspension"}).to_string(),
    )
    .await;
    assert_eq!(suspend_status, reqwest::StatusCode::OK, "{suspend_body}");
    assert_eq!(
        load_funding_source_status(&repository.pool, card_id, provider_id).await,
        "SUSPENDED"
    );
    let suspended_projection = load_card_projection_payload(&repository.pool, card_id).await;
    assert_eq!(suspended_projection["payload"]["state_version"], 5);
    assert_eq!(suspended_projection["payload"]["runtime_action"], "DELETE");
    assert_eq!(
        suspended_projection["payload"]["refresh_reason"],
        "PROVIDER_STATUS_CHANGED"
    );
    let suspend_operation_id = value_uuid(&suspended_projection, "operation_id");
    assert!(
        card_profile_locks
            .ensure_pending(&issued_pan, suspend_operation_id)
            .await
            .unwrap()
    );
    repository
        .apply_card_profile_receipt(RuntimeMaterializationReceipt {
            receipt_event_id: Uuid::new_v4(),
            operation_id: suspend_operation_id,
            profile_type: "CP".to_string(),
            aggregate_id: card_id,
            profile_id: None,
            materialized_version: 5,
            runtime_key: format!("CP:{issued_pan}"),
            materialized_at: chrono::Utc::now(),
        })
        .await
        .expect("provider suspension CP deletion receipt should complete");
    assert!(
        card_profile_locks
            .release_materialized(&issued_pan, suspend_operation_id)
            .await
            .unwrap()
    );

    let (reactivate_status, reactivate_body) = post_json(
        &client,
        address,
        &format!("/api/v1/providers/{provider_id}/activate"),
        &Uuid::new_v4().to_string(),
        "provider-reactivate-with-card",
        &serde_json::json!({"reason":"integration provider funding reactivation"}).to_string(),
    )
    .await;
    assert_eq!(
        reactivate_status,
        reqwest::StatusCode::OK,
        "{reactivate_body}"
    );
    assert_eq!(
        load_funding_source_status(&repository.pool, card_id, provider_id).await,
        "ACTIVE"
    );
    let reactivated_projection = load_card_projection_payload(&repository.pool, card_id).await;
    assert_eq!(reactivated_projection["payload"]["state_version"], 6);
    assert_eq!(
        reactivated_projection["payload"]["runtime_action"],
        "UPSERT"
    );
    assert_eq!(
        reactivated_projection["payload"]["funding_sources"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let reactivate_operation_id = value_uuid(&reactivated_projection, "operation_id");
    assert!(
        card_profile_locks
            .ensure_pending(&issued_pan, reactivate_operation_id)
            .await
            .unwrap()
    );
    let mut dragonfly_connection = dragonfly.get().await.unwrap();
    redis::cmd("SET")
        .arg(format!("CP:{issued_pan}"))
        .arg("reactivated-profile")
        .query_async::<String>(&mut dragonfly_connection)
        .await
        .unwrap();
    drop(dragonfly_connection);
    repository
        .apply_card_profile_receipt(RuntimeMaterializationReceipt {
            receipt_event_id: Uuid::new_v4(),
            operation_id: reactivate_operation_id,
            profile_type: "CP".to_string(),
            aggregate_id: card_id,
            profile_id: None,
            materialized_version: 6,
            runtime_key: format!("CP:{issued_pan}"),
            materialized_at: chrono::Utc::now(),
        })
        .await
        .expect("provider reactivation CP receipt should complete");
    assert!(
        card_profile_locks
            .release_materialized(&issued_pan, reactivate_operation_id)
            .await
            .unwrap()
    );

    let mappings = repository
        .get_provider_ledger_mappings(provider_id)
        .await
        .expect("provider mappings should load");
    assert_eq!(mappings.len(), 4);
    for mapping in mappings {
        let accounts = ledger_client
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

async fn post_json_with_token(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    token: &str,
    path: &str,
    key: &str,
    correlation: &str,
    body: &str,
) -> (reqwest::StatusCode, String) {
    let response = client
        .post(format!("http://{address}{path}"))
        .bearer_auth(token)
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

async fn put_json(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    token: &str,
    path: &str,
    key: &str,
    correlation: &str,
    body: &str,
) -> (reqwest::StatusCode, String) {
    let response = client
        .put(format!("http://{address}{path}"))
        .bearer_auth(token)
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

async fn get_json_with_token(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    token: &str,
    path: &str,
    correlation: &str,
) -> (reqwest::StatusCode, String) {
    let response = client
        .get(format!("http://{address}{path}"))
        .bearer_auth(token)
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
                "SELECT JSON_SERIALIZE(payload_json RETURNING CLOB) FROM integration_outbox WHERE aggregate_id=:1 AND event_type='CARD_PROFILE_REFRESH_REQUESTED' ORDER BY created_at DESC FETCH FIRST 1 ROW ONLY",
                &[&card_raw],
            )
            .map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        serde_json::from_str(&payload)
            .map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))
    })
    .await
    .expect("card projection outbox payload should load")
}

async fn load_funding_source_status(
    pool: &wurzburg::db::oracle::OraclePool,
    card_id: Uuid,
    provider_id: Uuid,
) -> String {
    let card_raw = wurzburg::db::oracle::types::uuid_to_raw16(card_id).to_vec();
    let provider_raw = wurzburg::db::oracle::types::uuid_to_raw16(provider_id).to_vec();
    pool.with_connection(move |connection| {
        connection
            .query_row_as(
                "SELECT status FROM card_provider_funding_sources WHERE card_id=:1 AND provider_id=:2",
                &[&card_raw, &provider_raw],
            )
            .map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))
    })
    .await
    .expect("funding-source status should load")
}

async fn count_idempotency_key(pool: &wurzburg::db::oracle::OraclePool, key: &str) -> i64 {
    let key = key.to_string();
    pool.with_connection(move |connection| {
        connection
            .query_row_as(
                "SELECT COUNT(*) FROM idempotency_records WHERE idempotency_key=:1",
                &[&key],
            )
            .map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))
    })
    .await
    .expect("idempotency count should load")
}

async fn stage_issuance_recovery(
    pool: &wurzburg::db::oracle::OraclePool,
    batch_id: Uuid,
    issuance_request_id: Uuid,
    card_id: Uuid,
    provider_user_id: Uuid,
) -> Uuid {
    let batch_raw = wurzburg::db::oracle::types::uuid_to_raw16(batch_id).to_vec();
    let request_raw = wurzburg::db::oracle::types::uuid_to_raw16(issuance_request_id).to_vec();
    let card_raw = wurzburg::db::oracle::types::uuid_to_raw16(card_id).to_vec();
    let provider_user_raw = wurzburg::db::oracle::types::uuid_to_raw16(provider_user_id).to_vec();
    pool.with_transaction("stage issuance recovery integration scenario", move |connection| {
        let operation_raw: Vec<u8> = connection.query_row_as(
            "SELECT operation_id FROM operation_wal WHERE aggregate_id=:1 AND operation_type='CARD_ISSUANCE_ACCOUNT_PROVISION'",
            &[&request_raw],
        ).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        let injected_error = serde_json::json!({"code":"INTEGRATION_INJECTED"}).to_string();
        connection.execute("UPDATE operation_wal SET status='FAILED',attempt_count=0,next_attempt_at=NULL,locked_by=NULL,locked_until=NULL,error_json=:1,completed_at=NULL,updated_at=SYSTIMESTAMP WHERE operation_id=:2", &[&injected_error,&operation_raw]).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        connection.execute("UPDATE cards SET status='RECOVERY_REQUIRED',publication_operation_id=NULL,updated_at=SYSTIMESTAMP WHERE card_id=:1", &[&card_raw]).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        connection.execute("UPDATE card_policy_usage_accounts SET status='RECOVERY_REQUIRED',updated_at=SYSTIMESTAMP WHERE card_id=:1", &[&card_raw]).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        connection.execute("UPDATE provider_users SET status='RECOVERY_REQUIRED',updated_at=SYSTIMESTAMP WHERE provider_user_id=:1", &[&provider_user_raw]).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        connection.execute("UPDATE provider_user_accounts SET status='RECOVERY_REQUIRED',updated_at=SYSTIMESTAMP WHERE provider_user_id=:1", &[&provider_user_raw]).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        connection.execute("UPDATE card_provider_funding_sources SET status='RECOVERY_REQUIRED',updated_at=SYSTIMESTAMP WHERE provider_user_id=:1", &[&provider_user_raw]).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        connection.execute("UPDATE card_issuance_requests SET status='RECOVERY_REQUIRED',updated_at=SYSTIMESTAMP WHERE card_issuance_request_id=:1", &[&request_raw]).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        connection.execute("UPDATE card_issuance_request_providers SET status='RECOVERY_REQUIRED',updated_at=SYSTIMESTAMP WHERE card_issuance_request_id=:1", &[&request_raw]).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        connection.execute("UPDATE card_issuance_batch_rows SET result_status='RECOVERY_REQUIRED',safe_result_code='INTEGRATION_INJECTED',processed_at=SYSTIMESTAMP WHERE card_issuance_batch_id=:1 AND card_issuance_request_id=:2", &[&batch_raw,&request_raw]).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        connection.execute("UPDATE card_issuance_batches SET status='PROCESSING_RESULT',issued_count=0,rejected_count=0,failed_count=0,completed_at=NULL,updated_at=SYSTIMESTAMP WHERE card_issuance_batch_id=:1", &[&batch_raw]).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        connection.execute("UPDATE idempotency_records SET status='IN_PROGRESS',response_snapshot=NULL,completed_at=NULL,updated_at=SYSTIMESTAMP WHERE operation_type='card_issuance_batches.process_result' AND resource_id=:1", &[&batch_raw]).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        wurzburg::db::oracle::types::raw16_to_uuid(&operation_raw)
    }).await.expect("issuance recovery state should be staged")
}

async fn wait_for_issuance_recovery(
    pool: &wurzburg::db::oracle::OraclePool,
    operation_id: Uuid,
    batch_id: Uuid,
) {
    let operation_raw = wurzburg::db::oracle::types::uuid_to_raw16(operation_id).to_vec();
    let batch_raw = wurzburg::db::oracle::types::uuid_to_raw16(batch_id).to_vec();
    for _ in 0..200 {
        let operation_raw = operation_raw.clone();
        let batch_raw = batch_raw.clone();
        let completed = pool.with_connection(move |connection| {
            let wal_status = connection.query_row_as::<String>(
                "SELECT status FROM operation_wal WHERE operation_id=:1", &[&operation_raw],
            ).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
            let batch_status = connection.query_row_as::<String>(
                "SELECT status FROM card_issuance_batches WHERE card_issuance_batch_id=:1", &[&batch_raw],
            ).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
            let idempotency_status = connection.query_row_as::<String>(
                "SELECT status FROM idempotency_records WHERE operation_type='card_issuance_batches.process_result' AND resource_id=:1", &[&batch_raw],
            ).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
            Ok(wal_status == "COMPLETED" && batch_status == "COMPLETED" && idempotency_status == "COMPLETED")
        }).await.expect("WAL recovery status should load");
        if completed {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("issuance WAL recovery did not complete before the test deadline");
}

async fn stage_provider_user_recovery(
    pool: &wurzburg::db::oracle::OraclePool,
    provider_user_id: Uuid,
    provider_id: Uuid,
    user_id: Uuid,
    card_id: Uuid,
    card_range_id: Uuid,
    account_id: Uuid,
) -> Uuid {
    let operation_id = Uuid::new_v4();
    let idempotency_id = Uuid::new_v4();
    let idempotency_key = Uuid::new_v4().to_string();
    let provider_user_raw = wurzburg::db::oracle::types::uuid_to_raw16(provider_user_id).to_vec();
    let provider_raw = wurzburg::db::oracle::types::uuid_to_raw16(provider_id).to_vec();
    let account_raw = wurzburg::db::oracle::types::uuid_to_raw16(account_id).to_vec();
    let command_context = serde_json::json!({
        "operation_type":"provider_users.enroll",
        "idempotency_key":idempotency_key,
        "audit":{
            "actor_subject":"integration-provider-user-recovery",
            "actor_client_id":"wurzburg-integration-test",
            "actor_provider_id":provider_id,
            "actor_user_id":null,
            "actor_issuer":"https://wso2.example.test",
            "source_ip":"198.51.100.20",
            "correlation_id":"provider-user-wal-recovery",
            "request_id":Uuid::new_v4().to_string()
        },
        "trace":{
            "correlation_id":"provider-user-wal-recovery",
            "request_id":Uuid::new_v4().to_string(),
            "causation_id":null,
            "traceparent":null,
            "tracestate":null
        }
    });
    let wal_request = serde_json::json!({
        "command_context":command_context,
        "provider_user_id":provider_user_id,
        "provider_id":provider_id,
        "user_id":user_id,
        "card_id":card_id,
        "card_range_id":card_range_id,
        "provider_user_account_id":account_id,
        "policy_usage_account_ids":wurzburg::domain::user_card::PolicyUsageAccountIds::for_card(card_id)
    }).to_string();
    let idempotency_key_for_db = command_context["idempotency_key"]
        .as_str()
        .unwrap()
        .to_string();
    pool.with_transaction("stage provider-user recovery integration scenario", move |connection| {
        connection.execute("UPDATE provider_users SET status='RECOVERY_REQUIRED',updated_at=SYSTIMESTAMP WHERE provider_user_id=:1", &[&provider_user_raw]).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        connection.execute("UPDATE provider_user_accounts SET status='RECOVERY_REQUIRED',updated_at=SYSTIMESTAMP WHERE provider_user_id=:1", &[&provider_user_raw]).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        connection.execute("UPDATE card_provider_funding_sources SET status='RECOVERY_REQUIRED',updated_at=SYSTIMESTAMP WHERE provider_user_id=:1", &[&provider_user_raw]).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        connection.execute("INSERT INTO idempotency_records (idempotency_record_id,operation_type,idempotency_key,request_hash,status,resource_type,resource_id,created_by_subject,created_by_client_id,actor_provider_id,correlation_id,request_id) VALUES (:1,'provider_users.enroll',:2,'integration-recovery-request','IN_PROGRESS','provider_user',:3,'integration-provider-user-recovery','wurzburg-integration-test',:4,'provider-user-wal-recovery',:5)", &[&wurzburg::db::oracle::types::uuid_to_raw16(idempotency_id).to_vec(),&idempotency_key_for_db,&provider_user_raw,&provider_raw,&Uuid::new_v4().to_string()]).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        connection.execute("INSERT INTO operation_wal (operation_id,operation_type,aggregate_type,aggregate_id,status,deterministic_external_id,request_json,error_json) VALUES (:1,'PROVIDER_USER_ACCOUNT_PROVISION','PROVIDER_USER',:2,'FAILED',:3,:4,:5)", &[&wurzburg::db::oracle::types::uuid_to_raw16(operation_id).to_vec(),&provider_user_raw,&account_raw,&wal_request,&serde_json::json!({"code":"INTEGRATION_INJECTED"}).to_string()]).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
        Ok(operation_id)
    }).await.expect("provider-user recovery state should be staged")
}

async fn wait_for_provider_user_recovery(
    pool: &wurzburg::db::oracle::OraclePool,
    operation_id: Uuid,
    provider_user_id: Uuid,
) {
    let operation_raw = wurzburg::db::oracle::types::uuid_to_raw16(operation_id).to_vec();
    let provider_user_raw = wurzburg::db::oracle::types::uuid_to_raw16(provider_user_id).to_vec();
    for _ in 0..200 {
        let operation_raw = operation_raw.clone();
        let provider_user_raw = provider_user_raw.clone();
        let completed = pool.with_connection(move |connection| {
            let wal_status = connection.query_row_as::<String>("SELECT status FROM operation_wal WHERE operation_id=:1", &[&operation_raw]).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
            let user_status = connection.query_row_as::<String>("SELECT status FROM provider_users WHERE provider_user_id=:1", &[&provider_user_raw]).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
            let idempotency_status = connection.query_row_as::<String>("SELECT status FROM idempotency_records WHERE operation_type='provider_users.enroll' AND resource_id=:1", &[&provider_user_raw]).map_err(|error| wurzburg::db::error::DbError::Query(error.to_string()))?;
            Ok(wal_status == "COMPLETED" && user_status == "ACTIVE" && idempotency_status == "COMPLETED")
        }).await.expect("provider-user recovery status should load");
        if completed {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("provider-user WAL recovery did not complete before the test deadline");
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

fn unique_valid_national_id() -> String {
    let seed = Uuid::new_v4().as_u128() % 900_000_000 + 100_000_000;
    let first_nine = format!("{seed:09}");
    let sum: u32 = first_nine
        .bytes()
        .enumerate()
        .map(|(index, digit)| u32::from(digit - b'0') * (10 - index as u32))
        .sum();
    let remainder = sum % 11;
    let check = if remainder < 2 {
        remainder
    } else {
        11 - remainder
    };
    format!("{first_nine}{check}")
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
