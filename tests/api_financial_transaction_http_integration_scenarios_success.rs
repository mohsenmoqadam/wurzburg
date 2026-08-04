mod support;

use std::{env, sync::Arc};

use tokio::net::TcpListener;
use uuid::Uuid;
use wurzburg::{
    api::router::build_app_router,
    config::Settings,
    db::oracle::{OraclePool, prepare_oracle_schema, types::uuid_to_raw16},
    state::AppState,
};

/// Scenario goal:
/// Prove that one canonical transaction API can safely expose Wurzburg credit
/// facts and Wolfsburg CMS facts for a multi-provider card.
///
/// Oracle facts: two Providers, one cardholder/card, one Wurzburg credit grant,
/// one Wolfsburg multi-provider Confirm, and one Provider-B fee transaction.
/// TigerBeetle facts: deterministic account/transfer IDs are persisted only as
/// immutable transaction evidence; no balance is read from Oracle.
/// Final proof: Provider A sees only its own entries, the cardholder sees every
/// Provider entry without Provider references, platform admin sees complete
/// transactions, and filter-bound keyset pagination does not drift.
#[tokio::test]
async fn lists_scoped_multi_provider_financial_transactions_through_running_server() {
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
    let provider_a = Uuid::new_v4();
    let provider_b = Uuid::new_v4();
    let user_id = Uuid::new_v4();
    let card_id = Uuid::new_v4();
    let card_number = "5111000000000001";
    seed_multi_provider_history(
        &state.db.pool,
        provider_a,
        provider_b,
        user_id,
        card_id,
        card_number,
    )
    .await;
    let address = start_server(state).await;
    let client = reqwest::Client::new();

    let first_provider_page = get(
        &client,
        address,
        &format!("/api/v1/providers/{provider_a}/transactions?page_size=1"),
        &support::signed_provider_admin_jwt(provider_a),
    )
    .await;
    assert_eq!(first_provider_page.0, reqwest::StatusCode::OK);
    let first_provider_page: serde_json::Value =
        serde_json::from_str(&first_provider_page.1).unwrap();
    assert_eq!(
        first_provider_page["items"][0]["transaction_type"],
        "WITHDRAWAL_CONFIRMED"
    );
    assert_eq!(
        first_provider_page["items"][0]["entries"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert!(
        first_provider_page["items"][0]["entries"]
            .as_array()
            .unwrap()
            .iter()
            .all(|entry| entry["provider_id"] == provider_a.to_string())
    );
    assert!(
        !first_provider_page
            .to_string()
            .contains(&provider_b.to_string())
    );
    let page_token = first_provider_page["next_page_token"]
        .as_str()
        .expect("Provider A has a second transaction");

    let second_provider_page = get(
        &client,
        address,
        &format!("/api/v1/providers/{provider_a}/transactions?page_size=1&page_token={page_token}"),
        &support::signed_provider_admin_jwt(provider_a),
    )
    .await;
    let second_provider_page: serde_json::Value =
        serde_json::from_str(&second_provider_page.1).unwrap();
    assert_eq!(
        second_provider_page["items"][0]["transaction_type"],
        "CREDIT_GRANTED"
    );
    assert_eq!(
        second_provider_page["next_page_token"],
        serde_json::Value::Null
    );

    let account_history = get(
        &client,
        address,
        &format!("/api/v1/providers/{provider_a}/accounts/PROVIDER_OWNED/transactions"),
        &support::signed_provider_admin_jwt(provider_a),
    )
    .await;
    let account_history: serde_json::Value = serde_json::from_str(&account_history.1).unwrap();
    assert_eq!(account_history["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        account_history["items"][0]["entries"][0]["account_category"],
        "PROVIDER_OWNED"
    );

    let cardholder_history = get(
        &client,
        address,
        &format!("/api/v1/cards/{card_number}/transactions"),
        &support::signed_cardholder_jwt(user_id),
    )
    .await;
    let cardholder_history: serde_json::Value =
        serde_json::from_str(&cardholder_history.1).unwrap();
    assert_eq!(cardholder_history["items"].as_array().unwrap().len(), 3);
    assert!(
        cardholder_history["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|transaction| transaction["reference"].is_null())
    );
    let confirm = cardholder_history["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|value| value["transaction_type"] == "WITHDRAWAL_CONFIRMED")
        .unwrap();
    assert_eq!(confirm["entries"].as_array().unwrap().len(), 4);

    let admin_provider_b = get(
        &client,
        address,
        &format!("/api/v1/admin/transactions?provider_id={provider_b}"),
        &support::signed_platform_admin_jwt(),
    )
    .await;
    let admin_provider_b: serde_json::Value = serde_json::from_str(&admin_provider_b.1).unwrap();
    assert_eq!(admin_provider_b["items"].as_array().unwrap().len(), 2);
    let admin_confirm = admin_provider_b["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|value| value["transaction_type"] == "WITHDRAWAL_CONFIRMED")
        .unwrap();
    assert_eq!(admin_confirm["entries"].as_array().unwrap().len(), 4);
    assert!(admin_confirm.to_string().contains(&provider_a.to_string()));
    assert!(admin_confirm.to_string().contains(&provider_b.to_string()));

    let filtered = get(
        &client,
        address,
        &format!("/api/v1/users/{user_id}/transactions?transaction_type=CREDIT_GRANTED"),
        &support::signed_cardholder_jwt(user_id),
    )
    .await;
    let filtered: serde_json::Value = serde_json::from_str(&filtered.1).unwrap();
    assert_eq!(filtered["items"].as_array().unwrap().len(), 1);
    assert_eq!(filtered["items"][0]["transaction_type"], "CREDIT_GRANTED");
    assert_eq!(filtered["items"][0]["amount_rials"], "250000");
    assert_eq!(
        filtered["items"][0]["masked_card_number"],
        "511100******0001"
    );
}

#[allow(clippy::too_many_arguments)]
async fn seed_multi_provider_history(
    pool: &OraclePool,
    provider_a: Uuid,
    provider_b: Uuid,
    user_id: Uuid,
    card_id: Uuid,
    card_number: &str,
) {
    let card_number = card_number.to_string();
    pool.with_transaction("seed financial transaction scenario", move |connection| {
        for (provider_id, suffix) in [(provider_a, "A"), (provider_b, "B")] {
            connection.execute(
                "INSERT INTO providers (provider_id,legal_name,trade_name,status,created_by_subject,updated_by_subject) VALUES (:1,:2,:3,'ACTIVE','scenario','scenario')",
                &[&raw(provider_id), &format!("Provider {suffix} Legal"), &format!("Provider {suffix}")],
            ).unwrap();
        }
        let range_id = Uuid::new_v4();
        connection.execute(
            "INSERT INTO card_ranges (card_range_id,start_card_number,end_card_number,funding_mode,withdrawal_limit_authority,limit_calendar_json,status,issuance_enabled,cms_operation_mode,operational_version,materialized_operational_version,metadata_json,created_by_subject,updated_by_subject) VALUES (:1,'5111000000000000','5111000000009999','MULTI_PROVIDER','PLATFORM','{\"timezone\":\"Asia/Tehran\",\"week_starts_on\":\"SATURDAY\",\"window_mode\":\"CALENDAR\"}','ACTIVE',1,'FULL',1,1,'{}','scenario','scenario')",
            &[&raw(range_id)],
        ).unwrap();
        connection.execute(
            "INSERT INTO users (user_id,national_id,first_name,last_name,status,created_by_subject,updated_by_subject) VALUES (:1,'1234567890','Scenario','Cardholder','ACTIVE','scenario','scenario')",
            &[&raw(user_id)],
        ).unwrap();
        connection.execute(
            "INSERT INTO cards (card_id,card_number,user_id,card_range_id,status,state_version,materialized_version,created_by_subject,updated_by_subject) VALUES (:1,:2,:3,:4,'ACTIVE',1,1,'scenario','scenario')",
            &[&raw(card_id), &card_number, &raw(user_id), &raw(range_id)],
        ).unwrap();

        let credit = insert_transaction(
            connection,
            "CREDIT_GRANTED",
            "WURZBURG",
            user_id,
            card_id,
            250_000,
            "2026-08-01T07:00:00.000Z",
            Some("provider-a-grant-1"),
        );
        insert_entry(connection, credit, 1, provider_a, "PROVIDER_OWNED", "DEBIT", 250_000);
        insert_entry(connection, credit, 2, provider_a, "PROVIDER_USER", "CREDIT", 250_000);

        let confirm = insert_transaction(
            connection,
            "WITHDRAWAL_CONFIRMED",
            "WOLFSBURG",
            user_id,
            card_id,
            180_000,
            "2026-08-01T08:00:00.000Z",
            Some("cms-confirm-1"),
        );
        insert_entry(connection, confirm, 1, provider_a, "PROVIDER_USER", "DEBIT", 100_000);
        insert_entry(connection, confirm, 2, provider_a, "CMS_SETTLEMENT", "CREDIT", 100_000);
        insert_entry(connection, confirm, 3, provider_b, "PROVIDER_USER", "DEBIT", 80_000);
        insert_entry(connection, confirm, 4, provider_b, "CMS_SETTLEMENT", "CREDIT", 80_000);

        let fee = insert_transaction(
            connection,
            "FEE_CHARGED",
            "WOLFSBURG",
            user_id,
            card_id,
            5_000,
            "2026-08-01T09:00:00.000Z",
            Some("cms-fee-1"),
        );
        insert_entry(connection, fee, 1, provider_b, "PROVIDER_FEE", "DEBIT", 5_000);
        insert_entry(connection, fee, 2, provider_b, "PLATFORM_FEE", "CREDIT", 5_000);
        Ok(())
    }).await.unwrap();
}

#[allow(clippy::too_many_arguments)]
fn insert_transaction(
    connection: &oracle::Connection,
    transaction_type: &str,
    source_system: &str,
    user_id: Uuid,
    card_id: Uuid,
    amount: i64,
    occurred_at: &str,
    reference: Option<&str>,
) -> Uuid {
    let transaction_id = Uuid::new_v4();
    let source_id = Uuid::new_v4();
    let (operation_id, event_id) = if source_system == "WURZBURG" {
        (Some(raw(source_id)), None)
    } else {
        (None, Some(raw(source_id)))
    };
    connection.execute(
        "INSERT INTO financial_transactions (transaction_id,transaction_type,source_system,source_operation_id,source_event_id,user_id,card_id,amount_rials,currency,status,external_reference,metadata_json,occurred_at) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,'IRR','POSTED',:9,'{}',TO_TIMESTAMP_TZ(:10,'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'))",
        &[&raw(transaction_id), &transaction_type, &source_system, &operation_id, &event_id, &raw(user_id), &raw(card_id), &amount, &reference, &occurred_at],
    ).unwrap();
    transaction_id
}

fn insert_entry(
    connection: &oracle::Connection,
    transaction_id: Uuid,
    sequence: i64,
    provider_id: Uuid,
    category: &str,
    direction: &str,
    amount: i64,
) {
    let entry_role = if matches!(category, "PROVIDER_FEE" | "PLATFORM_FEE") {
        "FEE"
    } else {
        "PRINCIPAL"
    };
    connection.execute(
        "INSERT INTO financial_transaction_entries (transaction_entry_id,transaction_id,entry_sequence,provider_id,account_id,account_category,direction,entry_role,amount_rials,tigerbeetle_transfer_id) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10)",
        &[&raw(Uuid::new_v4()), &raw(transaction_id), &sequence, &raw(provider_id), &raw(Uuid::new_v4()), &category, &direction, &entry_role, &amount, &raw(Uuid::new_v4())],
    ).unwrap();
}

async fn get(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    path: &str,
    jwt: &str,
) -> (reqwest::StatusCode, String) {
    let response = client
        .get(format!("http://{address}{path}"))
        .bearer_auth(jwt)
        .header("X-Correlation-Id", Uuid::new_v4().to_string())
        .header("X-Request-Id", Uuid::new_v4().to_string())
        .header("X-WSO2-Client-IP", "198.51.100.20")
        .header("X-WSO2-Gateway-Id", "wso2-integration-test")
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    (status, body)
}

async fn start_server(state: Arc<AppState>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, build_app_router(state))
            .await
            .unwrap();
    });
    address
}

fn raw(value: Uuid) -> Vec<u8> {
    uuid_to_raw16(value).to_vec()
}
