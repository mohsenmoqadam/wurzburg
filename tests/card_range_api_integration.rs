use std::{env, sync::Arc};

use anyhow::{Context, Result};
use axum::{
    body::{Body, to_bytes},
    http::{Method, Request, StatusCode},
};
use serde_json::json;
use tower::ServiceExt;
use uuid::Uuid;
use wurzburg::{
    api::router::build_app_router,
    config::Settings,
    db::oracle::{OracleConnectConfig, OracleMigrator, OraclePool, wurzburg_migrations},
    state::AppState,
};

static ORACLE_MIGRATION_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn card_range_single_provider_activation_requires_one_provider() -> Result<()> {
    if !full_integration_enabled() {
        eprintln!(
            "Skipping card range API integration scenario. Set RUN_FULL_INTEGRATION_TESTS=1 to run it."
        );
        return Ok(());
    }

    // Scenario purpose:
    // This is the baseline card-range lifecycle proof for the final Wurzburg
    // card foundation. It proves that a SingleProvider range can be created as
    // Draft, cannot become Active without exactly one eligible provider, can
    // attach a provider through the range/provider API, and then activates
    // through the public Axum router. The test uses the real Oracle-backed
    // AppState and deliberately avoids CP generation, because CP:{card_number}
    // belongs to a later card/user/funding slice.

    // Step 1: boot real app state from test configuration. This follows the
    // same route construction as production, so handler/service/repository
    // wiring is verified together.
    let settings = Settings::new()?;
    apply_oracle_migrations(&settings).await?;
    let app_state = Arc::new(AppState::new(settings.clone()).await?);
    let mut app = build_app_router(app_state);

    // Step 2: seed one minimal provider directly in Oracle. Provider creation
    // is not the behavior under test in this scenario, but card_range_providers
    // has a real foreign-key dependency on providers.
    let provider_id = Uuid::new_v4();
    seed_provider(&settings, provider_id).await?;

    // Step 3: create a unique SingleProvider card range through the public API.
    let range_suffix = unique_range_suffix();
    let idempotency_run_id = Uuid::new_v4();
    let create_body = json!({
        "start_card_number": format!("62198610{range_suffix}0000"),
        "end_card_number": format!("62198610{range_suffix}9999"),
        "funding_mode": "SingleProvider",
        "metadata": {
            "scenario": "single_provider_activation_requires_one_provider"
        }
    });
    let create_response = call_json(
        &mut app,
        Method::POST,
        "/api/v1/card-ranges",
        create_body,
        Some(&format!(
            "idem-create-single-provider-range-{idempotency_run_id}"
        )),
    )
    .await?;
    assert_status(&create_response, StatusCode::CREATED);
    let card_range_id = response_uuid(&create_response.body, "card_range_id")?;

    // Step 4: prove activation is rejected before an eligible provider exists.
    let early_activation = call_json(
        &mut app,
        Method::POST,
        &format!("/api/v1/card-ranges/{card_range_id}/activate"),
        json!({}),
        Some(&format!(
            "idem-activate-before-provider-{idempotency_run_id}"
        )),
    )
    .await?;
    assert_status(&early_activation, StatusCode::BAD_REQUEST);

    // Step 5: attach the seeded provider to the range. This row is eligibility
    // only: it does not define cardholder priority or generate any CP profile.
    let attach_response = call_json(
        &mut app,
        Method::POST,
        &format!("/api/v1/card-ranges/{card_range_id}/providers/{provider_id}"),
        json!({
            "metadata": {
                "scenario": "single_provider_activation_requires_one_provider"
            }
        }),
        Some(&format!("idem-attach-provider-{idempotency_run_id}")),
    )
    .await?;
    assert_status(&attach_response, StatusCode::OK);

    // Step 6: activation now succeeds because a SingleProvider range has
    // exactly one active provider.
    let activation = call_json(
        &mut app,
        Method::POST,
        &format!("/api/v1/card-ranges/{card_range_id}/activate"),
        json!({}),
        Some(&format!(
            "idem-activate-after-provider-{idempotency_run_id}"
        )),
    )
    .await?;
    assert_status(&activation, StatusCode::OK);
    assert_eq!(activation.body["status"], "Active");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn card_range_policy_replacement_keeps_immutable_profile_history() -> Result<()> {
    if !full_integration_enabled() {
        eprintln!(
            "Skipping card range policy integration scenario. Set RUN_FULL_INTEGRATION_TESTS=1 to run it."
        );
        return Ok(());
    }

    // Scenario purpose:
    // This proves the Section-11 replacement design documented in
    // WURZBURG_CARD_RANGE_POLICY_HANDOFF.md. A card range receives an active
    // policy profile, then receives a second policy profile. The API must expose
    // the second profile as active with a higher version while Oracle retains
    // the previous immutable profile row for audit and future FundingPlan
    // traceability.

    // Step 1: boot the real app and create a Draft MultiProvider range. Policy
    // assignment is range-scoped and does not require CP:{card_number}.
    let settings = Settings::new()?;
    apply_oracle_migrations(&settings).await?;
    let app_state = Arc::new(AppState::new(settings).await?);
    let mut app = build_app_router(app_state);

    let range_suffix = unique_range_suffix();
    let idempotency_run_id = Uuid::new_v4();
    let create_response = call_json(
        &mut app,
        Method::POST,
        "/api/v1/card-ranges",
        json!({
            "start_card_number": format!("62198620{range_suffix}0000"),
            "end_card_number": format!("62198620{range_suffix}9999"),
            "funding_mode": "MultiProvider",
            "metadata": {
                "scenario": "policy_replacement_keeps_history"
            }
        }),
        Some(&format!(
            "idem-create-multi-provider-range-{idempotency_run_id}"
        )),
    )
    .await?;
    assert_status(&create_response, StatusCode::CREATED);
    let card_range_id = response_uuid(&create_response.body, "card_range_id")?;

    // Step 2: assign the first active policy profile.
    let first_policy = call_json(
        &mut app,
        Method::POST,
        &format!("/api/v1/card-ranges/{card_range_id}/policy"),
        json!({
            "profile": {
                "withdrawal_limits": {
                    "per_transaction_min_amount": 1000,
                    "per_transaction_max_amount": 1000000,
                    "daily": { "max_amount": 5000000, "max_count": 5 }
                },
                "calendar": { "timezone": "Asia/Tehran" }
            }
        }),
        Some(&format!("idem-first-policy-{idempotency_run_id}")),
    )
    .await?;
    assert_status(&first_policy, StatusCode::CREATED);
    assert_eq!(first_policy.body["version"], 1);
    let first_policy_id = response_uuid(&first_policy.body, "card_policy_profile_id")?;

    // Step 3: assign a changed policy. The new active profile receives a new ID
    // and version. This is the object Nuremberg will later read from
    // CPOL:MultiProvider:{card_range_id}.
    let second_policy = call_json(
        &mut app,
        Method::POST,
        &format!("/api/v1/card-ranges/{card_range_id}/policy"),
        json!({
            "profile": {
                "withdrawal_limits": {
                    "per_transaction_min_amount": 2000,
                    "per_transaction_max_amount": 2000000,
                    "daily": { "max_amount": 6000000, "max_count": 6 }
                },
                "calendar": { "timezone": "Asia/Tehran" }
            }
        }),
        Some(&format!("idem-second-policy-{idempotency_run_id}")),
    )
    .await?;
    assert_status(&second_policy, StatusCode::CREATED);
    assert_eq!(second_policy.body["version"], 2);
    let second_policy_id = response_uuid(&second_policy.body, "card_policy_profile_id")?;
    assert_ne!(first_policy_id, second_policy_id);

    // Step 4: read back the active range policy and prove it points at the
    // second immutable profile.
    let active_policy = call_json(
        &mut app,
        Method::GET,
        &format!("/api/v1/card-ranges/{card_range_id}/policy"),
        json!({}),
        None,
    )
    .await?;
    assert_status(&active_policy, StatusCode::OK);
    assert_eq!(
        active_policy.body["card_policy_profile_id"],
        second_policy_id.to_string()
    );
    assert_eq!(active_policy.body["version"], 2);

    Ok(())
}

struct JsonResponse {
    status: StatusCode,
    body: serde_json::Value,
}

fn assert_status(response: &JsonResponse, expected: StatusCode) {
    assert_eq!(
        response.status, expected,
        "unexpected status, body={}",
        response.body
    );
}

async fn call_json(
    app: &mut axum::Router,
    method: Method,
    path: &str,
    body: serde_json::Value,
    idempotency_key: Option<&str>,
) -> Result<JsonResponse> {
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json")
        .header("X-Actor-Subject", "card-range-api-integration");

    if let Some(idempotency_key) = idempotency_key {
        builder = builder.header(
            "Idempotency-Key",
            format!("{idempotency_key}-{}", Uuid::new_v4()),
        );
    }

    let response = app
        .oneshot(builder.body(Body::from(body.to_string()))?)
        .await?;
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await?;
    let body = if bytes.is_empty() {
        json!({})
    } else {
        serde_json::from_slice(&bytes)?
    };

    Ok(JsonResponse { status, body })
}

async fn seed_provider(settings: &Settings, provider_id: Uuid) -> Result<()> {
    let oracle_config = OracleConnectConfig::from_driver_config(&settings.database)?;
    let pool = OraclePool::connect(oracle_config).await?;
    pool.with_transaction("card range integration provider seed", move |connection| {
        let provider_id = provider_id.as_bytes().to_vec();
        connection
            .execute(
                r#"
                INSERT INTO providers (
                    id, legal_name, trade_name, tax_id, email_address,
                    office_phone, mailing_address, status,
                    created_by_subject, updated_by_subject
                )
                VALUES (
                    :1, 'Integration Provider', 'Integration Provider', :2,
                    'integration@example.com', '+980000000',
                    'Integration Address', 'ACTIVE',
                    'card-range-api-integration', 'card-range-api-integration'
                )
                "#,
                &[&provider_id, &format!("tax-{}", Uuid::new_v4())],
            )
            .map_err(|error| {
                wurzburg::db::error::DbError::Query(format!(
                    "failed to seed provider for card range integration test: {error}"
                ))
            })?;
        Ok(())
    })
    .await?;

    Ok(())
}

fn response_uuid(body: &serde_json::Value, field: &str) -> Result<Uuid> {
    let value = body
        .get(field)
        .and_then(|value| value.as_str())
        .with_context(|| format!("response field `{field}` was not a string: {body}"))?;
    Ok(Uuid::parse_str(value)?)
}

fn unique_range_suffix() -> String {
    let simple = Uuid::new_v4().simple().to_string();
    let digits: String = simple
        .chars()
        .filter(char::is_ascii_digit)
        .take(4)
        .collect();
    format!("{digits:0<4}")
}

fn full_integration_enabled() -> bool {
    env::var("RUN_FULL_INTEGRATION_TESTS")
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

async fn apply_oracle_migrations(settings: &Settings) -> Result<()> {
    let _guard = ORACLE_MIGRATION_LOCK.lock().await;
    let oracle_config = OracleConnectConfig::from_driver_config(&settings.database)?;
    let oracle_pool = OraclePool::connect(oracle_config).await?;
    let migrator = OracleMigrator::new(oracle_pool);

    for migration in wurzburg_migrations() {
        migrator.apply(migration).await?;
    }

    Ok(())
}
