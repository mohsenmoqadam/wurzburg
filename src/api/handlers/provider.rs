// src/api/handlers/provider.rs
use axum::body::Body;
use axum::http::header::{CONTENT_DISPOSITION, CONTENT_TYPE};
use axum::response::Response;
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use chrono::{DateTime, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::api::validation::RuntimeValidatable;
use crate::config::Settings;
use crate::db::models::Provider;
use crate::state::AppState;
use crate::tigerbeetle::models::AppAccount;

// --- DTOs ---
#[derive(Deserialize, ToSchema, Debug)]
pub struct CreateProviderRequest {
    pub is_core: Option<bool>,
    pub legal_name: String,
    pub trade_name: String,
    pub tax_id: String,
    pub email_address: String,
    pub office_phone: String,
    pub website_url: Option<String>,
    pub mailing_address: String,
    pub alert_phone_numbers: Option<Vec<String>>,
    pub fee_rate_bps: Option<i32>,
    pub fixed_fee_amount: Option<i64>,
}

#[derive(Serialize, Deserialize, Clone, ToSchema)]
pub struct ProviderKafkaSettings {
    pub topic: String,
    pub brokers: Vec<String>,
    pub security_protocol: String,
    pub sasl_mechanism: String,
    pub username: String,
    pub password: String,
    pub security_cert: String,
}

#[derive(Serialize, ToSchema)]
pub struct CreateProviderResponse {
    pub provider_id: Uuid,
    pub ledger_account_id: Uuid,
    pub kafka: ProviderKafkaSettings,
}

#[derive(Serialize, ToSchema)]
pub struct GetProviderResponse {
    pub id: Uuid,
    pub is_core: bool,
    pub legal_name: String,
    pub trade_name: String,
    pub tax_id: String,
    pub email_address: String,
    pub office_phone: String,
    pub website_url: Option<String>,
    pub mailing_address: String,
    pub alert_phone_numbers: Vec<String>,
    pub is_active: bool,
    pub fee_rate_bps: i32,
    pub fixed_fee_amount: i64,
    pub ledger_account_id: Uuid,
    pub credit: i64,
    pub debit: i64,
    #[schema(value_type = String, format = DateTime)]
    pub created_at: DateTime<Utc>,
    #[schema(value_type = String, format = DateTime)]
    pub updated_at: DateTime<Utc>,
    pub kafka: ProviderKafkaSettings,
}

// Implement dynamic runtime validation
impl RuntimeValidatable for CreateProviderRequest {
    fn validate_with_config(&self, config: &Settings) -> Result<(), HashMap<&'static str, String>> {
        let mut errors = HashMap::new();
        let rules = &config.validation.provider;

        // Legal Name Validation
        if self.legal_name.trim().chars().count() < rules.legal_name_min {
            let msg = rules
                .legal_name_msg
                .replace("{min}", &rules.legal_name_min.to_string());
            errors.insert("legal_name", msg);
        }

        // Trade Name Validation
        if self.trade_name.trim().chars().count() < rules.trade_name_min {
            let msg = rules
                .trade_name_msg
                .replace("{min}", &rules.trade_name_min.to_string());
            errors.insert("trade_name", msg);
        }

        // Tax ID Validation
        if self.tax_id.trim().chars().count() < rules.tax_id_min {
            let msg = rules
                .tax_id_msg
                .replace("{min}", &rules.tax_id_min.to_string());
            errors.insert("tax_id", msg);
        }

        // Email Validation (using regex for standard format checking)
        if let Ok(email_regex) = Regex::new(&rules.email_regex) {
            if !email_regex.is_match(&self.email_address) {
                errors.insert("email_address", rules.email_msg.clone());
            }
        } else {
            errors.insert(
                "email_address",
                "Invalid regex pattern in config".to_string(),
            );
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

// --- Error Handling (Helper) ---
fn internal_error<E: std::fmt::Debug>(err: E) -> (StatusCode, String) {
    tracing::error!("Internal error: {:?}", err);
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "Internal Server Error".to_string(),
    )
}

// --- Handlers ---

/// Create a new provider
#[utoipa::path(
    post,
    path = "/api/v1/providers",
    request_body = CreateProviderRequest,
    responses(
        (status = 201, description = "Provider created", body = CreateProviderResponse),
        (status = 400, description = "Validation error")
    ),
    tag = "Providers"
)]
#[tracing::instrument(skip(state))]
pub async fn create(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<CreateProviderRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let now = Utc::now();

    // 1. Execute Runtime Validation
    if let Err(validation_errors) = payload.validate_with_config(&state.config) {
        let error_msg = validation_errors
            .into_iter()
            .map(|(field, msg)| format!("{}: {}", field, msg))
            .collect::<Vec<String>>()
            .join(" | ");

        return Err((StatusCode::BAD_REQUEST, error_msg));
    }

    let provider_id = Uuid::new_v4();
    let ledger_account_id = Uuid::new_v4();
    let alert_phones = payload.alert_phone_numbers.unwrap_or_default();
    let actor_id = Uuid::new_v4(); // In reality, extract from Auth context

    // 2. Create TigerBeetle Account
    // Convert UUID to u128 for TigerBeetle ID
    let tb_account = AppAccount {
        id: ledger_account_id.as_u128(),
        debits_pending: 0,
        debits_posted: 0,
        credits_pending: 0,
        credits_posted: 0,
        user_data_128: provider_id.as_u128(),
        user_data_64: 0,
        user_data_32: 0,
        reserved: 0,
        ledger: state.config.tigerbeetle.ledger_id,
        code: state.config.tigerbeetle.provider_account_code,
        flags: 0,
        timestamp: 0,
    };

    state
        .tb_client
        .create_account(tb_account)
        .await
        .map_err(|e| {
            tracing::error!("TigerBeetle account creation failed: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to provision ledger account".to_string(),
            )
        })?;

    // 3. Generate Kafka Credentials
    let topic_name = format!("provider.events.{}", provider_id.simple());
    let kafka_username = format!("provider_user_{}", provider_id.simple());
    let kafka_password = Uuid::new_v4().simple().to_string();

    let brokers: Vec<String> = state
        .config
        .kafka
        .consumer_defaults
        .bootstrap_servers
        .split(',')
        .map(String::from)
        .collect();
    let security_protocol = state
        .config
        .kafka
        .consumer_defaults
        .security_protocol
        .clone();

    let cert_path = "/home/mohsen/Codes/war/wurzburg/secrets/kafka.arshamnovin.ir.crt";
    let security_cert_content = std::fs::read_to_string(cert_path).unwrap_or_default();

    let kafka_settings = ProviderKafkaSettings {
        topic: topic_name.clone(),
        brokers,
        security_protocol,
        sasl_mechanism: "SCRAM-SHA-512".to_string(),
        username: kafka_username.clone(),
        password: kafka_password.clone(),
        security_cert: security_cert_content,
    };

    let kafka_config_json = serde_json::to_value(&kafka_settings).map_err(internal_error)?;

    // 4. Map DTO to Model
    let provider = Provider {
        id: provider_id,
        is_core: payload.is_core.unwrap_or(false),
        legal_name: payload.legal_name,
        trade_name: payload.trade_name,
        tax_id: payload.tax_id,
        email_address: payload.email_address,
        office_phone: payload.office_phone,
        website_url: payload.website_url,
        mailing_address: payload.mailing_address,
        alert_phone_numbers: alert_phones,
        banner_image_id: None,
        profile_image_id: None,
        is_active: true,
        fee_rate_bps: payload.fee_rate_bps.unwrap_or(0),
        fixed_fee_amount: payload.fixed_fee_amount.unwrap_or(0),
        kafka_config: kafka_config_json,
        ledger_account_id,
        created_at: now,
        updated_at: now,
    };

    // 5. Delegate to Repository
    state
        .db
        .create_provider(provider, actor_id)
        .await
        .map_err(internal_error)?;

    // 6. Execute Kafka Admin Commands Synchronously in a Blocking Thread
    let kafka_admin = state.kafka_admin.clone();
    let target_topic = topic_name;
    let target_user = kafka_username;
    let target_pass = kafka_password;

    let kafka_join_result = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        kafka_admin.create_provider_topic(&target_topic)?;
        kafka_admin.create_scram_user(&target_user, &target_pass)?;
        kafka_admin.grant_consumer_acls(&target_topic, &target_user)?;
        Ok(())
    })
    .await;

    match kafka_join_result {
        Ok(Ok(_)) => tracing::info!("Kafka setup success for provider: {}", provider_id),
        Ok(Err(e)) => tracing::error!("Kafka admin scripts failed: {:?}", e),
        Err(e) => tracing::error!("Thread pool failed: {:?}", e),
    }

    // 7. Return Response
    let response = CreateProviderResponse {
        provider_id,
        ledger_account_id,
        kafka: kafka_settings,
    };

    Ok((StatusCode::CREATED, Json(response)))
}

/// Get provider by ID
#[utoipa::path(
    get,
    path = "/api/v1/providers/{id}",
    operation_id = "get_provider",
    params(("id" = Uuid, Path, description = "Provider ID")),
    responses(
        (status = 200, description = "Provider found", body = GetProviderResponse),
        (status = 404, description = "Provider not found")
    ),
    tag = "Providers"
)]
#[tracing::instrument(skip(state))]
pub async fn get(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let provider = state
        .db
        .get_provider_by_id(id)
        .await
        .map_err(internal_error)?;

    match provider {
        Some(p) => {
            let tb_id = p.ledger_account_id.as_u128();
            let tb_accounts = state.tb_client.lookup_account(tb_id).await.map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Ledger error: {}", e),
                )
            })?;

            let (credit, debit) = tb_accounts
                .first()
                .map(|acc| (acc.credits_posted as i64, acc.debits_posted as i64))
                .unwrap_or((0, 0));

            let kafka_settings: ProviderKafkaSettings = serde_json::from_value(p.kafka_config)
                .unwrap_or_else(|_| ProviderKafkaSettings {
                    topic: "unknown".to_string(),
                    brokers: vec![],
                    security_protocol: String::new(),
                    sasl_mechanism: String::new(),
                    username: String::new(),
                    password: String::new(),
                    security_cert: String::new(),
                });

            let response = GetProviderResponse {
                id: p.id,
                is_core: p.is_core,
                legal_name: p.legal_name,
                trade_name: p.trade_name,
                tax_id: p.tax_id,
                email_address: p.email_address,
                office_phone: p.office_phone,
                website_url: p.website_url,
                mailing_address: p.mailing_address,
                alert_phone_numbers: p.alert_phone_numbers,
                is_active: p.is_active,
                fee_rate_bps: p.fee_rate_bps,
                fixed_fee_amount: p.fixed_fee_amount,
                ledger_account_id: p.ledger_account_id,
                credit,
                debit,
                created_at: p.created_at,
                updated_at: p.updated_at,
                kafka: kafka_settings,
            };

            Ok((StatusCode::OK, Json(response)))
        }
        None => Err((StatusCode::NOT_FOUND, "Provider not found".to_string())),
    }
}

/// Download Kafka Security Certificate
#[utoipa::path(
    get,
    path = "/api/v1/providers/certificate",
    responses(
        (status = 200, description = "Kafka Public Certificate downloaded", content_type = "application/x-x509-ca-cert"),
        (status = 500, description = "Internal Server Error")
    ),
    tag = "Providers"
)]
#[tracing::instrument(skip(state))]
pub async fn download_cert(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let cert_path_str = &state.config.kafka.producer.security_cert;
    let cert_path = std::path::Path::new(cert_path_str);

    let file_name = cert_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("kafka_certificate.crt");

    let cert_content = tokio::fs::read(cert_path).await.map_err(|e| {
        tracing::error!("Failed to read certificate file: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Certificate file not found".to_string(),
        )
    })?;

    let response = Response::builder()
        .header(CONTENT_TYPE, "application/x-x509-ca-cert")
        .header(
            CONTENT_DISPOSITION,
            format!("attachment; filename=\"{}\"", file_name),
        )
        .body(Body::from(cert_content))
        .unwrap();

    Ok(response)
}
