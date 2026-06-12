// src/api/handlers/user.rs
use std::sync::Arc;
use axum::{extract::State, Json, http::StatusCode, response::IntoResponse, extract::Path, extract::Query};
use serde::{Deserialize, Serialize};
use tigerbeetle_rustclient_tests_snapshot::AccountFlags;
use utoipa::ToSchema;
use uuid::Uuid;
use chrono::{DateTime, Utc};

use crate::db::models::AccountStatus;
use crate::state::AppState;
use crate::tigerbeetle::models::AppAccount;

// --- DTOs ---

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateUserRequest {
    pub nid: String,
    pub provider_id: Uuid,
    pub internal_metadata: Option<serde_json::Value>,
    pub external_metadata: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CreateUserResponse {
    pub user_id: Uuid,
    pub nid: String,
    pub provider_id: Uuid,
    pub ledger_account_id: Uuid,
    pub is_existing_user: bool,
    #[schema(value_type = String, format = DateTime)]
    pub created_at: DateTime<Utc>,
    #[schema(value_type = String, format = DateTime)]
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct GetUserQuery {
    pub provider_id: Uuid,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct GetUserResponse {
    pub user_id: Uuid,
    pub nid: String,
    pub internal_metadata: serde_json::Value,
    pub external_metadata: serde_json::Value,
    pub ledger_account_id: Uuid,
    pub credit: u128,
    pub debit: u128,
    pub status: AccountStatus,
    #[schema(value_type = String, format = DateTime)]
    pub created_at: DateTime<Utc>,
    #[schema(value_type = String, format = DateTime)]
    pub updated_at: DateTime<Utc>,
}

#[derive(Serialize, ToSchema)]
pub struct ProviderBalanceItem {
    pub provider_id: Uuid,
    pub provider_name: String,
    pub balance: i64,
}

#[derive(Serialize, ToSchema)]
pub struct UserBalanceResponse {
    pub total_balance: i64,
    pub provider_balances: Vec<ProviderBalanceItem>,
}

// --- Error Handling (Helper) ---
fn internal_error<E: std::fmt::Debug>(err: E) -> (StatusCode, String) {
    tracing::error!("Internal error: {:?}", err);
    (StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error".to_string())
}

// --- Handlers ---

/// Register a new user or link an existing one to a provider
#[utoipa::path(
    post,
    path = "/api/v1/users",
    request_body = CreateUserRequest,
    responses(
        (status = 201, description = "User created or linked", body = CreateUserResponse),
        (status = 400, description = "Validation error"),
        (status = 500, description = "Internal server error")
    ),
    tag = "Users"
)]
#[tracing::instrument(skip(state))]
pub async fn create_or_link(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<CreateUserRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let request_time = Utc::now();
    let ledger_account_id = Uuid::new_v4();
    let actor_id = Uuid::new_v4(); // In reality, extract from Auth context

    let internal_meta = payload.internal_metadata.unwrap_or_else(|| serde_json::json!({}));
    let external_meta = payload.external_metadata.unwrap_or_else(|| serde_json::json!({}));

    // 1. Delegate to DB Repository (Find or Create User + Account)
    let (user, account) = state.db.create_or_link_user(
        payload.nid.clone(),
        payload.provider_id,
        internal_meta,
        external_meta,
        ledger_account_id,
        actor_id,
    )
    .await
    .map_err(internal_error)?;

    // Determine if the user existed before this transaction
    // If the creation time is older than the start of our request, they already existed.
    let is_existing_user = user.created_at < request_time - chrono::Duration::seconds(1);

    // 2. Create TigerBeetle Account
    // We only need to create the ledger account if a new UserAccount was provisioned
    let is_new_account = account.created_at >= request_time - chrono::Duration::seconds(1);
    
    if is_new_account {
        let tb_account = AppAccount {
            id: account.ledger_account_id.as_u128(),
            debits_pending: 0,
            debits_posted: 0,
            credits_pending: 0,
            credits_posted: 0,
            user_data_128: user.id.as_u128(),
            user_data_64: 0,
            user_data_32: 0,
            reserved: 0,
            // Assuming config has fields for user ledger setup
            ledger: state.config.tigerbeetle.ledger_id, 
            code: state.config.tigerbeetle.user_account_code,
            flags: AccountFlags::DebitsMustNotExceedCredits.bits(),
            timestamp: 0,
        };

        state.tb_client.create_account(tb_account).await.map_err(|e| {
            tracing::error!("TigerBeetle user account creation failed: {:?}", e);
            // Note: In a production system, you might want to implement a saga pattern or rollback the DB transaction here.
            (StatusCode::INTERNAL_SERVER_ERROR, "Failed to provision ledger account".to_string())
        })?;
    }

    // 3. Construct and Return Response
    let response = CreateUserResponse {
        user_id: user.id,
        nid: user.nid,
        provider_id: payload.provider_id,
        ledger_account_id: account.ledger_account_id,
        is_existing_user,
        created_at: user.created_at,
        updated_at: user.updated_at,
    };

    Ok((StatusCode::CREATED, Json(response)))
}

/// Get user details and balances for a specific provider
#[utoipa::path(
    get,
    path = "/api/v1/users/{id}",
    operation_id = "get_user",
    params(
        ("id" = Uuid, Path, description = "User ID"),
        ("provider_id" = Uuid, Query, description = "Provider ID")
    ),
    responses(
        (status = 200, description = "User details found", body = GetUserResponse),
        (status = 404, description = "User or provider link not found"),
        (status = 500, description = "Internal server error")
    ),
    tag = "Users"
)]
#[tracing::instrument(skip(state))]
pub async fn get(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<Uuid>,
    Query(query): Query<GetUserQuery>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    
    let details_opt = state.db.get_user_provider_details(user_id, query.provider_id)
        .await
        .map_err(internal_error)?;

    match details_opt {
        Some(details) => {
            let tb_account_id = details.user_ledger_account_id.as_u128();
            let tb_accounts = state.tb_client.lookup_account(tb_account_id)
                .await
                .map_err(|e| {
                    (StatusCode::INTERNAL_SERVER_ERROR, format!("Ledger lookup failed: {}", e))
                })?;
            let (credit, debit) = tb_accounts
                .first()
                .map(|acc| (acc.credits_posted, acc.debits_posted))
                .unwrap_or((0, 0));    
            let response = GetUserResponse {
                user_id: details.user_id,
                nid: details.nid,
                internal_metadata: details.internal_metadata,
                external_metadata: details.external_metadata,
                ledger_account_id: details.user_ledger_account_id,
                credit,
                debit,
                status: details.status,
                created_at: details.created_at,
                updated_at: details.updated_at,
            };
            Ok((StatusCode::OK, Json(response)))
        }
        None => Err((
            StatusCode::NOT_FOUND,
            "User not found or not linked to the specified provider".to_string(),
        )),
    }
}

/// Get total and provider-specific balances for a user
#[utoipa::path(
    get,
    path = "/api/v1/users/{id}/balance",
    tag = "Users",
    params(
        ("id" = Uuid, Path, description = "Unique identifier of the user")
    ),
    responses(
        (status = 200, description = "Successfully retrieved user balance", body = UserBalanceResponse),
        (status = 500, description = "Internal server error")
    )
)]
#[tracing::instrument(skip(state))]
pub async fn get_user_balance(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<Uuid>,
) -> Result<Json<UserBalanceResponse>, (StatusCode, String)> { 
    let accounts_info = state.db.get_user_accounts_provider_info(user_id)
        .await
        .map_err(internal_error)?;
    
    if accounts_info.is_empty() {
        return Ok(Json(UserBalanceResponse {
            total_balance: 0,
            provider_balances: vec![],
        }));
    }
    
    let account_ids: Vec<u128> = accounts_info
        .iter()
        .map(|info| info.ledger_account_id.as_u128())
        .collect();
    let tb_accounts = state.tb_client.lookup_accounts(account_ids)
        .await
        .map_err(internal_error)?;
    let mut total_balance: i64 = 0;
    let mut provider_balances = Vec::with_capacity(accounts_info.len());
    for info in accounts_info {
        let tb_account = tb_accounts
            .iter()
            .find(|acc| acc.id == info.ledger_account_id.as_u128());
        let balance = if let Some(acc) = tb_account {
            acc.credits_posted as i64 - acc.debits_posted as i64
        } else {
            0
        };

        total_balance += balance;

        provider_balances.push(ProviderBalanceItem {
            provider_id: info.provider_id,
            provider_name: info.provider_name,
            balance,
        });
    }
    let response = UserBalanceResponse {
        total_balance,
        provider_balances,
    };

    Ok(Json(response))
}