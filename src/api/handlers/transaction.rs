// src/api/handlers/transaction.rs
use axum::{
    extract::{State, Path, Query},
    Json,
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use tracing::Instrument;
use std::sync::Arc;
use utoipa::{ToSchema, IntoParams};
use uuid::Uuid;
use crate::{db::models::{TransactionStatus, TransactionType}, state::AppState};
use crate::tigerbeetle::models::AppTransfer;

// --- DTOs ---

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreditAccountRequest {
    pub idempotency_key: Uuid,
    pub amount: u64,
    pub description: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TransactionResponse {
    pub transaction_id: Uuid,
    pub status: String,
    pub current_balance: Option<i64>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct DebitAccountRequest {
    pub idempotency_key: Uuid,
    pub amount: u64,
    pub description: Option<String>,
}

#[derive(Deserialize, IntoParams, Debug)]
pub struct PaginationQuery {
    #[serde(default = "default_page")]
    pub page: i64,
    #[serde(default = "default_limit")]
    pub limit: i64,
}

fn default_page() -> i64 { 1 }
fn default_limit() -> i64 { 20 }

#[derive(Serialize, ToSchema)]
pub struct TransactionItem {
    pub id: Uuid,
    pub idempotency_key: String,
    pub transaction_type: TransactionType, 
    pub amount: i64,
    pub dr_account_id: Uuid,
    pub cr_account_id: Uuid,
    pub user_id: Option<Uuid>,
    pub provider_id: Option<Uuid>,
    pub status: TransactionStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Serialize, ToSchema)]
pub struct PaginatedTransactionsResponse {
    pub data: Vec<TransactionItem>,
    pub total_count: i64,
    pub page: i64,
    pub total_pages: i64,
}

// --- Error Handling (Helper) ---
fn internal_error<E: std::fmt::Debug>(err: E) -> (StatusCode, String) {
    tracing::error!("Internal error: {:?}", err);
    (StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error".to_string())
}

// --- Handlers ---

/// Allocate credit to a user's account for a specific provider.
#[utoipa::path(
    post,
    path = "/api/v1/transactions/{provider_id}/users/{user_id}/credit",
    operation_id = "credit_user",
    request_body = CreditAccountRequest,
    responses(
        (status = 200, description = "Credit allocated successfully", body = TransactionResponse),
        (status = 400, description = "Bad request or idempotency conflict"),
        (status = 404, description = "Account not found"),
        (status = 500, description = "Internal server error")
    ),
    tag = "Transactions"
)]
#[tracing::instrument(skip(state))]
pub async fn credit_user(
    State(state): State<Arc<AppState>>,
    Path((provider_id, user_id)): Path<(Uuid, Uuid)>,
    Json(payload): Json<CreditAccountRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let idem_key_str = payload.idempotency_key.to_string();

    // 1. Fetch user account
    let user_account = state.db.get_user_provider_details(user_id, provider_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "User account not found".to_string()))?;

    // 2. Check idempotency
    if let Ok(Some(existing_tx)) = state.db.get_transaction_by_idempotency_key(&idem_key_str).await {
        return Ok((StatusCode::OK, Json(TransactionResponse {
            transaction_id: existing_tx.id,
            status: "ALREADY_PROCESSED".to_string(),
            current_balance: None,
        })));
    }

    // 3. Fetch provider details
    let provider_details = state.db.get_provider_by_id(provider_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Provider not found".to_string()))?;

    // 4. Prepare TigerBeetle Transfer (Provider -> User)
    let transfer = AppTransfer {
        id: payload.idempotency_key.as_u128(),
        debit_account_id: provider_details.ledger_account_id.as_u128(), // Provider is debited
        credit_account_id: user_account.user_ledger_account_id.as_u128(),   // User is credited
        amount: payload.amount as u128,
        pending_id: 0,
        user_data_128: 0,
        user_data_64: 0,
        user_data_32: 0,
        timeout: 0,
        ledger: state.config.tigerbeetle.ledger_id,
        code: state.config.tigerbeetle.transfer_code,
        flags: 0,
        timestamp: 0,
    };

    // 5. Execute in TigerBeetle
    let tb_result = state.tb_client.create_transfer(transfer)
        .instrument(tracing::info_span!("tb_create_credit_transfer"))
        .await
        .map_err(|e| {
        tracing::error!("Ledger transfer failed: {:?}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "Failed to execute ledger transfer".to_string())
    })?;

    if tb_result.is_empty() {
        // 6. Persist in durable database
        let pg_tx = state.db.execute_credit_transfer(
            idem_key_str,
            payload.amount as i64,
            provider_id,
            user_id,
            provider_details.ledger_account_id,
            user_account.user_ledger_account_id,
            provider_id,
            payload.description,
        )
        .await
        .map_err(internal_error)?;

        let tb_accounts = state.tb_client.lookup_account(user_account.user_ledger_account_id.as_u128())
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Ledger error: {}", e)))?;

        let current_balance = tb_accounts.first()
            .map(|acc| (acc.credits_posted as i64) - (acc.debits_posted as i64))
            .unwrap_or(0);

        Ok((StatusCode::OK, Json(TransactionResponse {
            transaction_id: pg_tx.id,
            status: "SUCCESS".to_string(),
            current_balance: Some(current_balance),
        })))
    } else {
        let error_msg = format!("Ledger rejected the credit allocation: {:?}", tb_result);
        tracing::warn!("{}", error_msg);
        Err((StatusCode::BAD_REQUEST, error_msg))
    }
}

/// Debit funds from a user's account for a specific provider.
#[utoipa::path(
    post,
    path = "/api/v1/transactions/{provider_id}/users/{user_id}/debit",
    operation_id = "debit_user",
    request_body = DebitAccountRequest,
    responses(
        (status = 200, description = "Account debited successfully", body = TransactionResponse),
        (status = 400, description = "Insufficient funds or bad request"),
        (status = 404, description = "Account not found"),
        (status = 500, description = "Internal server error")
    ),
    tag = "Transactions"
)]
#[tracing::instrument(skip(state))] 
pub async fn debit_user(
    State(state): State<Arc<AppState>>,
    Path((provider_id, user_id)): Path<(Uuid, Uuid)>,
    Json(payload): Json<DebitAccountRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let idem_key_str = payload.idempotency_key.to_string();

    // 1. Fetch user account
    let user_account = state.db.get_user_provider_details(user_id, provider_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "User account not found".to_string()))?;

    // 2. Check idempotency
    if let Ok(Some(existing_tx)) = state.db.get_transaction_by_idempotency_key(&idem_key_str).await {
        return Ok((StatusCode::OK, Json(TransactionResponse {
            transaction_id: existing_tx.id,
            status: "ALREADY_PROCESSED".to_string(),
            current_balance: None,
        })));
    }

    // 3. Fetch provider details
    let provider_details = state.db.get_provider_by_id(provider_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Provider not found".to_string()))?;

    // 4. Business Logic: Handle active priority cancellation on debit
    // If the user has an active priority list that includes the current provider,
    // we must cancel this priority config before proceeding with the debit.
    // The priority must remain disabled until explicitly re-enabled by the user.
    let active_priority_opt = state.db.get_active_priority_config(user_id)
        .await
        .map_err(internal_error)?;

    if let Some(active_priority) = active_priority_opt {
        // Assuming `provider_ids` is a Vec<Uuid> inside your priority model
        if active_priority.items.iter().any(|item| item.provider_id == provider_id) {
            tracing::info!(
                "User {} has an active priority containing provider {}. Cancelling priority.",
                user_id, provider_id
            );

            // Cancel the active priority in durable database
            state.db.cancel_active_priority_config(user_id, user_id)
                .await
                .map_err(internal_error)?;

            // Invalidate the active priority cache in Redis to prevent stale reads
            let redis_key = format!("priority:active:{{user:{}}}", user_id);
            if let Ok(mut redis_conn) = state.redis.get().await {
                let _: Result<(), _> = redis::cmd("DEL")
                    .arg(&redis_key)
                    .query_async(&mut redis_conn)
                    .await;
            } else {
                tracing::warn!("Failed to acquire Redis connection for cache invalidation. Key: {}", redis_key);
            }
        }
    }

    // 5. Prepare TigerBeetle Transfer (Reverse direction: User -> Provider)
    let transfer = AppTransfer {
        id: payload.idempotency_key.as_u128(),
        debit_account_id: user_account.user_ledger_account_id.as_u128(),    // User is debited
        credit_account_id: provider_details.ledger_account_id.as_u128(), // Provider is credited
        amount: payload.amount as u128,
        pending_id: 0,
        user_data_128: 0,
        user_data_64: 0,
        user_data_32: 0,
        timeout: 0,
        ledger: state.config.tigerbeetle.ledger_id,
        code: state.config.tigerbeetle.transfer_code,
        flags: 0,
        timestamp: 0,
    };

    // 6. Execute in TigerBeetle
    let tb_result = state.tb_client.create_transfer(transfer)
        .instrument(tracing::info_span!("tb_create_debit_transfer"))
        .await
        .map_err(|e| {
            tracing::error!("Ledger transfer failed: {:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Failed to execute ledger transfer".to_string())
        })?;

    // 7. Persist in durable database
    if tb_result.is_empty() {
        let pg_tx = state.db.execute_debit_transfer(
            idem_key_str,
            payload.amount as i64,
            provider_id,
            user_id,
            provider_details.ledger_account_id,
            user_account.user_ledger_account_id,
            provider_id,
            payload.description,
        )
        .await
        .map_err(internal_error)?;

        let tb_accounts = state.tb_client.lookup_account(user_account.user_ledger_account_id.as_u128())
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Ledger error: {}", e)))?;

        let current_balance = tb_accounts.first()
            .map(|acc| (acc.credits_posted as i64) - (acc.debits_posted as i64))
            .unwrap_or(0);

        Ok((StatusCode::OK, Json(TransactionResponse {
            transaction_id: pg_tx.id,
            status: "SUCCESS".to_string(),
            current_balance: Some(current_balance),
        })))
    } else {
        let error_msg = format!("Ledger rejected the debit: {:?}", tb_result);
        tracing::warn!("{}", error_msg);
        Err((StatusCode::BAD_REQUEST, error_msg)) // Typically Error 51 (Exceeds Credits)
    }
}

/// Retrieve paginated transactions associated with a specific provider.
#[utoipa::path(
    get,
    path = "/api/v1/transactions/providers/{provider_id}",
    operation_id = "get_provider_transactions",
    tag = "Transactions",
    params(
        ("provider_id" = Uuid, Path, description = "Provider ID"),
        PaginationQuery
    ),
    responses(
        (status = 200, description = "List of provider transactions", body = PaginatedTransactionsResponse)
    )
)]
#[tracing::instrument(skip(state))]
pub async fn get_provider_transactions(
    State(state): State<Arc<AppState>>,
    Path(provider_id): Path<Uuid>,
    Query(pagination): Query<PaginationQuery>,
) -> Result<Json<PaginatedTransactionsResponse>, (StatusCode, String)> {
    let limit = pagination.limit.max(1).min(100);
    let page = pagination.page.max(1);
    let offset = (page - 1) * limit;

    let (db_txs, total_count) = state.db
        .get_provider_transactions(provider_id, limit, offset)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let total_pages = (total_count as f64 / limit as f64).ceil() as i64;

    let data = db_txs.into_iter().map(|tx| TransactionItem {
        id: tx.id,
        idempotency_key: tx.idempotency_key,
        transaction_type: tx.transaction_type, 
        amount: tx.amount,
        dr_account_id: tx.dr_account_id,
        cr_account_id: tx.cr_account_id,
        user_id: tx.user_id,
        provider_id: tx.provider_id,
        status: tx.status, 
        created_at: tx.created_at,
    }).collect();

    Ok(Json(PaginatedTransactionsResponse {
        data,
        total_count,
        page,
        total_pages,
    }))
}

/// Retrieve paginated transactions for a specific user under a specific provider.
#[utoipa::path(
    get,
    path = "/api/v1/transactions/providers/{provider_id}/users/{user_id}",
    operation_id = "get_user_transactions",
    tag = "Transactions",
    params(
        ("provider_id" = Uuid, Path, description = "Provider ID"),
        ("user_id" = Uuid, Path, description = "User ID"),
        PaginationQuery
    ),
    responses(
        (status = 200, description = "List of user transactions", body = PaginatedTransactionsResponse)
    )
)]
#[tracing::instrument(skip(state))]
pub async fn get_user_transactions(
    State(state): State<Arc<AppState>>,
    Path((provider_id, user_id)): Path<(Uuid, Uuid)>,
    Query(pagination): Query<PaginationQuery>,
) -> Result<Json<PaginatedTransactionsResponse>, (StatusCode, String)> {
    let limit = pagination.limit.max(1).min(100);
    let page = pagination.page.max(1);
    let offset = (page - 1) * limit;

    let (db_txs, total_count) = state.db
        .get_user_transactions(provider_id, user_id, limit, offset)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let total_pages = (total_count as f64 / limit as f64).ceil() as i64;

    let data = db_txs.into_iter().map(|tx| TransactionItem {
        id: tx.id,
        idempotency_key: tx.idempotency_key,
        transaction_type: tx.transaction_type, 
        amount: tx.amount,
        dr_account_id: tx.dr_account_id,
        cr_account_id: tx.cr_account_id,
        user_id: tx.user_id,
        provider_id: tx.provider_id,
        status: tx.status, 
        created_at: tx.created_at,
    }).collect();

    Ok(Json(PaginatedTransactionsResponse {
        data,
        total_count,
        page,
        total_pages,
    }))
}
