// src/api/handlers/priority.rs
use axum::{
    Json, extract::{Path, State}, http::{HeaderMap, StatusCode}, response::IntoResponse
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::ToSchema;
use uuid::Uuid;
use chrono::{DateTime, Utc};

use crate::state::AppState;
use crate::db::models::{PriorityItemData, PriorityUsageType, UserPriorityDetails};

// --- DTOs (Data Transfer Objects) ---

#[derive(Debug, Deserialize, ToSchema)]
pub struct PriorityItemRequest {
    #[schema(example = "a1b2c3d4-e5f6-7890-1234-567890abcdef")]
    pub provider_id: Uuid,
    #[schema(example = 100000)]
    pub max_amount: i64,
    #[schema(example = "SingleUse")]
    pub usage_type: PriorityUsageType,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreatePriorityRequest {
    /// A list of providers and amounts, ordered by desired priority.
    pub priorities: Vec<PriorityItemRequest>,
    /// Optional expiration timestamp for the entire configuration.
    #[schema(example = "2026-06-01T12:00:00Z")]
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub enum ActionStatus {
    CREATED,
    IDEMPOTENT,
    NORMAL
}
#[derive(Debug, Serialize, ToSchema)]
pub struct PriorityConfigResponse {
    pub data: UserPriorityDetails,
    pub status: ActionStatus,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PriorityConfigListResponse {
    pub data: Vec<UserPriorityDetails>,
}

// --- Helper for error handling ---
fn internal_error<E: std::fmt::Display>(err: E) -> (StatusCode, String) {
    tracing::error!("Internal error: {}", err);
    (StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error".to_string())
}

// --- API Handlers ---

/// Create a new priority configuration for a user with ledger balance validation.
#[utoipa::path(
    post,
    path = "/api/v1/users/{user_id}/priorities",
    operation_id = "create_user_priority",
    request_body = CreatePriorityRequest,
    params(
        ("user_id" = Uuid, Path, description = "User's unique identifier"),
        ("Idempotency-Key" = String, Header, description = "Unique key for safe retries")
    ),
    responses(
        (status = 201, description = "Priority configuration created successfully", body = PriorityConfigResponse),
        (status = 200, description = "Returned existing configuration for the provided idempotency key", body = PriorityConfigResponse),
        (status = 400, description = "Bad request: Missing Idempotency-Key or empty priority list"),
        (status = 404, description = "User not found"),
        (status = 422, description = "Unprocessable Entity: Insufficient balance or unlinked provider"),
        (status = 500, description = "Internal server error")
    ),
    tag = "Priorities"
)]
#[tracing::instrument(skip(state, payload))]
pub async fn create_user_priority(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<Uuid>,
    headers: HeaderMap,
    Json(payload): Json<CreatePriorityRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    
    // 0. Structural Validations
    if payload.priorities.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "Priority list cannot be empty".to_string()));
    }

    let idempotency_key = headers
        .get("Idempotency-Key")
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string());

    let idempotency_key = match idempotency_key {
        Some(key) if !key.trim().is_empty() => key,
        _ => return Err((StatusCode::BAD_REQUEST, "Missing or invalid Idempotency-Key header".to_string())),
    };

    // 1. Check Idempotency: Return existing configuration if already processed
    if let Ok(Some(existing_details)) = state.db.get_priority_config_by_idempotency_key(&idempotency_key).await {
        tracing::info!("Returning existing priority config for idempotency key: {}", idempotency_key);
        return Ok((StatusCode::OK, Json(PriorityConfigResponse { status: ActionStatus::IDEMPOTENT, data: existing_details })));
    }

    // 2. Resolve Ledger Accounts for requested providers
    let mut resolved_accounts = Vec::new();
    let mut validation_errors = Vec::new();

    for item in &payload.priorities {
        match state.db.get_user_provider_details(user_id, item.provider_id).await {
            Ok(Some(details)) => resolved_accounts.push((
                item, 
                details.user_ledger_account_id, 
                details.provider_ledger_account_id
            )),
            _ => validation_errors.push(format!("Provider {} not found or linked", item.provider_id)),
        }
    }

    // Return 422 Unprocessable Entity for business/domain validation failures
    if !validation_errors.is_empty() {
        return Err((StatusCode::UNPROCESSABLE_ENTITY, validation_errors.join(", ")));
    }

    // 3. Fetch Balances from TigerBeetle (Ledger)
    let tb_account_ids: Vec<u128> = resolved_accounts.iter().map(|(_, u_id, _)| u_id.as_u128()).collect();
    let tb_accounts = state.tb_client.lookup_accounts(tb_account_ids).await
        .map_err(|e| internal_error(format!("Ledger lookup failed: {}", e)))?;

    // Convert vector to HashMap for O(1) lookups
    let accounts_map: std::collections::HashMap<u128, _> = tb_accounts
        .into_iter()
        .map(|acc| (acc.id, acc))
        .collect();

    // 4. Validate Sufficient Funds
    for (item, user_ledger_id, _) in &resolved_accounts {
        match accounts_map.get(&user_ledger_id.as_u128()) {
            Some(acc) => {
                let available_balance = acc.credits_posted.saturating_sub(acc.debits_posted) as i64;
                if available_balance < item.max_amount {
                    validation_errors.push(format!(
                        "Insufficient balance for provider {}. Requested: {}, Available: {}", 
                        item.provider_id, item.max_amount, available_balance
                    ));
                }
            },
            None => validation_errors.push(format!("Ledger account missing for provider {}", item.provider_id)),
        }
    }

    // Return 422 Unprocessable Entity for insufficient funds
    if !validation_errors.is_empty() {
        return Err((StatusCode::UNPROCESSABLE_ENTITY, validation_errors.join(" | ")));
    }

    let mut redis_conn = state.redis.get().await.map_err(internal_error)?;
    let redis_key = format!("priority:active:{{user:{}}}", user_id);

    // 5. Invalidate existing Redis state *before* DB mutation to prevent stale reads if DB fails
    let _: () = deadpool_redis::redis::cmd("DEL")
        .arg(&redis_key)
        .query_async(&mut redis_conn)
        .await
        .map_err(|e| internal_error(format!("Failed to delete Redis key: {}", e)))?;

    // 6. Cancel Existing Active Priority
    // Ensure the uniqueness constraint (only one ACTIVE config per user) is respected
    let has_active = state.db.get_active_priority_config(user_id).await
        .map_err(internal_error)?.is_some();

    if has_active {
        state.db.cancel_active_priority_config(user_id, user_id).await
            .map_err(internal_error)?;
    }
    
    // Transform DTOs for repository insertion
    let items_data: Vec<PriorityItemData> = resolved_accounts.into_iter().map(|(item, user_ledger, provider_ledger)| {
        PriorityItemData {
            provider_id: item.provider_id,
            usage_type: item.usage_type.clone(),
            max_amount: item.max_amount,
            user_ledger_account_id: user_ledger,
            provider_ledger_account_id: provider_ledger,
        }
    }).collect();

    // 7. Persist New Priority in Database (Source of Truth)
    let new_priority = state.db.create_priority_config(
        idempotency_key, 
        user_id, 
        items_data, 
        payload.expires_at, 
        user_id
    ).await.map_err(|e| {
        if e.to_string().contains("duplicate key value") {
            (StatusCode::CONFLICT, "Concurrent request detected".to_string())
        } else {
            internal_error(e)
        }
    })?;

    // 8. Sync Operational State to Redis (Set the newly created priority)
    let redis_value = serde_json::to_string(&new_priority)
        .map_err(|e| internal_error(format!("Failed to serialize priority for Redis: {}", e)))?;
    
    // Calculate TTL if expires_at is provided.
    let mut set_cmd = deadpool_redis::redis::cmd("SET");
    set_cmd.arg(&redis_key).arg(&redis_value);    
    if let Some(expires_at_ts) = payload.expires_at {
        let now = chrono::Utc::now();
        // Calculate the duration between now and the expiration time.
        // Convert duration to seconds. Use 0 if the time has already passed.
        let ttl_seconds = expires_at_ts.signed_duration_since(now).num_seconds();
        // Only set TTL if it's a positive value.
        if ttl_seconds > 0 {
            tracing::info!(
                "Setting Redis key '{}' with TTL of {} seconds.",
                redis_key,
                ttl_seconds
            );
            set_cmd.arg("EX").arg(ttl_seconds as u64);
        } else {
            tracing::warn!(
                "Expiration time for key '{}' is in the past. Key will not be set with a TTL.",
                redis_key
            );
        }
    } else {
        tracing::info!("No expiration time provided. Redis key '{}' will be persistent.", redis_key);
    }

    // Execute the command (either SET key value, or SET key value EX seconds)
    let _: () = set_cmd
        .query_async(&mut redis_conn)
        .await
        .map_err(|e| internal_error(format!("Redis SET operation failed: {}", e)))?;
    
    // 9. Return Successful Response
    Ok((StatusCode::CREATED, Json(PriorityConfigResponse {status: ActionStatus::CREATED, data: new_priority })))
}

/// Retrieve the currently active priority configuration for a specific user.
#[utoipa::path(
    get,
    path = "/api/v1/users/{user_id}/priorities/active",
    operation_id = "get_active_user_priority",
    params(
        ("user_id" = Uuid, Path, description = "User's unique identifier")
    ),
    responses(
        (status = 200, description = "Active priority configuration found", body = PriorityConfigResponse),
        (status = 404, description = "No active priority configuration found for this user"),
        (status = 500, description = "Internal server error")
    ),
    tag = "Priorities"
)]
#[tracing::instrument(skip(state))]
pub async fn get_active_user_priority(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<Uuid>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let result = state.db.get_active_priority_config(user_id).await.map_err(internal_error)?;

    match result {
        Some(details) => Ok((StatusCode::OK, Json(PriorityConfigResponse {status: ActionStatus::NORMAL, data: details }))),
        None => Err((StatusCode::NOT_FOUND, "No active priority configuration found".to_string())),
    }
}

/// Cancel the user's active priority configuration and invalidate the cache.
#[utoipa::path(
    delete,
    path = "/api/v1/users/{user_id}/priorities/active",
    operation_id = "cancel_active_user_priority",
    params(
        ("user_id" = Uuid, Path, description = "User's unique identifier")
    ),
    responses(
        (status = 200, description = "Active priority configuration cancelled successfully", body = PriorityConfigResponse),
        (status = 404, description = "No active priority configuration found to cancel"),
        (status = 500, description = "Internal server error")
    ),
    tag = "Priorities"
)]
#[tracing::instrument(skip(state))]
pub async fn cancel_active_user_priority(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<Uuid>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Placeholder actor_id for audit logs or permissions
    let actor_id = user_id;

    // 1. Cancel the priority configuration in the primary database.
    // This updates the user's priority status to cancelled/inactive and records the actor.
    let result = state
        .db
        .cancel_active_priority_config(user_id, actor_id)
        .await
        .map_err(internal_error)?;

    match result {
        Some(details) => {
            // 2. Invalidate the active priority cache in Redis.
            // If the database cancellation succeeds, we MUST purge the cached state
            // to prevent the system from applying stale priority rules to new operations.
            let redis_key = format!("priority:active:{{user:{}}}", user_id);

            // Acquire a connection from the Redis pool. 
            let mut redis_conn = state
                .redis
                .get()
                .await
                .map_err(|e| internal_error(e))?;

            // Execute the Redis DEL command.
            let _: () = redis::cmd("DEL")
                .arg(&redis_key)
                .query_async(&mut redis_conn)
                .await
                .map_err(|e| internal_error(e))?;

            Ok((
                StatusCode::OK,
                Json(PriorityConfigResponse {status: ActionStatus::NORMAL, data: details }),
            ))
        }
        None => Err((
            StatusCode::NOT_FOUND,
            "No active priority configuration found to cancel".to_string(),
        )),
    }
}

/// Retrieve the complete history of all priority configurations for a user.
#[utoipa::path(
    get,
    path = "/api/v1/users/{user_id}/priorities",
    operation_id = "get_all_user_priorities",
    params(
        ("user_id" = Uuid, Path, description = "User's unique identifier")
    ),
    responses(
        (status = 200, description = "List of all user priority configurations", body = PriorityConfigListResponse),
        (status = 500, description = "Internal server error")
    ),
    tag = "Priorities"
)]
#[tracing::instrument(skip(state))]
pub async fn get_all_user_priorities(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<Uuid>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let result = state.db.get_all_priority_configs(user_id).await.map_err(internal_error)?;
    
    Ok((StatusCode::OK, Json(PriorityConfigListResponse { data: result })))
}
