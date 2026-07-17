use std::sync::Arc;

use axum::{Json, extract::State};
use serde::Serialize;
use utoipa::ToSchema;

use crate::{
    api::{error::ApiError, result_codes::WurzburgResultCode},
    state::AppState,
};

#[derive(Debug, Serialize, ToSchema)]
pub struct DbHealthResponse {
    pub status: String,
    pub driver: String,
    pub database_time_utc: Option<String>,
}

#[utoipa::path(
    get,
    path = "/api/v1/system/db-health",
    responses(
        (status = 200, description = "Oracle database health check succeeded", body = DbHealthResponse),
        (status = 503, description = "Oracle database is unavailable", body = crate::api::error::ApiErrorResponse),
        (status = 501, description = "Database health repository is not available", body = crate::api::error::ApiErrorResponse)
    ),
    tag = "System"
)]
#[tracing::instrument(skip(state))]
pub async fn db_health(
    State(state): State<Arc<AppState>>,
) -> Result<Json<DbHealthResponse>, ApiError> {
    match state.oracle_health.as_ref() {
        Some(oracle_health) => {
            let health = oracle_health.check().await.map_err(|error| {
                ApiError::with_message(WurzburgResultCode::DatabaseUnavailable, error.to_string())
            })?;

            Ok(Json(DbHealthResponse {
                status: "ok".to_string(),
                driver: "oracle".to_string(),
                database_time_utc: Some(health.database_time_utc),
            }))
        }
        None => Err(ApiError::new(WurzburgResultCode::DatabaseHealthUnavailable)),
    }
}
