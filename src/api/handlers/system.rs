use std::sync::Arc;

use axum::{Json, extract::State, http::StatusCode};
use serde::Serialize;
use utoipa::ToSchema;

use crate::state::AppState;

#[derive(Debug, Serialize, ToSchema)]
pub struct DbHealthResponse {
    pub status: String,
    pub driver: String,
    pub database_time_utc: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SystemErrorResponse {
    pub error: SystemErrorBody,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SystemErrorBody {
    pub code: &'static str,
    pub message: String,
}

#[utoipa::path(
    get,
    path = "/api/v1/system/db-health",
    responses(
        (status = 200, description = "Oracle database health check succeeded", body = DbHealthResponse),
        (status = 503, description = "Oracle database is unavailable", body = SystemErrorResponse),
        (status = 501, description = "Database health repository is not available", body = SystemErrorResponse)
    ),
    tag = "System"
)]
#[tracing::instrument(skip(state))]
pub async fn db_health(
    State(state): State<Arc<AppState>>,
) -> Result<Json<DbHealthResponse>, (StatusCode, Json<SystemErrorResponse>)> {
    match state.oracle_health.as_ref() {
        Some(oracle_health) => {
            let health = oracle_health.check().await.map_err(|error| {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(SystemErrorResponse {
                        error: SystemErrorBody {
                            code: "DATABASE_UNAVAILABLE",
                            message: error.to_string(),
                        },
                    }),
                )
            })?;

            Ok(Json(DbHealthResponse {
                status: "ok".to_string(),
                driver: "oracle".to_string(),
                database_time_utc: Some(health.database_time_utc),
            }))
        }
        None => Err((
            StatusCode::NOT_IMPLEMENTED,
            Json(SystemErrorResponse {
                error: SystemErrorBody {
                    code: "DATABASE_HEALTH_UNAVAILABLE",
                    message: "Oracle health repository is not attached to application state"
                        .to_string(),
                },
            }),
        )),
    }
}
