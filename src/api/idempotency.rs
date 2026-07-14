use std::sync::Arc;

use axum::http::HeaderMap;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    api::{
        error::{ApiError, ApiResult},
        result_codes::WurzburgResultCode,
    },
    db::error::DbError,
    db::traits::AppRepository,
    domain::idempotency::{IdempotencyStatus, NewIdempotencyRecord},
};

pub enum IdempotencyStart {
    Execute { key: String, request_hash: String },
    Replay(serde_json::Value),
}

pub async fn begin(
    repository: Arc<dyn AppRepository>,
    headers: &HeaderMap,
    operation_type: &str,
    request_snapshot: &serde_json::Value,
    actor_subject: &str,
) -> ApiResult<IdempotencyStart> {
    let key = headers
        .get("Idempotency-Key")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| ApiError::new(WurzburgResultCode::MissingIdempotencyKey))?;

    let request_hash = request_hash(request_snapshot);

    if let Some(existing) = repository
        .get_idempotency_record(operation_type, &key)
        .await
        .map_err(idempotency_internal_error)?
    {
        return existing_start(existing, &request_hash);
    }

    match repository
        .create_idempotency_record(NewIdempotencyRecord {
            id: Uuid::new_v4(),
            operation_type: operation_type.to_string(),
            idempotency_key: key.clone(),
            request_hash: request_hash.clone(),
            created_by_subject: actor_subject.to_string(),
        })
        .await
    {
        Ok(()) => {}
        Err(error) if is_db_conflict(&error) => {
            if let Some(existing) = repository
                .get_idempotency_record(operation_type, &key)
                .await
                .map_err(idempotency_internal_error)?
            {
                return existing_start(existing, &request_hash);
            }

            return Err(ApiError::new(WurzburgResultCode::IdempotencyInProgress));
        }
        Err(error) => return Err(idempotency_internal_error(error)),
    }

    Ok(IdempotencyStart::Execute { key, request_hash })
}

pub async fn complete(
    repository: Arc<dyn AppRepository>,
    operation_type: &str,
    key: &str,
    resource_type: &str,
    resource_id: Uuid,
    response_snapshot: serde_json::Value,
) -> ApiResult<()> {
    repository
        .complete_idempotency_record(
            operation_type,
            key,
            resource_type,
            resource_id,
            response_snapshot,
        )
        .await
        .map_err(idempotency_internal_error)
}

fn request_hash(value: &serde_json::Value) -> String {
    let bytes = serde_json::to_vec(value).unwrap_or_default();
    let digest = Sha256::digest(bytes);
    format!("{digest:x}")
}

fn existing_start(
    existing: crate::domain::idempotency::IdempotencyRecord,
    request_hash: &str,
) -> ApiResult<IdempotencyStart> {
    if existing.request_hash != request_hash {
        return Err(ApiError::new(WurzburgResultCode::IdempotencyKeyConflict));
    }

    if existing.status == IdempotencyStatus::Completed {
        if let Some(snapshot) = existing.response_snapshot {
            return Ok(IdempotencyStart::Replay(snapshot));
        }
    }

    Err(ApiError::new(WurzburgResultCode::IdempotencyInProgress))
}

fn is_db_conflict(error: &anyhow::Error) -> bool {
    matches!(error.downcast_ref::<DbError>(), Some(DbError::Conflict(_)))
}

fn idempotency_internal_error(error: anyhow::Error) -> ApiError {
    ApiError::with_message(WurzburgResultCode::IdempotencyError, error.to_string())
}
