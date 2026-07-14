use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use uuid::Uuid;

use crate::{
    api::{
        dto::card_policy::{CardRangePolicyResponse, CreateCardRangePolicyRequest},
        error::{ApiError, ApiErrorResponse, ApiResult},
        idempotency::{self, IdempotencyStart},
        result_codes::WurzburgResultCode,
    },
    services::card_policy_service::{CardPolicyService, CardPolicyServiceError},
    state::AppState,
};

#[utoipa::path(
    post,
    path = "/api/v1/card-ranges/{card_range_id}/policy",
    request_body = CreateCardRangePolicyRequest,
    params(
        ("card_range_id" = Uuid, Path, description = "Card range ID"),
        ("Idempotency-Key" = String, Header, description = "Unique key used to make this mutating request safely retryable")
    ),
    responses(
        (status = 201, description = "Card range policy created", body = CardRangePolicyResponse),
        (status = 400, description = "Validation error", body = ApiErrorResponse)
    ),
    tag = "Card Policies"
)]
pub async fn create_range_policy(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(card_range_id): Path<Uuid>,
    Json(payload): Json<CreateCardRangePolicyRequest>,
) -> ApiResult<impl IntoResponse> {
    let actor = actor_subject(&headers);
    let request_snapshot = serde_json::json!({
        "card_range_id": card_range_id,
        "profile": payload.profile
    });
    let idem = idempotency::begin(
        state.db.clone(),
        &headers,
        "card_range_policies.create",
        &request_snapshot,
        &actor,
    )
    .await?;
    let IdempotencyStart::Execute { key, .. } = idem else {
        return Ok((StatusCode::OK, Json(idem_replay(idem))));
    };

    let service = CardPolicyService::new(state.db.clone());
    let details = service
        .create_or_replace_range_policy(card_range_id, payload.profile, actor)
        .await
        .map_err(card_policy_error)?;
    let resource_id = details.profile.id;
    let response = serde_json::to_value(CardRangePolicyResponse::from(details))
        .map_err(ApiError::serialization)?;
    idempotency::complete(
        state.db.clone(),
        "card_range_policies.create",
        &key,
        "card_policy_profile",
        resource_id,
        response.clone(),
    )
    .await?;

    Ok((StatusCode::CREATED, Json(response)))
}

#[utoipa::path(
    get,
    path = "/api/v1/card-ranges/{card_range_id}/policy",
    params(("card_range_id" = Uuid, Path, description = "Card range ID")),
    responses(
        (status = 200, description = "Active card range policy", body = CardRangePolicyResponse),
        (status = 404, description = "Active card range policy not found", body = ApiErrorResponse)
    ),
    tag = "Card Policies"
)]
pub async fn get_range_policy(
    State(state): State<Arc<AppState>>,
    Path(card_range_id): Path<Uuid>,
) -> ApiResult<impl IntoResponse> {
    let service = CardPolicyService::new(state.db.clone());
    let details = service
        .get_active_range_policy(card_range_id)
        .await
        .map_err(card_policy_error)?
        .ok_or_else(|| ApiError::new(WurzburgResultCode::CardRangePolicyNotFound))?;

    Ok((StatusCode::OK, Json(CardRangePolicyResponse::from(details))))
}

fn actor_subject(headers: &HeaderMap) -> String {
    headers
        .get("X-Actor-Subject")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("local-dev")
        .to_string()
}

fn idem_replay(start: IdempotencyStart) -> serde_json::Value {
    match start {
        IdempotencyStart::Replay(value) => value,
        IdempotencyStart::Execute { .. } => serde_json::json!({}),
    }
}

fn card_policy_error(error: CardPolicyServiceError) -> ApiError {
    match error {
        CardPolicyServiceError::CardRangeNotFound => {
            ApiError::new(WurzburgResultCode::CardRangeNotFound)
        }
        CardPolicyServiceError::InvalidPolicyProfile => {
            ApiError::new(WurzburgResultCode::InvalidCardPolicy)
        }
        CardPolicyServiceError::Repository(error) => ApiError::system(error),
    }
}
