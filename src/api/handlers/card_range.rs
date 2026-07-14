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
        dto::card_range::{
            AttachCardRangeProviderRequest, CardRangeListResponse, CardRangeProviderListResponse,
            CardRangeProviderResponse, CardRangeResponse, CreateCardRangeRequest,
            UpdateCardRangeRequest,
        },
        error::{ApiError, ApiErrorResponse, ApiResult},
        idempotency::{self, IdempotencyStart},
        result_codes::WurzburgResultCode,
    },
    services::card_range_service::{CardRangeService, CardRangeServiceError},
    state::AppState,
};

#[utoipa::path(
    post,
    path = "/api/v1/card-ranges",
    request_body = CreateCardRangeRequest,
    params(
        ("Idempotency-Key" = String, Header, description = "Unique key used to make this mutating request safely retryable")
    ),
    responses(
        (status = 201, description = "Card range created", body = CardRangeResponse),
        (status = 400, description = "Validation error", body = ApiErrorResponse)
    ),
    tag = "Card Ranges"
)]
pub async fn create(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(payload): Json<CreateCardRangeRequest>,
) -> ApiResult<impl IntoResponse> {
    let actor = actor_subject(&headers);
    let request_snapshot = serde_json::to_value(&payload).map_err(ApiError::serialization)?;
    let idem = idempotency::begin(
        state.db.clone(),
        &headers,
        "card_ranges.create",
        &request_snapshot,
        &actor,
    )
    .await?;
    let IdempotencyStart::Execute { key, .. } = idem else {
        return Ok((StatusCode::OK, Json(idem_replay(idem))));
    };

    let service = CardRangeService::new(state.db.clone());

    let card_range = service
        .create_card_range(
            payload.start_card_number,
            payload.end_card_number,
            payload.funding_mode.into(),
            payload.metadata.unwrap_or_else(|| serde_json::json!({})),
            actor,
        )
        .await
        .map_err(card_range_error)?;
    let resource_id = card_range.id;
    let response = serde_json::to_value(CardRangeResponse::from(card_range))
        .map_err(ApiError::serialization)?;
    idempotency::complete(
        state.db.clone(),
        "card_ranges.create",
        &key,
        "card_range",
        resource_id,
        response.clone(),
    )
    .await?;

    Ok((StatusCode::CREATED, Json(response)))
}

#[utoipa::path(
    get,
    path = "/api/v1/card-ranges/{card_range_id}",
    params(("card_range_id" = Uuid, Path, description = "Card range ID")),
    responses(
        (status = 200, description = "Card range found", body = CardRangeResponse),
        (status = 404, description = "Card range not found", body = ApiErrorResponse)
    ),
    tag = "Card Ranges"
)]
pub async fn get(
    State(state): State<Arc<AppState>>,
    Path(card_range_id): Path<Uuid>,
) -> ApiResult<impl IntoResponse> {
    let service = CardRangeService::new(state.db.clone());
    let card_range = service
        .get_card_range(card_range_id)
        .await
        .map_err(card_range_error)?
        .ok_or_else(|| ApiError::new(WurzburgResultCode::CardRangeNotFound))?;

    Ok((StatusCode::OK, Json(CardRangeResponse::from(card_range))))
}

#[utoipa::path(
    get,
    path = "/api/v1/card-ranges",
    responses((status = 200, description = "Card ranges", body = CardRangeListResponse)),
    tag = "Card Ranges"
)]
pub async fn list(State(state): State<Arc<AppState>>) -> ApiResult<impl IntoResponse> {
    let service = CardRangeService::new(state.db.clone());
    let ranges = service.list_card_ranges().await.map_err(card_range_error)?;
    let data = ranges.into_iter().map(CardRangeResponse::from).collect();

    Ok((StatusCode::OK, Json(CardRangeListResponse { data })))
}

#[utoipa::path(
    patch,
    path = "/api/v1/card-ranges/{card_range_id}",
    request_body = UpdateCardRangeRequest,
    params(
        ("card_range_id" = Uuid, Path, description = "Card range ID"),
        ("Idempotency-Key" = String, Header, description = "Unique key used to make this mutating request safely retryable")
    ),
    responses((status = 200, description = "Card range updated", body = CardRangeResponse)),
    tag = "Card Ranges"
)]
pub async fn update(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(card_range_id): Path<Uuid>,
    Json(payload): Json<UpdateCardRangeRequest>,
) -> ApiResult<impl IntoResponse> {
    let actor = actor_subject(&headers);
    let request_snapshot = serde_json::json!({
        "card_range_id": card_range_id,
        "metadata": payload.metadata
    });
    let idem = idempotency::begin(
        state.db.clone(),
        &headers,
        "card_ranges.update_metadata",
        &request_snapshot,
        &actor,
    )
    .await?;
    let IdempotencyStart::Execute { key, .. } = idem else {
        return Ok((StatusCode::OK, Json(idem_replay(idem))));
    };

    let service = CardRangeService::new(state.db.clone());
    let card_range = service
        .update_card_range_metadata(card_range_id, payload.metadata, actor)
        .await
        .map_err(card_range_error)?;
    let response = serde_json::to_value(CardRangeResponse::from(card_range))
        .map_err(ApiError::serialization)?;
    idempotency::complete(
        state.db.clone(),
        "card_ranges.update_metadata",
        &key,
        "card_range",
        card_range_id,
        response.clone(),
    )
    .await?;

    Ok((StatusCode::OK, Json(response)))
}

#[utoipa::path(
    post,
    path = "/api/v1/card-ranges/{card_range_id}/activate",
    params(
        ("card_range_id" = Uuid, Path, description = "Card range ID"),
        ("Idempotency-Key" = String, Header, description = "Unique key used to make this mutating request safely retryable")
    ),
    responses((status = 200, description = "Card range activated", body = CardRangeResponse)),
    tag = "Card Ranges"
)]
pub async fn activate(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(card_range_id): Path<Uuid>,
) -> ApiResult<impl IntoResponse> {
    let actor = actor_subject(&headers);
    let request_snapshot = serde_json::json!({ "card_range_id": card_range_id });
    let idem = idempotency::begin(
        state.db.clone(),
        &headers,
        "card_ranges.activate",
        &request_snapshot,
        &actor,
    )
    .await?;
    let IdempotencyStart::Execute { key, .. } = idem else {
        return Ok((StatusCode::OK, Json(idem_replay(idem))));
    };

    let service = CardRangeService::new(state.db.clone());
    let card_range = service
        .activate_card_range(card_range_id, actor)
        .await
        .map_err(card_range_error)?;
    let response = serde_json::to_value(CardRangeResponse::from(card_range))
        .map_err(ApiError::serialization)?;
    idempotency::complete(
        state.db.clone(),
        "card_ranges.activate",
        &key,
        "card_range",
        card_range_id,
        response.clone(),
    )
    .await?;

    Ok((StatusCode::OK, Json(response)))
}

#[utoipa::path(
    post,
    path = "/api/v1/card-ranges/{card_range_id}/suspend",
    params(
        ("card_range_id" = Uuid, Path, description = "Card range ID"),
        ("Idempotency-Key" = String, Header, description = "Unique key used to make this mutating request safely retryable")
    ),
    responses((status = 200, description = "Card range suspended", body = CardRangeResponse)),
    tag = "Card Ranges"
)]
pub async fn suspend(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(card_range_id): Path<Uuid>,
) -> ApiResult<impl IntoResponse> {
    let actor = actor_subject(&headers);
    let request_snapshot = serde_json::json!({ "card_range_id": card_range_id });
    let idem = idempotency::begin(
        state.db.clone(),
        &headers,
        "card_ranges.suspend",
        &request_snapshot,
        &actor,
    )
    .await?;
    let IdempotencyStart::Execute { key, .. } = idem else {
        return Ok((StatusCode::OK, Json(idem_replay(idem))));
    };

    let service = CardRangeService::new(state.db.clone());
    let card_range = service
        .suspend_card_range(card_range_id, actor)
        .await
        .map_err(card_range_error)?;
    let response = serde_json::to_value(CardRangeResponse::from(card_range))
        .map_err(ApiError::serialization)?;
    idempotency::complete(
        state.db.clone(),
        "card_ranges.suspend",
        &key,
        "card_range",
        card_range_id,
        response.clone(),
    )
    .await?;

    Ok((StatusCode::OK, Json(response)))
}

#[utoipa::path(
    post,
    path = "/api/v1/card-ranges/{card_range_id}/providers/{provider_id}",
    request_body = AttachCardRangeProviderRequest,
    params(
        ("card_range_id" = Uuid, Path, description = "Card range ID"),
        ("provider_id" = Uuid, Path, description = "Provider ID"),
        ("Idempotency-Key" = String, Header, description = "Unique key used to make this mutating request safely retryable")
    ),
    responses((status = 200, description = "Provider attached", body = CardRangeProviderResponse)),
    tag = "Card Ranges"
)]
pub async fn attach_provider(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((card_range_id, provider_id)): Path<(Uuid, Uuid)>,
    Json(payload): Json<AttachCardRangeProviderRequest>,
) -> ApiResult<impl IntoResponse> {
    let actor = actor_subject(&headers);
    let metadata = payload.metadata.unwrap_or_else(|| serde_json::json!({}));
    let request_snapshot = serde_json::json!({
        "card_range_id": card_range_id,
        "provider_id": provider_id,
        "metadata": metadata
    });
    let idem = idempotency::begin(
        state.db.clone(),
        &headers,
        "card_ranges.attach_provider",
        &request_snapshot,
        &actor,
    )
    .await?;
    let IdempotencyStart::Execute { key, .. } = idem else {
        return Ok((StatusCode::OK, Json(idem_replay(idem))));
    };

    let service = CardRangeService::new(state.db.clone());
    let provider = service
        .attach_provider(card_range_id, provider_id, metadata, actor)
        .await
        .map_err(card_range_error)?;
    let response = serde_json::to_value(CardRangeProviderResponse::from(provider))
        .map_err(ApiError::serialization)?;
    idempotency::complete(
        state.db.clone(),
        "card_ranges.attach_provider",
        &key,
        "card_range_provider",
        card_range_id,
        response.clone(),
    )
    .await?;

    Ok((StatusCode::OK, Json(response)))
}

#[utoipa::path(
    delete,
    path = "/api/v1/card-ranges/{card_range_id}/providers/{provider_id}",
    params(
        ("card_range_id" = Uuid, Path, description = "Card range ID"),
        ("provider_id" = Uuid, Path, description = "Provider ID"),
        ("Idempotency-Key" = String, Header, description = "Unique key used to make this mutating request safely retryable")
    ),
    responses((status = 200, description = "Provider suspended", body = CardRangeProviderResponse)),
    tag = "Card Ranges"
)]
pub async fn suspend_provider(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((card_range_id, provider_id)): Path<(Uuid, Uuid)>,
) -> ApiResult<impl IntoResponse> {
    let actor = actor_subject(&headers);
    let request_snapshot = serde_json::json!({
        "card_range_id": card_range_id,
        "provider_id": provider_id
    });
    let idem = idempotency::begin(
        state.db.clone(),
        &headers,
        "card_ranges.suspend_provider",
        &request_snapshot,
        &actor,
    )
    .await?;
    let IdempotencyStart::Execute { key, .. } = idem else {
        return Ok((StatusCode::OK, Json(idem_replay(idem))));
    };

    let service = CardRangeService::new(state.db.clone());
    let provider = service
        .suspend_provider(card_range_id, provider_id, actor)
        .await
        .map_err(card_range_error)?;
    let response = serde_json::to_value(CardRangeProviderResponse::from(provider))
        .map_err(ApiError::serialization)?;
    idempotency::complete(
        state.db.clone(),
        "card_ranges.suspend_provider",
        &key,
        "card_range_provider",
        card_range_id,
        response.clone(),
    )
    .await?;

    Ok((StatusCode::OK, Json(response)))
}

#[utoipa::path(
    get,
    path = "/api/v1/card-ranges/{card_range_id}/providers",
    params(("card_range_id" = Uuid, Path, description = "Card range ID")),
    responses((status = 200, description = "Range providers", body = CardRangeProviderListResponse)),
    tag = "Card Ranges"
)]
pub async fn list_providers(
    State(state): State<Arc<AppState>>,
    Path(card_range_id): Path<Uuid>,
) -> ApiResult<impl IntoResponse> {
    let service = CardRangeService::new(state.db.clone());
    let providers = service
        .list_range_providers(card_range_id)
        .await
        .map_err(card_range_error)?;
    let data = providers
        .into_iter()
        .map(CardRangeProviderResponse::from)
        .collect();

    Ok((StatusCode::OK, Json(CardRangeProviderListResponse { data })))
}

#[utoipa::path(
    get,
    path = "/api/v1/providers/{provider_id}/card-ranges",
    params(("provider_id" = Uuid, Path, description = "Provider ID")),
    responses((status = 200, description = "Provider card ranges", body = CardRangeListResponse)),
    tag = "Card Ranges"
)]
pub async fn list_provider_ranges(
    State(state): State<Arc<AppState>>,
    Path(provider_id): Path<Uuid>,
) -> ApiResult<impl IntoResponse> {
    let service = CardRangeService::new(state.db.clone());
    let ranges = service
        .list_provider_ranges(provider_id)
        .await
        .map_err(card_range_error)?;
    let data = ranges.into_iter().map(CardRangeResponse::from).collect();

    Ok((StatusCode::OK, Json(CardRangeListResponse { data })))
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

fn card_range_error(error: CardRangeServiceError) -> ApiError {
    match error {
        CardRangeServiceError::CardRangeNotFound => {
            ApiError::new(WurzburgResultCode::CardRangeNotFound)
        }
        CardRangeServiceError::CardRangeProviderNotFound => {
            ApiError::new(WurzburgResultCode::CardRangeProviderNotFound)
        }
        CardRangeServiceError::CardRangeOverlap => {
            ApiError::new(WurzburgResultCode::CardRangeOverlap)
        }
        CardRangeServiceError::InvalidCardNumber(message)
        | CardRangeServiceError::InvalidRangeOrder(message) => {
            ApiError::with_message(WurzburgResultCode::InvalidCardRange, message)
        }
        CardRangeServiceError::InvalidMetadata => {
            ApiError::new(WurzburgResultCode::InvalidMetadata)
        }
        CardRangeServiceError::SingleProviderActivationRequiresExactlyOneProvider
        | CardRangeServiceError::MultiProviderActivationRequiresAtLeastOneProvider => {
            ApiError::with_message(
                WurzburgResultCode::CardRangeActivationRuleFailed,
                error.to_string(),
            )
        }
        CardRangeServiceError::SingleProviderAllowsOnlyOneActiveProvider => ApiError::with_message(
            WurzburgResultCode::CardRangeProviderRuleFailed,
            error.to_string(),
        ),
        CardRangeServiceError::Repository(error) => ApiError::system(error),
    }
}
