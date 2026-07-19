use std::sync::Arc;

use axum::{
    Json,
    body::Bytes,
    extract::{OriginalUri, Path, Query, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{
    api::{
        auth::extract_trusted_actor,
        command::MutationCommandContext,
        error::ApiError,
        idempotency::{canonical_request_hash, require_idempotency_key},
        request_context::extract_trusted_request_context,
        result_codes::WurzburgResultCode,
    },
    db::oracle::CardRangeMutation,
    domain::card_range::{
        CardNumberRange, CardRangeControlChange, CardRangeListCursor, CardRangeListPage,
        CardRangeListQuery, CardRangeStatus, CmsOperationMode, DraftCardRangeUpdate, FundingMode,
        LimitCalendar, LimitWindowMode, NewCardRange, WeekStartDay, WithdrawalLimitAuthority,
    },
    services::card_range::{
        CardRangeMutationServiceOutcome, CardRangeService, CreateCardRangeOutcome,
    },
    state::AppState,
    telemetry::http::{RESULT_CODE_HEADER, RESULT_SYMBOL_HEADER},
};

const CREATE_CARD_RANGE_OPERATION: &str = "card_ranges.create";
const UPDATE_CARD_RANGE_OPERATION: &str = "card_ranges.update_draft";
const ACTIVATE_CARD_RANGE_OPERATION: &str = "card_ranges.activate";
const SUSPEND_CARD_RANGE_OPERATION: &str = "card_ranges.suspend";
const UPDATE_CARD_RANGE_CONTROLS_OPERATION: &str = "card_ranges.update_controls";

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateCardRangeRequest {
    pub start_card_number: String,
    pub end_card_number: String,
    pub funding_mode: CardRangeFundingModeDto,
    pub withdrawal_limit_authority: CardRangeWithdrawalLimitAuthorityDto,
    pub limit_calendar: Option<LimitCalendarDto>,
    pub issuance_enabled: bool,
    pub cms_operation_mode: CardRangeCmsOperationModeDto,
    #[serde(default = "empty_metadata")]
    pub metadata: serde_json::Value,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CardRangeResponse {
    pub card_range_id: Uuid,
    pub start_card_number: String,
    pub end_card_number: String,
    pub funding_mode: CardRangeFundingModeDto,
    pub withdrawal_limit_authority: CardRangeWithdrawalLimitAuthorityDto,
    pub limit_calendar: Option<LimitCalendarDto>,
    pub status: CardRangeStatusDto,
    pub issuance_enabled: bool,
    pub cms_operation_mode: CardRangeCmsOperationModeDto,
    pub operational_version: i64,
    pub materialized_operational_version: i64,
    pub range_control_operation_id: Option<Uuid>,
    pub metadata: serde_json::Value,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdateDraftCardRangeRequest {
    pub start_card_number: String,
    pub end_card_number: String,
    pub funding_mode: CardRangeFundingModeDto,
    pub withdrawal_limit_authority: CardRangeWithdrawalLimitAuthorityDto,
    pub limit_calendar: Option<LimitCalendarDto>,
    pub issuance_enabled: bool,
    pub cms_operation_mode: CardRangeCmsOperationModeDto,
    pub metadata: serde_json::Value,
    pub reason: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct CardRangeTransitionRequest {
    pub reason: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdateCardRangeControlsRequest {
    pub issuance_enabled: bool,
    pub cms_operation_mode: CardRangeCmsOperationModeDto,
    pub reason: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CardRangeMutationResponse {
    pub operation_id: Option<Uuid>,
    pub card_range: CardRangeResponse,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CardRangeProviderEligibilityResponse {
    pub provider_id: Uuid,
    pub status: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListCardRangeProvidersResponse {
    pub items: Vec<CardRangeProviderEligibilityResponse>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct IntegrationOperationResponse {
    pub operation_id: Uuid,
    pub event_id: Uuid,
    pub event_type: String,
    pub aggregate_type: String,
    pub aggregate_id: Uuid,
    pub status: String,
    pub attempt_count: i64,
    pub created_at: DateTime<Utc>,
    pub published_at: Option<DateTime<Utc>>,
    pub materialized_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ListCardRangesQuery {
    pub status: Option<String>,
    pub funding_mode: Option<String>,
    pub withdrawal_limit_authority: Option<String>,
    pub cursor: Option<String>,
    pub limit: Option<u16>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListCardRangesResponse {
    pub items: Vec<CardRangeResponse>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CardRangeFundingModeDto {
    SingleProvider,
    MultiProvider,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CardRangeWithdrawalLimitAuthorityDto {
    Platform,
    Cms,
}

#[derive(Debug, Clone, Copy, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CardRangeStatusDto {
    Draft,
    Active,
    Suspended,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CardRangeCmsOperationModeDto {
    Full,
    BalanceOnly,
    Blocked,
}

#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
pub struct LimitCalendarDto {
    pub timezone: String,
    pub week_starts_on: WeekStartDayDto,
    pub window_mode: LimitWindowModeDto,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WeekStartDayDto {
    Saturday,
    Sunday,
    Monday,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LimitWindowModeDto {
    Calendar,
}

#[utoipa::path(
    get,
    path = "/api/v1/card-ranges/{card_range_id}",
    tag = "Card Ranges",
    params(
        ("card_range_id" = Uuid, Path, description = "Card range identifier.", example = json!("018f2f68-3f2f-7f57-9a0a-16ef9cc00a01")),
        ("X-Correlation-Id" = String, Header, description = "WSO2 canonical business correlation ID.", example = json!("ce5c1b18-9050-49b2-9fd2-a2f208a56117")),
        ("X-Request-Id" = Uuid, Header, description = "WSO2 unique HTTP attempt ID.", example = json!("ce5c1b18-9050-49b2-9fd2-a2f208a56118")),
        ("X-WSO2-Client-IP" = String, Header, description = "Canonical original client IP.", example = json!("192.168.0.1")),
        ("X-WSO2-Gateway-Id" = String, Header, description = "Trusted WSO2 gateway instance ID.", example = json!("wso2-dev-gateway-1")),
        ("X-JWT-Assertion" = Option<String>, Header, description = "Alternative WSO2 backend assertion transport. Send the raw JWT without the Bearer prefix only when Wurzburg is configured for x_jwt_assertion.")
    ),
    security(("wso2_backend_bearer" = [])),
    responses(
        (status = 200, description = "Card range found", body = CardRangeResponse),
        (status = 401, description = "Trusted actor assertion is missing or invalid", body = crate::api::error::ApiErrorResponse),
        (status = 403, description = "Caller lacks platform.card_ranges:read", body = crate::api::error::ApiErrorResponse),
        (status = 404, description = "Card range was not found", body = crate::api::error::ApiErrorResponse),
        (status = 500, description = "Internal persistence failure", body = crate::api::error::ApiErrorResponse)
    )
)]
#[tracing::instrument(skip(state, headers), fields(card_range_id = %card_range_id))]
pub async fn get_card_range(
    State(state): State<Arc<AppState>>,
    Path(card_range_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let request_context =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request_context, &state.config.wso2)?;
    let service = CardRangeService::new(state.db.clone());
    let card_range = service.get_card_range(&actor, card_range_id).await?;

    Ok(success_response(
        StatusCode::OK,
        CardRangeResponse::from(card_range),
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/card-ranges",
    tag = "Card Ranges",
    params(
        ("status" = Option<String>, Query, description = "Optional card range status filter: DRAFT, ACTIVE, or SUSPENDED.", example = json!("DRAFT")),
        ("funding_mode" = Option<String>, Query, description = "Optional funding-mode filter: SINGLE_PROVIDER or MULTI_PROVIDER.", example = json!("SINGLE_PROVIDER")),
        ("withdrawal_limit_authority" = Option<String>, Query, description = "Optional withdrawal authority filter: PLATFORM or CMS.", example = json!("PLATFORM")),
        ("cursor" = Option<String>, Query, description = "Opaque cursor returned by the previous page."),
        ("limit" = Option<u16>, Query, description = "Page size from 1 through 100. Defaults to 50.", example = json!(50)),
        ("X-Correlation-Id" = String, Header, description = "WSO2 canonical business correlation ID.", example = json!("ce5c1b18-9050-49b2-9fd2-a2f208a56117")),
        ("X-Request-Id" = Uuid, Header, description = "WSO2 unique HTTP attempt ID.", example = json!("ce5c1b18-9050-49b2-9fd2-a2f208a56118")),
        ("X-WSO2-Client-IP" = String, Header, description = "Canonical original client IP.", example = json!("192.168.0.1")),
        ("X-WSO2-Gateway-Id" = String, Header, description = "Trusted WSO2 gateway instance ID.", example = json!("wso2-dev-gateway-1")),
        ("X-JWT-Assertion" = Option<String>, Header, description = "Alternative WSO2 backend assertion transport. Send the raw JWT without the Bearer prefix only when Wurzburg is configured for x_jwt_assertion.")
    ),
    security(("wso2_backend_bearer" = [])),
    responses(
        (status = 200, description = "Card ranges returned", body = ListCardRangesResponse),
        (status = 400, description = "Invalid filter, cursor, or limit", body = crate::api::error::ApiErrorResponse),
        (status = 401, description = "Trusted actor assertion is missing or invalid", body = crate::api::error::ApiErrorResponse),
        (status = 403, description = "Caller lacks platform.card_ranges:read", body = crate::api::error::ApiErrorResponse),
        (status = 500, description = "Internal persistence failure", body = crate::api::error::ApiErrorResponse)
    )
)]
#[tracing::instrument(skip(state, headers, query), fields(limit = query.limit.unwrap_or_default()))]
pub async fn list_card_ranges(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListCardRangesQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let request_context =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request_context, &state.config.wso2)?;
    let query = CardRangeListQuery::try_from(query)?;
    let service = CardRangeService::new(state.db.clone());
    let page = service.list_card_ranges(&actor, query).await?;

    Ok(success_response(
        StatusCode::OK,
        ListCardRangesResponse::from(page),
    ))
}

#[utoipa::path(
    post,
    path = "/api/v1/card-ranges",
    tag = "Card Ranges",
    request_body(
        content = CreateCardRangeRequest,
        description = "Creates a canonical card-number range and its initial control profile inputs.",
        content_type = "application/json",
        example = json!({
            "cms_operation_mode": "FULL",
            "start_card_number": "1111000000000000",
            "end_card_number": "1111000000001111",
            "funding_mode": "SINGLE_PROVIDER",
            "withdrawal_limit_authority": "PLATFORM",
            "limit_calendar": {
                "timezone": "Asia/Tehran",
                "week_starts_on": "SATURDAY",
                "window_mode": "CALENDAR"
            },
            "issuance_enabled": true,
            "metadata": {}
        })
    ),
    params(
        ("Idempotency-Key" = String, Header, description = "Required stable idempotency key for this mutation.", example = json!("ce5c1b18-9050-49b2-9fd2-a2f208a56116")),
        ("X-Correlation-Id" = String, Header, description = "WSO2 canonical business correlation ID.", example = json!("ce5c1b18-9050-49b2-9fd2-a2f208a56117")),
        ("X-Request-Id" = Uuid, Header, description = "WSO2 unique HTTP attempt ID.", example = json!("ce5c1b18-9050-49b2-9fd2-a2f208a56118")),
        ("X-WSO2-Client-IP" = String, Header, description = "Canonical original client IP.", example = json!("192.168.0.1")),
        ("X-WSO2-Gateway-Id" = String, Header, description = "Trusted WSO2 gateway instance ID.", example = json!("wso2-dev-gateway-1")),
        ("X-JWT-Assertion" = Option<String>, Header, description = "Alternative WSO2 backend assertion transport. Send the raw JWT without the Bearer prefix only when Wurzburg is configured for x_jwt_assertion.")
    ),
    security(("wso2_backend_bearer" = [])),
    responses(
        (status = 201, description = "Card range created", body = CardRangeResponse),
        (status = 200, description = "Previously completed idempotent response replayed", body = serde_json::Value),
        (status = 400, description = "Invalid trusted headers, idempotency key, or card range payload", body = crate::api::error::ApiErrorResponse),
        (status = 401, description = "Trusted actor assertion is missing or invalid", body = crate::api::error::ApiErrorResponse),
        (status = 403, description = "Caller lacks platform.card_ranges:write", body = crate::api::error::ApiErrorResponse),
        (status = 409, description = "Idempotency conflict/in-progress operation or overlapping card range", body = crate::api::error::ApiErrorResponse),
        (status = 500, description = "Internal persistence/idempotency failure", body = crate::api::error::ApiErrorResponse)
    )
)]
#[tracing::instrument(skip(state, headers, body))]
pub async fn create_card_range(
    State(state): State<Arc<AppState>>,
    method: Method,
    OriginalUri(original_uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let request_context =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request_context, &state.config.wso2)?;
    let idempotency_key = require_idempotency_key(&headers)?;
    let request_hash = canonical_request_hash(&method, original_uri.path(), &body);
    let request = serde_json::from_slice::<CreateCardRangeRequest>(&body).map_err(|error| {
        ApiError::with_message(
            WurzburgResultCode::InvalidCardRangeBoundary,
            error.to_string(),
        )
    })?;

    let command_context = MutationCommandContext {
        operation_type: CREATE_CARD_RANGE_OPERATION.to_string(),
        actor,
        request: request_context,
        idempotency_key,
        request_hash,
    };

    let service = CardRangeService::new(state.db.clone());
    match service
        .create_card_range(&command_context, request.try_into()?)
        .await?
    {
        CreateCardRangeOutcome::Created(card_range) => Ok(success_response(
            StatusCode::CREATED,
            CardRangeResponse::from(*card_range),
        )),
        CreateCardRangeOutcome::Replayed(snapshot) => {
            Ok(success_response(StatusCode::OK, snapshot))
        }
    }
}

#[utoipa::path(patch, path="/api/v1/card-ranges/{card_range_id}", tag="Card Ranges", request_body=UpdateDraftCardRangeRequest, params(("card_range_id"=Uuid, Path), ("Idempotency-Key"=String, Header)), responses((status=200, body=CardRangeMutationResponse), (status=409, body=crate::api::error::ApiErrorResponse)), security(("wso2_backend_bearer"=[])))]
pub async fn update_draft_card_range(
    State(state): State<Arc<AppState>>,
    Path(card_range_id): Path<Uuid>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let (context, request) = mutation_request::<UpdateDraftCardRangeRequest>(
        &state,
        &method,
        uri.path(),
        &headers,
        &body,
        UPDATE_CARD_RANGE_OPERATION,
    )?;
    let update = DraftCardRangeUpdate {
        numbers: Some(
            CardNumberRange::new(request.start_card_number, request.end_card_number).map_err(
                |error| {
                    ApiError::with_message(
                        WurzburgResultCode::InvalidCardRangeBoundary,
                        error.to_string(),
                    )
                },
            )?,
        ),
        funding_mode: Some(request.funding_mode.into()),
        withdrawal_limit_authority: Some(request.withdrawal_limit_authority.into()),
        limit_calendar: Some(request.limit_calendar.map(Into::into)),
        issuance_enabled: Some(request.issuance_enabled),
        cms_operation_mode: Some(request.cms_operation_mode.into()),
        metadata_json: Some(request.metadata),
        reason: request.reason,
    };
    mutation_response(
        CardRangeService::new(state.db.clone())
            .mutate_card_range(
                &context,
                card_range_id,
                CardRangeMutation::UpdateDraft(update),
            )
            .await?,
        StatusCode::OK,
    )
}

#[utoipa::path(post, path="/api/v1/card-ranges/{card_range_id}/activate", tag="Card Ranges", request_body=CardRangeTransitionRequest, params(("card_range_id"=Uuid, Path), ("Idempotency-Key"=String, Header)), responses((status=202, body=CardRangeMutationResponse), (status=409, body=crate::api::error::ApiErrorResponse)), security(("wso2_backend_bearer"=[])))]
pub async fn activate_card_range(
    State(state): State<Arc<AppState>>,
    Path(card_range_id): Path<Uuid>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let (context, request) = mutation_request::<CardRangeTransitionRequest>(
        &state,
        &method,
        uri.path(),
        &headers,
        &body,
        ACTIVATE_CARD_RANGE_OPERATION,
    )?;
    mutation_response(
        CardRangeService::new(state.db.clone())
            .mutate_card_range(
                &context,
                card_range_id,
                CardRangeMutation::Activate {
                    reason: request.reason,
                },
            )
            .await?,
        StatusCode::ACCEPTED,
    )
}

#[utoipa::path(post, path="/api/v1/card-ranges/{card_range_id}/suspend", tag="Card Ranges", request_body=CardRangeTransitionRequest, params(("card_range_id"=Uuid, Path), ("Idempotency-Key"=String, Header)), responses((status=202, body=CardRangeMutationResponse), (status=409, body=crate::api::error::ApiErrorResponse)), security(("wso2_backend_bearer"=[])))]
pub async fn suspend_card_range(
    State(state): State<Arc<AppState>>,
    Path(card_range_id): Path<Uuid>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let (context, request) = mutation_request::<CardRangeTransitionRequest>(
        &state,
        &method,
        uri.path(),
        &headers,
        &body,
        SUSPEND_CARD_RANGE_OPERATION,
    )?;
    mutation_response(
        CardRangeService::new(state.db.clone())
            .mutate_card_range(
                &context,
                card_range_id,
                CardRangeMutation::Suspend {
                    reason: request.reason,
                },
            )
            .await?,
        StatusCode::ACCEPTED,
    )
}

#[utoipa::path(put, path="/api/v1/card-ranges/{card_range_id}/operational-controls", tag="Card Ranges", request_body=UpdateCardRangeControlsRequest, params(("card_range_id"=Uuid, Path), ("Idempotency-Key"=String, Header)), responses((status=202, body=CardRangeMutationResponse), (status=409, body=crate::api::error::ApiErrorResponse)), security(("wso2_backend_bearer"=[])))]
pub async fn update_card_range_controls(
    State(state): State<Arc<AppState>>,
    Path(card_range_id): Path<Uuid>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let (context, request) = mutation_request::<UpdateCardRangeControlsRequest>(
        &state,
        &method,
        uri.path(),
        &headers,
        &body,
        UPDATE_CARD_RANGE_CONTROLS_OPERATION,
    )?;
    let change = CardRangeControlChange {
        issuance_enabled: request.issuance_enabled,
        cms_operation_mode: request.cms_operation_mode.into(),
        reason: request.reason,
    };
    mutation_response(
        CardRangeService::new(state.db.clone())
            .mutate_card_range(
                &context,
                card_range_id,
                CardRangeMutation::UpdateControls(change),
            )
            .await?,
        StatusCode::ACCEPTED,
    )
}

#[utoipa::path(get, path="/api/v1/card-ranges/{card_range_id}/providers", tag="Card Ranges", params(("card_range_id"=Uuid, Path)), responses((status=200, body=ListCardRangeProvidersResponse), (status=404, body=crate::api::error::ApiErrorResponse)), security(("wso2_backend_bearer"=[])))]
pub async fn list_card_range_providers(
    State(state): State<Arc<AppState>>,
    Path(card_range_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let request =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request, &state.config.wso2)?;
    let items = CardRangeService::new(state.db.clone())
        .list_provider_eligibility(&actor, card_range_id)
        .await?
        .into_iter()
        .map(|item| CardRangeProviderEligibilityResponse {
            provider_id: item.provider_id,
            status: item.status.as_db_value().to_string(),
        })
        .collect();
    Ok(success_response(
        StatusCode::OK,
        ListCardRangeProvidersResponse { items },
    ))
}

#[utoipa::path(get, path="/api/v1/operations/{operation_id}", tag="Operations", params(("operation_id"=Uuid, Path)), responses((status=200, body=IntegrationOperationResponse), (status=404, body=crate::api::error::ApiErrorResponse)), security(("wso2_backend_bearer"=[])))]
pub async fn get_integration_operation(
    State(state): State<Arc<AppState>>,
    Path(operation_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let request =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request, &state.config.wso2)?;
    let operation = CardRangeService::new(state.db.clone())
        .get_operation(&actor, operation_id)
        .await?;
    let status = match operation.status {
        crate::db::oracle::IntegrationOperationStatus::Pending => "PENDING",
        crate::db::oracle::IntegrationOperationStatus::Publishing => "PUBLISHING",
        crate::db::oracle::IntegrationOperationStatus::Published => "PUBLISHED",
        crate::db::oracle::IntegrationOperationStatus::Materialized => "MATERIALIZED",
        crate::db::oracle::IntegrationOperationStatus::DeadLetter => "DEAD_LETTER",
    };
    Ok(success_response(
        StatusCode::OK,
        IntegrationOperationResponse {
            operation_id: operation.operation_id,
            event_id: operation.event_id,
            event_type: operation.event_type,
            aggregate_type: operation.aggregate_type,
            aggregate_id: operation.aggregate_id,
            status: status.to_string(),
            attempt_count: operation.attempt_count,
            created_at: operation.created_at,
            published_at: operation.published_at,
            materialized_at: operation.materialized_at,
        },
    ))
}

fn mutation_request<T: for<'de> Deserialize<'de>>(
    state: &AppState,
    method: &Method,
    path: &str,
    headers: &HeaderMap,
    body: &[u8],
    operation_type: &str,
) -> Result<(MutationCommandContext, T), ApiError> {
    let request_context =
        extract_trusted_request_context(headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request_context, &state.config.wso2)?;
    let idempotency_key = require_idempotency_key(headers)?;
    let request = serde_json::from_slice(body)
        .map_err(|_| ApiError::new(WurzburgResultCode::InvalidCardRangeBoundary))?;
    Ok((
        MutationCommandContext {
            operation_type: operation_type.to_string(),
            actor,
            request: request_context,
            idempotency_key,
            request_hash: canonical_request_hash(method, path, body),
        },
        request,
    ))
}

fn mutation_response(
    outcome: CardRangeMutationServiceOutcome,
    status: StatusCode,
) -> Result<Response, ApiError> {
    match outcome {
        CardRangeMutationServiceOutcome::Applied(result) => Ok(success_response(
            status,
            CardRangeMutationResponse {
                operation_id: result.operation_id,
                card_range: result.card_range.into(),
            },
        )),
        CardRangeMutationServiceOutcome::Replayed(snapshot) => {
            Ok(success_response(StatusCode::OK, snapshot))
        }
    }
}

impl TryFrom<CreateCardRangeRequest> for NewCardRange {
    type Error = ApiError;

    fn try_from(request: CreateCardRangeRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            card_range_id: Uuid::new_v4(),
            numbers: CardNumberRange::new(request.start_card_number, request.end_card_number)
                .map_err(|error| {
                    ApiError::with_message(
                        WurzburgResultCode::InvalidCardRangeBoundary,
                        error.to_string(),
                    )
                })?,
            funding_mode: request.funding_mode.into(),
            withdrawal_limit_authority: request.withdrawal_limit_authority.into(),
            limit_calendar: request.limit_calendar.map(Into::into),
            issuance_enabled: request.issuance_enabled,
            cms_operation_mode: request.cms_operation_mode.into(),
            metadata_json: request.metadata,
        })
    }
}

impl TryFrom<ListCardRangesQuery> for CardRangeListQuery {
    type Error = ApiError;

    fn try_from(query: ListCardRangesQuery) -> Result<Self, Self::Error> {
        CardRangeListQuery::new(
            query
                .status
                .as_deref()
                .map(parse_card_range_status)
                .transpose()?,
            query
                .funding_mode
                .as_deref()
                .map(parse_funding_mode)
                .transpose()?,
            query
                .withdrawal_limit_authority
                .as_deref()
                .map(parse_withdrawal_limit_authority)
                .transpose()?,
            query.cursor.as_deref().map(parse_cursor).transpose()?,
            query.limit,
        )
        .map_err(|error| {
            ApiError::with_message(
                WurzburgResultCode::InvalidCardRangeFilter,
                error.to_string(),
            )
        })
    }
}

impl From<crate::domain::card_range::CardRange> for CardRangeResponse {
    fn from(card_range: crate::domain::card_range::CardRange) -> Self {
        Self {
            card_range_id: card_range.card_range_id,
            start_card_number: card_range.numbers.start,
            end_card_number: card_range.numbers.end,
            funding_mode: card_range.funding_mode.into(),
            withdrawal_limit_authority: card_range.withdrawal_limit_authority.into(),
            limit_calendar: card_range.limit_calendar.map(Into::into),
            status: card_range.status.into(),
            issuance_enabled: card_range.issuance_enabled,
            cms_operation_mode: card_range.cms_operation_mode.into(),
            operational_version: card_range.operational_version,
            materialized_operational_version: card_range.materialized_operational_version,
            range_control_operation_id: card_range.range_control_operation_id,
            metadata: card_range.metadata_json,
            created_at: card_range.created_at,
            updated_at: card_range.updated_at,
        }
    }
}

impl From<CardRangeListPage> for ListCardRangesResponse {
    fn from(page: CardRangeListPage) -> Self {
        Self {
            next_cursor: page.next_cursor.as_ref().map(format_cursor),
            items: page
                .items
                .into_iter()
                .map(CardRangeResponse::from)
                .collect(),
        }
    }
}

impl From<CardRangeFundingModeDto> for FundingMode {
    fn from(value: CardRangeFundingModeDto) -> Self {
        match value {
            CardRangeFundingModeDto::SingleProvider => Self::SingleProvider,
            CardRangeFundingModeDto::MultiProvider => Self::MultiProvider,
        }
    }
}

impl From<FundingMode> for CardRangeFundingModeDto {
    fn from(value: FundingMode) -> Self {
        match value {
            FundingMode::SingleProvider => Self::SingleProvider,
            FundingMode::MultiProvider => Self::MultiProvider,
        }
    }
}

impl From<CardRangeWithdrawalLimitAuthorityDto> for WithdrawalLimitAuthority {
    fn from(value: CardRangeWithdrawalLimitAuthorityDto) -> Self {
        match value {
            CardRangeWithdrawalLimitAuthorityDto::Platform => Self::Platform,
            CardRangeWithdrawalLimitAuthorityDto::Cms => Self::Cms,
        }
    }
}

impl From<WithdrawalLimitAuthority> for CardRangeWithdrawalLimitAuthorityDto {
    fn from(value: WithdrawalLimitAuthority) -> Self {
        match value {
            WithdrawalLimitAuthority::Platform => Self::Platform,
            WithdrawalLimitAuthority::Cms => Self::Cms,
        }
    }
}

impl From<CardRangeCmsOperationModeDto> for CmsOperationMode {
    fn from(value: CardRangeCmsOperationModeDto) -> Self {
        match value {
            CardRangeCmsOperationModeDto::Full => Self::Full,
            CardRangeCmsOperationModeDto::BalanceOnly => Self::BalanceOnly,
            CardRangeCmsOperationModeDto::Blocked => Self::Blocked,
        }
    }
}

impl From<CmsOperationMode> for CardRangeCmsOperationModeDto {
    fn from(value: CmsOperationMode) -> Self {
        match value {
            CmsOperationMode::Full => Self::Full,
            CmsOperationMode::BalanceOnly => Self::BalanceOnly,
            CmsOperationMode::Blocked => Self::Blocked,
        }
    }
}

impl From<LimitCalendarDto> for LimitCalendar {
    fn from(value: LimitCalendarDto) -> Self {
        Self {
            timezone: value.timezone,
            week_starts_on: value.week_starts_on.into(),
            window_mode: value.window_mode.into(),
        }
    }
}

impl From<LimitCalendar> for LimitCalendarDto {
    fn from(value: LimitCalendar) -> Self {
        Self {
            timezone: value.timezone,
            week_starts_on: value.week_starts_on.into(),
            window_mode: value.window_mode.into(),
        }
    }
}

impl From<WeekStartDayDto> for WeekStartDay {
    fn from(value: WeekStartDayDto) -> Self {
        match value {
            WeekStartDayDto::Saturday => Self::Saturday,
            WeekStartDayDto::Sunday => Self::Sunday,
            WeekStartDayDto::Monday => Self::Monday,
        }
    }
}

impl From<WeekStartDay> for WeekStartDayDto {
    fn from(value: WeekStartDay) -> Self {
        match value {
            WeekStartDay::Saturday => Self::Saturday,
            WeekStartDay::Sunday => Self::Sunday,
            WeekStartDay::Monday => Self::Monday,
        }
    }
}

impl From<LimitWindowModeDto> for LimitWindowMode {
    fn from(_value: LimitWindowModeDto) -> Self {
        Self::Calendar
    }
}

impl From<LimitWindowMode> for LimitWindowModeDto {
    fn from(_value: LimitWindowMode) -> Self {
        Self::Calendar
    }
}

impl From<crate::domain::card_range::CardRangeStatus> for CardRangeStatusDto {
    fn from(value: crate::domain::card_range::CardRangeStatus) -> Self {
        match value {
            crate::domain::card_range::CardRangeStatus::Draft => Self::Draft,
            crate::domain::card_range::CardRangeStatus::Active => Self::Active,
            crate::domain::card_range::CardRangeStatus::Suspended => Self::Suspended,
        }
    }
}

fn parse_card_range_status(value: &str) -> Result<CardRangeStatus, ApiError> {
    match value {
        "DRAFT" => Ok(CardRangeStatus::Draft),
        "ACTIVE" => Ok(CardRangeStatus::Active),
        "SUSPENDED" => Ok(CardRangeStatus::Suspended),
        _ => Err(ApiError::with_details(
            WurzburgResultCode::InvalidCardRangeFilter,
            serde_json::json!({ "filter": "status" }),
        )),
    }
}

fn parse_funding_mode(value: &str) -> Result<FundingMode, ApiError> {
    match value {
        "SINGLE_PROVIDER" => Ok(FundingMode::SingleProvider),
        "MULTI_PROVIDER" => Ok(FundingMode::MultiProvider),
        _ => Err(ApiError::with_details(
            WurzburgResultCode::InvalidCardRangeFilter,
            serde_json::json!({ "filter": "funding_mode" }),
        )),
    }
}

fn parse_withdrawal_limit_authority(value: &str) -> Result<WithdrawalLimitAuthority, ApiError> {
    match value {
        "PLATFORM" => Ok(WithdrawalLimitAuthority::Platform),
        "CMS" => Ok(WithdrawalLimitAuthority::Cms),
        _ => Err(ApiError::with_details(
            WurzburgResultCode::InvalidWithdrawalLimitAuthority,
            serde_json::json!({ "filter": "withdrawal_limit_authority" }),
        )),
    }
}

fn parse_cursor(value: &str) -> Result<CardRangeListCursor, ApiError> {
    let (created_at, card_range_id) = value.split_once('|').ok_or_else(invalid_cursor)?;
    let created_at = DateTime::parse_from_rfc3339(created_at)
        .map_err(|_| invalid_cursor())?
        .with_timezone(&Utc);
    let card_range_id = card_range_id
        .parse::<Uuid>()
        .map_err(|_| invalid_cursor())?;

    Ok(CardRangeListCursor {
        created_at,
        card_range_id,
    })
}

fn format_cursor(cursor: &CardRangeListCursor) -> String {
    format!(
        "{}|{}",
        cursor
            .created_at
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        cursor.card_range_id
    )
}

fn invalid_cursor() -> ApiError {
    ApiError::with_details(
        WurzburgResultCode::InvalidCardRangeFilter,
        serde_json::json!({ "filter": "cursor" }),
    )
}

fn success_response<T>(status: StatusCode, body: T) -> Response
where
    T: Serialize,
{
    let (rs_code, code, _, _) = WurzburgResultCode::Success.parts();
    let mut response = (status, Json(body)).into_response();
    response.headers_mut().insert(
        RESULT_CODE_HEADER.clone(),
        HeaderValue::from_str(&rs_code.to_string()).expect("static result code is valid"),
    );
    response
        .headers_mut()
        .insert(RESULT_SYMBOL_HEADER.clone(), HeaderValue::from_static(code));
    response
}

fn empty_metadata() -> serde_json::Value {
    serde_json::json!({})
}
