use std::sync::Arc;

use axum::{
    Json,
    body::Bytes,
    extract::{OriginalUri, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
};
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
    domain::card_range::{
        CardNumberRange, CmsOperationMode, FundingMode, LimitCalendar, LimitWindowMode,
        NewCardRange, WeekStartDay, WithdrawalLimitAuthority,
    },
    services::card_range::{CardRangeService, CreateCardRangeOutcome},
    state::AppState,
    telemetry::http::{RESULT_CODE_HEADER, RESULT_SYMBOL_HEADER},
};

const CREATE_CARD_RANGE_OPERATION: &str = "card_ranges.create";

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
    pub metadata: serde_json::Value,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
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
    post,
    path = "/api/v1/card-ranges",
    tag = "Card Ranges",
    request_body(
        content = CreateCardRangeRequest,
        description = "Creates a canonical card-number range and its initial control profile inputs.",
        content_type = "application/json"
    ),
    params(
        ("Idempotency-Key" = String, Header, description = "Required stable idempotency key for this mutation."),
        ("X-Correlation-Id" = String, Header, description = "WSO2 canonical business correlation ID."),
        ("X-Request-Id" = Uuid, Header, description = "WSO2 unique HTTP attempt ID."),
        ("X-WSO2-Client-IP" = String, Header, description = "Canonical original client IP."),
        ("X-WSO2-Gateway-Id" = String, Header, description = "Trusted WSO2 gateway instance ID."),
        ("X-JWT-Assertion" = String, Header, description = "Configured WSO2 backend assertion transport when enabled."),
        ("Authorization" = String, Header, description = "Bearer WSO2 backend assertion when Authorization transport is enabled.")
    ),
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
            CardRangeResponse::from(card_range),
        )),
        CreateCardRangeOutcome::Replayed(snapshot) => {
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
            metadata: card_range.metadata_json,
            created_at: card_range.created_at,
            updated_at: card_range.updated_at,
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
