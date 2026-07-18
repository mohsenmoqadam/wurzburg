use std::sync::Arc;

use axum::{
    Json,
    body::Bytes,
    extract::{OriginalUri, Path, Query, State},
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
    db::oracle::{PolicyMutationDisposition, SetCardPolicyResult},
    domain::card_policy::{
        CardPolicyProfile, CardPolicyStatus, CardPolicyTerms, DesiredCardPolicy, WithdrawalLimits,
        WithdrawalWindowLimit,
    },
    services::card_policy::{CardPolicyService, SetCardPolicyOutcome},
    state::AppState,
    telemetry::http::{RESULT_CODE_HEADER, RESULT_SYMBOL_HEADER},
};

const SET_CARD_POLICY_OPERATION: &str = "card_policies.set";

#[derive(Debug, Deserialize, ToSchema)]
pub struct SetCardPolicyRequest {
    pub withdrawal_limits: Option<WithdrawalLimitsDto>,
    pub reason: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
pub struct WithdrawalLimitsDto {
    pub per_transaction_min_amount: Option<u64>,
    pub per_transaction_max_amount: Option<u64>,
    pub daily: Option<WithdrawalWindowLimitDto>,
    pub weekly: Option<WithdrawalWindowLimitDto>,
    pub monthly: Option<WithdrawalWindowLimitDto>,
    pub yearly: Option<WithdrawalWindowLimitDto>,
}

#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
pub struct WithdrawalWindowLimitDto {
    /// Null means that this window does not enforce an amount limit.
    pub max_amount: Option<u64>,
    /// Null means that this window does not enforce a count limit.
    pub max_count: Option<u32>,
}

#[derive(Debug, Clone, Copy, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CardPolicyStatusDto {
    Draft,
    Active,
    Superseded,
}

#[derive(Debug, Clone, Copy, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PolicyMutationDispositionDto {
    Created,
    Updated,
    PublicationPending,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CardPolicyRecordResponse {
    pub card_policy_profile_id: Uuid,
    pub card_range_id: Uuid,
    pub withdrawal_limits: Option<WithdrawalLimitsDto>,
    pub status: CardPolicyStatusDto,
    pub version: i64,
    pub superseded_by_profile_id: Option<Uuid>,
    pub publication_operation_id: Option<Uuid>,
    pub created_by_subject: String,
    pub updated_by_subject: String,
    pub change_reason: String,
    pub activated_at: Option<chrono::DateTime<chrono::Utc>>,
    pub superseded_at: Option<chrono::DateTime<chrono::Utc>>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CardPolicyResponse {
    #[serde(flatten)]
    pub profile: CardPolicyRecordResponse,
    pub funding_mode: super::card_ranges::CardRangeFundingModeDto,
    pub withdrawal_limit_authority: super::card_ranges::CardRangeWithdrawalLimitAuthorityDto,
    pub limit_calendar: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SetCardPolicyResponse {
    pub disposition: PolicyMutationDispositionDto,
    pub profile: CardPolicyRecordResponse,
    pub operation_id: Option<Uuid>,
    pub funding_mode: super::card_ranges::CardRangeFundingModeDto,
    pub withdrawal_limit_authority: super::card_ranges::CardRangeWithdrawalLimitAuthorityDto,
    pub limit_calendar: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ListCardPoliciesQuery {
    pub before_version: Option<i64>,
    pub limit: Option<u16>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListCardPoliciesResponse {
    pub items: Vec<CardPolicyResponse>,
}

#[utoipa::path(
    put,
    path = "/api/v1/card-ranges/{card_range_id}/policy",
    tag = "Card Policies",
    request_body = SetCardPolicyRequest,
    params(
        ("card_range_id" = Uuid, Path, description = "Card range identifier."),
        ("Idempotency-Key" = String, Header, description = "Required stable idempotency key."),
        ("X-Correlation-Id" = String, Header, description = "WSO2 canonical correlation ID."),
        ("X-Request-Id" = Uuid, Header, description = "WSO2 HTTP attempt ID."),
        ("X-WSO2-Client-IP" = String, Header, description = "Canonical original client IP."),
        ("X-WSO2-Gateway-Id" = String, Header, description = "Trusted gateway instance ID."),
        ("X-JWT-Assertion" = Option<String>, Header, description = "Raw WSO2 JWT when configured for this transport.")
    ),
    security(("wso2_backend_bearer" = [])),
    responses(
        (status = 200, description = "Draft updated or idempotent response replayed", body = SetCardPolicyResponse),
        (status = 201, description = "Draft created", body = SetCardPolicyResponse),
        (status = 202, description = "Draft frozen and publication requested", body = SetCardPolicyResponse),
        (status = 400, description = "Invalid policy contract", body = crate::api::error::ApiErrorResponse),
        (status = 401, description = "Invalid trusted actor", body = crate::api::error::ApiErrorResponse),
        (status = 403, description = "Missing platform.policies:write", body = crate::api::error::ApiErrorResponse),
        (status = 404, description = "Card range not found", body = crate::api::error::ApiErrorResponse),
        (status = 409, description = "Frozen draft or idempotency conflict", body = crate::api::error::ApiErrorResponse)
    )
)]
#[tracing::instrument(skip(state, headers, body), fields(card_range_id = %card_range_id))]
pub async fn set_card_policy(
    State(state): State<Arc<AppState>>,
    Path(card_range_id): Path<Uuid>,
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
    let request = serde_json::from_slice::<SetCardPolicyRequest>(&body)
        .map_err(|_| ApiError::new(WurzburgResultCode::CardPolicyContractInvalid))?;
    let command_context = MutationCommandContext {
        operation_type: SET_CARD_POLICY_OPERATION.to_string(),
        actor,
        request: request_context,
        idempotency_key,
        request_hash,
    };
    let desired = DesiredCardPolicy {
        terms: CardPolicyTerms {
            withdrawal_limits: request.withdrawal_limits.map(Into::into),
        },
        reason: request.reason,
    };

    let service = CardPolicyService::new(state.db.clone());
    match service
        .set_policy(&command_context, card_range_id, desired)
        .await?
    {
        SetCardPolicyOutcome::Applied(result) => {
            let status = match result.disposition {
                PolicyMutationDisposition::Created => StatusCode::CREATED,
                PolicyMutationDisposition::Updated => StatusCode::OK,
                PolicyMutationDisposition::PublicationPending => StatusCode::ACCEPTED,
            };
            success_response(status, SetCardPolicyResponse::from(*result))
        }
        SetCardPolicyOutcome::Replayed(snapshot) => success_response(StatusCode::OK, snapshot),
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/card-ranges/{card_range_id}/policy",
    tag = "Card Policies",
    params(("card_range_id" = Uuid, Path), ("X-Correlation-Id" = String, Header), ("X-Request-Id" = Uuid, Header), ("X-WSO2-Client-IP" = String, Header), ("X-WSO2-Gateway-Id" = String, Header), ("X-JWT-Assertion" = Option<String>, Header)),
    security(("wso2_backend_bearer" = [])),
    responses((status = 200, body = CardPolicyResponse), (status = 404, body = crate::api::error::ApiErrorResponse))
)]
#[tracing::instrument(skip(state, headers), fields(card_range_id = %card_range_id))]
pub async fn get_current_card_policy(
    State(state): State<Arc<AppState>>,
    Path(card_range_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let request_context =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request_context, &state.config.wso2)?;
    let service = CardPolicyService::new(state.db.clone());
    let profile = service.get_current_policy(&actor, card_range_id).await?;
    success_response(StatusCode::OK, CardPolicyResponse::from(profile))
}

#[utoipa::path(
    get,
    path = "/api/v1/card-ranges/{card_range_id}/policies/{policy_id}",
    tag = "Card Policies",
    params(("card_range_id" = Uuid, Path), ("policy_id" = Uuid, Path), ("X-Correlation-Id" = String, Header), ("X-Request-Id" = Uuid, Header), ("X-WSO2-Client-IP" = String, Header), ("X-WSO2-Gateway-Id" = String, Header), ("X-JWT-Assertion" = Option<String>, Header)),
    security(("wso2_backend_bearer" = [])),
    responses((status = 200, body = CardPolicyResponse), (status = 404, body = crate::api::error::ApiErrorResponse))
)]
#[tracing::instrument(skip(state, headers), fields(card_range_id = %card_range_id, policy_id = %policy_id))]
pub async fn get_card_policy(
    State(state): State<Arc<AppState>>,
    Path((card_range_id, policy_id)): Path<(Uuid, Uuid)>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let request_context =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request_context, &state.config.wso2)?;
    let service = CardPolicyService::new(state.db.clone());
    let profile = service.get_policy(&actor, card_range_id, policy_id).await?;
    success_response(StatusCode::OK, CardPolicyResponse::from(profile))
}

#[utoipa::path(
    get,
    path = "/api/v1/card-ranges/{card_range_id}/policies",
    tag = "Card Policies",
    params(("card_range_id" = Uuid, Path), ("before_version" = Option<i64>, Query), ("limit" = Option<u16>, Query), ("X-Correlation-Id" = String, Header), ("X-Request-Id" = Uuid, Header), ("X-WSO2-Client-IP" = String, Header), ("X-WSO2-Gateway-Id" = String, Header), ("X-JWT-Assertion" = Option<String>, Header)),
    security(("wso2_backend_bearer" = [])),
    responses((status = 200, body = ListCardPoliciesResponse))
)]
#[tracing::instrument(skip(state, headers, query), fields(card_range_id = %card_range_id))]
pub async fn list_card_policies(
    State(state): State<Arc<AppState>>,
    Path(card_range_id): Path<Uuid>,
    Query(query): Query<ListCardPoliciesQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let request_context =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request_context, &state.config.wso2)?;
    let service = CardPolicyService::new(state.db.clone());
    let profiles = service
        .list_policies(
            &actor,
            card_range_id,
            query.before_version,
            query.limit.unwrap_or(50),
        )
        .await?;
    success_response(
        StatusCode::OK,
        ListCardPoliciesResponse {
            items: profiles.into_iter().map(Into::into).collect(),
        },
    )
}

impl From<WithdrawalLimitsDto> for WithdrawalLimits {
    fn from(value: WithdrawalLimitsDto) -> Self {
        Self {
            per_transaction_min_amount: value.per_transaction_min_amount,
            per_transaction_max_amount: value.per_transaction_max_amount,
            daily: value.daily.map(Into::into),
            weekly: value.weekly.map(Into::into),
            monthly: value.monthly.map(Into::into),
            yearly: value.yearly.map(Into::into),
        }
    }
}

impl From<WithdrawalLimits> for WithdrawalLimitsDto {
    fn from(value: WithdrawalLimits) -> Self {
        Self {
            per_transaction_min_amount: value.per_transaction_min_amount,
            per_transaction_max_amount: value.per_transaction_max_amount,
            daily: value.daily.map(Into::into),
            weekly: value.weekly.map(Into::into),
            monthly: value.monthly.map(Into::into),
            yearly: value.yearly.map(Into::into),
        }
    }
}

impl From<WithdrawalWindowLimitDto> for WithdrawalWindowLimit {
    fn from(value: WithdrawalWindowLimitDto) -> Self {
        Self {
            max_amount: value.max_amount,
            max_count: value.max_count,
        }
    }
}

impl From<WithdrawalWindowLimit> for WithdrawalWindowLimitDto {
    fn from(value: WithdrawalWindowLimit) -> Self {
        Self {
            max_amount: value.max_amount,
            max_count: value.max_count,
        }
    }
}

impl From<crate::services::card_policy::CardPolicyView> for CardPolicyResponse {
    fn from(value: crate::services::card_policy::CardPolicyView) -> Self {
        let limit_calendar = value.limit_calendar.map(|calendar| {
            serde_json::to_value(calendar).expect("calendar serialization is stable")
        });
        Self::from_parts(
            value.profile,
            value.funding_mode,
            value.withdrawal_limit_authority,
            limit_calendar,
        )
    }
}

impl CardPolicyResponse {
    fn from_parts(
        value: CardPolicyProfile,
        funding_mode: crate::domain::card_range::FundingMode,
        withdrawal_limit_authority: crate::domain::card_range::WithdrawalLimitAuthority,
        limit_calendar: Option<serde_json::Value>,
    ) -> Self {
        Self {
            profile: value.into(),
            funding_mode: funding_mode.into(),
            withdrawal_limit_authority: withdrawal_limit_authority.into(),
            limit_calendar,
        }
    }
}

impl From<CardPolicyProfile> for CardPolicyRecordResponse {
    fn from(value: CardPolicyProfile) -> Self {
        Self {
            card_policy_profile_id: value.card_policy_profile_id,
            card_range_id: value.card_range_id,
            withdrawal_limits: value.terms.withdrawal_limits.map(Into::into),
            status: value.status.into(),
            version: value.version,
            superseded_by_profile_id: value.superseded_by_profile_id,
            publication_operation_id: value.publication_operation_id,
            created_by_subject: value.created_by_subject,
            updated_by_subject: value.updated_by_subject,
            change_reason: value.change_reason,
            activated_at: value.activated_at,
            superseded_at: value.superseded_at,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}

impl From<CardPolicyStatus> for CardPolicyStatusDto {
    fn from(value: CardPolicyStatus) -> Self {
        match value {
            CardPolicyStatus::Draft => Self::Draft,
            CardPolicyStatus::Active => Self::Active,
            CardPolicyStatus::Superseded => Self::Superseded,
        }
    }
}

impl From<SetCardPolicyResult> for SetCardPolicyResponse {
    fn from(value: SetCardPolicyResult) -> Self {
        let profile = value.profile.into();
        Self {
            disposition: match value.disposition {
                PolicyMutationDisposition::Created => PolicyMutationDispositionDto::Created,
                PolicyMutationDisposition::Updated => PolicyMutationDispositionDto::Updated,
                PolicyMutationDisposition::PublicationPending => {
                    PolicyMutationDispositionDto::PublicationPending
                }
            },
            profile,
            operation_id: value.operation_id,
            funding_mode: value.funding_mode.into(),
            withdrawal_limit_authority: value.withdrawal_limit_authority.into(),
            limit_calendar: value.limit_calendar,
        }
    }
}

fn success_response<T: Serialize>(status: StatusCode, body: T) -> Result<Response, ApiError> {
    let (rs_code, code, _, _) = WurzburgResultCode::Success.parts();
    let mut response = (status, Json(body)).into_response();
    response.headers_mut().insert(
        RESULT_CODE_HEADER.clone(),
        HeaderValue::from_str(&rs_code.to_string()).expect("static result code is valid"),
    );
    response
        .headers_mut()
        .insert(RESULT_SYMBOL_HEADER.clone(), HeaderValue::from_static(code));
    Ok(response)
}
