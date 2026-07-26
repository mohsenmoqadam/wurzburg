use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{OriginalUri, Path, Query, State},
    http::{HeaderMap, Method, StatusCode},
    response::Response,
};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

use crate::{
    api::{
        auth::extract_trusted_actor,
        command::MutationCommandContext,
        error::ApiError,
        idempotency::{canonical_request_hash, require_idempotency_key},
        request_context::extract_trusted_request_context,
        response::success_response,
        result_codes::WurzburgResultCode,
    },
    domain::provider_fee::{
        DesiredProviderFeeProfile, FeePayer, FeePolicy, ProviderFeeProfile,
        ProviderFeeProfileStatus,
    },
    services::provider_fee::{ProviderFeeService, SetProviderFeeProfileOutcome},
    state::AppState,
};

const SET_FEE_PROFILE_OPERATION: &str = "provider_fee_profiles.set";

#[derive(Debug, Deserialize, ToSchema)]
pub struct SetProviderFeeProfileRequest {
    /// Percentage component in basis points. 100 basis points equals 1%.
    pub rate_bps: u32,
    /// Fixed fee in Iranian rials. Zero is a valid explicit fee.
    pub fixed_amount_rials: u64,
    pub fee_payer: FeePayerDto,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FeePayerDto {
    ProviderUser,
    Provider,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct FeePolicyDto {
    pub rate_bps: u32,
    pub fixed_amount_rials: u64,
    pub fee_payer: FeePayerDto,
}

#[derive(Debug, Clone, Copy, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProviderFeeProfileStatusDto {
    Draft,
    Active,
    Superseded,
}

#[derive(Debug, Clone, Copy, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FeeProfileMutationDispositionDto {
    Created,
    Updated,
    PublicationPending,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ProviderFeeProfileResponse {
    pub provider_fee_profile_id: Uuid,
    pub provider_id: Uuid,
    pub fee_policy: FeePolicyDto,
    pub status: ProviderFeeProfileStatusDto,
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
pub struct SetProviderFeeProfileResponse {
    pub disposition: FeeProfileMutationDispositionDto,
    pub profile: ProviderFeeProfileResponse,
    pub operation_id: Option<Uuid>,
}

#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct ListProviderFeeProfilesQuery {
    pub before_version: Option<i64>,
    pub limit: Option<u16>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListProviderFeeProfilesResponse {
    pub items: Vec<ProviderFeeProfileResponse>,
}

#[utoipa::path(
    put, path="/api/v1/providers/{provider_id}/fee-profile", tag="Provider Fees",
    request_body=SetProviderFeeProfileRequest,
    params(
        ("provider_id"=Uuid, Path, description="Provider identifier."),
        ("Idempotency-Key"=String, Header, description="Required stable idempotency key."),
        ("X-Correlation-Id"=String, Header), ("X-Request-Id"=Uuid, Header),
        ("X-WSO2-Client-IP"=String, Header), ("X-WSO2-Gateway-Id"=String, Header)
    ),
    responses((status=200, body=SetProviderFeeProfileResponse), (status=202, body=SetProviderFeeProfileResponse), (status=400, body=crate::api::error::ApiErrorResponse), (status=409, body=crate::api::error::ApiErrorResponse)),
    security(("wso2_backend_bearer"=[]))
)]
#[tracing::instrument(skip(state, headers, body), fields(provider_id=%provider_id))]
pub async fn set_provider_fee_profile(
    State(state): State<Arc<AppState>>,
    Path(provider_id): Path<Uuid>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let request_context =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request_context, &state.config.wso2)?;
    let idempotency_key = require_idempotency_key(&headers)?;
    let request_hash = canonical_request_hash(&method, uri.path(), &body);
    let request: SetProviderFeeProfileRequest = serde_json::from_slice(&body)
        .map_err(|_| ApiError::new(WurzburgResultCode::ProviderFeeProfileContractInvalid))?;
    let desired = DesiredProviderFeeProfile {
        fee_policy: FeePolicy {
            rate_bps: request.rate_bps,
            fixed_amount_rials: request.fixed_amount_rials,
            fee_payer: request.fee_payer.into(),
        },
        reason: request.reason,
    };
    let context = MutationCommandContext {
        operation_type: SET_FEE_PROFILE_OPERATION.to_string(),
        actor,
        request: request_context,
        idempotency_key,
        request_hash,
    };
    match ProviderFeeService::new(state.db.clone())
        .set_profile(&context, provider_id, desired)
        .await?
    {
        SetProviderFeeProfileOutcome::Applied(value) => {
            let status = match (value.disposition, value.operation_id) {
                (_, Some(_)) => StatusCode::ACCEPTED,
                (crate::db::oracle::FeeProfileMutationDisposition::Created, None) => {
                    StatusCode::CREATED
                }
                _ => StatusCode::OK,
            };
            success_response(
                status,
                SetProviderFeeProfileResponse {
                    disposition: value.disposition.into(),
                    profile: value.profile.into(),
                    operation_id: value.operation_id,
                },
            )
        }
        SetProviderFeeProfileOutcome::Replayed(value) => success_response(StatusCode::OK, value),
    }
}

#[utoipa::path(
    get, path="/api/v1/providers/{provider_id}/fee-profile", tag="Provider Fees",
    params(("provider_id"=Uuid, Path), ("X-Correlation-Id"=String, Header), ("X-Request-Id"=Uuid, Header), ("X-WSO2-Client-IP"=String, Header), ("X-WSO2-Gateway-Id"=String, Header)),
    responses((status=200, body=ProviderFeeProfileResponse), (status=404, body=crate::api::error::ApiErrorResponse)), security(("wso2_backend_bearer"=[]))
)]
#[tracing::instrument(skip(state, headers), fields(provider_id=%provider_id))]
pub async fn get_current_provider_fee_profile(
    State(state): State<Arc<AppState>>,
    Path(provider_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let request =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request, &state.config.wso2)?;
    let profile: ProviderFeeProfileResponse = ProviderFeeService::new(state.db.clone())
        .get_current(&actor, provider_id)
        .await?
        .into();
    success_response(StatusCode::OK, profile)
}

#[utoipa::path(
    get, path="/api/v1/providers/{provider_id}/fee-profiles", tag="Provider Fees",
    params(("provider_id"=Uuid, Path), ListProviderFeeProfilesQuery, ("X-Correlation-Id"=String, Header), ("X-Request-Id"=Uuid, Header), ("X-WSO2-Client-IP"=String, Header), ("X-WSO2-Gateway-Id"=String, Header)),
    responses((status=200, body=ListProviderFeeProfilesResponse)), security(("wso2_backend_bearer"=[]))
)]
#[tracing::instrument(skip(state,headers),fields(provider_id=%provider_id))]
pub async fn list_provider_fee_profiles(
    State(state): State<Arc<AppState>>,
    Path(provider_id): Path<Uuid>,
    Query(query): Query<ListProviderFeeProfilesQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let request =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request, &state.config.wso2)?;
    let items = ProviderFeeService::new(state.db.clone())
        .list(
            &actor,
            provider_id,
            query.before_version,
            query.limit.unwrap_or(50),
        )
        .await?
        .into_iter()
        .map(Into::into)
        .collect();
    success_response(StatusCode::OK, ListProviderFeeProfilesResponse { items })
}

#[utoipa::path(
    get, path="/api/v1/providers/{provider_id}/fee-profiles/{fee_profile_id}", tag="Provider Fees",
    params(("provider_id"=Uuid, Path), ("fee_profile_id"=Uuid, Path), ("X-Correlation-Id"=String, Header), ("X-Request-Id"=Uuid, Header), ("X-WSO2-Client-IP"=String, Header), ("X-WSO2-Gateway-Id"=String, Header)),
    responses((status=200,body=ProviderFeeProfileResponse),(status=404,body=crate::api::error::ApiErrorResponse)),security(("wso2_backend_bearer"=[]))
)]
#[tracing::instrument(skip(state,headers),fields(provider_id=%provider_id,fee_profile_id=%fee_profile_id))]
pub async fn get_provider_fee_profile(
    State(state): State<Arc<AppState>>,
    Path((provider_id, fee_profile_id)): Path<(Uuid, Uuid)>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let request =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request, &state.config.wso2)?;
    let profile: ProviderFeeProfileResponse = ProviderFeeService::new(state.db.clone())
        .get(&actor, provider_id, fee_profile_id)
        .await?
        .into();
    success_response(StatusCode::OK, profile)
}

impl From<FeePayerDto> for FeePayer {
    fn from(value: FeePayerDto) -> Self {
        match value {
            FeePayerDto::ProviderUser => Self::ProviderUser,
            FeePayerDto::Provider => Self::Provider,
        }
    }
}
impl From<FeePayer> for FeePayerDto {
    fn from(value: FeePayer) -> Self {
        match value {
            FeePayer::ProviderUser => Self::ProviderUser,
            FeePayer::Provider => Self::Provider,
        }
    }
}
impl From<ProviderFeeProfileStatus> for ProviderFeeProfileStatusDto {
    fn from(value: ProviderFeeProfileStatus) -> Self {
        match value {
            ProviderFeeProfileStatus::Draft => Self::Draft,
            ProviderFeeProfileStatus::Active => Self::Active,
            ProviderFeeProfileStatus::Superseded => Self::Superseded,
        }
    }
}
impl From<crate::db::oracle::FeeProfileMutationDisposition> for FeeProfileMutationDispositionDto {
    fn from(value: crate::db::oracle::FeeProfileMutationDisposition) -> Self {
        match value {
            crate::db::oracle::FeeProfileMutationDisposition::Created => Self::Created,
            crate::db::oracle::FeeProfileMutationDisposition::Updated => Self::Updated,
            crate::db::oracle::FeeProfileMutationDisposition::PublicationPending => {
                Self::PublicationPending
            }
        }
    }
}
impl From<ProviderFeeProfile> for ProviderFeeProfileResponse {
    fn from(value: ProviderFeeProfile) -> Self {
        Self {
            provider_fee_profile_id: value.provider_fee_profile_id,
            provider_id: value.provider_id,
            fee_policy: FeePolicyDto {
                rate_bps: value.fee_policy.rate_bps,
                fixed_amount_rials: value.fee_policy.fixed_amount_rials,
                fee_payer: value.fee_policy.fee_payer.into(),
            },
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
