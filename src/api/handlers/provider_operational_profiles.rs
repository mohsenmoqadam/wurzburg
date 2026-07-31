use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{OriginalUri, Path, Query, State},
    http::{HeaderMap, Method, StatusCode},
    response::Response,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

use crate::{
    api::{
        auth::extract_trusted_actor,
        command::MutationCommandContext,
        error::ApiError,
        handlers::providers::{
            CreditGrantLimitModeDto, ProviderActiveWindowRequest,
            ProviderCardOperationsControlRequest, ProviderCreditGrantControlRequest,
            ProviderEnabledControlRequest, ProviderEventDeliveryControlRequest,
            ProviderOperationalControlsRequest, ProviderUserOnboardingControlRequest,
            ProviderWeekdayDto,
        },
        idempotency::{canonical_request_hash, require_idempotency_key},
        request_context::extract_trusted_request_context,
        response::success_response,
        result_codes::WurzburgResultCode,
    },
    db::oracle::OperationalProfileMutationDisposition,
    domain::provider::{
        CreditGrantLimitMode, DesiredProviderOperationalProfile, ProviderOperationalControls,
        ProviderOperationalProfileRecord, ProviderOperationalProfileStatus, ProviderWeekday,
    },
    services::provider_operational_profile::{
        CancelProviderOperationalProfileOutcome, ProviderOperationalProfileService,
        SetProviderOperationalProfileOutcome,
    },
    state::AppState,
};

const SET_OPERATION: &str = "provider_operational_profiles.set";
const CANCEL_OPERATION: &str = "provider_operational_profiles.cancel";

#[derive(Debug, Deserialize, ToSchema)]
pub struct SetProviderOperationalProfileRequest {
    pub effective_at: DateTime<Utc>,
    pub profile: ProviderOperationalControlsRequest,
    pub reason: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct CancelProviderOperationalProfileRequest {
    pub reason: String,
}

#[derive(Debug, Clone, Copy, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProviderOperationalProfileStatusDto {
    Scheduled,
    Active,
    Superseded,
    Cancelled,
}

#[derive(Debug, Clone, Copy, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OperationalProfileMutationDispositionDto {
    Activated,
    Scheduled,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ProviderOperationalProfileResponse {
    pub provider_operational_profile_id: Uuid,
    pub provider_id: Uuid,
    pub status: ProviderOperationalProfileStatusDto,
    pub version: i64,
    pub effective_at: DateTime<Utc>,
    pub profile: ProviderOperationalControlsRequest,
    pub superseded_by_profile_id: Option<Uuid>,
    pub created_by_subject: String,
    pub updated_by_subject: String,
    pub change_reason: String,
    pub activated_at: Option<DateTime<Utc>>,
    pub superseded_at: Option<DateTime<Utc>>,
    pub cancelled_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SetProviderOperationalProfileResponse {
    pub disposition: OperationalProfileMutationDispositionDto,
    pub profile: ProviderOperationalProfileResponse,
}

#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct ListProviderOperationalProfilesQuery {
    pub before_version: Option<i64>,
    pub limit: Option<u16>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListProviderOperationalProfilesResponse {
    pub items: Vec<ProviderOperationalProfileResponse>,
    pub next_before_version: Option<i64>,
}

#[utoipa::path(
    post, path="/api/v1/providers/{provider_id}/operational-profiles", tag="Provider Operational Profiles",
    request_body=SetProviderOperationalProfileRequest,
    params(("provider_id"=Uuid, Path), ("Idempotency-Key"=String, Header), ("X-Correlation-Id"=String, Header), ("X-Request-Id"=Uuid, Header), ("X-WSO2-Client-IP"=String, Header), ("X-WSO2-Gateway-Id"=String, Header)),
    responses((status=201,body=SetProviderOperationalProfileResponse),(status=400,body=crate::api::error::ApiErrorResponse),(status=404,body=crate::api::error::ApiErrorResponse),(status=409,body=crate::api::error::ApiErrorResponse)),
    security(("wso2_backend_bearer"=[]))
)]
#[tracing::instrument(skip(state,headers,body),fields(provider_id=%provider_id))]
pub async fn set_provider_operational_profile(
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
    let request: SetProviderOperationalProfileRequest =
        serde_json::from_slice(&body).map_err(|_| {
            ApiError::new(WurzburgResultCode::ProviderOperationalProfileContractInvalid)
        })?;
    let desired = DesiredProviderOperationalProfile {
        effective_at: request.effective_at,
        controls: request.profile.into(),
        reason: request.reason,
    };
    let context = MutationCommandContext {
        operation_type: SET_OPERATION.to_string(),
        actor,
        request: request_context,
        idempotency_key,
        request_hash,
    };
    match ProviderOperationalProfileService::new(state.db.clone())
        .set_profile(&context, provider_id, desired)
        .await?
    {
        SetProviderOperationalProfileOutcome::Applied(result) => success_response(
            StatusCode::CREATED,
            SetProviderOperationalProfileResponse {
                disposition: result.disposition.into(),
                profile: result.profile.into(),
            },
        ),
        SetProviderOperationalProfileOutcome::Replayed(value) => {
            success_response(StatusCode::OK, value)
        }
    }
}

#[utoipa::path(
    get, path="/api/v1/providers/{provider_id}/operational-profile", tag="Provider Operational Profiles",
    params(("provider_id"=Uuid, Path), ("X-Correlation-Id"=String, Header), ("X-Request-Id"=Uuid, Header), ("X-WSO2-Client-IP"=String, Header), ("X-WSO2-Gateway-Id"=String, Header)),
    responses((status=200,body=ProviderOperationalProfileResponse),(status=404,body=crate::api::error::ApiErrorResponse)),security(("wso2_backend_bearer"=[]))
)]
#[tracing::instrument(skip(state,headers),fields(provider_id=%provider_id))]
pub async fn get_current_provider_operational_profile(
    State(state): State<Arc<AppState>>,
    Path(provider_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let request =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request, &state.config.wso2)?;
    let profile: ProviderOperationalProfileResponse =
        ProviderOperationalProfileService::new(state.db.clone())
            .get_current(&actor, provider_id)
            .await?
            .into();
    success_response(StatusCode::OK, profile)
}

#[utoipa::path(
    get, path="/api/v1/providers/{provider_id}/operational-profiles", tag="Provider Operational Profiles",
    params(("provider_id"=Uuid, Path), ListProviderOperationalProfilesQuery, ("X-Correlation-Id"=String, Header), ("X-Request-Id"=Uuid, Header), ("X-WSO2-Client-IP"=String, Header), ("X-WSO2-Gateway-Id"=String, Header)),
    responses((status=200,body=ListProviderOperationalProfilesResponse),(status=404,body=crate::api::error::ApiErrorResponse)),security(("wso2_backend_bearer"=[]))
)]
#[tracing::instrument(skip(state,headers),fields(provider_id=%provider_id))]
pub async fn list_provider_operational_profiles(
    State(state): State<Arc<AppState>>,
    Path(provider_id): Path<Uuid>,
    Query(query): Query<ListProviderOperationalProfilesQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let request =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request, &state.config.wso2)?;
    let page = ProviderOperationalProfileService::new(state.db.clone())
        .list(
            &actor,
            provider_id,
            query.before_version,
            query.limit.unwrap_or(50),
        )
        .await?;
    success_response(
        StatusCode::OK,
        ListProviderOperationalProfilesResponse {
            items: page.items.into_iter().map(Into::into).collect(),
            next_before_version: page.next_before_version,
        },
    )
}

#[utoipa::path(
    post, path="/api/v1/providers/{provider_id}/operational-profiles/{profile_id}/cancel", tag="Provider Operational Profiles",
    request_body=CancelProviderOperationalProfileRequest,
    params(("provider_id"=Uuid, Path), ("profile_id"=Uuid, Path), ("Idempotency-Key"=String, Header), ("X-Correlation-Id"=String, Header), ("X-Request-Id"=Uuid, Header), ("X-WSO2-Client-IP"=String, Header), ("X-WSO2-Gateway-Id"=String, Header)),
    responses((status=200,body=ProviderOperationalProfileResponse),(status=400,body=crate::api::error::ApiErrorResponse),(status=404,body=crate::api::error::ApiErrorResponse),(status=409,body=crate::api::error::ApiErrorResponse)),security(("wso2_backend_bearer"=[]))
)]
#[tracing::instrument(skip(state,headers,body),fields(provider_id=%provider_id,provider_operational_profile_id=%profile_id))]
pub async fn cancel_scheduled_provider_operational_profile(
    State(state): State<Arc<AppState>>,
    Path((provider_id, profile_id)): Path<(Uuid, Uuid)>,
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
    let request: CancelProviderOperationalProfileRequest =
        serde_json::from_slice(&body).map_err(|_| {
            ApiError::new(WurzburgResultCode::ProviderOperationalProfileContractInvalid)
        })?;
    let context = MutationCommandContext {
        operation_type: CANCEL_OPERATION.to_string(),
        actor,
        request: request_context,
        idempotency_key,
        request_hash,
    };
    match ProviderOperationalProfileService::new(state.db.clone())
        .cancel_scheduled(&context, provider_id, profile_id, request.reason)
        .await?
    {
        CancelProviderOperationalProfileOutcome::Applied(profile) => success_response(
            StatusCode::OK,
            ProviderOperationalProfileResponse::from(*profile),
        ),
        CancelProviderOperationalProfileOutcome::Replayed(value) => {
            success_response(StatusCode::OK, value)
        }
    }
}

impl From<ProviderOperationalControlsRequest> for ProviderOperationalControls {
    fn from(value: ProviderOperationalControlsRequest) -> Self {
        Self {
            timezone: value.timezone,
            user_onboarding_enabled: value.user_onboarding.enabled,
            active_windows: value
                .user_onboarding
                .active_windows
                .into_iter()
                .map(Into::into)
                .collect(),
            max_total_users: value.user_onboarding.max_total_users,
            credit_grant_enabled: value.credit_grant.enabled,
            credit_grant_mode: value.credit_grant.mode.into(),
            credit_grant_limit_amount_rials: value.credit_grant.limit_amount_rials,
            credit_return_enabled: value.credit_return.enabled,
            new_assignment_enabled: value.card_operations.new_assignment_enabled,
            same_pan_reprint_enabled: value.card_operations.same_pan_reprint_enabled,
            new_pan_replacement_enabled: value.card_operations.new_pan_replacement_enabled,
            attach_existing_multi_provider_card_enabled: value
                .card_operations
                .attach_existing_multi_provider_card_enabled,
            event_delivery_enabled: value.event_delivery.enabled,
            event_delivery_disabled_reason: value.event_delivery.disabled_reason,
        }
    }
}

impl From<ProviderOperationalControls> for ProviderOperationalControlsRequest {
    fn from(value: ProviderOperationalControls) -> Self {
        Self {
            timezone: value.timezone,
            user_onboarding: ProviderUserOnboardingControlRequest {
                enabled: value.user_onboarding_enabled,
                active_windows: value.active_windows.into_iter().map(Into::into).collect(),
                max_total_users: value.max_total_users,
            },
            credit_grant: ProviderCreditGrantControlRequest {
                enabled: value.credit_grant_enabled,
                mode: value.credit_grant_mode.into(),
                limit_amount_rials: value.credit_grant_limit_amount_rials,
            },
            credit_return: ProviderEnabledControlRequest {
                enabled: value.credit_return_enabled,
            },
            card_operations: ProviderCardOperationsControlRequest {
                new_assignment_enabled: value.new_assignment_enabled,
                same_pan_reprint_enabled: value.same_pan_reprint_enabled,
                new_pan_replacement_enabled: value.new_pan_replacement_enabled,
                attach_existing_multi_provider_card_enabled: value
                    .attach_existing_multi_provider_card_enabled,
            },
            event_delivery: ProviderEventDeliveryControlRequest {
                enabled: value.event_delivery_enabled,
                disabled_reason: value.event_delivery_disabled_reason,
            },
        }
    }
}

impl From<ProviderOperationalProfileRecord> for ProviderOperationalProfileResponse {
    fn from(value: ProviderOperationalProfileRecord) -> Self {
        Self {
            provider_operational_profile_id: value.provider_operational_profile_id,
            provider_id: value.provider_id,
            status: value.status.into(),
            version: value.version,
            effective_at: value.effective_at,
            profile: value.controls.into(),
            superseded_by_profile_id: value.superseded_by_profile_id,
            created_by_subject: value.created_by_subject,
            updated_by_subject: value.updated_by_subject,
            change_reason: value.change_reason,
            activated_at: value.activated_at,
            superseded_at: value.superseded_at,
            cancelled_at: value.cancelled_at,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}

impl From<ProviderOperationalProfileStatus> for ProviderOperationalProfileStatusDto {
    fn from(value: ProviderOperationalProfileStatus) -> Self {
        match value {
            ProviderOperationalProfileStatus::Scheduled => Self::Scheduled,
            ProviderOperationalProfileStatus::Active => Self::Active,
            ProviderOperationalProfileStatus::Superseded => Self::Superseded,
            ProviderOperationalProfileStatus::Cancelled => Self::Cancelled,
        }
    }
}

impl From<OperationalProfileMutationDisposition> for OperationalProfileMutationDispositionDto {
    fn from(value: OperationalProfileMutationDisposition) -> Self {
        match value {
            OperationalProfileMutationDisposition::Activated => Self::Activated,
            OperationalProfileMutationDisposition::Scheduled => Self::Scheduled,
        }
    }
}

impl From<ProviderWeekday> for ProviderWeekdayDto {
    fn from(value: ProviderWeekday) -> Self {
        match value {
            ProviderWeekday::Saturday => Self::Saturday,
            ProviderWeekday::Sunday => Self::Sunday,
            ProviderWeekday::Monday => Self::Monday,
            ProviderWeekday::Tuesday => Self::Tuesday,
            ProviderWeekday::Wednesday => Self::Wednesday,
            ProviderWeekday::Thursday => Self::Thursday,
            ProviderWeekday::Friday => Self::Friday,
        }
    }
}

impl From<crate::domain::provider::ProviderActiveWindow> for ProviderActiveWindowRequest {
    fn from(value: crate::domain::provider::ProviderActiveWindow) -> Self {
        Self {
            days: value.days.into_iter().map(Into::into).collect(),
            start_local_time: value.start_local_time,
            end_local_time: value.end_local_time,
        }
    }
}

impl From<CreditGrantLimitMode> for CreditGrantLimitModeDto {
    fn from(value: CreditGrantLimitMode) -> Self {
        match value {
            CreditGrantLimitMode::FixedLimit => Self::FixedLimit,
            CreditGrantLimitMode::CmsDebtLimit => Self::CmsDebtLimit,
            CreditGrantLimitMode::OutstandingCreditLimit => Self::OutstandingCreditLimit,
        }
    }
}
