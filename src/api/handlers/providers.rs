use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{OriginalUri, Path, State},
    http::{HeaderMap, Method, StatusCode},
    response::Response,
};
use chrono::{DateTime, NaiveTime, Utc};
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
        response::success_response,
        result_codes::WurzburgResultCode,
    },
    domain::provider::{
        CreditGrantLimitMode, NewProvider, Provider, ProviderActiveWindow, ProviderContact,
        ProviderContactType, ProviderOperationalProfile, ProviderStatus, ProviderWeekday,
    },
    services::provider::{CreateProviderOutcome, ProviderProvisioningDisposition, ProviderService},
    state::AppState,
};

const CREATE_PROVIDER_OPERATION: &str = "providers.create";
const ASSIGN_PROVIDER_RANGE_OPERATION: &str = "providers.assign_card_range";
const ACTIVATE_PROVIDER_OPERATION: &str = "providers.activate";
const SUSPEND_PROVIDER_OPERATION: &str = "providers.suspend";
const DEACTIVATE_PROVIDER_OPERATION: &str = "providers.deactivate";

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateProviderRequest {
    pub legal_name: String,
    pub trade_name: String,
    pub tax_id: Option<String>,
    pub registration_number: Option<String>,
    pub email_address: Option<String>,
    pub website_url: Option<String>,
    pub mailing_address: Option<String>,
    #[serde(default = "empty_metadata")]
    pub metadata: serde_json::Value,
    #[serde(default)]
    pub contacts: Vec<ProviderContactRequest>,
    pub operational_profile: ProviderOperationalProfileRequest,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ProviderContactRequest {
    pub contact_type: ProviderContactTypeDto,
    pub name: Option<String>,
    pub email: Option<String>,
    pub phone: Option<String>,
    pub mobile: Option<String>,
    #[serde(default)]
    pub sms_enabled: bool,
    #[serde(default = "empty_metadata")]
    pub metadata: serde_json::Value,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ProviderOperationalProfileRequest {
    pub effective_at: DateTime<Utc>,
    pub profile: ProviderOperationalControlsRequest,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ProviderOperationalControlsRequest {
    pub timezone: String,
    pub user_onboarding: ProviderUserOnboardingControlRequest,
    pub credit_grant: ProviderCreditGrantControlRequest,
    pub credit_return: ProviderEnabledControlRequest,
    pub card_operations: ProviderCardOperationsControlRequest,
    pub event_delivery: ProviderEventDeliveryControlRequest,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ProviderUserOnboardingControlRequest {
    pub enabled: bool,
    #[serde(default)]
    pub active_windows: Vec<ProviderActiveWindowRequest>,
    pub max_total_users: Option<u64>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ProviderActiveWindowRequest {
    pub days: Vec<ProviderWeekdayDto>,
    pub start_local_time: NaiveTime,
    pub end_local_time: NaiveTime,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ProviderCreditGrantControlRequest {
    pub enabled: bool,
    pub mode: CreditGrantLimitModeDto,
    pub limit_amount_rials: u64,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ProviderEnabledControlRequest {
    pub enabled: bool,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ProviderCardOperationsControlRequest {
    pub new_assignment_enabled: bool,
    pub same_pan_reprint_enabled: bool,
    pub new_pan_replacement_enabled: bool,
    pub attach_existing_multi_provider_card_enabled: bool,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ProviderEventDeliveryControlRequest {
    pub enabled: bool,
    pub disabled_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProviderContactTypeDto {
    Finance,
    Technical,
    Operations,
    Security,
    Notification,
    Legal,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub enum CreditGrantLimitModeDto {
    FixedLimit,
    CmsDebtLimit,
    OutstandingCreditLimit,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProviderWeekdayDto {
    Saturday,
    Sunday,
    Monday,
    Tuesday,
    Wednesday,
    Thursday,
    Friday,
}

#[derive(Debug, Clone, Copy, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProviderStatusDto {
    PendingProvisioning,
    Ready,
    Active,
    Suspended,
    Inactive,
    Failed,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ProviderResponse {
    pub provider_id: Uuid,
    pub legal_name: String,
    pub trade_name: String,
    pub tax_id: Option<String>,
    pub registration_number: Option<String>,
    pub email_address: Option<String>,
    pub website_url: Option<String>,
    pub mailing_address: Option<String>,
    pub status: ProviderStatusDto,
    pub metadata: serde_json::Value,
    pub core_provisioning_status: ProviderCoreProvisioningStatusDto,
    pub kafka_provisioning_status: ProviderKafkaProvisioningStatusDto,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProviderCoreProvisioningStatusDto {
    Pending,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Copy, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProviderKafkaProvisioningStatusDto {
    Pending,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct AssignProviderCardRangeRequest {
    pub card_range_id: Uuid,
    pub reason: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ProviderCardRangeAssignmentResponse {
    pub provider_id: Uuid,
    pub card_range_id: Uuid,
    pub status: String,
    pub range_control_operation_id: Uuid,
    pub policy_operation_id: Option<Uuid>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ProviderLifecycleRequest {
    pub reason: String,
}

#[utoipa::path(
    post,
    path = "/api/v1/providers",
    tag = "Providers",
    request_body = CreateProviderRequest,
    params(("Idempotency-Key" = String, Header)),
    responses(
        (status = 201, description = "Provider core accounts verified", body = ProviderResponse),
        (status = 202, description = "Provider core provisioning continues durably", body = ProviderResponse),
        (status = 200, description = "Idempotent response replay", body = serde_json::Value),
        (status = 400, body = crate::api::error::ApiErrorResponse),
        (status = 401, body = crate::api::error::ApiErrorResponse),
        (status = 403, body = crate::api::error::ApiErrorResponse),
        (status = 409, body = crate::api::error::ApiErrorResponse),
        (status = 500, body = crate::api::error::ApiErrorResponse)
    ),
    security(("wso2_backend_bearer" = []))
)]
#[tracing::instrument(skip(state, headers, body))]
pub async fn create_provider(
    State(state): State<Arc<AppState>>,
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
    let request: CreateProviderRequest = serde_json::from_slice(&body)
        .map_err(|_| ApiError::new(WurzburgResultCode::InvalidProviderContract))?;
    let context = MutationCommandContext {
        operation_type: CREATE_PROVIDER_OPERATION.to_string(),
        actor,
        request: request_context,
        idempotency_key,
        request_hash,
    };
    let service = ProviderService::new(
        state.db.clone(),
        state.tb_client.clone(),
        state.config.tigerbeetle.clone(),
    );
    match service.create_provider(&context, request.into()).await? {
        CreateProviderOutcome::Created {
            provider,
            provisioning,
        } => {
            let status = match provisioning {
                ProviderProvisioningDisposition::Ready => StatusCode::CREATED,
                ProviderProvisioningDisposition::Pending => StatusCode::ACCEPTED,
            };
            success_response(status, ProviderResponse::new(*provider))
        }
        CreateProviderOutcome::Replayed(snapshot) => success_response(StatusCode::OK, snapshot),
    }
}

#[utoipa::path(get, path="/api/v1/providers/{provider_id}", tag="Providers", params(("provider_id"=Uuid, Path)), responses((status=200, body=ProviderResponse), (status=404, body=crate::api::error::ApiErrorResponse)), security(("wso2_backend_bearer"=[])))]
#[tracing::instrument(skip(state, headers), fields(provider_id=%provider_id))]
pub async fn get_provider(
    State(state): State<Arc<AppState>>,
    Path(provider_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let request_context =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request_context, &state.config.wso2)?;
    let service = ProviderService::new(
        state.db.clone(),
        state.tb_client.clone(),
        state.config.tigerbeetle.clone(),
    );
    let provider = service.get_provider(&actor, provider_id).await?;
    success_response(StatusCode::OK, ProviderResponse::new(provider))
}

#[utoipa::path(put, path="/api/v1/providers/{provider_id}/card-range", tag="Providers", request_body=AssignProviderCardRangeRequest, params(("provider_id"=Uuid, Path), ("Idempotency-Key"=String, Header)), responses((status=202, body=ProviderCardRangeAssignmentResponse), (status=409, body=crate::api::error::ApiErrorResponse)), security(("wso2_backend_bearer"=[])))]
#[tracing::instrument(skip(state, headers, body), fields(provider_id=%provider_id))]
pub async fn assign_provider_card_range(
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
    let request: AssignProviderCardRangeRequest = serde_json::from_slice(&body)
        .map_err(|_| ApiError::new(WurzburgResultCode::InvalidProviderContract))?;
    let context = MutationCommandContext {
        operation_type: ASSIGN_PROVIDER_RANGE_OPERATION.to_string(),
        actor,
        request: request_context,
        idempotency_key,
        request_hash,
    };
    let service = ProviderService::new(
        state.db.clone(),
        state.tb_client.clone(),
        state.config.tigerbeetle.clone(),
    );
    match service
        .assign_card_range(&context, provider_id, request.card_range_id, request.reason)
        .await?
    {
        crate::db::oracle::ProviderRangeAssignmentOutcome::Applied(result) => success_response(
            StatusCode::ACCEPTED,
            ProviderCardRangeAssignmentResponse {
                provider_id: result.provider_id,
                card_range_id: result.card_range_id,
                status: "ACTIVE".to_string(),
                range_control_operation_id: result.range_control_operation_id,
                policy_operation_id: result.policy_operation_id,
            },
        ),
        crate::db::oracle::ProviderRangeAssignmentOutcome::Replayed(snapshot) => {
            success_response(StatusCode::OK, snapshot)
        }
        _ => unreachable!("service maps non-success provider range outcomes"),
    }
}

#[utoipa::path(post, path="/api/v1/providers/{provider_id}/activate", tag="Providers", request_body=ProviderLifecycleRequest, params(("provider_id"=Uuid, Path), ("Idempotency-Key"=String, Header)), responses((status=200, body=ProviderResponse), (status=409, body=crate::api::error::ApiErrorResponse)), security(("wso2_backend_bearer"=[])))]
pub async fn activate_provider(
    state: State<Arc<AppState>>,
    path: Path<Uuid>,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    transition_provider(
        state,
        path,
        method,
        uri,
        headers,
        body,
        ProviderTransition {
            operation_type: ACTIVATE_PROVIDER_OPERATION,
            target: ProviderStatus::Active,
        },
    )
    .await
}

#[utoipa::path(post, path="/api/v1/providers/{provider_id}/suspend", tag="Providers", request_body=ProviderLifecycleRequest, params(("provider_id"=Uuid, Path), ("Idempotency-Key"=String, Header)), responses((status=200, body=ProviderResponse), (status=409, body=crate::api::error::ApiErrorResponse)), security(("wso2_backend_bearer"=[])))]
pub async fn suspend_provider(
    state: State<Arc<AppState>>,
    path: Path<Uuid>,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    transition_provider(
        state,
        path,
        method,
        uri,
        headers,
        body,
        ProviderTransition {
            operation_type: SUSPEND_PROVIDER_OPERATION,
            target: ProviderStatus::Suspended,
        },
    )
    .await
}

#[utoipa::path(post, path="/api/v1/providers/{provider_id}/deactivate", tag="Providers", request_body=ProviderLifecycleRequest, params(("provider_id"=Uuid, Path), ("Idempotency-Key"=String, Header)), responses((status=200, body=ProviderResponse), (status=409, body=crate::api::error::ApiErrorResponse)), security(("wso2_backend_bearer"=[])))]
pub async fn deactivate_provider(
    state: State<Arc<AppState>>,
    path: Path<Uuid>,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    transition_provider(
        state,
        path,
        method,
        uri,
        headers,
        body,
        ProviderTransition {
            operation_type: DEACTIVATE_PROVIDER_OPERATION,
            target: ProviderStatus::Inactive,
        },
    )
    .await
}

async fn transition_provider(
    State(state): State<Arc<AppState>>,
    Path(provider_id): Path<Uuid>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
    transition: ProviderTransition,
) -> Result<Response, ApiError> {
    let request_context =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request_context, &state.config.wso2)?;
    let idempotency_key = require_idempotency_key(&headers)?;
    let request_hash = canonical_request_hash(&method, uri.path(), &body);
    let request: ProviderLifecycleRequest = serde_json::from_slice(&body)
        .map_err(|_| ApiError::new(WurzburgResultCode::InvalidProviderContract))?;
    let context = MutationCommandContext {
        operation_type: transition.operation_type.to_string(),
        actor,
        request: request_context,
        idempotency_key,
        request_hash,
    };
    let service = ProviderService::new(
        state.db.clone(),
        state.tb_client.clone(),
        state.config.tigerbeetle.clone(),
    );
    match service
        .transition_provider(&context, provider_id, transition.target, request.reason)
        .await?
    {
        crate::db::oracle::ProviderLifecycleOutcome::Applied(provider) => {
            success_response(StatusCode::OK, ProviderResponse::new(*provider))
        }
        crate::db::oracle::ProviderLifecycleOutcome::Replayed(snapshot) => {
            success_response(StatusCode::OK, snapshot)
        }
        _ => unreachable!("service maps non-success provider lifecycle outcomes"),
    }
}

#[derive(Clone, Copy)]
struct ProviderTransition {
    operation_type: &'static str,
    target: ProviderStatus,
}

impl ProviderResponse {
    fn new(provider: Provider) -> Self {
        let core_provisioning_status = match provider.status {
            ProviderStatus::PendingProvisioning => ProviderCoreProvisioningStatusDto::Pending,
            ProviderStatus::Failed => ProviderCoreProvisioningStatusDto::Failed,
            ProviderStatus::Ready
            | ProviderStatus::Active
            | ProviderStatus::Suspended
            | ProviderStatus::Inactive => ProviderCoreProvisioningStatusDto::Succeeded,
        };
        Self {
            provider_id: provider.provider_id,
            legal_name: provider.legal_name,
            trade_name: provider.trade_name,
            tax_id: provider.tax_id,
            registration_number: provider.registration_number,
            email_address: provider.email_address,
            website_url: provider.website_url,
            mailing_address: provider.mailing_address,
            status: provider.status.into(),
            metadata: provider.metadata,
            core_provisioning_status,
            kafka_provisioning_status: ProviderKafkaProvisioningStatusDto::Pending,
            created_at: provider.created_at,
            updated_at: provider.updated_at,
        }
    }
}

impl From<CreateProviderRequest> for NewProvider {
    fn from(request: CreateProviderRequest) -> Self {
        let controls = request.operational_profile.profile;
        Self {
            provider_id: Uuid::new_v4(),
            legal_name: request.legal_name,
            trade_name: request.trade_name,
            tax_id: request.tax_id,
            registration_number: request.registration_number,
            email_address: request.email_address,
            website_url: request.website_url,
            mailing_address: request.mailing_address,
            metadata: request.metadata,
            contacts: request.contacts.into_iter().map(Into::into).collect(),
            operational_profile: ProviderOperationalProfile {
                effective_at: request.operational_profile.effective_at,
                timezone: controls.timezone,
                user_onboarding_enabled: controls.user_onboarding.enabled,
                active_windows: controls
                    .user_onboarding
                    .active_windows
                    .into_iter()
                    .map(Into::into)
                    .collect(),
                max_total_users: controls.user_onboarding.max_total_users,
                credit_grant_enabled: controls.credit_grant.enabled,
                credit_grant_mode: controls.credit_grant.mode.into(),
                credit_grant_limit_amount_rials: u128::from(
                    controls.credit_grant.limit_amount_rials,
                ),
                credit_return_enabled: controls.credit_return.enabled,
                new_assignment_enabled: controls.card_operations.new_assignment_enabled,
                same_pan_reprint_enabled: controls.card_operations.same_pan_reprint_enabled,
                new_pan_replacement_enabled: controls.card_operations.new_pan_replacement_enabled,
                attach_existing_multi_provider_card_enabled: controls
                    .card_operations
                    .attach_existing_multi_provider_card_enabled,
                event_delivery_enabled: controls.event_delivery.enabled,
                event_delivery_disabled_reason: controls.event_delivery.disabled_reason,
            },
        }
    }
}

impl From<ProviderContactRequest> for ProviderContact {
    fn from(value: ProviderContactRequest) -> Self {
        Self {
            provider_contact_id: Uuid::new_v4(),
            contact_type: value.contact_type.into(),
            name: value.name,
            email: value.email,
            phone: value.phone,
            mobile: value.mobile,
            sms_enabled: value.sms_enabled,
            metadata: value.metadata,
        }
    }
}

impl From<ProviderActiveWindowRequest> for ProviderActiveWindow {
    fn from(value: ProviderActiveWindowRequest) -> Self {
        Self {
            days: value.days.into_iter().map(Into::into).collect(),
            start_local_time: value.start_local_time,
            end_local_time: value.end_local_time,
        }
    }
}

macro_rules! enum_mapping {
    ($source:ty => $target:ty { $($variant:ident),+ $(,)? }) => {
        impl From<$source> for $target {
            fn from(value: $source) -> Self {
                match value { $(<$source>::$variant => <$target>::$variant),+ }
            }
        }
    };
}

enum_mapping!(ProviderContactTypeDto => ProviderContactType { Finance, Technical, Operations, Security, Notification, Legal });
enum_mapping!(CreditGrantLimitModeDto => CreditGrantLimitMode { FixedLimit, CmsDebtLimit, OutstandingCreditLimit });
enum_mapping!(ProviderWeekdayDto => ProviderWeekday { Saturday, Sunday, Monday, Tuesday, Wednesday, Thursday, Friday });
enum_mapping!(ProviderStatus => ProviderStatusDto { PendingProvisioning, Ready, Active, Suspended, Inactive, Failed });

fn empty_metadata() -> serde_json::Value {
    serde_json::json!({})
}
