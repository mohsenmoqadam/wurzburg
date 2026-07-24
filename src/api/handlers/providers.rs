use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{OriginalUri, Path, Query, State, rejection::QueryRejection},
    http::{HeaderMap, Method, StatusCode},
    response::Response,
};
use chrono::{DateTime, NaiveTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{
    api::{
        auth::{extract_trusted_actor, require_scope},
        command::{MutationCommandContext, trusted_audit_context},
        error::ApiError,
        idempotency::{canonical_request_hash, require_idempotency_key},
        request_context::extract_trusted_request_context,
        response::success_response,
        result_codes::WurzburgResultCode,
    },
    domain::provider::{
        CreditGrantLimitMode, NewProvider, Provider, ProviderActiveWindow, ProviderContact,
        ProviderContactType, ProviderLedgerAccountBalance, ProviderListCursor, ProviderListItem,
        ProviderListQuery, ProviderOperationalProfile, ProviderStatus, ProviderWeekday,
    },
    services::{
        provider::{CreateProviderOutcome, ProviderProvisioningDisposition, ProviderService},
        provider_kafka::{
            ProviderKafkaCommandResult, ProviderKafkaCredentialBundle, ProviderKafkaService,
        },
    },
    state::AppState,
};

const CREATE_PROVIDER_OPERATION: &str = "providers.create";
const ASSIGN_PROVIDER_RANGE_OPERATION: &str = "providers.assign_card_range";
const ACTIVATE_PROVIDER_OPERATION: &str = "providers.activate";
const SUSPEND_PROVIDER_OPERATION: &str = "providers.suspend";
const DEACTIVATE_PROVIDER_OPERATION: &str = "providers.deactivate";
const PROVISION_PROVIDER_KAFKA_OPERATION: &str = "providers.kafka.provision";
const ROTATE_PROVIDER_KAFKA_OPERATION: &str = "providers.kafka.rotate";
const SUSPEND_PROVIDER_KAFKA_OPERATION: &str = "providers.kafka.suspend";
const RESUME_PROVIDER_KAFKA_OPERATION: &str = "providers.kafka.resume";

#[derive(Clone, Copy)]
struct ProviderKafkaCommandDefinition {
    operation_type: &'static str,
    action: crate::db::oracle::ProviderKafkaCommandAction,
}

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

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ListProvidersQuery {
    pub status: Option<String>,
    pub tax_id: Option<String>,
    pub page_size: Option<u32>,
    pub page_token: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ProviderListItemResponse {
    pub provider_id: Uuid,
    pub legal_name: String,
    pub trade_name: String,
    pub tax_id: Option<String>,
    pub status: ProviderStatusDto,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListProvidersResponse {
    pub data: Vec<ProviderListItemResponse>,
    pub next_page_token: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ProviderLedgerResponse {
    pub provider_id: Uuid,
    pub currency: String,
    pub accounts: Vec<ProviderLedgerAccountResponse>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ProviderLedgerAccountResponse {
    pub account_category: String,
    pub tigerbeetle_account_id: Uuid,
    pub debits_posted: String,
    pub credits_posted: String,
    pub debits_pending: String,
    pub credits_pending: String,
    pub posted_balance: String,
    pub effective_balance: String,
    pub status: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct ProviderPageToken {
    created_at: DateTime<Utc>,
    provider_id: Uuid,
    filter_hash: String,
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
    Disabled,
    Pending,
    Succeeded,
    Failed,
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

#[derive(Serialize, ToSchema)]
pub struct ProviderKafkaCredentialsResponse {
    pub provider_id: Uuid,
    pub topic: String,
    pub brokers: Vec<String>,
    pub security_protocol: String,
    pub sasl_mechanism: String,
    pub username: String,
    pub consumer_group: String,
    #[schema(value_type = String, format = Password)]
    pub password: crate::security::provider_kafka_cipher::SecretBytes,
    pub security_cert: Option<String>,
    pub credential_version: u64,
    pub credential_status: String,
}

#[derive(Serialize, ToSchema)]
pub struct ProviderKafkaStatusResponse {
    pub provider_id: Uuid,
    pub access_status: String,
    pub active_credential_version: Option<u64>,
    pub candidate_credential_version: Option<u64>,
    pub latest_operation: Option<ProviderKafkaOperationStatusResponse>,
}

#[derive(Serialize, ToSchema)]
pub struct ProviderKafkaOperationStatusResponse {
    pub operation_id: Uuid,
    pub operation_type: String,
    pub status: String,
    pub attempt_count: u32,
    pub error_code: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub struct ProviderKafkaCertificateResponse {
    pub security_cert: String,
}

#[derive(Deserialize, ToSchema)]
pub struct ProviderKafkaCommandRequest {
    pub reason: String,
}

#[derive(Serialize, ToSchema)]
pub struct ProviderKafkaCommandResponse {
    pub provider_id: Uuid,
    pub operation_id: Uuid,
    pub credential_status: String,
    pub credential_version: Option<u64>,
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
        state.provider_kafka_credentials.clone(),
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
        state.provider_kafka_credentials.clone(),
    );
    let provider = service.get_provider(&actor, provider_id).await?;
    success_response(StatusCode::OK, ProviderResponse::new(provider))
}

#[utoipa::path(
    get,
    path = "/api/v1/providers",
    tag = "Providers",
    params(
        ("status" = Option<String>, Query, description = "Exact provider lifecycle status."),
        ("tax_id" = Option<String>, Query, description = "Exact normalized tax identifier."),
        ("page_size" = Option<u32>, Query, description = "Page size from 1 through 200; defaults to 50."),
        ("page_token" = Option<String>, Query, description = "Opaque token returned by the previous page.")
    ),
    responses(
        (status = 200, body = ListProvidersResponse),
        (status = 400, body = crate::api::error::ApiErrorResponse),
        (status = 401, body = crate::api::error::ApiErrorResponse),
        (status = 403, body = crate::api::error::ApiErrorResponse),
        (status = 503, body = crate::api::error::ApiErrorResponse)
    ),
    security(("wso2_backend_bearer"=[]))
)]
#[tracing::instrument(skip(state, headers, query))]
pub async fn list_providers(
    State(state): State<Arc<AppState>>,
    query: Result<Query<ListProvidersQuery>, QueryRejection>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let Query(query) = query.map_err(|_| invalid_provider_filter("query"))?;
    let request_context =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request_context, &state.config.wso2)?;
    let status = query
        .status
        .as_deref()
        .map(parse_provider_status)
        .transpose()?;
    let tax_id = normalize_provider_filter(query.tax_id, "tax_id")?;
    let page_size = query.page_size.unwrap_or(50);
    if !(1..=200).contains(&page_size) {
        return Err(invalid_provider_filter("page_size"));
    }
    let cursor = query
        .page_token
        .as_deref()
        .map(|token| parse_provider_page_token(token, status, tax_id.as_deref()))
        .transpose()?;
    let service = ProviderService::new(
        state.db.clone(),
        state.tb_client.clone(),
        state.config.tigerbeetle.clone(),
        state.provider_kafka_credentials.clone(),
    );
    let page = service
        .list_providers(
            &actor,
            ProviderListQuery {
                status,
                tax_id: tax_id.clone(),
                limit: page_size,
                cursor,
            },
        )
        .await?;
    let next_page_token = page
        .next_cursor
        .map(|cursor| format_provider_page_token(cursor, status, tax_id));
    success_response(
        StatusCode::OK,
        ListProvidersResponse {
            data: page.items.into_iter().map(Into::into).collect(),
            next_page_token,
        },
    )
}

#[utoipa::path(
    get,
    path = "/api/v1/providers/{provider_id}/ledger",
    tag = "Providers",
    params(("provider_id" = Uuid, Path)),
    responses(
        (status = 200, body = ProviderLedgerResponse),
        (status = 401, body = crate::api::error::ApiErrorResponse),
        (status = 403, body = crate::api::error::ApiErrorResponse),
        (status = 404, body = crate::api::error::ApiErrorResponse),
        (status = 503, body = crate::api::error::ApiErrorResponse)
    ),
    security(("wso2_backend_bearer"=[]))
)]
#[tracing::instrument(skip(state, headers), fields(provider_id=%provider_id))]
pub async fn get_provider_ledger(
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
        state.provider_kafka_credentials.clone(),
    );
    let accounts = service.get_provider_ledger(&actor, provider_id).await?;
    success_response(
        StatusCode::OK,
        ProviderLedgerResponse {
            provider_id,
            currency: "IRR".to_string(),
            accounts: accounts.into_iter().map(Into::into).collect(),
        },
    )
}

#[utoipa::path(
    get,
    path = "/api/v1/providers/{provider_id}/kafka/credentials",
    tag = "Providers",
    params(("provider_id" = Uuid, Path)),
    responses(
        (status = 200, body = ProviderKafkaCredentialsResponse),
        (status = 401, body = crate::api::error::ApiErrorResponse),
        (status = 403, body = crate::api::error::ApiErrorResponse),
        (status = 404, body = crate::api::error::ApiErrorResponse),
        (status = 409, body = crate::api::error::ApiErrorResponse)
    ),
    security(("wso2_backend_bearer" = []))
)]
#[tracing::instrument(skip(state, headers), fields(provider_id=%provider_id))]
pub async fn get_provider_kafka_credentials(
    State(state): State<Arc<AppState>>,
    Path(provider_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let request_context =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request_context, &state.config.wso2)?;
    let credentials = state
        .provider_kafka_credentials
        .clone()
        .ok_or_else(|| ApiError::new(WurzburgResultCode::ProviderKafkaAccessNotFound))?;
    let service = ProviderKafkaService::new(
        state.db.clone(),
        state.kafka_admin.clone(),
        credentials,
        state.config.provider_kafka_access.scram_iterations,
    );
    let response = service
        .read_credentials(
            &actor,
            trusted_audit_context(&actor, &request_context),
            provider_id,
        )
        .await?;
    success_response(
        StatusCode::OK,
        ProviderKafkaCredentialsResponse::from(response),
    )
}

#[utoipa::path(
    get,
    path = "/api/v1/providers/{provider_id}/kafka/status",
    tag = "Providers",
    params(("provider_id" = Uuid, Path)),
    responses(
        (status = 200, body = ProviderKafkaStatusResponse),
        (status = 401, body = crate::api::error::ApiErrorResponse),
        (status = 403, body = crate::api::error::ApiErrorResponse),
        (status = 404, body = crate::api::error::ApiErrorResponse)
    ),
    security(("wso2_backend_bearer" = []))
)]
#[tracing::instrument(skip(state, headers), fields(provider_id=%provider_id))]
pub async fn get_provider_kafka_status(
    State(state): State<Arc<AppState>>,
    Path(provider_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let request_context =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request_context, &state.config.wso2)?;
    let credentials = state
        .provider_kafka_credentials
        .clone()
        .ok_or_else(|| ApiError::new(WurzburgResultCode::ProviderKafkaAccessNotFound))?;
    let service = ProviderKafkaService::new(
        state.db.clone(),
        state.kafka_admin.clone(),
        credentials,
        state.config.provider_kafka_access.scram_iterations,
    );
    let status = service.read_status(&actor, provider_id).await?;
    success_response(StatusCode::OK, ProviderKafkaStatusResponse::from(status))
}

#[utoipa::path(
    get,
    path = "/api/v1/providers/kafka/certificate",
    tag = "Providers",
    responses(
        (status = 200, body = ProviderKafkaCertificateResponse),
        (status = 401, body = crate::api::error::ApiErrorResponse),
        (status = 403, body = crate::api::error::ApiErrorResponse),
        (status = 404, body = crate::api::error::ApiErrorResponse)
    ),
    security(("wso2_backend_bearer" = []))
)]
#[tracing::instrument(skip(state, headers))]
pub async fn get_provider_kafka_certificate(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let request_context =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request_context, &state.config.wso2)?;
    require_scope(&actor, "provider.kafka_credentials:read")?;
    let security_cert = state
        .provider_kafka_credentials
        .as_ref()
        .and_then(|factory| factory.security_cert().map(ToString::to_string))
        .ok_or_else(|| ApiError::new(WurzburgResultCode::ProviderKafkaAccessNotFound))?;
    success_response(
        StatusCode::OK,
        ProviderKafkaCertificateResponse { security_cert },
    )
}

macro_rules! provider_kafka_command_handler {
    ($function:ident, $path:literal, $operation:expr, $action:expr) => {
        #[utoipa::path(
            post,
            path = $path,
            tag = "Providers",
            request_body = ProviderKafkaCommandRequest,
            params(("provider_id" = Uuid, Path), ("Idempotency-Key" = String, Header)),
            responses(
                (status = 202, body = ProviderKafkaCommandResponse),
                (status = 200, description = "Idempotent response replay", body = serde_json::Value),
                (status = 400, body = crate::api::error::ApiErrorResponse),
                (status = 401, body = crate::api::error::ApiErrorResponse),
                (status = 403, body = crate::api::error::ApiErrorResponse),
                (status = 404, body = crate::api::error::ApiErrorResponse),
                (status = 409, body = crate::api::error::ApiErrorResponse)
            ),
            security(("wso2_backend_bearer" = []))
        )]
        pub async fn $function(
            state: State<Arc<AppState>>,
            path: Path<Uuid>,
            method: Method,
            uri: OriginalUri,
            headers: HeaderMap,
            body: Bytes,
        ) -> Result<Response, ApiError> {
            command_provider_kafka(
                state,
                path,
                method,
                uri,
                headers,
                body,
                ProviderKafkaCommandDefinition {
                    operation_type: $operation,
                    action: $action,
                },
            )
            .await
        }
    };
}

provider_kafka_command_handler!(
    provision_provider_kafka,
    "/api/v1/providers/{provider_id}/kafka/provision",
    PROVISION_PROVIDER_KAFKA_OPERATION,
    crate::db::oracle::ProviderKafkaCommandAction::Provision
);
provider_kafka_command_handler!(
    rotate_provider_kafka_credentials,
    "/api/v1/providers/{provider_id}/kafka/rotate-credentials",
    ROTATE_PROVIDER_KAFKA_OPERATION,
    crate::db::oracle::ProviderKafkaCommandAction::Rotate
);
provider_kafka_command_handler!(
    suspend_provider_kafka,
    "/api/v1/providers/{provider_id}/kafka/suspend",
    SUSPEND_PROVIDER_KAFKA_OPERATION,
    crate::db::oracle::ProviderKafkaCommandAction::Suspend
);
provider_kafka_command_handler!(
    resume_provider_kafka,
    "/api/v1/providers/{provider_id}/kafka/resume",
    RESUME_PROVIDER_KAFKA_OPERATION,
    crate::db::oracle::ProviderKafkaCommandAction::Resume
);

async fn command_provider_kafka(
    State(state): State<Arc<AppState>>,
    Path(provider_id): Path<Uuid>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
    definition: ProviderKafkaCommandDefinition,
) -> Result<Response, ApiError> {
    let request_context =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request_context, &state.config.wso2)?;
    let idempotency_key = require_idempotency_key(&headers)?;
    let request_hash = canonical_request_hash(&method, uri.path(), &body);
    let request: ProviderKafkaCommandRequest = serde_json::from_slice(&body)
        .map_err(|_| ApiError::new(WurzburgResultCode::InvalidProviderContract))?;
    let context = MutationCommandContext {
        operation_type: definition.operation_type.to_string(),
        actor,
        request: request_context,
        idempotency_key,
        request_hash,
    };
    let credentials = state
        .provider_kafka_credentials
        .clone()
        .ok_or_else(|| ApiError::new(WurzburgResultCode::ProviderKafkaAccessNotFound))?;
    let service = ProviderKafkaService::new(
        state.db.clone(),
        state.kafka_admin.clone(),
        credentials,
        state.config.provider_kafka_access.scram_iterations,
    );
    match service
        .command_access(&context, provider_id, definition.action, request.reason)
        .await?
    {
        ProviderKafkaCommandResult::Accepted(snapshot) => {
            success_response(StatusCode::ACCEPTED, snapshot)
        }
        ProviderKafkaCommandResult::Replayed(snapshot) => {
            success_response(StatusCode::OK, snapshot)
        }
    }
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
        state.provider_kafka_credentials.clone(),
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
        state.provider_kafka_credentials.clone(),
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
            kafka_provisioning_status: match provider.kafka_provisioning_status {
                crate::domain::provider::ProviderKafkaProvisioningStatus::Disabled => {
                    ProviderKafkaProvisioningStatusDto::Disabled
                }
                crate::domain::provider::ProviderKafkaProvisioningStatus::Pending => {
                    ProviderKafkaProvisioningStatusDto::Pending
                }
                crate::domain::provider::ProviderKafkaProvisioningStatus::Succeeded => {
                    ProviderKafkaProvisioningStatusDto::Succeeded
                }
                crate::domain::provider::ProviderKafkaProvisioningStatus::Failed => {
                    ProviderKafkaProvisioningStatusDto::Failed
                }
            },
            created_at: provider.created_at,
            updated_at: provider.updated_at,
        }
    }
}

impl From<ProviderListItem> for ProviderListItemResponse {
    fn from(value: ProviderListItem) -> Self {
        Self {
            provider_id: value.provider_id,
            legal_name: value.legal_name,
            trade_name: value.trade_name,
            tax_id: value.tax_id,
            status: value.status.into(),
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}

impl From<ProviderLedgerAccountBalance> for ProviderLedgerAccountResponse {
    fn from(value: ProviderLedgerAccountBalance) -> Self {
        Self {
            account_category: value.account_category.as_db_value().to_string(),
            tigerbeetle_account_id: value.tigerbeetle_account_id,
            debits_posted: value.debits_posted,
            credits_posted: value.credits_posted,
            debits_pending: value.debits_pending,
            credits_pending: value.credits_pending,
            posted_balance: value.posted_balance,
            effective_balance: value.effective_balance,
            status: value.status,
        }
    }
}

impl From<ProviderKafkaCredentialBundle> for ProviderKafkaCredentialsResponse {
    fn from(value: ProviderKafkaCredentialBundle) -> Self {
        Self {
            provider_id: value.provider_id,
            topic: value.topic_name,
            brokers: value.bootstrap_servers,
            security_protocol: value.security_protocol,
            sasl_mechanism: value.sasl_mechanism,
            username: value.username,
            consumer_group: value.consumer_group,
            password: value.password,
            security_cert: value.security_cert,
            credential_version: value.credential_version,
            credential_status: value.credential_status,
        }
    }
}

impl From<crate::services::provider_kafka::ProviderKafkaAccessStatus>
    for ProviderKafkaStatusResponse
{
    fn from(value: crate::services::provider_kafka::ProviderKafkaAccessStatus) -> Self {
        Self {
            provider_id: value.provider_id,
            access_status: value.access_status,
            active_credential_version: value.active_credential_version,
            candidate_credential_version: value.candidate_credential_version,
            latest_operation: value.latest_operation.map(|operation| {
                ProviderKafkaOperationStatusResponse {
                    operation_id: operation.operation_id,
                    operation_type: operation.operation_type,
                    status: operation.status,
                    attempt_count: operation.attempt_count,
                    error_code: operation.error_code,
                }
            }),
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
            kafka_access: None,
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

fn parse_provider_status(value: &str) -> Result<ProviderStatus, ApiError> {
    ProviderStatus::from_db_value(value).ok_or_else(|| invalid_provider_filter("status"))
}

fn normalize_provider_filter(
    value: Option<String>,
    filter: &'static str,
) -> Result<Option<String>, ApiError> {
    value
        .map(|value| {
            let value = value.trim().to_string();
            if value.is_empty() || value.len() > 64 || value.chars().any(char::is_control) {
                Err(invalid_provider_filter(filter))
            } else {
                Ok(value)
            }
        })
        .transpose()
}

fn parse_provider_page_token(
    value: &str,
    status: Option<ProviderStatus>,
    tax_id: Option<&str>,
) -> Result<ProviderListCursor, ApiError> {
    let bytes = decode_hex(value).ok_or_else(|| invalid_provider_filter("page_token"))?;
    let token: ProviderPageToken =
        serde_json::from_slice(&bytes).map_err(|_| invalid_provider_filter("page_token"))?;
    if token.filter_hash != provider_filter_hash(status, tax_id) {
        return Err(invalid_provider_filter("page_token"));
    }
    Ok(ProviderListCursor {
        created_at: token.created_at,
        provider_id: token.provider_id,
    })
}

fn format_provider_page_token(
    cursor: ProviderListCursor,
    status: Option<ProviderStatus>,
    tax_id: Option<String>,
) -> String {
    let token = ProviderPageToken {
        created_at: cursor.created_at,
        provider_id: cursor.provider_id,
        filter_hash: provider_filter_hash(status, tax_id.as_deref()),
    };
    encode_hex(&serde_json::to_vec(&token).expect("provider page token is serializable"))
}

fn provider_filter_hash(status: Option<ProviderStatus>, tax_id: Option<&str>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(status.map(ProviderStatus::as_db_value).unwrap_or(""));
    hasher.update([0]);
    hasher.update(tax_id.unwrap_or(""));
    encode_hex(&hasher.finalize())
}

fn encode_hex(bytes: &[u8]) -> String {
    use std::fmt::Write;

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

fn decode_hex(value: &str) -> Option<Vec<u8>> {
    if value.len() % 2 != 0 || value.len() > 4096 {
        return None;
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair).ok()?;
            u8::from_str_radix(pair, 16).ok()
        })
        .collect()
}

fn invalid_provider_filter(filter: &'static str) -> ApiError {
    ApiError::with_details(
        WurzburgResultCode::InvalidProviderFilter,
        serde_json::json!({ "filter": filter }),
    )
}

fn empty_metadata() -> serde_json::Value {
    serde_json::json!({})
}
