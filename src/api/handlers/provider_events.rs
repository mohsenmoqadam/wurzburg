use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{OriginalUri, Path, State},
    http::{HeaderMap, Method, StatusCode},
    response::Response,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{
    api::{
        auth::{extract_trusted_actor, require_scope},
        command::MutationCommandContext,
        error::ApiError,
        idempotency::{canonical_request_hash, require_idempotency_key},
        request_context::extract_trusted_request_context,
        response::success_response,
        result_codes::WurzburgResultCode,
    },
    db::oracle::{ProviderEventSubscriptionRecord, ProviderEventSubscriptionSet},
    domain::provider_event::ProviderEventType,
    services::provider_event_subscription::{
        ProviderEventSubscriptionCommandResult, ProviderEventSubscriptionService,
    },
    state::AppState,
};

const REPLACE_SUBSCRIPTIONS_OPERATION: &str = "providers.event_subscriptions.replace";

#[derive(Debug, Clone, Copy, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProviderEventTypeDto {
    ProviderStatusChanged,
    UserOnboarded,
    CardAssigned,
    CardReplaced,
    CreditGranted,
    CreditReturned,
    WithdrawalConfirmed,
    WithdrawalRolledBack,
    FeeCharged,
}

#[derive(Serialize, ToSchema)]
pub struct ProviderEventTypeCatalogResponse {
    pub data: Vec<ProviderEventTypeCatalogItem>,
}

#[derive(Serialize, ToSchema)]
pub struct ProviderEventTypeCatalogItem {
    pub event_type: ProviderEventTypeDto,
    pub schema_versions: Vec<u16>,
    pub contract_artifact: String,
}

#[derive(Deserialize, ToSchema)]
pub struct ReplaceProviderEventSubscriptionsRequest {
    pub expected_version: u64,
    pub subscriptions: Vec<ProviderEventSubscriptionInput>,
    pub reason: String,
}

#[derive(Deserialize, ToSchema)]
pub struct ProviderEventSubscriptionInput {
    pub event_type: ProviderEventTypeDto,
    pub enabled: bool,
}

#[derive(Serialize, ToSchema)]
pub struct ProviderEventSubscriptionsResponse {
    pub provider_id: Uuid,
    pub version: u64,
    pub global_delivery_enabled: bool,
    pub provider_delivery_enabled: bool,
    pub credential_status: Option<String>,
    pub subscriptions: Vec<ProviderEventSubscriptionResponse>,
}

#[derive(Serialize, ToSchema)]
pub struct ProviderEventSubscriptionResponse {
    pub event_type: ProviderEventTypeDto,
    pub schema_versions: Vec<u16>,
    pub configured_enabled: bool,
    pub effective_enabled: bool,
    pub blocked_by: Vec<String>,
}

#[utoipa::path(get, path="/api/v1/admin/provider-event-types", tag="Provider Events", responses((status=200, body=ProviderEventTypeCatalogResponse), (status=403, body=crate::api::error::ApiErrorResponse)), security(("wso2_backend_bearer"=[])))]
#[tracing::instrument(skip(state, headers))]
pub async fn list_provider_event_types(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let request =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request, &state.config.wso2)?;
    require_scope(&actor, "platform.provider_events:read")?;
    success_response(
        StatusCode::OK,
        ProviderEventTypeCatalogResponse {
            data: ProviderEventType::ALL
                .into_iter()
                .map(|event_type| ProviderEventTypeCatalogItem {
                    event_type: event_type.into(),
                    schema_versions: vec![1],
                    contract_artifact: format!(
                        "contracts/provider-events/v1/{}.schema.json",
                        event_type.as_str().to_ascii_lowercase()
                    ),
                })
                .collect(),
        },
    )
}

#[utoipa::path(get, path="/api/v1/admin/providers/{provider_id}/event-subscriptions", tag="Provider Events", params(("provider_id"=Uuid, Path)), responses((status=200, body=ProviderEventSubscriptionsResponse), (status=404, body=crate::api::error::ApiErrorResponse)), security(("wso2_backend_bearer"=[])))]
pub async fn get_admin_provider_event_subscriptions(
    state: State<Arc<AppState>>,
    path: Path<Uuid>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    get_subscriptions(state, path, headers, "admin").await
}

#[utoipa::path(get, path="/api/v1/providers/{provider_id}/event-subscriptions", tag="Provider Events", params(("provider_id"=Uuid, Path)), responses((status=200, body=ProviderEventSubscriptionsResponse), (status=403, body=crate::api::error::ApiErrorResponse), (status=404, body=crate::api::error::ApiErrorResponse)), security(("wso2_backend_bearer"=[])))]
pub async fn get_provider_event_subscriptions(
    state: State<Arc<AppState>>,
    path: Path<Uuid>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    get_subscriptions(state, path, headers, "provider").await
}

async fn get_subscriptions(
    State(state): State<Arc<AppState>>,
    Path(provider_id): Path<Uuid>,
    headers: HeaderMap,
    view: &'static str,
) -> Result<Response, ApiError> {
    let request =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request, &state.config.wso2)?;
    let value = ProviderEventSubscriptionService::new(state.db.clone())
        .get(&actor, provider_id, view)
        .await?;
    success_response(StatusCode::OK, response_from_set(value))
}

#[utoipa::path(put, path="/api/v1/admin/providers/{provider_id}/event-subscriptions", tag="Provider Events", request_body=ReplaceProviderEventSubscriptionsRequest, params(("provider_id"=Uuid, Path), ("Idempotency-Key"=String, Header)), responses((status=200, body=ProviderEventSubscriptionsResponse), (status=400, body=crate::api::error::ApiErrorResponse), (status=409, body=crate::api::error::ApiErrorResponse)), security(("wso2_backend_bearer"=[])))]
#[tracing::instrument(skip(state, headers, body), fields(provider_id=%provider_id))]
pub async fn replace_provider_event_subscriptions(
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
    let request: ReplaceProviderEventSubscriptionsRequest = serde_json::from_slice(&body)
        .map_err(|_| ApiError::new(WurzburgResultCode::ProviderEventSubscriptionInvalid))?;
    let context = MutationCommandContext {
        operation_type: REPLACE_SUBSCRIPTIONS_OPERATION.to_string(),
        actor,
        request: request_context,
        idempotency_key,
        request_hash,
    };
    let subscriptions = request
        .subscriptions
        .into_iter()
        .map(|item| ProviderEventSubscriptionRecord {
            event_type: item.event_type.into(),
            enabled: item.enabled,
        })
        .collect();
    let result = ProviderEventSubscriptionService::new(state.db.clone())
        .replace(
            &context,
            provider_id,
            request.expected_version,
            subscriptions,
            request.reason,
        )
        .await?;
    match result {
        ProviderEventSubscriptionCommandResult::Applied(snapshot)
        | ProviderEventSubscriptionCommandResult::Replayed(snapshot) => {
            success_response(StatusCode::OK, snapshot)
        }
    }
}

fn response_from_set(value: ProviderEventSubscriptionSet) -> ProviderEventSubscriptionsResponse {
    let provider_active = value.provider_status == "ACTIVE";
    let credential_active = value.credential_status.as_deref() == Some("ACTIVE");
    ProviderEventSubscriptionsResponse {
        provider_id: value.provider_id,
        version: value.version,
        global_delivery_enabled: value.global_delivery_enabled,
        provider_delivery_enabled: value.provider_delivery_enabled,
        credential_status: value.credential_status,
        subscriptions: value
            .subscriptions
            .into_iter()
            .map(|item| {
                let mut blocked_by = Vec::new();
                if !value.global_delivery_enabled {
                    blocked_by.push("GLOBAL_DELIVERY_DISABLED".to_string());
                }
                if !value.provider_delivery_enabled {
                    blocked_by.push("PROVIDER_DELIVERY_DISABLED".to_string());
                }
                if !provider_active {
                    blocked_by.push("PROVIDER_NOT_ACTIVE".to_string());
                }
                if !credential_active {
                    blocked_by.push("KAFKA_CREDENTIAL_NOT_ACTIVE".to_string());
                }
                if !item.enabled {
                    blocked_by.push("EVENT_DISABLED".to_string());
                }
                ProviderEventSubscriptionResponse {
                    event_type: item.event_type.into(),
                    schema_versions: vec![1],
                    configured_enabled: item.enabled,
                    effective_enabled: item.enabled
                        && value.global_delivery_enabled
                        && value.provider_delivery_enabled
                        && provider_active
                        && credential_active,
                    blocked_by,
                }
            })
            .collect(),
    }
}

macro_rules! event_type_mapping {
    ($($variant:ident),+ $(,)?) => {
        impl From<ProviderEventType> for ProviderEventTypeDto {
            fn from(value: ProviderEventType) -> Self {
                match value { $(ProviderEventType::$variant => Self::$variant),+ }
            }
        }
        impl From<ProviderEventTypeDto> for ProviderEventType {
            fn from(value: ProviderEventTypeDto) -> Self {
                match value { $(ProviderEventTypeDto::$variant => Self::$variant),+ }
            }
        }
    };
}

event_type_mapping!(
    ProviderStatusChanged,
    UserOnboarded,
    CardAssigned,
    CardReplaced,
    CreditGranted,
    CreditReturned,
    WithdrawalConfirmed,
    WithdrawalRolledBack,
    FeeCharged
);
