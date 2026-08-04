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
        auth::{TrustedActor, extract_trusted_actor, require_scope},
        command::MutationCommandContext,
        error::ApiError,
        idempotency::{canonical_request_hash, require_idempotency_key},
        request_context::extract_trusted_request_context,
        response::success_response,
        result_codes::WurzburgResultCode,
    },
    domain::provider_credit::{
        CreditMovementInitiator, CreditMovementResult, CreditMovementType, ProviderCreditBalance,
    },
    services::provider_credit::{
        GrantCreditCommand, ProviderCreditCommandOutcome, ProviderCreditService,
        ProviderCreditServiceError, ReturnCreditCommand,
    },
    state::AppState,
};

const GRANT_OPERATION: &str = "providers.credits.grant";
const PROVIDER_RETURN_OPERATION: &str = "providers.credits.return";
const CARDHOLDER_RETURN_OPERATION: &str = "cards.credits.return";
const MAX_RIALS: u64 = 9_007_199_254_740_991;

#[derive(Debug, Deserialize, ToSchema)]
pub struct GrantCreditRequest {
    pub user_id: Uuid,
    pub card_number: String,
    pub amount_rials: u64,
    pub provider_reference: String,
    pub reason: String,
    #[schema(value_type=Object)]
    pub metadata: serde_json::Value,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ProviderReturnCreditRequest {
    pub user_id: Uuid,
    pub card_number: String,
    pub expected_remaining_amount_rials: u64,
    pub provider_reference: String,
    pub reason: String,
    #[schema(value_type=Object)]
    pub metadata: serde_json::Value,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct CardholderReturnCreditRequest {
    pub expected_remaining_amount_rials: u64,
    pub reason: String,
}

#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct CreditBalanceQuery {
    pub card_number: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ProviderCreditBalanceResponse {
    pub provider_id: Uuid,
    pub user_id: Uuid,
    pub card_id: Uuid,
    pub currency: String,
    pub observed_remaining_amount_rials: u64,
    pub observed_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CreditMovementResponse {
    pub movement_id: Uuid,
    pub operation_id: Uuid,
    pub movement_type: String,
    pub provider_id: Uuid,
    pub user_id: Uuid,
    pub card_id: Uuid,
    pub amount_rials: u64,
    pub provider_reference: Option<String>,
    pub command_status: String,
    pub event_publication_status: String,
    pub profile_materialization_status: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[utoipa::path(
    post,path="/api/v1/providers/{provider_id}/credits/grant",tag="Provider Credits",request_body=GrantCreditRequest,
    params(("provider_id"=Uuid,Path),("Idempotency-Key"=String,Header),("X-Correlation-Id"=String,Header),("X-Request-Id"=Uuid,Header),("X-WSO2-Client-IP"=String,Header),("X-WSO2-Gateway-Id"=String,Header)),
    responses((status=202,body=CreditMovementResponse),(status=200,body=CreditMovementResponse),(status=400,body=crate::api::error::ApiErrorResponse),(status=403,body=crate::api::error::ApiErrorResponse),(status=404,body=crate::api::error::ApiErrorResponse),(status=409,body=crate::api::error::ApiErrorResponse),(status=503,body=crate::api::error::ApiErrorResponse)),security(("wso2_backend_bearer"=[]))
)]
#[tracing::instrument(skip(state, headers, body), fields(operation.type=GRANT_OPERATION, provider.id=%provider_id))]
pub async fn grant_credit(
    State(state): State<Arc<AppState>>,
    Path(provider_id): Path<Uuid>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let (actor, request) = trusted(&state, &headers)?;
    require_provider(&actor, provider_id, "provider.funding:write")?;
    let idempotency_key = require_idempotency_key(&headers)?;
    let request_hash = canonical_request_hash(&method, uri.path(), &body);
    let request_body: GrantCreditRequest = serde_json::from_slice(&body)
        .map_err(|_| ApiError::new(WurzburgResultCode::ProviderCreditContractInvalid))?;
    validate_grant(&request_body)?;
    let command = GrantCreditCommand {
        user_id: request_body.user_id,
        card_number: request_body.card_number,
        amount_rials: request_body.amount_rials,
        provider_reference: request_body.provider_reference.trim().to_string(),
        reason: request_body.reason.trim().to_string(),
        metadata: request_body.metadata,
    };
    let context = MutationCommandContext {
        operation_type: GRANT_OPERATION.to_string(),
        actor,
        request,
        idempotency_key,
        request_hash,
    };
    respond_command(
        service(&state)
            .grant(&context, provider_id, command)
            .await
            .map_err(map_service_error)?,
    )
}

#[utoipa::path(
    get,path="/api/v1/providers/{provider_id}/users/{user_id}/credit",tag="Provider Credits",
    params(("provider_id"=Uuid,Path),("user_id"=Uuid,Path),CreditBalanceQuery,("X-Correlation-Id"=String,Header),("X-Request-Id"=Uuid,Header),("X-WSO2-Client-IP"=String,Header),("X-WSO2-Gateway-Id"=String,Header)),
    responses((status=200,body=ProviderCreditBalanceResponse),(status=403,body=crate::api::error::ApiErrorResponse),(status=404,body=crate::api::error::ApiErrorResponse),(status=503,body=crate::api::error::ApiErrorResponse)),security(("wso2_backend_bearer"=[]))
)]
#[tracing::instrument(skip(state, headers, query), fields(operation.type="provider_credit.read", provider.id=%provider_id, user.id=%user_id))]
pub async fn get_provider_user_credit(
    State(state): State<Arc<AppState>>,
    Path((provider_id, user_id)): Path<(Uuid, Uuid)>,
    Query(query): Query<CreditBalanceQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let (actor, _) = trusted(&state, &headers)?;
    require_provider(&actor, provider_id, "provider.funding:read")?;
    validate_pan(&query.card_number)?;
    respond_balance(
        service(&state)
            .read_balance(provider_id, user_id, query.card_number)
            .await
            .map_err(map_service_error)?,
    )
}

#[utoipa::path(
    post,path="/api/v1/providers/{provider_id}/credits/return",tag="Provider Credits",request_body=ProviderReturnCreditRequest,
    params(("provider_id"=Uuid,Path),("Idempotency-Key"=String,Header),("X-Correlation-Id"=String,Header),("X-Request-Id"=Uuid,Header),("X-WSO2-Client-IP"=String,Header),("X-WSO2-Gateway-Id"=String,Header)),
    responses((status=202,body=CreditMovementResponse),(status=200,body=CreditMovementResponse),(status=400,body=crate::api::error::ApiErrorResponse),(status=403,body=crate::api::error::ApiErrorResponse),(status=404,body=crate::api::error::ApiErrorResponse),(status=409,body=crate::api::error::ApiErrorResponse),(status=503,body=crate::api::error::ApiErrorResponse)),security(("wso2_backend_bearer"=[]))
)]
#[tracing::instrument(skip(state, headers, body), fields(operation.type=PROVIDER_RETURN_OPERATION, provider.id=%provider_id))]
pub async fn return_provider_credit(
    State(state): State<Arc<AppState>>,
    Path(provider_id): Path<Uuid>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let (actor, request) = trusted(&state, &headers)?;
    require_provider(&actor, provider_id, "provider.funding:write")?;
    let idempotency_key = require_idempotency_key(&headers)?;
    let request_hash = canonical_request_hash(&method, uri.path(), &body);
    let value: ProviderReturnCreditRequest = serde_json::from_slice(&body)
        .map_err(|_| ApiError::new(WurzburgResultCode::ProviderCreditContractInvalid))?;
    validate_return(
        &value.card_number,
        value.expected_remaining_amount_rials,
        &value.reason,
        Some(&value.provider_reference),
    )?;
    let command = ReturnCreditCommand {
        user_id: value.user_id,
        card_number: value.card_number,
        expected_remaining_amount_rials: value.expected_remaining_amount_rials,
        provider_reference: Some(value.provider_reference.trim().to_string()),
        initiated_by: CreditMovementInitiator::Provider,
        reason: value.reason.trim().to_string(),
        metadata: value.metadata,
    };
    let context = MutationCommandContext {
        operation_type: PROVIDER_RETURN_OPERATION.to_string(),
        actor,
        request,
        idempotency_key,
        request_hash,
    };
    respond_command(
        service(&state)
            .return_full_balance(&context, provider_id, command)
            .await
            .map_err(map_service_error)?,
    )
}

#[utoipa::path(
    get,path="/api/v1/cards/{card_number}/providers/{provider_id}/credit",tag="Provider Credits",
    params(("card_number"=String,Path),("provider_id"=Uuid,Path),("X-Correlation-Id"=String,Header),("X-Request-Id"=Uuid,Header),("X-WSO2-Client-IP"=String,Header),("X-WSO2-Gateway-Id"=String,Header)),
    responses((status=200,body=ProviderCreditBalanceResponse),(status=403,body=crate::api::error::ApiErrorResponse),(status=404,body=crate::api::error::ApiErrorResponse),(status=503,body=crate::api::error::ApiErrorResponse)),security(("wso2_backend_bearer"=[]))
)]
#[tracing::instrument(skip(state, headers, card_number), fields(operation.type="cardholder_credit.read", provider.id=%provider_id))]
pub async fn get_cardholder_credit(
    State(state): State<Arc<AppState>>,
    Path((card_number, provider_id)): Path<(String, Uuid)>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let (actor, _) = trusted(&state, &headers)?;
    require_scope(&actor, "card.credit:read")?;
    let user_id = require_cardholder(&actor)?;
    validate_pan(&card_number)?;
    respond_balance(
        service(&state)
            .read_balance(provider_id, user_id, card_number)
            .await
            .map_err(map_service_error)?,
    )
}

#[utoipa::path(
    post,path="/api/v1/cards/{card_number}/providers/{provider_id}/credit/return",tag="Provider Credits",request_body=CardholderReturnCreditRequest,
    params(("card_number"=String,Path),("provider_id"=Uuid,Path),("Idempotency-Key"=String,Header),("X-Correlation-Id"=String,Header),("X-Request-Id"=Uuid,Header),("X-WSO2-Client-IP"=String,Header),("X-WSO2-Gateway-Id"=String,Header)),
    responses((status=202,body=CreditMovementResponse),(status=200,body=CreditMovementResponse),(status=400,body=crate::api::error::ApiErrorResponse),(status=403,body=crate::api::error::ApiErrorResponse),(status=404,body=crate::api::error::ApiErrorResponse),(status=409,body=crate::api::error::ApiErrorResponse),(status=503,body=crate::api::error::ApiErrorResponse)),security(("wso2_backend_bearer"=[]))
)]
#[tracing::instrument(skip(state, headers, body, card_number), fields(operation.type=CARDHOLDER_RETURN_OPERATION, provider.id=%provider_id))]
pub async fn return_cardholder_credit(
    State(state): State<Arc<AppState>>,
    Path((card_number, provider_id)): Path<(String, Uuid)>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let (actor, request) = trusted(&state, &headers)?;
    require_scope(&actor, "card.credit:return")?;
    let user_id = require_cardholder(&actor)?;
    let idempotency_key = require_idempotency_key(&headers)?;
    let request_hash = canonical_request_hash(&method, uri.path(), &body);
    let value: CardholderReturnCreditRequest = serde_json::from_slice(&body)
        .map_err(|_| ApiError::new(WurzburgResultCode::ProviderCreditContractInvalid))?;
    validate_return(
        &card_number,
        value.expected_remaining_amount_rials,
        &value.reason,
        None,
    )?;
    let command = ReturnCreditCommand {
        user_id,
        card_number,
        expected_remaining_amount_rials: value.expected_remaining_amount_rials,
        provider_reference: None,
        initiated_by: CreditMovementInitiator::Cardholder,
        reason: value.reason.trim().to_string(),
        metadata: serde_json::json!({}),
    };
    let context = MutationCommandContext {
        operation_type: CARDHOLDER_RETURN_OPERATION.to_string(),
        actor,
        request,
        idempotency_key,
        request_hash,
    };
    respond_command(
        service(&state)
            .return_full_balance(&context, provider_id, command)
            .await
            .map_err(map_service_error)?,
    )
}

fn service(state: &AppState) -> ProviderCreditService {
    ProviderCreditService::new(
        state.db.clone(),
        state.ledger_client.clone(),
        state.config.tigerbeetle.clone(),
        state.card_profile_locks.clone(),
        state.provider_credit_locks.clone(),
    )
}

fn trusted(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<
    (
        TrustedActor,
        crate::api::request_context::TrustedRequestContext,
    ),
    ApiError,
> {
    let request =
        extract_trusted_request_context(headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request, &state.config.wso2)?;
    Ok((actor, request))
}

fn require_provider(
    actor: &TrustedActor,
    provider_id: Uuid,
    scope: &'static str,
) -> Result<(), ApiError> {
    require_scope(actor, scope)?;
    if actor.has_role("wurzburg_platform_admin") {
        return Ok(());
    }
    match actor.provider_id {
        Some(value) if value == provider_id => Ok(()),
        _ => Err(ApiError::new(WurzburgResultCode::ProviderScopeMismatch)),
    }
}

fn require_cardholder(actor: &TrustedActor) -> Result<Uuid, ApiError> {
    if !actor.has_role("wurzburg_cardholder") {
        return Err(ApiError::new(WurzburgResultCode::CardholderScopeMismatch));
    }
    actor
        .user_id
        .ok_or_else(|| ApiError::new(WurzburgResultCode::InvalidActorClaim))
}

fn validate_grant(value: &GrantCreditRequest) -> Result<(), ApiError> {
    validate_pan(&value.card_number)?;
    let valid = value.amount_rials > 0
        && value.amount_rials <= MAX_RIALS
        && valid_text(&value.provider_reference, 255)
        && valid_text(&value.reason, 1000)
        && value.metadata.is_object();
    valid
        .then_some(())
        .ok_or_else(|| ApiError::new(WurzburgResultCode::ProviderCreditContractInvalid))
}

fn validate_return(
    card_number: &str,
    expected: u64,
    reason: &str,
    reference: Option<&str>,
) -> Result<(), ApiError> {
    validate_pan(card_number)?;
    let valid = expected > 0
        && expected <= MAX_RIALS
        && valid_text(reason, 1000)
        && reference.is_none_or(|value| valid_text(value, 255));
    valid
        .then_some(())
        .ok_or_else(|| ApiError::new(WurzburgResultCode::ProviderCreditContractInvalid))
}

fn validate_pan(value: &str) -> Result<(), ApiError> {
    let valid = value.len() == 16
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && !value.starts_with('0');
    valid
        .then_some(())
        .ok_or_else(|| ApiError::new(WurzburgResultCode::ProviderCreditContractInvalid))
}

fn valid_text(value: &str, max: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max && !value.chars().any(char::is_control)
}

fn respond_balance(
    value: Result<ProviderCreditBalance, ProviderCreditCommandOutcome>,
) -> Result<Response, ApiError> {
    match value {
        Ok(value) => success_response(StatusCode::OK, ProviderCreditBalanceResponse::from(value)),
        Err(outcome) => Err(map_outcome(outcome)),
    }
}

fn respond_command(value: ProviderCreditCommandOutcome) -> Result<Response, ApiError> {
    match value {
        ProviderCreditCommandOutcome::Applied(value) => {
            success_response(StatusCode::ACCEPTED, CreditMovementResponse::from(value))
        }
        ProviderCreditCommandOutcome::Replayed(value) => {
            success_response(StatusCode::OK, CreditMovementResponse::from(value))
        }
        other => Err(map_outcome(other)),
    }
}

fn map_outcome(value: ProviderCreditCommandOutcome) -> ApiError {
    let code = match value {
        ProviderCreditCommandOutcome::Applied(_) | ProviderCreditCommandOutcome::Replayed(_) => {
            unreachable!()
        }
        ProviderCreditCommandOutcome::ProviderNotFound => WurzburgResultCode::ProviderNotFound,
        ProviderCreditCommandOutcome::ProviderNotActive => WurzburgResultCode::ProviderNotActive,
        ProviderCreditCommandOutcome::RelationshipNotFound
        | ProviderCreditCommandOutcome::RelationshipNotActive => {
            WurzburgResultCode::ProviderUserNotFound
        }
        ProviderCreditCommandOutcome::CardNotFound => WurzburgResultCode::CardNotFound,
        ProviderCreditCommandOutcome::CardNotActive => WurzburgResultCode::CardNotActive,
        ProviderCreditCommandOutcome::FundingSourceNotActive => {
            WurzburgResultCode::ProviderCreditNotFound
        }
        ProviderCreditCommandOutcome::GrantDisabled => {
            WurzburgResultCode::ProviderCreditGrantDisabled
        }
        ProviderCreditCommandOutcome::ReturnDisabled => {
            WurzburgResultCode::ProviderCreditReturnDisabled
        }
        ProviderCreditCommandOutcome::ExposureLimitExceeded => {
            WurzburgResultCode::ProviderCreditExposureLimitExceeded
        }
        ProviderCreditCommandOutcome::ExpectedBalanceChanged => {
            WurzburgResultCode::ProviderCreditBalanceChanged
        }
        ProviderCreditCommandOutcome::NoRemainingCredit => WurzburgResultCode::ProviderCreditEmpty,
        ProviderCreditCommandOutcome::ProviderCreditLocked => {
            WurzburgResultCode::ProviderCreditLocked
        }
        ProviderCreditCommandOutcome::CardProfileLocked => WurzburgResultCode::CardProfileLocked,
        ProviderCreditCommandOutcome::ProviderReferenceConflict => {
            WurzburgResultCode::ProviderCreditReferenceConflict
        }
        ProviderCreditCommandOutcome::IdempotencyConflict => {
            WurzburgResultCode::IdempotencyKeyConflict
        }
        ProviderCreditCommandOutcome::IdempotencyInProgress => {
            WurzburgResultCode::IdempotencyInProgress
        }
        ProviderCreditCommandOutcome::RecoveryRequired => {
            WurzburgResultCode::ProviderCreditRecoveryRequired
        }
    };
    ApiError::new(code)
}

fn map_service_error(error: ProviderCreditServiceError) -> ApiError {
    match error {
        ProviderCreditServiceError::Database(error) => ApiError::from_database(error),
        ProviderCreditServiceError::Ledger(error) => {
            tracing::error!(
                error.kind = error.diagnostic_kind(),
                "TigerBeetle credit operation failed"
            );
            ApiError::new(WurzburgResultCode::ProviderLedgerUnavailable)
        }
        ProviderCreditServiceError::CardRuntime(error) => {
            tracing::error!(
                error.kind = error.diagnostic_kind(),
                "Dragonfly card credit coordination failed"
            );
            ApiError::new(WurzburgResultCode::RuntimeProfileUnavailable)
        }
        ProviderCreditServiceError::ProviderRuntime(error) => {
            tracing::error!(
                error.kind = error.diagnostic_kind(),
                "Dragonfly provider credit coordination failed"
            );
            ApiError::new(WurzburgResultCode::RuntimeProfileUnavailable)
        }
        ProviderCreditServiceError::LedgerContract => {
            ApiError::new(WurzburgResultCode::ProviderLedgerUnavailable)
        }
    }
}

impl From<ProviderCreditBalance> for ProviderCreditBalanceResponse {
    fn from(value: ProviderCreditBalance) -> Self {
        Self {
            provider_id: value.provider_id,
            user_id: value.user_id,
            card_id: value.card_id,
            currency: value.currency,
            observed_remaining_amount_rials: value.observed_remaining_amount_rials,
            observed_at: value.observed_at,
        }
    }
}
impl From<CreditMovementResult> for CreditMovementResponse {
    fn from(value: CreditMovementResult) -> Self {
        Self {
            movement_id: value.movement_id,
            operation_id: value.operation_id,
            movement_type: match value.movement_type {
                CreditMovementType::Grant => "GRANT",
                CreditMovementType::ReturnFullBalance => "RETURN_FULL_BALANCE",
            }
            .to_string(),
            provider_id: value.provider_id,
            user_id: value.user_id,
            card_id: value.card_id,
            amount_rials: value.amount_rials,
            provider_reference: value.provider_reference,
            command_status: value.command_status,
            event_publication_status: value.event_publication_status,
            profile_materialization_status: value.profile_materialization_status,
            created_at: value.created_at,
        }
    }
}
