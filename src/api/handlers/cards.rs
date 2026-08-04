use std::{collections::BTreeSet, sync::Arc};

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
    db::oracle::FundingOrderPersistenceOutcome,
    domain::user_card::{FundingOrderResult, FundingOrderSource},
    services::card_funding::{CardFundingService, CardFundingServiceError},
    state::AppState,
};

const OPERATION_TYPE: &str = "cards.funding_order.update";
const MAX_FUNDING_SOURCES: usize = 100;
const MAX_RIALS: u64 = 9_007_199_254_740_991;

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdateFundingOrderRequest {
    pub sources: Vec<FundingOrderSourceRequest>,
    pub expected_card_state_version: i64,
    pub reason: String,
}
#[derive(Debug, Deserialize, ToSchema)]
pub struct FundingOrderSourceRequest {
    pub provider_id: Uuid,
    pub max_amount_rials: Option<u64>,
}
#[derive(Debug, Serialize, ToSchema)]
pub struct FundingOrderResponse {
    pub card_id: Uuid,
    pub masked_card_number: String,
    pub state_version: i64,
    pub sources: Vec<FundingOrderAppliedSourceResponse>,
    pub operation_id: Uuid,
    pub command_status: String,
    pub event_publication_status: String,
    pub profile_materialization_status: String,
}
#[derive(Debug, Serialize, ToSchema)]
pub struct FundingOrderAppliedSourceResponse {
    pub provider_id: Uuid,
    pub priority: u16,
    pub max_amount_rials: Option<u64>,
}

#[utoipa::path(
    put,path="/api/v1/cards/{card_number}/funding-order",tag="Cards",request_body=UpdateFundingOrderRequest,
    params(("card_number"=String,Path),("Idempotency-Key"=String,Header),("X-Correlation-Id"=String,Header),("X-Request-Id"=Uuid,Header),("X-WSO2-Client-IP"=String,Header),("X-WSO2-Gateway-Id"=String,Header)),
    responses((status=202,body=FundingOrderResponse),(status=200,body=FundingOrderResponse),(status=400,body=crate::api::error::ApiErrorResponse),(status=403,body=crate::api::error::ApiErrorResponse),(status=404,body=crate::api::error::ApiErrorResponse),(status=409,body=crate::api::error::ApiErrorResponse),(status=503,body=crate::api::error::ApiErrorResponse)),security(("wso2_backend_bearer"=[]))
)]
#[tracing::instrument(skip(state,headers,body,card_number),fields(operation.type=OPERATION_TYPE))]
pub async fn update_funding_order(
    State(state): State<Arc<AppState>>,
    Path(card_number): Path<String>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let request_context =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request_context, &state.config.wso2)?;
    let expected_owner = if actor.has_scope("platform.cards.funding-order:write") {
        None
    } else {
        require_scope(&actor, "card.funding-order:write")?;
        if !actor.has_role("wurzburg_cardholder") {
            return Err(ApiError::new(WurzburgResultCode::CardholderScopeMismatch));
        }
        Some(
            actor
                .user_id
                .ok_or_else(|| ApiError::new(WurzburgResultCode::InvalidActorClaim))?,
        )
    };
    let idempotency_key = require_idempotency_key(&headers)?;
    let request_hash = canonical_request_hash(&method, uri.path(), &body);
    let request: UpdateFundingOrderRequest = serde_json::from_slice(&body)
        .map_err(|_| ApiError::new(WurzburgResultCode::FundingOrderContractInvalid))?;
    validate(&card_number, &request)?;
    let sources = request
        .sources
        .into_iter()
        .map(|source| FundingOrderSource {
            provider_id: source.provider_id,
            max_amount_rials: source.max_amount_rials,
        })
        .collect();
    let context = MutationCommandContext {
        operation_type: OPERATION_TYPE.to_string(),
        actor,
        request: request_context,
        idempotency_key,
        request_hash,
    };
    let service = CardFundingService::new(state.db.clone(), state.card_profile_locks.clone());
    let outcome = service
        .update_order(
            &context,
            &card_number,
            request.expected_card_state_version,
            expected_owner,
            sources,
            request.reason.trim().to_string(),
        )
        .await
        .map_err(map_service_error)?;
    match outcome {
        FundingOrderPersistenceOutcome::Applied(value) => {
            success_response(StatusCode::ACCEPTED, FundingOrderResponse::from(value))
        }
        FundingOrderPersistenceOutcome::Replayed(value) => {
            success_response(StatusCode::OK, FundingOrderResponse::from(value))
        }
        FundingOrderPersistenceOutcome::IdempotencyConflict => {
            Err(ApiError::new(WurzburgResultCode::IdempotencyKeyConflict))
        }
        FundingOrderPersistenceOutcome::IdempotencyInProgress => {
            Err(ApiError::new(WurzburgResultCode::IdempotencyInProgress))
        }
        FundingOrderPersistenceOutcome::CardNotFound => {
            Err(ApiError::new(WurzburgResultCode::CardNotFound))
        }
        FundingOrderPersistenceOutcome::CardNotActive => {
            Err(ApiError::new(WurzburgResultCode::CardNotActive))
        }
        FundingOrderPersistenceOutcome::CardOwnerMismatch => {
            Err(ApiError::new(WurzburgResultCode::CardholderScopeMismatch))
        }
        FundingOrderPersistenceOutcome::StateVersionConflict => {
            Err(ApiError::new(WurzburgResultCode::CardStateVersionConflict))
        }
        FundingOrderPersistenceOutcome::PublicationPending => Err(ApiError::new(
            WurzburgResultCode::CardProfilePublicationPending,
        )),
        FundingOrderPersistenceOutcome::CardProfileLocked => {
            Err(ApiError::new(WurzburgResultCode::CardProfileLocked))
        }
        FundingOrderPersistenceOutcome::SourceSetMismatch => {
            Err(ApiError::new(WurzburgResultCode::FundingSourceSetMismatch))
        }
    }
}

fn validate(card_number: &str, request: &UpdateFundingOrderRequest) -> Result<(), ApiError> {
    let valid_pan = card_number.len() == 16
        && card_number.bytes().all(|value| value.is_ascii_digit())
        && !card_number.starts_with('0');
    let providers: BTreeSet<_> = request
        .sources
        .iter()
        .map(|source| source.provider_id)
        .collect();
    let valid_reason = !request.reason.trim().is_empty()
        && request.reason.len() <= 1000
        && !request.reason.chars().any(char::is_control);
    let valid = request.expected_card_state_version >= 1
        && !request.sources.is_empty()
        && request.sources.len() <= MAX_FUNDING_SOURCES
        && providers.len() == request.sources.len()
        && request.sources.iter().all(|source| {
            source
                .max_amount_rials
                .is_none_or(|value| value <= MAX_RIALS)
        })
        && valid_reason
        && valid_pan;
    if valid {
        Ok(())
    } else {
        Err(ApiError::new(
            WurzburgResultCode::FundingOrderContractInvalid,
        ))
    }
}
fn map_service_error(error: CardFundingServiceError) -> ApiError {
    match error {
        CardFundingServiceError::Database(error) => ApiError::from_database(error),
        CardFundingServiceError::RuntimeProfile(error) => {
            tracing::error!(
                error.kind = error.diagnostic_kind(),
                "Dragonfly card-profile coordination failed"
            );
            ApiError::new(WurzburgResultCode::RuntimeProfileUnavailable)
        }
    }
}
impl From<FundingOrderResult> for FundingOrderResponse {
    fn from(value: FundingOrderResult) -> Self {
        Self {
            card_id: value.card_id,
            masked_card_number: value.masked_card_number,
            state_version: value.state_version,
            sources: value
                .sources
                .into_iter()
                .map(|source| FundingOrderAppliedSourceResponse {
                    provider_id: source.provider_id,
                    priority: source.priority,
                    max_amount_rials: source.max_amount_rials,
                })
                .collect(),
            operation_id: value.operation_id,
            command_status: value.command_status,
            event_publication_status: value.event_publication_status,
            profile_materialization_status: value.profile_materialization_status,
        }
    }
}
