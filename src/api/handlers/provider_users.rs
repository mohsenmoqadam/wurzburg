use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{OriginalUri, Path, Query, State},
    http::{HeaderMap, Method, StatusCode},
    response::Response,
};
use chrono::NaiveDate;
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
    domain::user_card::{
        CardInstruction, CardResolution, NewCardDelivery, NewProviderUserEnrollment,
        PolicyUsageAccountIds, ProviderUserCursor, ProviderUserRecord, ProviderUserStatus,
        ProviderUserView, UserCardSummary, UserProviderSummary,
    },
    services::provider_user::{EnrollProviderUserOutcome, ProviderUserService},
    state::AppState,
};

const ENROLL_PROVIDER_USER_OPERATION: &str = "provider_users.enroll";

#[derive(Debug, Deserialize, ToSchema)]
pub struct EnrollProviderUserRequest {
    pub national_id: String,
    pub first_name: String,
    pub last_name: String,
    pub provider_customer_reference: String,
    /// Provider-side evidence that this customer selected the provider.
    pub selection_reference: String,
    pub card_instruction: CardInstructionRequest,
    #[schema(value_type=Object)]
    pub metadata: serde_json::Value,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CardInstructionRequest {
    UseExisting {
        card_number: String,
    },
    IssueNew {
        birth_date: Option<NaiveDate>,
        mobile: String,
        delivery_province: String,
        delivery_city: String,
        delivery_address: String,
        postal_code: String,
    },
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ProviderUserEnrollmentResponse {
    pub enrollment_id: Uuid,
    pub provider_user_id: Uuid,
    pub provider_id: Uuid,
    pub user_id: Uuid,
    pub provider_customer_reference: String,
    pub identity_mismatch: bool,
    pub mismatch_fields: Vec<String>,
    pub status: String,
    pub card_resolution: String,
    pub card_id: Option<Uuid>,
    pub masked_card_number: Option<String>,
    pub card_range_id: Uuid,
    pub provider_user_account_id: Option<Uuid>,
    pub policy_usage_account_ids: Option<PolicyUsageAccountIdsResponse>,
    pub issuance_request_id: Option<Uuid>,
    pub profile_materialization_status: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PolicyUsageAccountIdsResponse {
    pub amount_daily: Uuid,
    pub amount_weekly: Uuid,
    pub amount_monthly: Uuid,
    pub amount_yearly: Uuid,
    pub count_daily: Uuid,
    pub count_weekly: Uuid,
    pub count_monthly: Uuid,
    pub count_yearly: Uuid,
}

#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct ListProviderUsersQuery {
    pub status: Option<String>,
    pub limit: Option<u16>,
    pub before_created_at: Option<chrono::DateTime<chrono::Utc>>,
    pub before_provider_user_id: Option<Uuid>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ProviderUserResponse {
    pub provider_user_id: Uuid,
    pub enrollment_id: Uuid,
    pub provider_id: Uuid,
    pub user_id: Uuid,
    pub national_id: String,
    pub first_name: String,
    pub last_name: String,
    pub provider_customer_reference: String,
    pub identity_mismatch: bool,
    pub mismatch_fields: Vec<String>,
    pub status: String,
    pub card_id: Option<Uuid>,
    pub masked_card_number: Option<String>,
    pub card_range_id: Option<Uuid>,
    pub provider_user_account_id: Option<Uuid>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListProviderUsersResponse {
    pub items: Vec<ProviderUserResponse>,
    pub next_before_created_at: Option<chrono::DateTime<chrono::Utc>>,
    pub next_before_provider_user_id: Option<Uuid>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct UserCardSummaryResponse {
    pub card_id: Uuid,
    pub masked_card_number: String,
    pub card_range_id: Uuid,
    pub status: String,
    pub state_version: i64,
    pub materialized_version: i64,
    pub provider_ids: Vec<Uuid>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct UserProviderSummaryResponse {
    pub provider_id: Uuid,
    pub provider_user_id: Uuid,
    pub provider_user_status: String,
    pub provider_user_account_id: Option<Uuid>,
    pub card_id: Option<Uuid>,
    pub masked_card_number: Option<String>,
    pub card_range_id: Option<Uuid>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[utoipa::path(
    post,
    path="/api/v1/providers/{provider_id}/users",
    tag="Provider Users",
    request_body=EnrollProviderUserRequest,
    params(
        ("provider_id"=Uuid, Path, description="Provider identifier."),
        ("Idempotency-Key"=String, Header, description="Required stable idempotency key."),
        ("X-Correlation-Id"=String, Header),
        ("X-Request-Id"=Uuid, Header),
        ("X-WSO2-Client-IP"=String, Header),
        ("X-WSO2-Gateway-Id"=String, Header)
    ),
    responses(
        (status=201, body=ProviderUserEnrollmentResponse, description="Existing card linked and ledger accounts verified."),
        (status=202, body=ProviderUserEnrollmentResponse, description="New issuance is pending bank processing."),
        (status=400, body=crate::api::error::ApiErrorResponse),
        (status=404, body=crate::api::error::ApiErrorResponse),
        (status=409, body=crate::api::error::ApiErrorResponse),
        (status=503, body=crate::api::error::ApiErrorResponse)
    ),
    security(("wso2_backend_bearer"=[]))
)]
#[tracing::instrument(skip(state, headers, body), fields(provider_id=%provider_id))]
pub async fn enroll_provider_user(
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
    let request: EnrollProviderUserRequest = serde_json::from_slice(&body)
        .map_err(|_| ApiError::new(WurzburgResultCode::ProviderUserContractInvalid))?;
    let enrollment = NewProviderUserEnrollment {
        national_id: request.national_id,
        first_name: request.first_name,
        last_name: request.last_name,
        provider_customer_reference: request.provider_customer_reference,
        selection_reference: request.selection_reference,
        card_instruction: request.card_instruction.into(),
        metadata: request.metadata,
    };
    let context = MutationCommandContext {
        operation_type: ENROLL_PROVIDER_USER_OPERATION.to_string(),
        actor,
        request: request_context,
        idempotency_key,
        request_hash,
    };
    let service = ProviderUserService::new(
        state.db.clone(),
        state.ledger_client.clone(),
        state.config.tigerbeetle.clone(),
    );
    match service.enroll(&context, provider_id, enrollment).await? {
        EnrollProviderUserOutcome::Activated(view) => success_response(
            StatusCode::CREATED,
            ProviderUserEnrollmentResponse::from(*view),
        ),
        EnrollProviderUserOutcome::IssuancePending(view) => success_response(
            StatusCode::ACCEPTED,
            ProviderUserEnrollmentResponse::from(*view),
        ),
        EnrollProviderUserOutcome::Replayed(value) => success_response(StatusCode::OK, value),
    }
}

#[utoipa::path(
    get, path="/api/v1/users/{user_id}/cards", tag="Provider Users",
    params(("user_id"=Uuid, Path), ("X-Correlation-Id"=String, Header), ("X-Request-Id"=Uuid, Header), ("X-WSO2-Client-IP"=String, Header), ("X-WSO2-Gateway-Id"=String, Header)),
    responses((status=200, body=Vec<UserCardSummaryResponse>), (status=403, body=crate::api::error::ApiErrorResponse)), security(("wso2_backend_bearer"=[]))
)]
#[tracing::instrument(skip(state, headers), fields(user_id=%user_id))]
pub async fn list_user_cards(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let request =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request, &state.config.wso2)?;
    let values = ProviderUserService::new(
        state.db.clone(),
        state.ledger_client.clone(),
        state.config.tigerbeetle.clone(),
    )
    .list_user_cards(&actor, user_id)
    .await?;
    success_response(
        StatusCode::OK,
        values
            .into_iter()
            .map(UserCardSummaryResponse::from)
            .collect::<Vec<_>>(),
    )
}

#[utoipa::path(
    get, path="/api/v1/users/{user_id}/providers", tag="Provider Users",
    params(("user_id"=Uuid, Path), ("X-Correlation-Id"=String, Header), ("X-Request-Id"=Uuid, Header), ("X-WSO2-Client-IP"=String, Header), ("X-WSO2-Gateway-Id"=String, Header)),
    responses((status=200, body=Vec<UserProviderSummaryResponse>), (status=403, body=crate::api::error::ApiErrorResponse)), security(("wso2_backend_bearer"=[]))
)]
#[tracing::instrument(skip(state, headers), fields(user_id=%user_id))]
pub async fn list_user_providers(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let request =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request, &state.config.wso2)?;
    let values = ProviderUserService::new(
        state.db.clone(),
        state.ledger_client.clone(),
        state.config.tigerbeetle.clone(),
    )
    .list_user_providers(&actor, user_id)
    .await?;
    success_response(
        StatusCode::OK,
        values
            .into_iter()
            .map(UserProviderSummaryResponse::from)
            .collect::<Vec<_>>(),
    )
}

#[utoipa::path(
    get, path="/api/v1/providers/{provider_id}/users", tag="Provider Users",
    params(("provider_id"=Uuid, Path), ListProviderUsersQuery, ("X-Correlation-Id"=String, Header), ("X-Request-Id"=Uuid, Header), ("X-WSO2-Client-IP"=String, Header), ("X-WSO2-Gateway-Id"=String, Header)),
    responses((status=200, body=ListProviderUsersResponse), (status=400, body=crate::api::error::ApiErrorResponse)), security(("wso2_backend_bearer"=[]))
)]
#[tracing::instrument(skip(state,headers),fields(provider_id=%provider_id))]
pub async fn list_provider_users(
    State(state): State<Arc<AppState>>,
    Path(provider_id): Path<Uuid>,
    Query(query): Query<ListProviderUsersQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let request =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request, &state.config.wso2)?;
    let status = query.status.as_deref().map(parse_status).transpose()?;
    let cursor = match (query.before_created_at, query.before_provider_user_id) {
        (Some(created_at), Some(provider_user_id)) => Some(ProviderUserCursor {
            created_at,
            provider_user_id,
        }),
        (None, None) => None,
        _ => {
            return Err(ApiError::new(
                WurzburgResultCode::ProviderUserContractInvalid,
            ));
        }
    };
    let page = ProviderUserService::new(
        state.db.clone(),
        state.ledger_client.clone(),
        state.config.tigerbeetle.clone(),
    )
    .list(
        &actor,
        provider_id,
        status,
        cursor,
        query.limit.unwrap_or(50),
    )
    .await?;
    let next_before_created_at = page.next_cursor.as_ref().map(|value| value.created_at);
    let next_before_provider_user_id = page
        .next_cursor
        .as_ref()
        .map(|value| value.provider_user_id);
    success_response(
        StatusCode::OK,
        ListProviderUsersResponse {
            items: page.items.into_iter().map(Into::into).collect(),
            next_before_created_at,
            next_before_provider_user_id,
        },
    )
}

#[utoipa::path(
    get, path="/api/v1/providers/{provider_id}/users/{user_id}", tag="Provider Users",
    params(("provider_id"=Uuid, Path), ("user_id"=Uuid, Path), ("X-Correlation-Id"=String, Header), ("X-Request-Id"=Uuid, Header), ("X-WSO2-Client-IP"=String, Header), ("X-WSO2-Gateway-Id"=String, Header)),
    responses((status=200, body=ProviderUserResponse), (status=404, body=crate::api::error::ApiErrorResponse)), security(("wso2_backend_bearer"=[]))
)]
#[tracing::instrument(skip(state,headers),fields(provider_id=%provider_id,user_id=%user_id))]
pub async fn get_provider_user(
    State(state): State<Arc<AppState>>,
    Path((provider_id, user_id)): Path<(Uuid, Uuid)>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let request =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request, &state.config.wso2)?;
    let value = ProviderUserService::new(
        state.db.clone(),
        state.ledger_client.clone(),
        state.config.tigerbeetle.clone(),
    )
    .get(&actor, provider_id, user_id)
    .await?;
    success_response(StatusCode::OK, ProviderUserResponse::from(value))
}

impl From<CardInstructionRequest> for CardInstruction {
    fn from(value: CardInstructionRequest) -> Self {
        match value {
            CardInstructionRequest::UseExisting { card_number } => {
                Self::UseExisting { card_number }
            }
            CardInstructionRequest::IssueNew {
                birth_date,
                mobile,
                delivery_province,
                delivery_city,
                delivery_address,
                postal_code,
            } => Self::IssueNew(NewCardDelivery {
                birth_date,
                mobile,
                delivery_province,
                delivery_city,
                delivery_address,
                postal_code,
            }),
        }
    }
}

impl From<ProviderUserView> for ProviderUserEnrollmentResponse {
    fn from(value: ProviderUserView) -> Self {
        Self {
            enrollment_id: value.enrollment_id,
            provider_user_id: value.provider_user_id,
            provider_id: value.provider_id,
            user_id: value.user_id,
            provider_customer_reference: value.provider_customer_reference,
            identity_mismatch: value.identity_mismatch,
            mismatch_fields: value.mismatch_fields,
            status: provider_user_status(value.status).to_string(),
            card_resolution: card_resolution(value.card_resolution).to_string(),
            card_id: value.card_id,
            masked_card_number: value.masked_card_number,
            card_range_id: value.card_range_id,
            provider_user_account_id: value.provider_user_account_id,
            policy_usage_account_ids: value.policy_usage_account_ids.map(Into::into),
            issuance_request_id: value.issuance_request_id,
            profile_materialization_status: value.profile_materialization_status,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}

impl From<PolicyUsageAccountIds> for PolicyUsageAccountIdsResponse {
    fn from(value: PolicyUsageAccountIds) -> Self {
        Self {
            amount_daily: value.amount_daily,
            amount_weekly: value.amount_weekly,
            amount_monthly: value.amount_monthly,
            amount_yearly: value.amount_yearly,
            count_daily: value.count_daily,
            count_weekly: value.count_weekly,
            count_monthly: value.count_monthly,
            count_yearly: value.count_yearly,
        }
    }
}

fn provider_user_status(value: ProviderUserStatus) -> &'static str {
    match value {
        ProviderUserStatus::CardIssuancePending => "CARD_ISSUANCE_PENDING",
        ProviderUserStatus::Provisioning => "PROVISIONING",
        ProviderUserStatus::Active => "ACTIVE",
        ProviderUserStatus::Suspended => "SUSPENDED",
        ProviderUserStatus::IssuanceRejected => "ISSUANCE_REJECTED",
        ProviderUserStatus::RecoveryRequired => "RECOVERY_REQUIRED",
    }
}

fn card_resolution(value: CardResolution) -> &'static str {
    match value {
        CardResolution::ExistingCardAttached => "EXISTING_CARD_ATTACHED",
        CardResolution::IssuanceRequested => "ISSUANCE_REQUESTED",
        CardResolution::JoinedPendingIssuance => "JOINED_PENDING_ISSUANCE",
    }
}

impl From<ProviderUserRecord> for ProviderUserResponse {
    fn from(value: ProviderUserRecord) -> Self {
        Self {
            provider_user_id: value.provider_user_id,
            enrollment_id: value.enrollment_id,
            provider_id: value.provider_id,
            user_id: value.user_id,
            national_id: value.national_id,
            first_name: value.first_name,
            last_name: value.last_name,
            provider_customer_reference: value.provider_customer_reference,
            identity_mismatch: value.identity_mismatch,
            mismatch_fields: value.mismatch_fields,
            status: provider_user_status(value.status).to_string(),
            card_id: value.card_id,
            masked_card_number: value.masked_card_number,
            card_range_id: value.card_range_id,
            provider_user_account_id: value.provider_user_account_id,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}

impl From<UserCardSummary> for UserCardSummaryResponse {
    fn from(value: UserCardSummary) -> Self {
        Self {
            card_id: value.card_id,
            masked_card_number: value.masked_card_number,
            card_range_id: value.card_range_id,
            status: value.status,
            state_version: value.state_version,
            materialized_version: value.materialized_version,
            provider_ids: value.provider_ids,
            created_at: value.created_at,
        }
    }
}

impl From<UserProviderSummary> for UserProviderSummaryResponse {
    fn from(value: UserProviderSummary) -> Self {
        Self {
            provider_id: value.provider_id,
            provider_user_id: value.provider_user_id,
            provider_user_status: provider_user_status(value.provider_user_status).to_string(),
            provider_user_account_id: value.provider_user_account_id,
            card_id: value.card_id,
            masked_card_number: value.masked_card_number,
            card_range_id: value.card_range_id,
            created_at: value.created_at,
        }
    }
}

fn parse_status(value: &str) -> Result<ProviderUserStatus, ApiError> {
    ProviderUserStatus::from_db_value(value)
        .ok_or_else(|| ApiError::new(WurzburgResultCode::ProviderUserContractInvalid))
}
