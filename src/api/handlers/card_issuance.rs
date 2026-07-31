use std::sync::Arc;

use axum::{
    body::{Body, Bytes},
    extract::{OriginalUri, Path, Query, State},
    http::{HeaderMap, Method, StatusCode, header},
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
    domain::card_issuance::{CardIssuanceBatch, CardIssuanceBatchStatus},
    services::card_issuance::{CardIssuanceService, CreateCardIssuanceBatchOutcome},
    state::AppState,
    telemetry::http::{RESULT_CODE_HEADER, RESULT_SYMBOL_HEADER},
};

const CREATE_BATCH_OPERATION: &str = "card_issuance_batches.create";

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateCardIssuanceBatchRequest {
    /// Maximum number of physical issuance requests in this batch.
    pub batch_size: u16,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CardIssuanceBatchResponse {
    pub batch_id: Uuid,
    pub status: String,
    pub request_checksum_sha256: Option<String>,
    pub result_checksum_sha256: Option<String>,
    pub request_count: u32,
    pub issued_count: u32,
    pub rejected_count: u32,
    pub failed_count: u32,
    pub expires_at: chrono::DateTime<chrono::Utc>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct ListCardIssuanceBatchesQuery {
    pub limit: Option<u16>,
    pub before_created_at: Option<chrono::DateTime<chrono::Utc>>,
    pub before_batch_id: Option<Uuid>,
}
#[derive(Debug, Serialize, ToSchema)]
pub struct ListCardIssuanceBatchesResponse {
    pub items: Vec<CardIssuanceBatchResponse>,
    pub next_before_created_at: Option<chrono::DateTime<chrono::Utc>>,
    pub next_before_batch_id: Option<Uuid>,
}
#[derive(Debug, Serialize, ToSchema)]
pub struct CardIssuanceResultRowResponse {
    pub issuance_request_id: Uuid,
    pub row_number: u32,
    pub status: String,
    pub result_code: Option<String>,
    pub result_message: Option<String>,
}
#[derive(Debug, Serialize, ToSchema)]
pub struct CardIssuanceBatchResultResponse {
    pub batch_id: Uuid,
    pub rows: Vec<CardIssuanceResultRowResponse>,
}

#[utoipa::path(
    post, path="/api/v1/admin/card-issuance-batches", tag="Card Issuance",
    request_body=CreateCardIssuanceBatchRequest,
    params(("Idempotency-Key"=String, Header), ("X-Correlation-Id"=String, Header), ("X-Request-Id"=Uuid, Header), ("X-WSO2-Client-IP"=String, Header), ("X-WSO2-Gateway-Id"=String, Header)),
    responses((status=201, body=CardIssuanceBatchResponse), (status=400, body=crate::api::error::ApiErrorResponse), (status=409, body=crate::api::error::ApiErrorResponse), (status=503, body=crate::api::error::ApiErrorResponse)),
    security(("wso2_backend_bearer"=[]))
)]
#[tracing::instrument(skip(state, headers, body))]
pub async fn create_card_issuance_batch(
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
    let request: CreateCardIssuanceBatchRequest = serde_json::from_slice(&body)
        .map_err(|_| ApiError::new(WurzburgResultCode::CardIssuanceBatchContractInvalid))?;
    let context = MutationCommandContext {
        operation_type: CREATE_BATCH_OPERATION.to_string(),
        actor,
        request: request_context,
        idempotency_key,
        request_hash,
    };
    let service = CardIssuanceService::new(
        state.db.clone(),
        state.object_storage.clone(),
        state.config.object_storage.batch_retention_days,
    );
    match service.create_batch(&context, request.batch_size).await? {
        CreateCardIssuanceBatchOutcome::Created(batch) => {
            success_response(StatusCode::CREATED, CardIssuanceBatchResponse::from(batch))
        }
        CreateCardIssuanceBatchOutcome::Replayed(value) => success_response(StatusCode::OK, value),
    }
}

#[utoipa::path(get,path="/api/v1/admin/card-issuance-batches",tag="Card Issuance",params(ListCardIssuanceBatchesQuery,("X-Correlation-Id"=String,Header),("X-Request-Id"=Uuid,Header),("X-WSO2-Client-IP"=String,Header),("X-WSO2-Gateway-Id"=String,Header)),responses((status=200,body=ListCardIssuanceBatchesResponse)),security(("wso2_backend_bearer"=[])))]
#[tracing::instrument(skip(state, headers))]
pub async fn list_card_issuance_batches(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListCardIssuanceBatchesQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let request =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request, &state.config.wso2)?;
    let cursor = match (query.before_created_at, query.before_batch_id) {
        (Some(created_at), Some(batch_id)) => {
            Some(crate::domain::card_issuance::CardIssuanceBatchCursor {
                created_at,
                batch_id,
            })
        }
        (None, None) => None,
        _ => {
            return Err(ApiError::new(
                WurzburgResultCode::CardIssuanceBatchContractInvalid,
            ));
        }
    };
    let page = CardIssuanceService::new(
        state.db.clone(),
        state.object_storage.clone(),
        state.config.object_storage.batch_retention_days,
    )
    .list_batches(&actor, cursor, query.limit.unwrap_or(50))
    .await?;
    let next_before_created_at = page.next_cursor.as_ref().map(|value| value.created_at);
    let next_before_batch_id = page.next_cursor.as_ref().map(|value| value.batch_id);
    success_response(
        StatusCode::OK,
        ListCardIssuanceBatchesResponse {
            items: page.items.into_iter().map(Into::into).collect(),
            next_before_created_at,
            next_before_batch_id,
        },
    )
}

#[utoipa::path(
    get, path="/api/v1/admin/card-issuance-batches/{batch_id}", tag="Card Issuance",
    params(("batch_id"=Uuid, Path), ("X-Correlation-Id"=String, Header), ("X-Request-Id"=Uuid, Header), ("X-WSO2-Client-IP"=String, Header), ("X-WSO2-Gateway-Id"=String, Header)),
    responses((status=200, body=CardIssuanceBatchResponse), (status=404, body=crate::api::error::ApiErrorResponse)), security(("wso2_backend_bearer"=[]))
)]
#[tracing::instrument(skip(state,headers),fields(batch_id=%batch_id))]
pub async fn get_card_issuance_batch(
    State(state): State<Arc<AppState>>,
    Path(batch_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let request =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request, &state.config.wso2)?;
    let batch = CardIssuanceService::new(
        state.db.clone(),
        state.object_storage.clone(),
        state.config.object_storage.batch_retention_days,
    )
    .get_batch(&actor, batch_id)
    .await?;
    success_response(StatusCode::OK, CardIssuanceBatchResponse::from(batch))
}

#[utoipa::path(
    get, path="/api/v1/admin/card-issuance-batches/{batch_id}/request-file", tag="Card Issuance",
    params(("batch_id"=Uuid, Path), ("X-Correlation-Id"=String, Header), ("X-Request-Id"=Uuid, Header), ("X-WSO2-Client-IP"=String, Header), ("X-WSO2-Gateway-Id"=String, Header)),
    responses((status=200, content_type="text/csv"), (status=404, body=crate::api::error::ApiErrorResponse), (status=503, body=crate::api::error::ApiErrorResponse)), security(("wso2_backend_bearer"=[]))
)]
#[tracing::instrument(skip(state,headers),fields(batch_id=%batch_id))]
pub async fn download_card_issuance_request_file(
    State(state): State<Arc<AppState>>,
    Path(batch_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let request =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request, &state.config.wso2)?;
    let bytes = CardIssuanceService::new(
        state.db.clone(),
        state.object_storage.clone(),
        state.config.object_storage.batch_retention_days,
    )
    .download_request_file(&actor, batch_id)
    .await?;
    let (rs_code, symbol, _, _) = WurzburgResultCode::Success.parts();
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/csv; charset=utf-8")
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=card-issuance-{batch_id}.csv"),
        )
        .header(RESULT_CODE_HEADER.as_str(), rs_code.to_string())
        .header(RESULT_SYMBOL_HEADER.as_str(), symbol)
        .body(Body::from(bytes))
        .expect("static issuance download headers are valid"))
}

#[utoipa::path(
    post, path="/api/v1/admin/card-issuance-batches/{batch_id}/result-file", tag="Card Issuance",
    request_body(content=String, content_type="text/csv", description="UTF-8 bank result CSV using the documented exact header set."),
    params(("batch_id"=Uuid, Path), ("Idempotency-Key"=String, Header), ("X-Correlation-Id"=String, Header), ("X-Request-Id"=Uuid, Header), ("X-WSO2-Client-IP"=String, Header), ("X-WSO2-Gateway-Id"=String, Header)),
    responses((status=200, body=CardIssuanceBatchResponse), (status=400, body=crate::api::error::ApiErrorResponse), (status=409, body=crate::api::error::ApiErrorResponse), (status=503, body=crate::api::error::ApiErrorResponse)), security(("wso2_backend_bearer"=[]))
)]
#[tracing::instrument(skip(state,headers,body),fields(batch_id=%batch_id))]
pub async fn upload_card_issuance_result_file(
    State(state): State<Arc<AppState>>,
    Path(batch_id): Path<Uuid>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    if body.len() > state.config.object_storage.max_upload_bytes {
        return Err(ApiError::new(
            WurzburgResultCode::CardIssuanceResultContractInvalid,
        ));
    }
    let request_context =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request_context, &state.config.wso2)?;
    let idempotency_key = require_idempotency_key(&headers)?;
    let request_hash = canonical_request_hash(&method, uri.path(), &body);
    let context = MutationCommandContext {
        operation_type: "card_issuance_batches.process_result".to_string(),
        actor,
        request: request_context,
        idempotency_key,
        request_hash,
    };
    let issuance_service = CardIssuanceService::new(
        state.db.clone(),
        state.object_storage.clone(),
        state.config.object_storage.batch_retention_days,
    );
    let provider_user_service = crate::services::provider_user::ProviderUserService::new(
        state.db.clone(),
        state.tb_client.clone(),
        state.config.tigerbeetle.clone(),
    );
    match issuance_service
        .process_result_file(&context, batch_id, &body, &provider_user_service)
        .await?
    {
        CreateCardIssuanceBatchOutcome::Created(batch) => {
            success_response(StatusCode::OK, CardIssuanceBatchResponse::from(batch))
        }
        CreateCardIssuanceBatchOutcome::Replayed(value) => success_response(StatusCode::OK, value),
    }
}

#[utoipa::path(get,path="/api/v1/admin/card-issuance-batches/{batch_id}/result",tag="Card Issuance",params(("batch_id"=Uuid,Path),("X-Correlation-Id"=String,Header),("X-Request-Id"=Uuid,Header),("X-WSO2-Client-IP"=String,Header),("X-WSO2-Gateway-Id"=String,Header)),responses((status=200,body=CardIssuanceBatchResultResponse),(status=404,body=crate::api::error::ApiErrorResponse)),security(("wso2_backend_bearer"=[])))]
#[tracing::instrument(skip(state,headers),fields(batch_id=%batch_id))]
pub async fn get_card_issuance_batch_result(
    State(state): State<Arc<AppState>>,
    Path(batch_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let request =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request, &state.config.wso2)?;
    let rows = CardIssuanceService::new(
        state.db.clone(),
        state.object_storage.clone(),
        state.config.object_storage.batch_retention_days,
    )
    .get_result(&actor, batch_id)
    .await?;
    success_response(
        StatusCode::OK,
        CardIssuanceBatchResultResponse {
            batch_id,
            rows: rows
                .into_iter()
                .map(|value| CardIssuanceResultRowResponse {
                    issuance_request_id: value.issuance_request_id,
                    row_number: value.row_number,
                    status: value.status,
                    result_code: value.result_code,
                    result_message: value.result_message,
                })
                .collect(),
        },
    )
}

impl From<CardIssuanceBatch> for CardIssuanceBatchResponse {
    fn from(value: CardIssuanceBatch) -> Self {
        Self {
            batch_id: value.batch_id,
            status: status(value.status).to_string(),
            request_checksum_sha256: value.request_checksum_sha256,
            result_checksum_sha256: value.result_checksum_sha256,
            request_count: value.request_count,
            issued_count: value.issued_count,
            rejected_count: value.rejected_count,
            failed_count: value.failed_count,
            expires_at: value.expires_at,
            created_at: value.created_at,
            updated_at: value.updated_at,
            completed_at: value.completed_at,
        }
    }
}

fn status(value: CardIssuanceBatchStatus) -> &'static str {
    match value {
        CardIssuanceBatchStatus::Creating => "CREATING",
        CardIssuanceBatchStatus::Ready => "READY",
        CardIssuanceBatchStatus::ProcessingResult => "PROCESSING_RESULT",
        CardIssuanceBatchStatus::Completed => "COMPLETED",
        CardIssuanceBatchStatus::PartiallyCompleted => "PARTIALLY_COMPLETED",
        CardIssuanceBatchStatus::Failed => "FAILED",
    }
}
