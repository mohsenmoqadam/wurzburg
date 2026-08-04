use std::{fmt::Write, sync::Arc};

use axum::{
    extract::{Path, Query, State, rejection::QueryRejection},
    http::{HeaderMap, StatusCode},
    response::Response,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

use crate::{
    api::{
        auth::extract_trusted_actor, error::ApiError,
        request_context::extract_trusted_request_context, response::success_response,
        result_codes::WurzburgResultCode,
    },
    domain::financial_transaction::{
        FinancialAccountCategory, FinancialTransaction, FinancialTransactionCursor,
        FinancialTransactionEntry, FinancialTransactionQuery, FinancialTransactionType,
        TransactionVisibility,
    },
    services::financial_transaction::FinancialTransactionService,
    state::AppState,
};

#[derive(Debug, Clone, Deserialize, IntoParams, ToSchema)]
pub struct ListTransactionsQuery {
    pub transaction_type: Option<String>,
    pub occurred_from: Option<DateTime<Utc>>,
    pub occurred_to: Option<DateTime<Utc>>,
    pub page_size: Option<u16>,
    pub page_token: Option<String>,
}

#[derive(Debug, Clone, Deserialize, IntoParams, ToSchema)]
pub struct ListAdminTransactionsQuery {
    pub provider_id: Option<Uuid>,
    pub user_id: Option<Uuid>,
    pub card_number: Option<String>,
    pub transaction_type: Option<String>,
    pub occurred_from: Option<DateTime<Utc>>,
    pub occurred_to: Option<DateTime<Utc>>,
    pub page_size: Option<u16>,
    pub page_token: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct FinancialTransactionEntryResponse {
    pub provider_id: Uuid,
    pub account_category: String,
    pub direction: String,
    pub entry_role: String,
    pub amount_rials: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct FinancialTransactionResponse {
    pub transaction_id: Uuid,
    pub transaction_type: String,
    pub source_system: String,
    pub status: String,
    pub user_id: Uuid,
    pub card_id: Uuid,
    pub masked_card_number: String,
    pub amount_rials: String,
    pub currency: String,
    pub reference: Option<String>,
    pub original_transaction_id: Option<Uuid>,
    pub entries: Vec<FinancialTransactionEntryResponse>,
    pub occurred_at: DateTime<Utc>,
    pub recorded_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListFinancialTransactionsResponse {
    pub items: Vec<FinancialTransactionResponse>,
    pub next_page_token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TransactionPageToken {
    occurred_at: DateTime<Utc>,
    transaction_id: Uuid,
    filter_hash: String,
}

#[utoipa::path(
    get, path = "/api/v1/providers/{provider_id}/transactions", tag = "Transactions",
    params(("provider_id" = Uuid, Path), ListTransactionsQuery),
    responses((status = 200, body = ListFinancialTransactionsResponse), (status = 400, body = crate::api::error::ApiErrorResponse), (status = 403, body = crate::api::error::ApiErrorResponse), (status = 503, body = crate::api::error::ApiErrorResponse)),
    security(("wso2_backend_bearer" = []))
)]
#[tracing::instrument(skip(state, query, headers), fields(transaction.visibility = "provider", provider.id = %provider_id))]
pub async fn list_provider_transactions(
    State(state): State<Arc<AppState>>,
    Path(provider_id): Path<Uuid>,
    query: Result<Query<ListTransactionsQuery>, QueryRejection>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    provider_response(
        &state,
        &headers,
        provider_id,
        None,
        None,
        None,
        parse(query)?,
    )
    .await
}

#[utoipa::path(
    get, path = "/api/v1/providers/{provider_id}/users/{user_id}/transactions", tag = "Transactions",
    params(("provider_id" = Uuid, Path), ("user_id" = Uuid, Path), ListTransactionsQuery),
    responses((status = 200, body = ListFinancialTransactionsResponse), (status = 400, body = crate::api::error::ApiErrorResponse), (status = 403, body = crate::api::error::ApiErrorResponse), (status = 503, body = crate::api::error::ApiErrorResponse)),
    security(("wso2_backend_bearer" = []))
)]
#[tracing::instrument(skip(state, query, headers), fields(transaction.visibility = "provider", provider.id = %provider_id, user.id = %user_id))]
pub async fn list_provider_user_transactions(
    State(state): State<Arc<AppState>>,
    Path((provider_id, user_id)): Path<(Uuid, Uuid)>,
    query: Result<Query<ListTransactionsQuery>, QueryRejection>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    provider_response(
        &state,
        &headers,
        provider_id,
        Some(user_id),
        None,
        None,
        parse(query)?,
    )
    .await
}

#[utoipa::path(
    get, path = "/api/v1/providers/{provider_id}/cards/{card_number}/transactions", tag = "Transactions",
    params(("provider_id" = Uuid, Path), ("card_number" = String, Path), ListTransactionsQuery),
    responses((status = 200, body = ListFinancialTransactionsResponse), (status = 400, body = crate::api::error::ApiErrorResponse), (status = 403, body = crate::api::error::ApiErrorResponse), (status = 503, body = crate::api::error::ApiErrorResponse)),
    security(("wso2_backend_bearer" = []))
)]
#[tracing::instrument(skip(state, query, headers, card_number), fields(transaction.visibility = "provider", provider.id = %provider_id))]
pub async fn list_provider_card_transactions(
    State(state): State<Arc<AppState>>,
    Path((provider_id, card_number)): Path<(Uuid, String)>,
    query: Result<Query<ListTransactionsQuery>, QueryRejection>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    validate_pan(&card_number)?;
    provider_response(
        &state,
        &headers,
        provider_id,
        None,
        Some(card_number),
        None,
        parse(query)?,
    )
    .await
}

#[utoipa::path(
    get, path = "/api/v1/providers/{provider_id}/accounts/{account_category}/transactions", tag = "Transactions",
    params(("provider_id" = Uuid, Path), ("account_category" = String, Path), ListTransactionsQuery),
    responses((status = 200, body = ListFinancialTransactionsResponse), (status = 400, body = crate::api::error::ApiErrorResponse), (status = 403, body = crate::api::error::ApiErrorResponse), (status = 503, body = crate::api::error::ApiErrorResponse)),
    security(("wso2_backend_bearer" = []))
)]
#[tracing::instrument(skip(state, query, headers), fields(transaction.visibility = "provider", provider.id = %provider_id))]
pub async fn list_provider_account_transactions(
    State(state): State<Arc<AppState>>,
    Path((provider_id, account_category)): Path<(Uuid, String)>,
    query: Result<Query<ListTransactionsQuery>, QueryRejection>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let account_category = parse_account_category(&account_category)?;
    provider_response(
        &state,
        &headers,
        provider_id,
        None,
        None,
        Some(account_category),
        parse(query)?,
    )
    .await
}

#[utoipa::path(
    get, path = "/api/v1/users/{user_id}/transactions", tag = "Transactions",
    params(("user_id" = Uuid, Path), ListTransactionsQuery),
    responses((status = 200, body = ListFinancialTransactionsResponse), (status = 400, body = crate::api::error::ApiErrorResponse), (status = 403, body = crate::api::error::ApiErrorResponse), (status = 503, body = crate::api::error::ApiErrorResponse)),
    security(("wso2_backend_bearer" = []))
)]
#[tracing::instrument(skip(state, query, headers), fields(transaction.visibility = "cardholder", user.id = %user_id))]
pub async fn list_cardholder_transactions(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<Uuid>,
    query: Result<Query<ListTransactionsQuery>, QueryRejection>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    cardholder_response(&state, &headers, user_id, None, parse(query)?).await
}

#[utoipa::path(
    get, path = "/api/v1/cards/{card_number}/transactions", tag = "Transactions",
    params(("card_number" = String, Path), ListTransactionsQuery),
    responses((status = 200, body = ListFinancialTransactionsResponse), (status = 400, body = crate::api::error::ApiErrorResponse), (status = 403, body = crate::api::error::ApiErrorResponse), (status = 503, body = crate::api::error::ApiErrorResponse)),
    security(("wso2_backend_bearer" = []))
)]
#[tracing::instrument(skip(state, query, headers, card_number), fields(transaction.visibility = "cardholder"))]
pub async fn list_cardholder_card_transactions(
    State(state): State<Arc<AppState>>,
    Path(card_number): Path<String>,
    query: Result<Query<ListTransactionsQuery>, QueryRejection>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    validate_pan(&card_number)?;
    let actor = actor(&state, &headers)?;
    let user_id = actor
        .user_id
        .ok_or_else(|| ApiError::new(WurzburgResultCode::InvalidActorClaim))?;
    let request = parse(query)?;
    let domain_query = build_query(
        TransactionVisibility::Cardholder {
            user_id,
            card_number: Some(card_number),
        },
        request,
    )?;
    respond(
        FinancialTransactionService::new(state.db.clone())
            .list_cardholder(&actor, user_id, domain_query.0)
            .await?,
        domain_query.1,
    )
}

#[utoipa::path(
    get, path = "/api/v1/admin/transactions", tag = "Transactions",
    params(ListAdminTransactionsQuery),
    responses((status = 200, body = ListFinancialTransactionsResponse), (status = 400, body = crate::api::error::ApiErrorResponse), (status = 403, body = crate::api::error::ApiErrorResponse), (status = 503, body = crate::api::error::ApiErrorResponse)),
    security(("wso2_backend_bearer" = []))
)]
#[tracing::instrument(skip(state, query, headers), fields(transaction.visibility = "platform"))]
pub async fn list_admin_transactions(
    State(state): State<Arc<AppState>>,
    query: Result<Query<ListAdminTransactionsQuery>, QueryRejection>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let request = query.map_err(|_| invalid_query("query"))?.0;
    if let Some(card_number) = request.card_number.as_deref() {
        validate_pan(card_number)?;
    }
    let common = ListTransactionsQuery {
        transaction_type: request.transaction_type,
        occurred_from: request.occurred_from,
        occurred_to: request.occurred_to,
        page_size: request.page_size,
        page_token: request.page_token,
    };
    let domain_query = build_query(
        TransactionVisibility::Platform {
            provider_id: request.provider_id,
            user_id: request.user_id,
            card_number: request.card_number,
        },
        common,
    )?;
    let actor = actor(&state, &headers)?;
    respond(
        FinancialTransactionService::new(state.db.clone())
            .list_platform(&actor, domain_query.0)
            .await?,
        domain_query.1,
    )
}

#[utoipa::path(
    get, path = "/api/v1/admin/cards/{card_number}/transactions", tag = "Transactions",
    params(("card_number" = String, Path), ListTransactionsQuery),
    responses((status = 200, body = ListFinancialTransactionsResponse), (status = 400, body = crate::api::error::ApiErrorResponse), (status = 403, body = crate::api::error::ApiErrorResponse), (status = 503, body = crate::api::error::ApiErrorResponse)),
    security(("wso2_backend_bearer" = []))
)]
#[tracing::instrument(skip(state, query, headers, card_number), fields(transaction.visibility = "platform"))]
pub async fn list_admin_card_transactions(
    State(state): State<Arc<AppState>>,
    Path(card_number): Path<String>,
    query: Result<Query<ListTransactionsQuery>, QueryRejection>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    validate_pan(&card_number)?;
    let domain_query = build_query(
        TransactionVisibility::Platform {
            provider_id: None,
            user_id: None,
            card_number: Some(card_number),
        },
        parse(query)?,
    )?;
    let actor = actor(&state, &headers)?;
    respond(
        FinancialTransactionService::new(state.db.clone())
            .list_platform(&actor, domain_query.0)
            .await?,
        domain_query.1,
    )
}

async fn provider_response(
    state: &AppState,
    headers: &HeaderMap,
    provider_id: Uuid,
    user_id: Option<Uuid>,
    card_number: Option<String>,
    account_category: Option<FinancialAccountCategory>,
    request: ListTransactionsQuery,
) -> Result<Response, ApiError> {
    let actor = actor(state, headers)?;
    let domain_query = build_query(
        TransactionVisibility::Provider {
            provider_id,
            user_id,
            card_number,
            account_category,
        },
        request,
    )?;
    respond(
        FinancialTransactionService::new(state.db.clone())
            .list_provider(&actor, provider_id, domain_query.0)
            .await?,
        domain_query.1,
    )
}

async fn cardholder_response(
    state: &AppState,
    headers: &HeaderMap,
    user_id: Uuid,
    card_number: Option<String>,
    request: ListTransactionsQuery,
) -> Result<Response, ApiError> {
    let actor = actor(state, headers)?;
    let domain_query = build_query(
        TransactionVisibility::Cardholder {
            user_id,
            card_number,
        },
        request,
    )?;
    respond(
        FinancialTransactionService::new(state.db.clone())
            .list_cardholder(&actor, user_id, domain_query.0)
            .await?,
        domain_query.1,
    )
}

fn build_query(
    visibility: TransactionVisibility,
    request: ListTransactionsQuery,
) -> Result<(FinancialTransactionQuery, String), ApiError> {
    let page_size = request.page_size.unwrap_or(50);
    if !(1..=200).contains(&page_size) {
        return Err(invalid_query("page_size"));
    }
    let transaction_type = request
        .transaction_type
        .as_deref()
        .map(parse_transaction_type)
        .transpose()?;
    if request
        .occurred_from
        .zip(request.occurred_to)
        .is_some_and(|(from, to)| from >= to)
    {
        return Err(invalid_query("occurred_at"));
    }
    let filter_hash = filter_hash(
        &visibility,
        transaction_type,
        request.occurred_from,
        request.occurred_to,
    );
    let cursor = request
        .page_token
        .as_deref()
        .map(|value| parse_page_token(value, &filter_hash))
        .transpose()?;
    Ok((
        FinancialTransactionQuery {
            visibility,
            transaction_type,
            occurred_from: request.occurred_from,
            occurred_to: request.occurred_to,
            limit: page_size,
            cursor,
        },
        filter_hash,
    ))
}

fn respond(
    page: crate::domain::financial_transaction::FinancialTransactionPage,
    filter_hash: String,
) -> Result<Response, ApiError> {
    let next_page_token = page.next_cursor.map(|cursor| {
        encode_hex(
            &serde_json::to_vec(&TransactionPageToken {
                occurred_at: cursor.occurred_at,
                transaction_id: cursor.transaction_id,
                filter_hash,
            })
            .expect("transaction page token is serializable"),
        )
    });
    success_response(
        StatusCode::OK,
        ListFinancialTransactionsResponse {
            items: page.items.into_iter().map(Into::into).collect(),
            next_page_token,
        },
    )
}

fn actor(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<crate::api::auth::TrustedActor, ApiError> {
    let context =
        extract_trusted_request_context(headers, state.config.wso2.backend_token_transport)?;
    extract_trusted_actor(&context, &state.config.wso2)
}

fn parse(
    query: Result<Query<ListTransactionsQuery>, QueryRejection>,
) -> Result<ListTransactionsQuery, ApiError> {
    query
        .map(|value| value.0)
        .map_err(|_| invalid_query("query"))
}

fn parse_transaction_type(value: &str) -> Result<FinancialTransactionType, ApiError> {
    FinancialTransactionType::from_db_value(value).ok_or_else(|| invalid_query("transaction_type"))
}

fn parse_account_category(value: &str) -> Result<FinancialAccountCategory, ApiError> {
    FinancialAccountCategory::from_db_value(value).ok_or_else(|| invalid_query("account_category"))
}

fn validate_pan(value: &str) -> Result<(), ApiError> {
    (value.len() == 16
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && !value.starts_with('0'))
    .then_some(())
    .ok_or_else(|| invalid_query("card_number"))
}

fn filter_hash(
    visibility: &TransactionVisibility,
    transaction_type: Option<FinancialTransactionType>,
    occurred_from: Option<DateTime<Utc>>,
    occurred_to: Option<DateTime<Utc>>,
) -> String {
    let visibility = match visibility {
        TransactionVisibility::Provider {
            provider_id,
            user_id,
            card_number,
            account_category,
        } => serde_json::json!({
            "kind": "PROVIDER", "provider_id": provider_id, "user_id": user_id,
            "card_number": card_number, "account_category": account_category.map(FinancialAccountCategory::as_db_value)
        }),
        TransactionVisibility::Cardholder {
            user_id,
            card_number,
        } => {
            serde_json::json!({"kind": "CARDHOLDER", "user_id": user_id, "card_number": card_number})
        }
        TransactionVisibility::Platform {
            provider_id,
            user_id,
            card_number,
        } => serde_json::json!({
            "kind": "PLATFORM", "provider_id": provider_id, "user_id": user_id, "card_number": card_number
        }),
    };
    let canonical = serde_json::json!({
        "visibility": visibility,
        "transaction_type": transaction_type.map(FinancialTransactionType::as_db_value),
        "occurred_from": occurred_from,
        "occurred_to": occurred_to
    });
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(&canonical).expect("transaction filters are serializable"));
    encode_hex(&hasher.finalize())
}

fn parse_page_token(
    value: &str,
    expected_filter_hash: &str,
) -> Result<FinancialTransactionCursor, ApiError> {
    let bytes = decode_hex(value).ok_or_else(|| invalid_query("page_token"))?;
    let token: TransactionPageToken =
        serde_json::from_slice(&bytes).map_err(|_| invalid_query("page_token"))?;
    if token.filter_hash != expected_filter_hash {
        return Err(invalid_query("page_token"));
    }
    Ok(FinancialTransactionCursor {
        occurred_at: token.occurred_at,
        transaction_id: token.transaction_id,
    })
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

fn decode_hex(value: &str) -> Option<Vec<u8>> {
    if !value.len().is_multiple_of(2) || value.len() > 4096 {
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

fn invalid_query(filter: &'static str) -> ApiError {
    ApiError::with_details(
        WurzburgResultCode::InvalidTransactionQuery,
        serde_json::json!({ "filter": filter }),
    )
}

impl From<FinancialTransaction> for FinancialTransactionResponse {
    fn from(value: FinancialTransaction) -> Self {
        Self {
            transaction_id: value.transaction_id,
            transaction_type: value.transaction_type.as_db_value().to_string(),
            source_system: value.source_system,
            status: value.status,
            user_id: value.user_id,
            card_id: value.card_id,
            masked_card_number: value.masked_card_number,
            amount_rials: value.amount_rials,
            currency: value.currency,
            reference: value.reference,
            original_transaction_id: value.original_transaction_id,
            entries: value.entries.into_iter().map(Into::into).collect(),
            occurred_at: value.occurred_at,
            recorded_at: value.recorded_at,
        }
    }
}

impl From<FinancialTransactionEntry> for FinancialTransactionEntryResponse {
    fn from(value: FinancialTransactionEntry) -> Self {
        Self {
            provider_id: value.provider_id,
            account_category: value.account_category.as_db_value().to_string(),
            direction: match value.direction {
                crate::domain::financial_transaction::FinancialEntryDirection::Debit => "DEBIT",
                crate::domain::financial_transaction::FinancialEntryDirection::Credit => "CREDIT",
            }
            .to_string(),
            entry_role: value.entry_role,
            amount_rials: value.amount_rials,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_token_is_bound_to_visibility_and_filters() {
        let provider_id = Uuid::new_v4();
        let first = filter_hash(
            &TransactionVisibility::Provider {
                provider_id,
                user_id: None,
                card_number: None,
                account_category: None,
            },
            None,
            None,
            None,
        );
        let second = filter_hash(
            &TransactionVisibility::Provider {
                provider_id: Uuid::new_v4(),
                user_id: None,
                card_number: None,
                account_category: None,
            },
            None,
            None,
            None,
        );
        assert_ne!(first, second);
    }
}
