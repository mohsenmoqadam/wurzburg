use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{OriginalUri, Path, Query, State},
    http::{HeaderMap, Method, StatusCode},
    response::Response,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{
    api::{
        auth::extract_trusted_actor,
        command::MutationCommandContext,
        error::ApiError,
        handlers::providers::{ProviderContactRequest, ProviderContactTypeDto, ProviderResponse},
        idempotency::{canonical_request_hash, require_idempotency_key},
        request_context::extract_trusted_request_context,
        response::success_response,
        result_codes::WurzburgResultCode,
    },
    db::oracle::{ProviderContactMutationOutcome, ProviderIdentityMutationOutcome},
    domain::provider::{
        FieldUpdate, ProviderContact, ProviderContactCursor, ProviderContactListQuery,
        ProviderContactRecord, ProviderContactStatus, ProviderContactType, ProviderContactUpdate,
        ProviderIdentityUpdate,
    },
    services::provider_identity::ProviderIdentityService,
    state::AppState,
};

const UPDATE_IDENTITY_OPERATION: &str = "providers.identity.update";
const CREATE_CONTACT_OPERATION: &str = "provider_contacts.create";
const UPDATE_CONTACT_OPERATION: &str = "provider_contacts.update";
const SUSPEND_CONTACT_OPERATION: &str = "provider_contacts.suspend";
const REACTIVATE_CONTACT_OPERATION: &str = "provider_contacts.reactivate";

#[derive(Clone, Copy)]
struct ContactTransitionDefinition {
    operation: &'static str,
    target: ProviderContactStatus,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct UpdateProviderIdentityRequest {
    #[serde(default, deserialize_with = "deserialize_nullable")]
    pub legal_name: Option<Option<String>>,
    #[serde(default, deserialize_with = "deserialize_nullable")]
    pub trade_name: Option<Option<String>>,
    #[serde(default, deserialize_with = "deserialize_nullable")]
    pub tax_id: Option<Option<String>>,
    #[serde(default, deserialize_with = "deserialize_nullable")]
    pub registration_number: Option<Option<String>>,
    #[serde(default, deserialize_with = "deserialize_nullable")]
    pub email_address: Option<Option<String>>,
    #[serde(default, deserialize_with = "deserialize_nullable")]
    pub website_url: Option<Option<String>>,
    #[serde(default, deserialize_with = "deserialize_nullable")]
    pub mailing_address: Option<Option<String>>,
    #[serde(default, deserialize_with = "deserialize_nullable")]
    pub metadata: Option<Option<serde_json::Value>>,
    pub reason: String,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateProviderContactRequest {
    #[serde(flatten)]
    pub contact: ProviderContactRequest,
    pub reason: String,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct UpdateProviderContactRequest {
    pub contact_type: Option<ProviderContactTypeDto>,
    #[serde(default, deserialize_with = "deserialize_nullable")]
    pub name: Option<Option<String>>,
    #[serde(default, deserialize_with = "deserialize_nullable")]
    pub email: Option<Option<String>>,
    #[serde(default, deserialize_with = "deserialize_nullable")]
    pub phone: Option<Option<String>>,
    #[serde(default, deserialize_with = "deserialize_nullable")]
    pub mobile: Option<Option<String>>,
    pub sms_enabled: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_nullable")]
    pub metadata: Option<Option<serde_json::Value>>,
    pub reason: String,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ProviderContactTransitionRequest {
    pub reason: String,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ListProviderContactsQuery {
    pub contact_type: Option<String>,
    pub status: Option<String>,
    pub page_size: Option<u32>,
    pub page_token: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProviderContactStatusDto {
    Active,
    Suspended,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ProviderContactResponse {
    pub provider_contact_id: Uuid,
    pub provider_id: Uuid,
    pub contact_type: ProviderContactTypeDto,
    pub name: Option<String>,
    pub email: Option<String>,
    pub phone: Option<String>,
    pub mobile: Option<String>,
    pub sms_enabled: bool,
    pub metadata: serde_json::Value,
    pub status: ProviderContactStatusDto,
    pub created_by_subject: String,
    pub updated_by_subject: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListProviderContactsResponse {
    pub data: Vec<ProviderContactResponse>,
    pub next_page_token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ContactPageToken {
    created_at: DateTime<Utc>,
    provider_contact_id: Uuid,
    filter_hash: String,
}

#[utoipa::path(
    patch,path="/api/v1/providers/{provider_id}",tag="Provider Identity and Contacts",request_body=UpdateProviderIdentityRequest,
    params(("provider_id"=Uuid,Path),("Idempotency-Key"=String,Header),("X-Correlation-Id"=String,Header),("X-Request-Id"=Uuid,Header),("X-WSO2-Client-IP"=String,Header),("X-WSO2-Gateway-Id"=String,Header)),
    responses((status=200,body=ProviderResponse),(status=400,body=crate::api::error::ApiErrorResponse),(status=404,body=crate::api::error::ApiErrorResponse),(status=409,body=crate::api::error::ApiErrorResponse)),security(("wso2_backend_bearer"=[]))
)]
#[tracing::instrument(skip(state,headers,body),fields(provider_id=%provider_id))]
pub async fn update_provider_identity(
    State(state): State<Arc<AppState>>,
    Path(provider_id): Path<Uuid>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let (context, request) = mutation_context::<UpdateProviderIdentityRequest>(
        &state,
        method,
        &uri,
        &headers,
        &body,
        UPDATE_IDENTITY_OPERATION,
        WurzburgResultCode::InvalidProviderIdentityContract,
    )?;
    let update = identity_update(request)?;
    match ProviderIdentityService::new(state.db.clone())
        .update_identity(&context, provider_id, update)
        .await?
    {
        ProviderIdentityMutationOutcome::Applied(provider) => {
            success_response(StatusCode::OK, ProviderResponse::new(*provider))
        }
        ProviderIdentityMutationOutcome::Replayed(value) => success_response(StatusCode::OK, value),
        _ => unreachable!("service maps provider identity failures"),
    }
}

#[utoipa::path(
    post,path="/api/v1/providers/{provider_id}/contacts",tag="Provider Identity and Contacts",request_body=CreateProviderContactRequest,
    params(("provider_id"=Uuid,Path),("Idempotency-Key"=String,Header),("X-Correlation-Id"=String,Header),("X-Request-Id"=Uuid,Header),("X-WSO2-Client-IP"=String,Header),("X-WSO2-Gateway-Id"=String,Header)),responses((status=201,body=ProviderContactResponse),(status=400,body=crate::api::error::ApiErrorResponse),(status=404,body=crate::api::error::ApiErrorResponse),(status=409,body=crate::api::error::ApiErrorResponse)),security(("wso2_backend_bearer"=[]))
)]
#[tracing::instrument(skip(state,headers,body),fields(provider_id=%provider_id))]
pub async fn create_provider_contact(
    State(state): State<Arc<AppState>>,
    Path(provider_id): Path<Uuid>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let (context, request) = mutation_context::<CreateProviderContactRequest>(
        &state,
        method,
        &uri,
        &headers,
        &body,
        CREATE_CONTACT_OPERATION,
        WurzburgResultCode::InvalidProviderContactContract,
    )?;
    let reason = request.reason;
    let contact: ProviderContact = request.contact.into();
    respond_contact(
        ProviderIdentityService::new(state.db.clone())
            .create_contact(&context, provider_id, contact, reason)
            .await?,
        StatusCode::CREATED,
    )
}

#[utoipa::path(
    get,path="/api/v1/providers/{provider_id}/contacts",tag="Provider Identity and Contacts",
    params(("provider_id"=Uuid,Path),("contact_type"=Option<String>,Query),("status"=Option<String>,Query),("page_size"=Option<u32>,Query),("page_token"=Option<String>,Query),("X-Correlation-Id"=String,Header),("X-Request-Id"=Uuid,Header),("X-WSO2-Client-IP"=String,Header),("X-WSO2-Gateway-Id"=String,Header)),responses((status=200,body=ListProviderContactsResponse),(status=400,body=crate::api::error::ApiErrorResponse),(status=404,body=crate::api::error::ApiErrorResponse)),security(("wso2_backend_bearer"=[]))
)]
#[tracing::instrument(skip(state,headers,query),fields(provider_id=%provider_id))]
pub async fn list_provider_contacts(
    State(state): State<Arc<AppState>>,
    Path(provider_id): Path<Uuid>,
    Query(query): Query<ListProviderContactsQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let request =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request, &state.config.wso2)?;
    let contact_type = query
        .contact_type
        .as_deref()
        .map(parse_contact_type)
        .transpose()?;
    let status = query
        .status
        .as_deref()
        .map(parse_contact_status)
        .transpose()?;
    let cursor = query
        .page_token
        .as_deref()
        .map(|value| parse_page_token(value, contact_type, status))
        .transpose()?;
    let page = ProviderIdentityService::new(state.db.clone())
        .list_contacts(
            &actor,
            provider_id,
            ProviderContactListQuery {
                contact_type,
                status,
                limit: query.page_size.unwrap_or(50),
                cursor,
            },
        )
        .await?;
    let next_page_token = page
        .next_cursor
        .map(|value| format_page_token(value, contact_type, status));
    success_response(
        StatusCode::OK,
        ListProviderContactsResponse {
            data: page.items.into_iter().map(Into::into).collect(),
            next_page_token,
        },
    )
}

#[utoipa::path(
    patch,path="/api/v1/providers/{provider_id}/contacts/{contact_id}",tag="Provider Identity and Contacts",request_body=UpdateProviderContactRequest,
    params(("provider_id"=Uuid,Path),("contact_id"=Uuid,Path),("Idempotency-Key"=String,Header),("X-Correlation-Id"=String,Header),("X-Request-Id"=Uuid,Header),("X-WSO2-Client-IP"=String,Header),("X-WSO2-Gateway-Id"=String,Header)),responses((status=200,body=ProviderContactResponse),(status=400,body=crate::api::error::ApiErrorResponse),(status=404,body=crate::api::error::ApiErrorResponse),(status=409,body=crate::api::error::ApiErrorResponse)),security(("wso2_backend_bearer"=[]))
)]
#[tracing::instrument(skip(state,headers,body),fields(provider_id=%provider_id,provider_contact_id=%contact_id))]
pub async fn update_provider_contact(
    State(state): State<Arc<AppState>>,
    Path((provider_id, contact_id)): Path<(Uuid, Uuid)>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let (context, request) = mutation_context::<UpdateProviderContactRequest>(
        &state,
        method,
        &uri,
        &headers,
        &body,
        UPDATE_CONTACT_OPERATION,
        WurzburgResultCode::InvalidProviderContactContract,
    )?;
    let update = contact_update(request)?;
    respond_contact(
        ProviderIdentityService::new(state.db.clone())
            .update_contact(&context, provider_id, contact_id, update)
            .await?,
        StatusCode::OK,
    )
}

#[utoipa::path(post,path="/api/v1/providers/{provider_id}/contacts/{contact_id}/suspend",tag="Provider Identity and Contacts",request_body=ProviderContactTransitionRequest,params(("provider_id"=Uuid,Path),("contact_id"=Uuid,Path),("Idempotency-Key"=String,Header),("X-Correlation-Id"=String,Header),("X-Request-Id"=Uuid,Header),("X-WSO2-Client-IP"=String,Header),("X-WSO2-Gateway-Id"=String,Header)),responses((status=200,body=ProviderContactResponse),(status=404,body=crate::api::error::ApiErrorResponse),(status=409,body=crate::api::error::ApiErrorResponse)),security(("wso2_backend_bearer"=[])))]
#[tracing::instrument(skip(state, headers, body))]
pub async fn suspend_provider_contact(
    state: State<Arc<AppState>>,
    path: Path<(Uuid, Uuid)>,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    transition_contact(
        state,
        path,
        method,
        uri,
        headers,
        body,
        ContactTransitionDefinition {
            operation: SUSPEND_CONTACT_OPERATION,
            target: ProviderContactStatus::Suspended,
        },
    )
    .await
}

#[utoipa::path(post,path="/api/v1/providers/{provider_id}/contacts/{contact_id}/reactivate",tag="Provider Identity and Contacts",request_body=ProviderContactTransitionRequest,params(("provider_id"=Uuid,Path),("contact_id"=Uuid,Path),("Idempotency-Key"=String,Header),("X-Correlation-Id"=String,Header),("X-Request-Id"=Uuid,Header),("X-WSO2-Client-IP"=String,Header),("X-WSO2-Gateway-Id"=String,Header)),responses((status=200,body=ProviderContactResponse),(status=404,body=crate::api::error::ApiErrorResponse),(status=409,body=crate::api::error::ApiErrorResponse)),security(("wso2_backend_bearer"=[])))]
#[tracing::instrument(skip(state, headers, body))]
pub async fn reactivate_provider_contact(
    state: State<Arc<AppState>>,
    path: Path<(Uuid, Uuid)>,
    method: Method,
    uri: OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    transition_contact(
        state,
        path,
        method,
        uri,
        headers,
        body,
        ContactTransitionDefinition {
            operation: REACTIVATE_CONTACT_OPERATION,
            target: ProviderContactStatus::Active,
        },
    )
    .await
}

async fn transition_contact(
    State(state): State<Arc<AppState>>,
    Path((provider_id, contact_id)): Path<(Uuid, Uuid)>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
    transition: ContactTransitionDefinition,
) -> Result<Response, ApiError> {
    let (context, request) = mutation_context::<ProviderContactTransitionRequest>(
        &state,
        method,
        &uri,
        &headers,
        &body,
        transition.operation,
        WurzburgResultCode::InvalidProviderContactContract,
    )?;
    respond_contact(
        ProviderIdentityService::new(state.db.clone())
            .transition_contact(
                &context,
                provider_id,
                contact_id,
                transition.target,
                request.reason,
            )
            .await?,
        StatusCode::OK,
    )
}

fn mutation_context<T: for<'de> Deserialize<'de>>(
    state: &AppState,
    method: Method,
    uri: &axum::http::Uri,
    headers: &HeaderMap,
    body: &Bytes,
    operation: &str,
    invalid: WurzburgResultCode,
) -> Result<(MutationCommandContext, T), ApiError> {
    let request_context =
        extract_trusted_request_context(headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request_context, &state.config.wso2)?;
    let idempotency_key = require_idempotency_key(headers)?;
    let request_hash = canonical_request_hash(&method, uri.path(), body);
    let request = serde_json::from_slice(body).map_err(|_| ApiError::new(invalid))?;
    Ok((
        MutationCommandContext {
            operation_type: operation.to_string(),
            actor,
            request: request_context,
            idempotency_key,
            request_hash,
        },
        request,
    ))
}
fn respond_contact(
    outcome: ProviderContactMutationOutcome,
    created: StatusCode,
) -> Result<Response, ApiError> {
    match outcome {
        ProviderContactMutationOutcome::Applied(value) => {
            success_response(created, ProviderContactResponse::from(*value))
        }
        ProviderContactMutationOutcome::Replayed(value) => success_response(StatusCode::OK, value),
        _ => unreachable!("service maps provider contact failures"),
    }
}

fn identity_update(
    request: UpdateProviderIdentityRequest,
) -> Result<ProviderIdentityUpdate, ApiError> {
    if request.legal_name == Some(None)
        || request.trade_name == Some(None)
        || request.metadata == Some(None)
    {
        return Err(ApiError::new(
            WurzburgResultCode::InvalidProviderIdentityContract,
        ));
    }
    Ok(ProviderIdentityUpdate {
        legal_name: request.legal_name.flatten(),
        trade_name: request.trade_name.flatten(),
        tax_id: field_update(request.tax_id),
        registration_number: field_update(request.registration_number),
        email_address: field_update(request.email_address),
        website_url: field_update(request.website_url),
        mailing_address: field_update(request.mailing_address),
        metadata: request.metadata.flatten(),
        reason: request.reason,
    })
}
fn contact_update(
    request: UpdateProviderContactRequest,
) -> Result<ProviderContactUpdate, ApiError> {
    if request.metadata == Some(None) {
        return Err(ApiError::new(
            WurzburgResultCode::InvalidProviderContactContract,
        ));
    }
    Ok(ProviderContactUpdate {
        contact_type: request.contact_type.map(Into::into),
        name: field_update(request.name),
        email: field_update(request.email),
        phone: field_update(request.phone),
        mobile: field_update(request.mobile),
        sms_enabled: request.sms_enabled,
        metadata: request.metadata.flatten(),
        reason: request.reason,
    })
}
fn field_update<T>(value: Option<Option<T>>) -> FieldUpdate<T> {
    match value {
        None => FieldUpdate::Unchanged,
        Some(Some(value)) => FieldUpdate::Set(value),
        Some(None) => FieldUpdate::Clear,
    }
}
fn deserialize_nullable<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

fn parse_contact_type(value: &str) -> Result<ProviderContactType, ApiError> {
    ProviderContactType::from_db_value(value)
        .ok_or_else(|| ApiError::new(WurzburgResultCode::InvalidProviderContactFilter))
}
fn parse_contact_status(value: &str) -> Result<ProviderContactStatus, ApiError> {
    ProviderContactStatus::from_db_value(value)
        .ok_or_else(|| ApiError::new(WurzburgResultCode::InvalidProviderContactFilter))
}
fn parse_page_token(
    value: &str,
    contact_type: Option<ProviderContactType>,
    status: Option<ProviderContactStatus>,
) -> Result<ProviderContactCursor, ApiError> {
    let bytes = decode_hex(value)
        .ok_or_else(|| ApiError::new(WurzburgResultCode::InvalidProviderContactFilter))?;
    let token: ContactPageToken = serde_json::from_slice(&bytes)
        .map_err(|_| ApiError::new(WurzburgResultCode::InvalidProviderContactFilter))?;
    if token.filter_hash != filter_hash(contact_type, status) {
        return Err(ApiError::new(
            WurzburgResultCode::InvalidProviderContactFilter,
        ));
    }
    Ok(ProviderContactCursor {
        created_at: token.created_at,
        provider_contact_id: token.provider_contact_id,
    })
}
fn format_page_token(
    cursor: ProviderContactCursor,
    contact_type: Option<ProviderContactType>,
    status: Option<ProviderContactStatus>,
) -> String {
    encode_hex(
        &serde_json::to_vec(&ContactPageToken {
            created_at: cursor.created_at,
            provider_contact_id: cursor.provider_contact_id,
            filter_hash: filter_hash(contact_type, status),
        })
        .expect("contact page token is serializable"),
    )
}
fn filter_hash(
    contact_type: Option<ProviderContactType>,
    status: Option<ProviderContactStatus>,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(
        contact_type
            .map(ProviderContactType::as_db_value)
            .unwrap_or(""),
    );
    hasher.update([0]);
    hasher.update(status.map(ProviderContactStatus::as_db_value).unwrap_or(""));
    encode_hex(&hasher.finalize())
}
fn encode_hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut value, "{byte:02x}").expect("writing to String cannot fail");
    }
    value
}
fn decode_hex(value: &str) -> Option<Vec<u8>> {
    if !value.len().is_multiple_of(2) || value.len() > 4096 {
        return None;
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok())
        .collect()
}

impl From<ProviderContactRecord> for ProviderContactResponse {
    fn from(value: ProviderContactRecord) -> Self {
        Self {
            provider_contact_id: value.provider_contact_id,
            provider_id: value.provider_id,
            contact_type: value.contact_type.into(),
            name: value.name,
            email: value.email,
            phone: value.phone,
            mobile: value.mobile,
            sms_enabled: value.sms_enabled,
            metadata: value.metadata,
            status: value.status.into(),
            created_by_subject: value.created_by_subject,
            updated_by_subject: value.updated_by_subject,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}
impl From<ProviderContactStatus> for ProviderContactStatusDto {
    fn from(value: ProviderContactStatus) -> Self {
        match value {
            ProviderContactStatus::Active => Self::Active,
            ProviderContactStatus::Suspended => Self::Suspended,
        }
    }
}

impl From<ProviderContactType> for ProviderContactTypeDto {
    fn from(value: ProviderContactType) -> Self {
        match value {
            ProviderContactType::Finance => Self::Finance,
            ProviderContactType::Technical => Self::Technical,
            ProviderContactType::Operations => Self::Operations,
            ProviderContactType::Security => Self::Security,
            ProviderContactType::Notification => Self::Notification,
            ProviderContactType::Legal => Self::Legal,
        }
    }
}
