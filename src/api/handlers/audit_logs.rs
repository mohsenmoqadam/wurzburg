use std::sync::Arc;

use axum::{
    extract::{Query, State, rejection::QueryRejection},
    http::{HeaderMap, StatusCode},
    response::Response,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{
    api::{
        auth::extract_trusted_actor, error::ApiError,
        request_context::extract_trusted_request_context, response::success_response,
        result_codes::WurzburgResultCode,
    },
    domain::audit::{AuditAction, AuditLogCursor, AuditLogQuery, AuditLogRecord},
    services::audit::AuditLogService,
    state::AppState,
};

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ListAuditLogsQuery {
    pub entity_type: Option<String>,
    pub entity_id: Option<Uuid>,
    pub action_type: Option<String>,
    pub actor_subject: Option<String>,
    pub actor_client_id: Option<String>,
    pub actor_provider_id: Option<Uuid>,
    pub actor_user_id: Option<Uuid>,
    pub source_ip: Option<String>,
    pub correlation_id: Option<String>,
    pub request_id: Option<String>,
    pub created_from: Option<DateTime<Utc>>,
    pub created_to: Option<DateTime<Utc>>,
    pub page_size: Option<u32>,
    pub page_token: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AuditLogResponse {
    pub audit_log_id: Uuid,
    pub entity_type: String,
    pub entity_id: Uuid,
    pub action_type: String,
    pub reason: Option<String>,
    pub old_values: Option<serde_json::Value>,
    pub new_values: Option<serde_json::Value>,
    pub actor_subject: String,
    pub actor_client_id: Option<String>,
    pub actor_provider_id: Option<Uuid>,
    pub actor_user_id: Option<Uuid>,
    pub actor_issuer: Option<String>,
    pub source_ip: Option<String>,
    pub correlation_id: String,
    pub request_id: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListAuditLogsResponse {
    pub data: Vec<AuditLogResponse>,
    pub next_page_token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct AuditPageToken {
    created_at: DateTime<Utc>,
    audit_log_id: Uuid,
    filter_hash: String,
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/audit-logs",
    tag = "Audit",
    params(
        ("entity_type" = Option<String>, Query),
        ("entity_id" = Option<Uuid>, Query),
        ("action_type" = Option<String>, Query),
        ("actor_subject" = Option<String>, Query),
        ("actor_client_id" = Option<String>, Query),
        ("actor_provider_id" = Option<Uuid>, Query),
        ("actor_user_id" = Option<Uuid>, Query),
        ("source_ip" = Option<String>, Query),
        ("correlation_id" = Option<String>, Query),
        ("request_id" = Option<String>, Query),
        ("created_from" = Option<DateTime<Utc>>, Query, description = "Inclusive UTC lower bound."),
        ("created_to" = Option<DateTime<Utc>>, Query, description = "Exclusive UTC upper bound."),
        ("page_size" = Option<u32>, Query, description = "Page size from 1 through 200; defaults to 50."),
        ("page_token" = Option<String>, Query, description = "Opaque token returned by the previous page.")
    ),
    responses(
        (status = 200, body = ListAuditLogsResponse),
        (status = 400, body = crate::api::error::ApiErrorResponse),
        (status = 401, body = crate::api::error::ApiErrorResponse),
        (status = 403, body = crate::api::error::ApiErrorResponse),
        (status = 503, body = crate::api::error::ApiErrorResponse)
    ),
    security(("wso2_backend_bearer"=[]))
)]
#[tracing::instrument(skip(state, headers, query))]
pub async fn list_audit_logs(
    State(state): State<Arc<AppState>>,
    query: Result<Query<ListAuditLogsQuery>, QueryRejection>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let Query(query) = query.map_err(|_| invalid_filter("query"))?;
    let request_context =
        extract_trusted_request_context(&headers, state.config.wso2.backend_token_transport)?;
    let actor = extract_trusted_actor(&request_context, &state.config.wso2)?;
    let entity_type = normalize_filter(query.entity_type, "entity_type", 100)?;
    if entity_type.as_deref().is_some_and(|value| {
        !value.chars().all(|character| {
            character.is_ascii_uppercase() || character.is_ascii_digit() || character == '_'
        })
    }) {
        return Err(invalid_filter("entity_type"));
    }
    let action_type = query
        .action_type
        .as_deref()
        .map(parse_action_type)
        .transpose()?;
    let actor_subject = normalize_filter(query.actor_subject, "actor_subject", 255)?;
    let actor_client_id = normalize_filter(query.actor_client_id, "actor_client_id", 255)?;
    let source_ip = normalize_source_ip(query.source_ip)?;
    let correlation_id = normalize_filter(query.correlation_id, "correlation_id", 128)?;
    let request_id = normalize_filter(query.request_id, "request_id", 128)?;
    if query
        .created_from
        .zip(query.created_to)
        .is_some_and(|(from, to)| from >= to)
    {
        return Err(invalid_filter("created_range"));
    }
    let page_size = query.page_size.unwrap_or(50);
    if !(1..=200).contains(&page_size) {
        return Err(invalid_filter("page_size"));
    }
    let filter_hash = audit_filter_hash(
        entity_type.as_deref(),
        query.entity_id,
        action_type,
        actor_subject.as_deref(),
        actor_client_id.as_deref(),
        query.actor_provider_id,
        query.actor_user_id,
        source_ip.as_deref(),
        correlation_id.as_deref(),
        request_id.as_deref(),
        query.created_from,
        query.created_to,
    );
    let cursor = query
        .page_token
        .as_deref()
        .map(|token| parse_page_token(token, &filter_hash))
        .transpose()?;
    let page = AuditLogService::new(state.db.clone())
        .list(
            &actor,
            AuditLogQuery {
                entity_type,
                entity_id: query.entity_id,
                action_type,
                actor_subject,
                actor_client_id,
                actor_provider_id: query.actor_provider_id,
                actor_user_id: query.actor_user_id,
                source_ip,
                correlation_id,
                request_id,
                created_from: query.created_from,
                created_to: query.created_to,
                limit: page_size,
                cursor,
            },
        )
        .await?;
    let next_page_token = page
        .next_cursor
        .map(|cursor| format_page_token(cursor, filter_hash));
    success_response(
        StatusCode::OK,
        ListAuditLogsResponse {
            data: page.items.into_iter().map(Into::into).collect(),
            next_page_token,
        },
    )
}

impl From<AuditLogRecord> for AuditLogResponse {
    fn from(value: AuditLogRecord) -> Self {
        Self {
            audit_log_id: value.audit_log_id,
            entity_type: value.entity_type,
            entity_id: value.entity_id,
            action_type: value.action_type.as_db_value().to_string(),
            reason: value.reason,
            old_values: value.old_values,
            new_values: value.new_values,
            actor_subject: value.actor_subject,
            actor_client_id: value.actor_client_id,
            actor_provider_id: value.actor_provider_id,
            actor_user_id: value.actor_user_id,
            actor_issuer: value.actor_issuer,
            source_ip: value.source_ip,
            correlation_id: value.correlation_id,
            request_id: value.request_id,
            created_at: value.created_at,
        }
    }
}

fn parse_action_type(value: &str) -> Result<AuditAction, ApiError> {
    AuditAction::from_db_value(value).ok_or_else(|| invalid_filter("action_type"))
}

fn normalize_filter(
    value: Option<String>,
    field: &'static str,
    max_length: usize,
) -> Result<Option<String>, ApiError> {
    value
        .map(|value| {
            let value = value.trim().to_string();
            if value.is_empty() || value.len() > max_length || value.chars().any(char::is_control) {
                Err(invalid_filter(field))
            } else {
                Ok(value)
            }
        })
        .transpose()
}

fn normalize_source_ip(value: Option<String>) -> Result<Option<String>, ApiError> {
    value
        .map(|value| {
            value
                .trim()
                .parse::<std::net::IpAddr>()
                .map(|address| address.to_string())
                .map_err(|_| invalid_filter("source_ip"))
        })
        .transpose()
}

#[allow(clippy::too_many_arguments)]
fn audit_filter_hash(
    entity_type: Option<&str>,
    entity_id: Option<Uuid>,
    action_type: Option<AuditAction>,
    actor_subject: Option<&str>,
    actor_client_id: Option<&str>,
    actor_provider_id: Option<Uuid>,
    actor_user_id: Option<Uuid>,
    source_ip: Option<&str>,
    correlation_id: Option<&str>,
    request_id: Option<&str>,
    created_from: Option<DateTime<Utc>>,
    created_to: Option<DateTime<Utc>>,
) -> String {
    let canonical = serde_json::json!({
        "entity_type": entity_type,
        "entity_id": entity_id,
        "action_type": action_type.map(AuditAction::as_db_value),
        "actor_subject": actor_subject,
        "actor_client_id": actor_client_id,
        "actor_provider_id": actor_provider_id,
        "actor_user_id": actor_user_id,
        "source_ip": source_ip,
        "correlation_id": correlation_id,
        "request_id": request_id,
        "created_from": created_from,
        "created_to": created_to,
    });
    encode_hex(&Sha256::digest(canonical.to_string().as_bytes()))
}

fn parse_page_token(value: &str, filter_hash: &str) -> Result<AuditLogCursor, ApiError> {
    let bytes = decode_hex(value).ok_or_else(|| invalid_filter("page_token"))?;
    let token: AuditPageToken =
        serde_json::from_slice(&bytes).map_err(|_| invalid_filter("page_token"))?;
    if token.filter_hash != filter_hash {
        return Err(invalid_filter("page_token"));
    }
    Ok(AuditLogCursor {
        created_at: token.created_at,
        audit_log_id: token.audit_log_id,
    })
}

fn format_page_token(cursor: AuditLogCursor, filter_hash: String) -> String {
    let token = AuditPageToken {
        created_at: cursor.created_at,
        audit_log_id: cursor.audit_log_id,
        filter_hash,
    };
    encode_hex(&serde_json::to_vec(&token).expect("audit page token is serializable"))
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

fn invalid_filter(filter: &'static str) -> ApiError {
    ApiError::with_details(
        WurzburgResultCode::InvalidAuditLogFilter,
        serde_json::json!({ "filter": filter }),
    )
}
