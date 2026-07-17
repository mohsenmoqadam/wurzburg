use std::{fmt, net::IpAddr};

use axum::http::{HeaderMap, HeaderName};
use uuid::Uuid;

use crate::{
    api::{error::ApiError, result_codes::WurzburgResultCode},
    config::BackendTokenTransport,
};

const AUTHORIZATION: &str = "authorization";
const X_JWT_ASSERTION: &str = "x-jwt-assertion";
const X_CORRELATION_ID: &str = "x-correlation-id";
const X_REQUEST_ID: &str = "x-request-id";
const X_WSO2_CLIENT_IP: &str = "x-wso2-client-ip";
const X_WSO2_GATEWAY_ID: &str = "x-wso2-gateway-id";

#[derive(Clone, PartialEq, Eq)]
pub struct BackendToken {
    pub transport: BackendTokenTransport,
    token: String,
}

impl BackendToken {
    pub fn from_verified_transport(
        transport: BackendTokenTransport,
        token: impl Into<String>,
    ) -> Result<Self, ApiError> {
        let token = token.into();
        if token.is_empty() || token.contains(char::is_whitespace) {
            return Err(ApiError::new(
                WurzburgResultCode::InvalidBackendTokenTransport,
            ));
        }

        Ok(Self { transport, token })
    }

    pub fn redacted_transport_only(&self) -> BackendTokenTransport {
        self.transport
    }

    pub fn expose_for_signature_validation(&self) -> &str {
        &self.token
    }
}

impl fmt::Debug for BackendToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BackendToken")
            .field("transport", &self.transport)
            .field("token", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct TrustedRequestContext {
    pub correlation_id: String,
    pub request_id: Uuid,
    pub client_ip: IpAddr,
    pub gateway_id: String,
    pub backend_token: BackendToken,
}

impl fmt::Debug for TrustedRequestContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TrustedRequestContext")
            .field("correlation_id", &self.correlation_id)
            .field("request_id", &self.request_id)
            .field("client_ip", &self.client_ip)
            .field("gateway_id", &self.gateway_id)
            .field("backend_token", &self.backend_token)
            .finish()
    }
}

pub fn extract_trusted_request_context(
    headers: &HeaderMap,
    token_transport: BackendTokenTransport,
) -> Result<TrustedRequestContext, ApiError> {
    let correlation_id = required_header(headers, X_CORRELATION_ID)?;
    let request_id = required_header(headers, X_REQUEST_ID)?;
    let client_ip = required_header(headers, X_WSO2_CLIENT_IP)?;
    let gateway_id = required_header(headers, X_WSO2_GATEWAY_ID)?;

    if !is_valid_correlation_id(&correlation_id) {
        return Err(invalid_header(X_CORRELATION_ID));
    }

    let request_id = request_id
        .parse::<Uuid>()
        .map_err(|_| invalid_header(X_REQUEST_ID))?;
    let client_ip = client_ip
        .parse::<IpAddr>()
        .map_err(|_| invalid_header(X_WSO2_CLIENT_IP))?;
    let backend_token = extract_backend_token(headers, token_transport)?;

    Ok(TrustedRequestContext {
        correlation_id,
        request_id,
        client_ip,
        gateway_id,
        backend_token,
    })
}

pub fn extract_backend_token(
    headers: &HeaderMap,
    token_transport: BackendTokenTransport,
) -> Result<BackendToken, ApiError> {
    let authorization = optional_header(headers, AUTHORIZATION)?;
    let jwt_assertion = optional_header(headers, X_JWT_ASSERTION)?;

    if authorization.is_some() && jwt_assertion.is_some() {
        return Err(ApiError::new(WurzburgResultCode::AmbiguousBackendToken));
    }

    match token_transport {
        BackendTokenTransport::AuthorizationBearer => {
            let authorization = authorization
                .ok_or_else(|| ApiError::new(WurzburgResultCode::MissingBackendToken))?;
            if jwt_assertion.is_some() {
                return Err(ApiError::new(
                    WurzburgResultCode::InvalidBackendTokenTransport,
                ));
            }

            let token = authorization
                .strip_prefix("Bearer ")
                .filter(|token| !token.is_empty() && !token.contains(char::is_whitespace))
                .ok_or_else(|| ApiError::new(WurzburgResultCode::InvalidBackendTokenTransport))?;

            BackendToken::from_verified_transport(token_transport, token)
        }
        BackendTokenTransport::XJwtAssertion => {
            if authorization.is_some() {
                return Err(ApiError::new(
                    WurzburgResultCode::InvalidBackendTokenTransport,
                ));
            }

            let token = jwt_assertion
                .ok_or_else(|| ApiError::new(WurzburgResultCode::MissingBackendToken))?;

            if token.is_empty() || token.contains(char::is_whitespace) {
                return Err(ApiError::new(
                    WurzburgResultCode::InvalidBackendTokenTransport,
                ));
            }

            BackendToken::from_verified_transport(token_transport, token)
        }
    }
}

fn required_header(headers: &HeaderMap, name: &'static str) -> Result<String, ApiError> {
    optional_header(headers, name)?.ok_or_else(|| {
        ApiError::with_details(
            WurzburgResultCode::MissingTrustedRequestHeader,
            serde_json::json!({ "header": name }),
        )
    })
}

fn optional_header(headers: &HeaderMap, name: &'static str) -> Result<Option<String>, ApiError> {
    let name = HeaderName::from_static(name);
    let Some(value) = headers.get(name) else {
        return Ok(None);
    };

    value
        .to_str()
        .map(|value| Some(value.to_string()))
        .map_err(|_| ApiError::new(WurzburgResultCode::InvalidTrustedRequestHeader))
}

fn invalid_header(name: &'static str) -> ApiError {
    ApiError::with_details(
        WurzburgResultCode::InvalidTrustedRequestHeader,
        serde_json::json!({ "header": name }),
    )
}

fn is_valid_correlation_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
}
