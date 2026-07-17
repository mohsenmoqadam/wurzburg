use axum::{
    extract::{MatchedPath, State},
    http::{HeaderMap, HeaderName, HeaderValue, Request, StatusCode},
    middleware::Next,
    response::Response,
};
use tracing::{Instrument, field};

use crate::api::{
    request_context::extract_trusted_request_context, result_codes::WurzburgResultCode,
};
use crate::config::BackendTokenTransport;

pub static RESULT_CODE_HEADER: HeaderName = HeaderName::from_static("x-wurzburg-result-code");
pub static RESULT_SYMBOL_HEADER: HeaderName = HeaderName::from_static("x-wurzburg-result-symbol");

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpTraceSummary {
    pub method: String,
    pub route: String,
    pub status: StatusCode,
    pub result_code: u16,
    pub result_symbol: String,
    pub correlation_id: Option<String>,
    pub request_id: Option<String>,
    pub client_ip: Option<String>,
    pub gateway_id: Option<String>,
}

/// Creates the mandatory HTTP server span for every request.
///
/// The middleware records only contract-safe metadata. It intentionally never
/// records request bodies, JWTs, assertion headers, idempotency keys, PANs,
/// national IDs, SQL values, or arbitrary user metadata.
pub async fn trace_http_request(
    State(token_transport): State<BackendTokenTransport>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let method = request.method().clone();
    let route = route_template(&request);
    let trusted_context = extract_trusted_request_context(request.headers(), token_transport).ok();

    if let Some(context) = trusted_context.clone() {
        request.extensions_mut().insert(context);
    }

    let correlation_id = trusted_context
        .as_ref()
        .map(|context| context.correlation_id.as_str())
        .unwrap_or("");
    let request_id = trusted_context
        .as_ref()
        .map(|context| context.request_id.to_string())
        .unwrap_or_default();
    let client_ip = trusted_context
        .as_ref()
        .map(|context| context.client_ip.to_string())
        .unwrap_or_default();
    let gateway_id = trusted_context
        .as_ref()
        .map(|context| context.gateway_id.as_str())
        .unwrap_or("");

    let span = tracing::info_span!(
        "http.server.request",
        http.request.method = %method,
        http.route = %route,
        http.response.status_code = field::Empty,
        wurzburg.result_code = field::Empty,
        wurzburg.result_symbol = field::Empty,
        wurzburg.correlation_id = %correlation_id,
        wurzburg.request_id = %request_id,
        wurzburg.client_ip = %client_ip,
        wurzburg.gateway_id = %gateway_id,
    );

    async move {
        let mut response = next.run(request).await;
        let status = response.status();
        ensure_result_headers(response.headers_mut(), status);

        let result_code = response_result_code(response.headers());
        let result_symbol = response_result_symbol(response.headers());

        tracing::Span::current().record("http.response.status_code", response.status().as_u16());
        tracing::Span::current().record("wurzburg.result_code", result_code);
        tracing::Span::current().record("wurzburg.result_symbol", result_symbol.as_str());

        response
    }
    .instrument(span)
    .await
}

pub fn summarize_response_for_trace(
    method: &str,
    route: &str,
    status: StatusCode,
    headers: &HeaderMap,
) -> HttpTraceSummary {
    HttpTraceSummary {
        method: method.to_string(),
        route: route.to_string(),
        status,
        result_code: response_result_code(headers),
        result_symbol: response_result_symbol(headers),
        correlation_id: header_to_string(headers, "x-correlation-id"),
        request_id: header_to_string(headers, "x-request-id"),
        client_ip: header_to_string(headers, "x-wso2-client-ip"),
        gateway_id: header_to_string(headers, "x-wso2-gateway-id"),
    }
}

pub fn ensure_result_headers(headers: &mut HeaderMap, status: StatusCode) {
    if headers.contains_key(&RESULT_CODE_HEADER) && headers.contains_key(&RESULT_SYMBOL_HEADER) {
        return;
    }

    let (result_code, result_symbol) = if status.is_success() {
        let (rs_code, code, _, _) = WurzburgResultCode::Success.parts();
        (rs_code, code)
    } else {
        let (rs_code, code, _, _) = WurzburgResultCode::SystemError.parts();
        (rs_code, code)
    };

    headers.insert(
        RESULT_CODE_HEADER.clone(),
        HeaderValue::from_str(&result_code.to_string()).expect("static result code is valid"),
    );
    headers.insert(
        RESULT_SYMBOL_HEADER.clone(),
        HeaderValue::from_static(result_symbol),
    );
}

pub fn response_result_code(headers: &HeaderMap) -> u16 {
    headers
        .get(&RESULT_CODE_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or_else(|| WurzburgResultCode::SystemError.parts().0)
}

pub fn response_result_symbol(headers: &HeaderMap) -> String {
    headers
        .get(&RESULT_SYMBOL_HEADER)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_else(|| WurzburgResultCode::SystemError.parts().1)
        .to_string()
}

fn route_template(request: &Request<axum::body::Body>) -> String {
    request
        .extensions()
        .get::<MatchedPath>()
        .map(|matched| matched.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string())
}

fn header_to_string(headers: &HeaderMap, name: &'static str) -> Option<String> {
    headers
        .get(HeaderName::from_static(name))
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}
