use axum::{
    body::Body,
    extract::Request,
    http::{HeaderMap, header::HeaderName},
    middleware::Next,
    response::Response,
};
use opentelemetry::propagation::Extractor;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub async fn trace_http_request(request: Request<Body>, next: Next) -> Response {
    let method = request.method().clone();
    let uri = request.uri().clone();
    let path = uri.path().to_string();
    let user_agent = request
        .headers()
        .get(axum::http::header::USER_AGENT)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string();
    let idempotency_key = request
        .headers()
        .get("Idempotency-Key")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string();
    let parent_context = opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.extract(&HeaderExtractor(request.headers()))
    });

    let span = tracing::info_span!(
        "http.server.request",
        otel.kind = "server",
        http.request.method = %method,
        url.path = %path,
        url.query = uri.query().unwrap_or(""),
        user_agent.original = %user_agent,
        idempotency_key = %idempotency_key,
        http.response.status_code = tracing::field::Empty,
    );
    span.set_parent(parent_context);
    let response_span = span.clone();

    async move {
        let response = next.run(request).await;
        response_span.record("http.response.status_code", response.status().as_u16());

        response
    }
    .instrument(span)
    .await
}

struct HeaderExtractor<'a>(&'a HeaderMap);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        let header_name = HeaderName::from_bytes(key.as_bytes()).ok()?;
        self.0
            .get(header_name)
            .and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(HeaderName::as_str).collect()
    }
}
