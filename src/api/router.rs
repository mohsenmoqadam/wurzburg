use axum::{
    Router, middleware,
    routing::{get, post},
};
use std::sync::Arc;

use super::handlers::{card_ranges, system};
use crate::telemetry::http::trace_http_request;
use crate::{api::cors::manual_cors_middleware, state::AppState};

pub fn build_app_router(state: Arc<AppState>) -> Router {
    let token_transport = state.config.wso2.backend_token_transport;
    let system_routes = Router::new().route("/db-health", get(system::db_health));

    let api_router = Router::new()
        .route(
            "/card-ranges",
            post(card_ranges::create_card_range).get(card_ranges::list_card_ranges),
        )
        .route(
            "/card-ranges/{card_range_id}",
            get(card_ranges::get_card_range),
        )
        .nest("/system", system_routes);

    let app = Router::new()
        .route("/ping", get(|| async { "pong" }))
        .nest("/api/v1", api_router)
        .with_state(state)
        .layer(middleware::from_fn_with_state(
            token_transport,
            trace_http_request,
        ))
        .layer(axum::middleware::from_fn(manual_cors_middleware));

    app
}
