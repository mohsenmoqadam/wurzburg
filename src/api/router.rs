use axum::{
    Router,
    routing::{get, post},
};
use std::sync::Arc;
//use tower_http::trace::{DefaultMakeSpan, DefaultOnRequest, DefaultOnResponse, TraceLayer};
//use tracing::Level;

use super::handlers::system;
use crate::api::handlers::{card_policy, card_range};
use crate::{
    api::cors::manual_cors_middleware, state::AppState, telemetry::http::trace_http_request,
};

// use super::handlers::{provider, user, system, reserve, fund};

pub fn build_app_router(state: Arc<AppState>) -> Router {
    let card_range_routes = Router::new()
        .route("/", post(card_range::create).get(card_range::list))
        .route(
            "/{card_range_id}",
            get(card_range::get).patch(card_range::update),
        )
        .route("/{card_range_id}/activate", post(card_range::activate))
        .route("/{card_range_id}/suspend", post(card_range::suspend))
        .route(
            "/{card_range_id}/providers",
            get(card_range::list_providers),
        )
        .route(
            "/{card_range_id}/providers/{provider_id}",
            post(card_range::attach_provider).delete(card_range::suspend_provider),
        )
        .route(
            "/{card_range_id}/policy",
            post(card_policy::create_range_policy).get(card_policy::get_range_policy),
        );

    let system_routes = Router::new().route("/db-health", get(system::db_health));

    let api_router = Router::new()
        .route(
            "/providers/{provider_id}/card-ranges",
            get(card_range::list_provider_ranges),
        )
        .nest("/card-ranges", card_range_routes)
        .nest("/system", system_routes)
        .layer(axum::middleware::from_fn(trace_http_request));

    // Return the application router with state attached
    if state.config.swagger.enabled {
        Router::new()
            .route("/ping", get(|| async { "pong" }))
            .nest("/api/v1", api_router) // Good practice to version APIs
            .with_state(state)
            .layer(axum::middleware::from_fn(manual_cors_middleware))
    } else {
        Router::new()
            .route("/ping", get(|| async { "pong" }))
            .nest("/api/v1", api_router) // Good practice to version APIs
            .with_state(state)
    }
}
