use axum::{
    Router, middleware,
    routing::{get, post},
};
use std::sync::Arc;

use super::handlers::{audit_logs, card_policies, card_ranges, provider_events, providers, system};
use crate::telemetry::http::trace_http_request;
use crate::{api::cors::manual_cors_middleware, state::AppState};

pub fn build_app_router(state: Arc<AppState>) -> Router {
    let token_transport = state.config.wso2.backend_token_transport;
    let system_routes = Router::new().route("/db-health", get(system::db_health));

    let api_router = Router::new()
        .route(
            "/providers",
            post(providers::create_provider).get(providers::list_providers),
        )
        .route("/providers/{provider_id}", get(providers::get_provider))
        .route(
            "/providers/{provider_id}/ledger",
            get(providers::get_provider_ledger),
        )
        .route("/admin/audit-logs", get(audit_logs::list_audit_logs))
        .route(
            "/providers/{provider_id}/kafka/credentials",
            get(providers::get_provider_kafka_credentials),
        )
        .route(
            "/providers/{provider_id}/kafka/status",
            get(providers::get_provider_kafka_status),
        )
        .route(
            "/providers/{provider_id}/kafka/provision",
            post(providers::provision_provider_kafka),
        )
        .route(
            "/providers/{provider_id}/kafka/rotate-credentials",
            post(providers::rotate_provider_kafka_credentials),
        )
        .route(
            "/providers/{provider_id}/kafka/suspend",
            post(providers::suspend_provider_kafka),
        )
        .route(
            "/providers/{provider_id}/kafka/resume",
            post(providers::resume_provider_kafka),
        )
        .route(
            "/providers/kafka/certificate",
            get(providers::get_provider_kafka_certificate),
        )
        .route(
            "/providers/{provider_id}/event-subscriptions",
            get(provider_events::get_provider_event_subscriptions),
        )
        .route(
            "/admin/provider-event-types",
            get(provider_events::list_provider_event_types),
        )
        .route(
            "/admin/provider-event-types/{event_type}/schemas/{schema_version}",
            get(provider_events::get_provider_event_contract),
        )
        .route(
            "/admin/providers/{provider_id}/event-subscriptions",
            get(provider_events::get_admin_provider_event_subscriptions)
                .put(provider_events::replace_provider_event_subscriptions),
        )
        .route(
            "/providers/{provider_id}/card-range",
            axum::routing::put(providers::assign_provider_card_range),
        )
        .route(
            "/providers/{provider_id}/activate",
            post(providers::activate_provider),
        )
        .route(
            "/providers/{provider_id}/suspend",
            post(providers::suspend_provider),
        )
        .route(
            "/providers/{provider_id}/deactivate",
            post(providers::deactivate_provider),
        )
        .route(
            "/card-ranges",
            post(card_ranges::create_card_range).get(card_ranges::list_card_ranges),
        )
        .route(
            "/card-ranges/{card_range_id}",
            get(card_ranges::get_card_range).patch(card_ranges::update_draft_card_range),
        )
        .route(
            "/card-ranges/{card_range_id}/activate",
            post(card_ranges::activate_card_range),
        )
        .route(
            "/card-ranges/{card_range_id}/suspend",
            post(card_ranges::suspend_card_range),
        )
        .route(
            "/card-ranges/{card_range_id}/operational-controls",
            axum::routing::put(card_ranges::update_card_range_controls),
        )
        .route(
            "/card-ranges/{card_range_id}/providers",
            get(card_ranges::list_card_range_providers),
        )
        .route(
            "/operations/{operation_id}",
            get(card_ranges::get_integration_operation),
        )
        .route(
            "/card-ranges/{card_range_id}/policy",
            get(card_policies::get_current_card_policy).put(card_policies::set_card_policy),
        )
        .route(
            "/card-ranges/{card_range_id}/policies",
            get(card_policies::list_card_policies),
        )
        .route(
            "/card-ranges/{card_range_id}/policies/{policy_id}",
            get(card_policies::get_card_policy),
        )
        .nest("/system", system_routes);

    Router::new()
        .route("/ping", get(|| async { "pong" }))
        .nest("/api/v1", api_router)
        .with_state(state)
        .layer(middleware::from_fn_with_state(
            token_transport,
            trace_http_request,
        ))
        .layer(axum::middleware::from_fn(manual_cors_middleware))
}
