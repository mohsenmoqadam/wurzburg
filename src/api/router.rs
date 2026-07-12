use std::sync::Arc;
use axum::{routing::{get, post}, Router};
//use tower_http::trace::{DefaultMakeSpan, DefaultOnRequest, DefaultOnResponse, TraceLayer};
//use tracing::Level;

use crate::{api::cors::manual_cors_middleware, state::AppState};
use super::handlers::provider;
use super::handlers::system;
use crate::api::handlers::user;
use crate::api::handlers::transaction;
use crate::api::handlers::priority;


// use super::handlers::{provider, user, system, reserve, fund};

pub fn build_app_router(state: Arc<AppState>) -> Router {
    // 1. Providers Routes
    let provider_routes = Router::new()
        .route("/", post(provider::create))
        .route("/{id}", get(provider::get))
        .route("/certificate", get(provider::download_cert));

    // 2. Users Routes (Nested under providers usually, or separate)
    let user_routes = Router::new()
        .route("/", post(user::create_or_link))
        .route("/{id}", get(user::get))
        .route("/{id}/balance", get(user::get_user_balance)) 
        .route("/{user_id}/priorities", post(priority::create_user_priority).get(priority::get_all_user_priorities))
        .route("/{user_id}/priorities/active", get(priority::get_active_user_priority).delete(priority::cancel_active_user_priority)); 

    // 3. Credit Routes
    let transaction_routes = Router::new()
        .route("/{provider_id}/users/{user_id}/credit", post(transaction::credit_user))
        .route("/{provider_id}/users/{user_id}/debit", post(transaction::debit_user))
        .route("/providers/{provider_id}", get(transaction::get_provider_transactions))
        .route("/providers/{provider_id}/users/{user_id}", get(transaction::get_user_transactions));

    // 4. System Routes
    let system_routes = Router::new()
        .route("/db-health", get(system::db_health))
        // .route("/metrics", get(system::metrics))
        ;

    // Main API Router combining all sub-routers
    let api_router = Router::new()
        .nest("/providers", provider_routes)
        .nest("/transactions", transaction_routes) 
        .nest("/users", user_routes) // Based on your spec
        .nest("/system", system_routes);

    // Return the application router with state attached
    if state.config.swagger.enabled {
        Router::new()
        .route("/ping", get(|| async { "pong" })) 
        .nest("/api/v1", api_router) // Good practice to version APIs
        .with_state(state)
        .layer(axum::middleware::from_fn(manual_cors_middleware))  
    }
    else {
        Router::new()
        .route("/ping", get(|| async { "pong" })) 
        .nest("/api/v1", api_router) // Good practice to version APIs
        .with_state(state)
    }
}
