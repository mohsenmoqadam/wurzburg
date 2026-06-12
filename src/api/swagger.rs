use utoipa::OpenApi;
use utoipa::openapi::ServerBuilder;
use utoipa_swagger_ui::SwaggerUi;
use axum::Router;
use super::handlers::{provider, user, transaction, priority};

#[derive(OpenApi)]
#[openapi(
    paths(
        provider::create,
        provider::get,
        provider::download_cert,
        user::create_or_link,
        user::get,
        user::get_user_balance, 
        transaction::credit_user,
        transaction::debit_user,
        transaction::get_provider_transactions,
        transaction::get_user_transactions,
        priority::create_user_priority,
        priority::get_active_user_priority,
        priority::cancel_active_user_priority,
        priority::get_all_user_priorities 
    ),
    components(
        schemas(
            provider::CreateProviderRequest,
            provider::CreateProviderResponse,
            provider::GetProviderResponse,
            user::CreateUserRequest,
            user::CreateUserResponse,
            user::GetUserResponse,
            user::UserBalanceResponse,
            user::ProviderBalanceItem, 
            transaction::CreditAccountRequest,
            transaction::DebitAccountRequest,
            transaction::TransactionResponse,
            transaction::TransactionItem,
            transaction::PaginatedTransactionsResponse,
            crate::db::models::TransactionType,
            crate::db::models::TransactionStatus,
            priority::CreatePriorityRequest,
            priority::PriorityItemRequest,
            priority::PriorityConfigResponse,
            priority::PriorityConfigListResponse,
            crate::db::models::UserPriorityDetails,
            crate::db::models::UserPriorityConfig,
            crate::db::models::UserPriorityItem,
            crate::db::models::PriorityStatus,
            crate::db::models::PriorityUsageType     
        )
    ),
    tags(
        (name = "Providers", description = "Provider management APIs"),
        (name = "Users", description = "User management APIs"),
        (name = "Transactions", description = "Transaction management APIs"),
        (name = "Priorities", description = "User balance consumption priority APIs") 
    )
)]
pub struct ApiDoc;

// Pass host and port from your config as arguments
pub fn swagger_router(swagger_path: &str, api_host: &str, api_port: u16) -> Router {
    let mut openapi = ApiDoc::openapi();

    // Create the dynamic URL
    let api_url = format!("http://{}:{}", api_host, api_port);
    
    // Set the server URL dynamically
    openapi.servers = Some(vec![
        ServerBuilder::new()
            .url(api_url)
            .description(Some("Main API Server"))
            .build(),
    ]);

    // Build and return the router
    Router::new()
        .merge(SwaggerUi::new(swagger_path.to_string()).url("/api-docs/openapi.json", openapi))
}
