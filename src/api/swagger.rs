use super::handlers::{card_policy, card_range, system};
use axum::Router;
use utoipa::OpenApi;
use utoipa::openapi::ServerBuilder;
use utoipa_swagger_ui::SwaggerUi;

#[derive(OpenApi)]
#[openapi(
    paths(
        system::db_health,
        card_range::create,
        card_range::get,
        card_range::list,
        card_range::update,
        card_range::activate,
        card_range::suspend,
        card_range::attach_provider,
        card_range::suspend_provider,
        card_range::list_providers,
        card_range::list_provider_ranges,
        card_policy::create_range_policy,
        card_policy::get_range_policy
    ),
    components(
        schemas(
            system::DbHealthResponse,
            system::SystemErrorResponse,
            system::SystemErrorBody,
            crate::api::error::ApiErrorResponse,
            crate::api::error::ApiErrorBody,
            crate::api::dto::card_range::CreateCardRangeRequest,
            crate::api::dto::card_range::UpdateCardRangeRequest,
            crate::api::dto::card_range::AttachCardRangeProviderRequest,
            crate::api::dto::card_range::CardRangeResponse,
            crate::api::dto::card_range::CardRangeListResponse,
            crate::api::dto::card_range::CardRangeProviderResponse,
            crate::api::dto::card_range::CardRangeProviderListResponse,
            crate::api::dto::card_range::FundingModeDto,
            crate::api::dto::card_range::CardRangeStatusDto,
            crate::api::dto::card_range::CardRangeProviderStatusDto,
            crate::api::dto::card_policy::CreateCardRangePolicyRequest,
            crate::api::dto::card_policy::CardRangePolicyResponse,
            crate::api::dto::card_policy::CardPolicyProfileStatusDto,
            crate::api::dto::card_policy::CardRangePolicyAssignmentStatusDto
        )
    ),
    tags(
        (name = "Card Ranges", description = "Card range and provider eligibility APIs"),
        (name = "Card Policies", description = "Range-scoped card policy APIs"),
        (name = "System", description = "System health and diagnostics APIs")
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
