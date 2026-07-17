use axum::Router;
use utoipa::OpenApi;
use utoipa::openapi::ServerBuilder;
use utoipa_swagger_ui::SwaggerUi;

use super::handlers::{card_ranges, system};

#[derive(OpenApi)]
#[openapi(
    paths(
            card_ranges::create_card_range,
            system::db_health
    ),
    components(
        schemas(
            card_ranges::CreateCardRangeRequest,
            card_ranges::CardRangeResponse,
            card_ranges::CardRangeFundingModeDto,
            card_ranges::CardRangeWithdrawalLimitAuthorityDto,
            card_ranges::CardRangeStatusDto,
            card_ranges::CardRangeCmsOperationModeDto,
            card_ranges::LimitCalendarDto,
            card_ranges::WeekStartDayDto,
            card_ranges::LimitWindowModeDto,
            system::DbHealthResponse,
            crate::api::error::ApiErrorResponse,
            crate::api::error::ApiErrorBody
        )
    ),
    tags(
        (name = "Card Ranges", description = "Platform-admin card range control APIs"),
        (name = "System", description = "Operational health APIs")
    )
)]
pub struct ApiDoc;

pub fn swagger_router(swagger_path: &str, api_host: &str, api_port: u16) -> Router {
    let mut openapi = ApiDoc::openapi();
    let api_url = format!("http://{}:{}", api_host, api_port);

    openapi.servers = Some(vec![
        ServerBuilder::new()
            .url(api_url)
            .description(Some("Main API Server"))
            .build(),
    ]);

    Router::new()
        .merge(SwaggerUi::new(swagger_path.to_string()).url("/api-docs/openapi.json", openapi))
}
