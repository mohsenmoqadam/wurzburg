use axum::Router;
use utoipa::openapi::ServerBuilder;
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi};
use utoipa_swagger_ui::SwaggerUi;

use super::handlers::{card_ranges, system};

#[derive(OpenApi)]
#[openapi(
    modifiers(&SecurityAddon),
    paths(
            card_ranges::create_card_range,
            card_ranges::get_card_range,
            card_ranges::list_card_ranges,
            system::db_health
    ),
    components(
        schemas(
            card_ranges::CreateCardRangeRequest,
            card_ranges::CardRangeResponse,
            card_ranges::ListCardRangesQuery,
            card_ranges::ListCardRangesResponse,
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

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Default::default);

        components.add_security_scheme(
            "wso2_backend_bearer",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .bearer_format("JWT")
                    .description(Some(
                        "WSO2 backend JWT assertion transported through the Authorization header.",
                    ))
                    .build(),
            ),
        );
    }
}

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
