use axum::Router;
use utoipa::openapi::ServerBuilder;
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi};
use utoipa_swagger_ui::SwaggerUi;

use super::handlers::{card_policies, card_ranges, provider_events, providers, system};

#[derive(OpenApi)]
#[openapi(
    modifiers(&SecurityAddon),
    paths(
            providers::create_provider,
            providers::get_provider,
            providers::get_provider_kafka_credentials,
            providers::get_provider_kafka_status,
            providers::get_provider_kafka_certificate,
            providers::provision_provider_kafka,
            providers::rotate_provider_kafka_credentials,
            providers::suspend_provider_kafka,
            providers::resume_provider_kafka,
            provider_events::list_provider_event_types,
            provider_events::get_admin_provider_event_subscriptions,
            provider_events::get_provider_event_subscriptions,
            provider_events::replace_provider_event_subscriptions,
            providers::assign_provider_card_range,
            providers::activate_provider,
            providers::suspend_provider,
            providers::deactivate_provider,
            card_ranges::create_card_range,
            card_ranges::get_card_range,
            card_ranges::list_card_ranges,
            card_ranges::update_draft_card_range,
            card_ranges::activate_card_range,
            card_ranges::suspend_card_range,
            card_ranges::update_card_range_controls,
            card_ranges::list_card_range_providers,
            card_ranges::get_integration_operation,
            card_policies::set_card_policy,
            card_policies::get_current_card_policy,
            card_policies::get_card_policy,
            card_policies::list_card_policies,
            system::db_health
    ),
    components(
        schemas(
            providers::CreateProviderRequest,
            providers::ProviderContactRequest,
            providers::ProviderOperationalProfileRequest,
            providers::ProviderOperationalControlsRequest,
            providers::ProviderUserOnboardingControlRequest,
            providers::ProviderActiveWindowRequest,
            providers::ProviderCreditGrantControlRequest,
            providers::ProviderEnabledControlRequest,
            providers::ProviderCardOperationsControlRequest,
            providers::ProviderEventDeliveryControlRequest,
            providers::ProviderContactTypeDto,
            providers::CreditGrantLimitModeDto,
            providers::ProviderWeekdayDto,
            providers::ProviderStatusDto,
            providers::ProviderResponse,
            providers::ProviderCoreProvisioningStatusDto,
            providers::ProviderKafkaProvisioningStatusDto,
            providers::AssignProviderCardRangeRequest,
            providers::ProviderCardRangeAssignmentResponse,
            providers::ProviderLifecycleRequest,
            providers::ProviderKafkaCredentialsResponse,
            providers::ProviderKafkaStatusResponse,
            providers::ProviderKafkaOperationStatusResponse,
            providers::ProviderKafkaCertificateResponse,
            providers::ProviderKafkaCommandRequest,
            providers::ProviderKafkaCommandResponse,
            provider_events::ProviderEventTypeDto,
            provider_events::ProviderEventTypeCatalogResponse,
            provider_events::ProviderEventTypeCatalogItem,
            provider_events::ReplaceProviderEventSubscriptionsRequest,
            provider_events::ProviderEventSubscriptionInput,
            provider_events::ProviderEventSubscriptionsResponse,
            provider_events::ProviderEventSubscriptionResponse,
            card_ranges::CreateCardRangeRequest,
            card_ranges::CardRangeResponse,
            card_ranges::ListCardRangesQuery,
            card_ranges::ListCardRangesResponse,
            card_ranges::UpdateDraftCardRangeRequest,
            card_ranges::CardRangeTransitionRequest,
            card_ranges::UpdateCardRangeControlsRequest,
            card_ranges::CardRangeMutationResponse,
            card_ranges::CardRangeProviderEligibilityResponse,
            card_ranges::ListCardRangeProvidersResponse,
            card_ranges::IntegrationOperationResponse,
            card_ranges::CardRangeFundingModeDto,
            card_ranges::CardRangeWithdrawalLimitAuthorityDto,
            card_ranges::CardRangeStatusDto,
            card_ranges::CardRangeCmsOperationModeDto,
            card_ranges::LimitCalendarDto,
            card_ranges::WeekStartDayDto,
            card_ranges::LimitWindowModeDto,
            card_policies::SetCardPolicyRequest,
            card_policies::WithdrawalLimitsDto,
            card_policies::WithdrawalWindowLimitDto,
            card_policies::CardPolicyStatusDto,
            card_policies::PolicyMutationDispositionDto,
            card_policies::CardPolicyRecordResponse,
            card_policies::CardPolicyResponse,
            card_policies::SetCardPolicyResponse,
            card_policies::ListCardPoliciesQuery,
            card_policies::ListCardPoliciesResponse,
            system::DbHealthResponse,
            crate::api::error::ApiErrorResponse,
            crate::api::error::ApiErrorBody
        )
    ),
    tags(
        (name = "Providers", description = "Platform-admin provider lifecycle and provisioning APIs"),
        (name = "Provider Events", description = "Provider event catalog and delivery subscription controls"),
        (name = "Card Ranges", description = "Platform-admin card range control APIs"),
        (name = "Card Policies", description = "Platform-admin range policy APIs"),
        (name = "Operations", description = "Durable integration operation status APIs"),
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
