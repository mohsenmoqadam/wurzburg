use axum::Router;
use serde_json::json;
use utoipa::openapi::Required;
use utoipa::openapi::ServerBuilder;
use utoipa::openapi::path::{Operation, Parameter, ParameterBuilder, ParameterIn, PathItem};
use utoipa::openapi::schema::{ObjectBuilder, Type};
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi};
use utoipa_swagger_ui::SwaggerUi;

use super::handlers::{
    audit_logs, card_issuance, card_policies, card_ranges, cards, financial_transactions,
    provider_credits, provider_events, provider_fees, provider_identity,
    provider_operational_profiles, provider_users, providers, system,
};

#[derive(OpenApi)]
#[openapi(
    modifiers(&SecurityAddon),
    paths(
            providers::create_provider,
            providers::list_providers,
            providers::get_provider,
            provider_identity::update_provider_identity,
            provider_identity::create_provider_contact,
            provider_identity::list_provider_contacts,
            provider_identity::update_provider_contact,
            provider_identity::suspend_provider_contact,
            provider_identity::reactivate_provider_contact,
            providers::get_provider_ledger,
            audit_logs::list_audit_logs,
            providers::get_provider_kafka_credentials,
            providers::get_provider_kafka_status,
            providers::get_provider_kafka_certificate,
            providers::provision_provider_kafka,
            providers::rotate_provider_kafka_credentials,
            providers::suspend_provider_kafka,
            providers::resume_provider_kafka,
            provider_events::list_provider_event_types,
            provider_events::get_provider_event_contract,
            provider_events::get_admin_provider_event_subscriptions,
            provider_events::get_provider_event_subscriptions,
            provider_events::replace_provider_event_subscriptions,
            providers::assign_provider_card_range,
            providers::activate_provider,
            providers::suspend_provider,
            providers::deactivate_provider,
            provider_fees::set_provider_fee_profile,
            provider_fees::get_current_provider_fee_profile,
            provider_fees::list_provider_fee_profiles,
            provider_fees::get_provider_fee_profile,
            provider_operational_profiles::set_provider_operational_profile,
            provider_operational_profiles::get_current_provider_operational_profile,
            provider_operational_profiles::list_provider_operational_profiles,
            provider_operational_profiles::cancel_scheduled_provider_operational_profile,
            provider_users::enroll_provider_user,
            provider_users::list_provider_users,
            provider_users::get_provider_user,
            provider_users::list_user_cards,
            provider_users::list_user_providers,
            cards::update_funding_order,
            provider_credits::grant_credit,
            provider_credits::get_provider_user_credit,
            provider_credits::return_provider_credit,
            provider_credits::get_cardholder_credit,
            provider_credits::return_cardholder_credit,
            financial_transactions::list_provider_transactions,
            financial_transactions::list_provider_user_transactions,
            financial_transactions::list_provider_card_transactions,
            financial_transactions::list_provider_account_transactions,
            financial_transactions::list_cardholder_transactions,
            financial_transactions::list_cardholder_card_transactions,
            financial_transactions::list_admin_transactions,
            financial_transactions::list_admin_card_transactions,
            card_issuance::create_card_issuance_batch,
            card_issuance::list_card_issuance_batches,
            card_issuance::get_card_issuance_batch,
            card_issuance::download_card_issuance_request_file,
            card_issuance::upload_card_issuance_result_file,
            card_issuance::get_card_issuance_batch_result,
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
            providers::ListProvidersQuery,
            providers::ProviderListItemResponse,
            providers::ListProvidersResponse,
            providers::ProviderLedgerResponse,
            providers::ProviderLedgerAccountResponse,
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
            provider_identity::UpdateProviderIdentityRequest,
            provider_identity::CreateProviderContactRequest,
            provider_identity::UpdateProviderContactRequest,
            provider_identity::ProviderContactTransitionRequest,
            provider_identity::ListProviderContactsQuery,
            provider_identity::ProviderContactStatusDto,
            provider_identity::ProviderContactResponse,
            provider_identity::ListProviderContactsResponse,
            provider_fees::SetProviderFeeProfileRequest,
            provider_fees::FeePayerDto,
            provider_fees::FeePolicyDto,
            provider_fees::ProviderFeeProfileStatusDto,
            provider_fees::FeeProfileMutationDispositionDto,
            provider_fees::ProviderFeeProfileResponse,
            provider_fees::SetProviderFeeProfileResponse,
            provider_fees::ListProviderFeeProfilesQuery,
            provider_fees::ListProviderFeeProfilesResponse,
            provider_operational_profiles::SetProviderOperationalProfileRequest,
            provider_operational_profiles::CancelProviderOperationalProfileRequest,
            provider_operational_profiles::ProviderOperationalProfileStatusDto,
            provider_operational_profiles::OperationalProfileMutationDispositionDto,
            provider_operational_profiles::ProviderOperationalProfileResponse,
            provider_operational_profiles::SetProviderOperationalProfileResponse,
            provider_operational_profiles::ListProviderOperationalProfilesQuery,
            provider_operational_profiles::ListProviderOperationalProfilesResponse,
            provider_users::EnrollProviderUserRequest,
            provider_users::CardInstructionRequest,
            provider_users::ProviderUserEnrollmentResponse,
            provider_users::PolicyUsageAccountIdsResponse,
            provider_users::ListProviderUsersQuery,
            provider_users::ProviderUserResponse,
            provider_users::ListProviderUsersResponse,
            provider_users::UserCardSummaryResponse,
            provider_users::UserProviderSummaryResponse,
            cards::UpdateFundingOrderRequest,
            cards::FundingOrderSourceRequest,
            cards::FundingOrderResponse,
            cards::FundingOrderAppliedSourceResponse,
            provider_credits::GrantCreditRequest,
            provider_credits::ProviderReturnCreditRequest,
            provider_credits::CardholderReturnCreditRequest,
            provider_credits::CreditBalanceQuery,
            provider_credits::ProviderCreditBalanceResponse,
            provider_credits::CreditMovementResponse,
            financial_transactions::ListTransactionsQuery,
            financial_transactions::ListAdminTransactionsQuery,
            financial_transactions::FinancialTransactionEntryResponse,
            financial_transactions::FinancialTransactionResponse,
            financial_transactions::ListFinancialTransactionsResponse,
            card_issuance::CreateCardIssuanceBatchRequest,
            card_issuance::CardIssuanceBatchResponse,
            card_issuance::ListCardIssuanceBatchesQuery,
            card_issuance::ListCardIssuanceBatchesResponse,
            card_issuance::CardIssuanceResultRowResponse,
            card_issuance::CardIssuanceBatchResultResponse,
            audit_logs::ListAuditLogsQuery,
            audit_logs::AuditLogResponse,
            audit_logs::ListAuditLogsResponse,
            provider_events::ProviderEventTypeDto,
            provider_events::ProviderEventTypeCatalogResponse,
            provider_events::ProviderEventTypeCatalogItem,
            provider_events::ProviderEventContractResponse,
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
        (name = "Provider Identity and Contacts", description = "Platform-admin provider identity and contact APIs"),
        (name = "Provider Fees", description = "Platform-admin provider fee profile APIs"),
        (name = "Provider Users", description = "Provider-scoped user enrollment and card selection APIs"),
        (name = "Provider Credits", description = "Live TigerBeetle credit reads, grants, and full-balance returns"),
        (name = "Cards", description = "Cardholder and platform card funding-control APIs"),
        (name = "Card Issuance", description = "Platform-admin bank card-issuance batch APIs"),
        (name = "Audit", description = "Platform-admin immutable business audit queries"),
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

        add_trusted_gateway_headers(openapi);
    }
}

fn add_trusted_gateway_headers(openapi: &mut utoipa::openapi::OpenApi) {
    for (path, path_item) in openapi.paths.paths.iter_mut() {
        if path.starts_with("/api/v1/") {
            add_trusted_gateway_headers_to_path(path_item);
        }
    }
}

fn add_trusted_gateway_headers_to_path(path_item: &mut PathItem) {
    let operations = [
        &mut path_item.get,
        &mut path_item.put,
        &mut path_item.post,
        &mut path_item.delete,
        &mut path_item.options,
        &mut path_item.head,
        &mut path_item.patch,
        &mut path_item.trace,
    ];

    for operation in operations.into_iter().flatten() {
        ensure_header_example(
            operation,
            "X-Correlation-Id",
            "WSO2 canonical business correlation ID.",
            "ce5c1b18-9050-49b2-9fd2-a2f208a56117",
        );
        ensure_header_example(
            operation,
            "X-Request-Id",
            "WSO2 unique HTTP attempt ID.",
            "ce5c1b18-9050-49b2-9fd2-a2f208a56118",
        );
        ensure_header_example(
            operation,
            "X-WSO2-Client-IP",
            "Client IP address verified and forwarded by WSO2.",
            "172.16.245.5",
        );
        ensure_header_example(
            operation,
            "X-WSO2-Gateway-Id",
            "Identifier of the trusted WSO2 gateway that forwarded the request.",
            "wso2-dev-gateway",
        );
    }
}

fn ensure_header_example(
    operation: &mut Operation,
    name: &'static str,
    description: &'static str,
    example: &'static str,
) {
    let parameters = operation.parameters.get_or_insert_with(Vec::new);
    if let Some(parameter) = parameters.iter_mut().find(|parameter| {
        parameter.parameter_in == ParameterIn::Header && parameter.name.eq_ignore_ascii_case(name)
    }) {
        // Handler annotations may already define the header with a more precise
        // schema (for example UUID). Rebuild it only to add the shared example,
        // preserving that endpoint-specific schema and description.
        let builder: ParameterBuilder = parameter.clone().into();
        *parameter = builder.example(Some(json!(example))).build();
        return;
    }

    parameters.push(header_parameter(name, description, example));
}

fn header_parameter(
    name: &'static str,
    description: &'static str,
    example: &'static str,
) -> Parameter {
    ParameterBuilder::new()
        .name(name)
        .parameter_in(ParameterIn::Header)
        .required(Required::True)
        .description(Some(description))
        .schema(Some(ObjectBuilder::new().schema_type(Type::String)))
        .example(Some(json!(example)))
        .build()
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

#[cfg(test)]
mod tests {
    use serde_json::Value;
    use utoipa::OpenApi;
    use utoipa::openapi::path::{HttpMethod, ParameterIn};

    use super::ApiDoc;

    #[test]
    fn secured_api_operations_document_trusted_gateway_headers() {
        let openapi = ApiDoc::openapi();
        let operation = openapi
            .paths
            .get_path_operation("/api/v1/providers", HttpMethod::Post)
            .expect("create provider operation must be documented");
        let parameters = operation
            .parameters
            .as_ref()
            .expect("create provider operation must document headers");

        for header in [
            "Idempotency-Key",
            "X-Correlation-Id",
            "X-Request-Id",
            "X-WSO2-Client-IP",
            "X-WSO2-Gateway-Id",
        ] {
            assert!(
                parameters.iter().any(|parameter| {
                    parameter.parameter_in == ParameterIn::Header && parameter.name == header
                }),
                "{header} must be documented for Swagger UI"
            );
        }
    }

    #[test]
    fn existing_trusted_headers_receive_shared_swagger_examples() {
        let openapi = ApiDoc::openapi();
        let operation = openapi
            .paths
            .get_path_operation(
                "/api/v1/card-ranges/{card_range_id}/policies",
                HttpMethod::Get,
            )
            .expect("card policy history operation must be documented");
        let parameters = serde_json::to_value(
            operation
                .parameters
                .as_ref()
                .expect("card policy history must document trusted headers"),
        )
        .expect("OpenAPI parameters must serialize");

        let expected = [
            ("X-Correlation-Id", "ce5c1b18-9050-49b2-9fd2-a2f208a56117"),
            ("X-Request-Id", "ce5c1b18-9050-49b2-9fd2-a2f208a56118"),
            ("X-WSO2-Client-IP", "172.16.245.5"),
            ("X-WSO2-Gateway-Id", "wso2-dev-gateway"),
        ];
        let parameters = parameters
            .as_array()
            .expect("serialized OpenAPI parameters must be an array");

        for (name, example) in expected {
            let parameter = parameters
                .iter()
                .find(|parameter| parameter["name"] == Value::String(name.to_string()))
                .unwrap_or_else(|| panic!("{name} must be documented"));
            assert_eq!(parameter["example"], Value::String(example.to_string()));
        }
    }
}
