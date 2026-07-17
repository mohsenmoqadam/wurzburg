mod support;

use serde_json::json;
use support::{signed_platform_admin_jwt, test_wso2_config};
use utoipa::OpenApi;
use wurzburg::{
    api::{
        auth::extract_trusted_actor,
        handlers::card_ranges::{
            CardRangeCmsOperationModeDto, CardRangeFundingModeDto,
            CardRangeWithdrawalLimitAuthorityDto, CreateCardRangeRequest, LimitCalendarDto,
            LimitWindowModeDto, ListCardRangesQuery, WeekStartDayDto,
        },
        request_context::{BackendToken, TrustedRequestContext},
        swagger::ApiDoc,
    },
    config::BackendTokenTransport,
    domain::card_range::{
        CardRangeListQuery, CardRangeStatus, FundingMode, NewCardRange, WithdrawalLimitAuthority,
    },
};

#[test]
fn maps_create_card_range_request_to_domain_command() {
    let request = CreateCardRangeRequest {
        start_card_number: "6219861000000001".to_string(),
        end_card_number: "6219861000000999".to_string(),
        funding_mode: CardRangeFundingModeDto::SingleProvider,
        withdrawal_limit_authority: CardRangeWithdrawalLimitAuthorityDto::Platform,
        limit_calendar: Some(LimitCalendarDto {
            timezone: "Asia/Tehran".to_string(),
            week_starts_on: WeekStartDayDto::Saturday,
            window_mode: LimitWindowModeDto::Calendar,
        }),
        issuance_enabled: true,
        cms_operation_mode: CardRangeCmsOperationModeDto::Full,
        metadata: json!({ "ticket": "CARD-RANGE-001" }),
    };

    let card_range =
        NewCardRange::try_from(request).expect("valid API request should map to a domain command");

    assert_eq!(card_range.numbers.start, "6219861000000001");
    assert_eq!(card_range.numbers.end, "6219861000000999");
    assert_eq!(card_range.funding_mode, FundingMode::SingleProvider);
    assert_eq!(
        card_range.withdrawal_limit_authority,
        WithdrawalLimitAuthority::Platform
    );
    assert_eq!(
        card_range.limit_calendar.expect("calendar").timezone,
        "Asia/Tehran"
    );
}

#[test]
fn swagger_exposes_card_range_create_with_required_test_headers() {
    let openapi = serde_json::to_value(ApiDoc::openapi())
        .expect("OpenAPI document should serialize for inspection");
    let operation = &openapi["paths"]["/api/v1/card-ranges"]["post"];
    let operation_text = operation.to_string();

    assert_eq!(operation["tags"][0], "Card Ranges");
    assert!(operation_text.contains("Idempotency-Key"));
    assert!(operation_text.contains("X-Correlation-Id"));
    assert!(operation_text.contains("X-Request-Id"));
    assert!(operation_text.contains("X-WSO2-Client-IP"));
    assert!(operation_text.contains("X-WSO2-Gateway-Id"));
    assert!(operation_text.contains("X-JWT-Assertion"));
    assert_header_is_optional(operation, "X-JWT-Assertion");
    assert_header_is_not_parameter(operation, "Authorization");
    assert_uses_wso2_bearer_security(&openapi, operation);
    assert_header_example(
        operation,
        "Idempotency-Key",
        "ce5c1b18-9050-49b2-9fd2-a2f208a56116",
    );
    assert_header_example(
        operation,
        "X-Correlation-Id",
        "ce5c1b18-9050-49b2-9fd2-a2f208a56117",
    );
    assert_header_example(operation, "X-WSO2-Client-IP", "192.168.0.1");
    assert_eq!(
        operation["requestBody"]["content"]["application/json"]["example"],
        json!({
            "cms_operation_mode": "FULL",
            "start_card_number": "1111000000000000",
            "end_card_number": "1111000000001111",
            "funding_mode": "SINGLE_PROVIDER",
            "withdrawal_limit_authority": "PLATFORM",
            "limit_calendar": {
                "timezone": "Asia/Tehran",
                "week_starts_on": "SATURDAY",
                "window_mode": "CALENDAR"
            },
            "issuance_enabled": true,
            "metadata": {}
        })
    );
    assert!(operation_text.contains("platform.card_ranges:write"));
}

#[test]
fn maps_list_card_ranges_query_to_domain_filter() {
    let query = ListCardRangesQuery {
        status: Some("DRAFT".to_string()),
        funding_mode: Some("SINGLE_PROVIDER".to_string()),
        withdrawal_limit_authority: Some("PLATFORM".to_string()),
        cursor: None,
        limit: Some(25),
    };

    let query = CardRangeListQuery::try_from(query).expect("valid list query should map");

    assert_eq!(query.status, Some(CardRangeStatus::Draft));
    assert_eq!(query.funding_mode, Some(FundingMode::SingleProvider));
    assert_eq!(
        query.withdrawal_limit_authority,
        Some(WithdrawalLimitAuthority::Platform)
    );
    assert_eq!(query.limit, 25);
}

#[test]
fn swagger_exposes_card_range_read_apis_with_required_test_headers() {
    let openapi = serde_json::to_value(ApiDoc::openapi())
        .expect("OpenAPI document should serialize for inspection");
    let get_one = &openapi["paths"]["/api/v1/card-ranges/{card_range_id}"]["get"];
    let list = &openapi["paths"]["/api/v1/card-ranges"]["get"];
    let contract_text = format!("{get_one}{list}");

    assert_eq!(get_one["tags"][0], "Card Ranges");
    assert_eq!(list["tags"][0], "Card Ranges");
    assert!(contract_text.contains("platform.card_ranges:read"));
    assert!(contract_text.contains("X-Correlation-Id"));
    assert!(contract_text.contains("X-Request-Id"));
    assert!(contract_text.contains("X-WSO2-Client-IP"));
    assert!(contract_text.contains("X-WSO2-Gateway-Id"));
    assert!(contract_text.contains("X-JWT-Assertion"));
    assert_header_is_optional(get_one, "X-JWT-Assertion");
    assert_header_is_not_parameter(get_one, "Authorization");
    assert_header_is_optional(list, "X-JWT-Assertion");
    assert_header_is_not_parameter(list, "Authorization");
    assert_uses_wso2_bearer_security(&openapi, get_one);
    assert_uses_wso2_bearer_security(&openapi, list);
    assert!(list.to_string().contains("cursor"));
    assert!(list.to_string().contains("limit"));
}

fn assert_header_is_optional(operation: &serde_json::Value, header_name: &str) {
    let parameter = operation["parameters"]
        .as_array()
        .expect("operation parameters should be an array")
        .iter()
        .find(|parameter| parameter["name"] == header_name)
        .expect("header should be documented");

    assert_eq!(parameter["required"], false);
}

fn assert_header_example(operation: &serde_json::Value, header_name: &str, expected: &str) {
    let parameter = operation["parameters"]
        .as_array()
        .expect("operation parameters should be an array")
        .iter()
        .find(|parameter| parameter["name"] == header_name)
        .expect("header should be documented");

    assert_eq!(parameter["example"], expected);
}

fn assert_header_is_not_parameter(operation: &serde_json::Value, header_name: &str) {
    let exists = operation["parameters"]
        .as_array()
        .expect("operation parameters should be an array")
        .iter()
        .any(|parameter| parameter["name"] == header_name);

    assert!(
        !exists,
        "{header_name} must be modeled as security, not a header parameter"
    );
}

fn assert_uses_wso2_bearer_security(openapi: &serde_json::Value, operation: &serde_json::Value) {
    let scheme = &openapi["components"]["securitySchemes"]["wso2_backend_bearer"];

    assert_eq!(scheme["type"], "http");
    assert_eq!(scheme["scheme"], "bearer");
    assert_eq!(scheme["bearerFormat"], "JWT");
    assert!(
        operation["security"]
            .as_array()
            .expect("operation security should be an array")
            .iter()
            .any(|requirement| requirement.get("wso2_backend_bearer").is_some())
    );
}

#[test]
fn extracts_trusted_actor_from_signed_rs256_jwt() {
    let context = TrustedRequestContext {
        correlation_id: "corr-card-range-1".to_string(),
        request_id: uuid::Uuid::new_v4(),
        client_ip: "198.51.100.10".parse().expect("valid IP"),
        gateway_id: "wso2-gw-1".to_string(),
        backend_token: BackendToken::from_verified_transport(
            BackendTokenTransport::XJwtAssertion,
            signed_platform_admin_jwt(),
        )
        .expect("signed assertion should be accepted"),
    };

    let actor = extract_trusted_actor(&context, &test_wso2_config())
        .expect("valid signed WSO2 assertion should become a trusted actor");

    assert_eq!(actor.subject, "admin-1");
    assert!(actor.has_scope("platform.card_ranges:write"));
    assert!(actor.has_role("wurzburg_platform_admin"));
}
