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
            LimitWindowModeDto, WeekStartDayDto,
        },
        request_context::{BackendToken, TrustedRequestContext},
        swagger::ApiDoc,
    },
    config::BackendTokenTransport,
    domain::card_range::{FundingMode, NewCardRange, WithdrawalLimitAuthority},
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
    assert!(operation_text.contains("platform.card_ranges:write"));
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
