mod support;

use axum::http::StatusCode;
use serde_json::json;
use support::{signed_conflicting_client_jwt, test_wso2_config};
use wurzburg::{
    api::{
        auth::extract_trusted_actor,
        handlers::card_ranges::{
            CardRangeCmsOperationModeDto, CardRangeFundingModeDto,
            CardRangeWithdrawalLimitAuthorityDto, CreateCardRangeRequest, ListCardRangesQuery,
        },
        request_context::{BackendToken, TrustedRequestContext},
        result_codes::WurzburgResultCode,
    },
    config::BackendTokenTransport,
    domain::card_range::{CardRangeListQuery, NewCardRange},
};

fn trusted_context(assertion: String) -> TrustedRequestContext {
    TrustedRequestContext {
        correlation_id: "corr-card-range-failed".to_string(),
        request_id: uuid::Uuid::new_v4(),
        client_ip: "198.51.100.10".parse().expect("valid IP"),
        gateway_id: "wso2-gw-1".to_string(),
        backend_token: BackendToken::from_verified_transport(
            BackendTokenTransport::XJwtAssertion,
            assertion,
        )
        .expect("test assertion transport should be accepted"),
    }
}

#[test]
fn rejects_conflicting_authorized_party_claims() {
    let context = trusted_context(signed_conflicting_client_jwt());

    let error = extract_trusted_actor(&context, &test_wso2_config())
        .expect_err("conflicting azp/client_id must be rejected");

    assert_eq!(error.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(error.body().error.code, "INVALID_ACTOR_CLAIM");
}

#[test]
fn rejects_create_request_when_platform_authority_has_no_calendar() {
    let request = CreateCardRangeRequest {
        start_card_number: "6219861000000001".to_string(),
        end_card_number: "6219861000000999".to_string(),
        funding_mode: CardRangeFundingModeDto::SingleProvider,
        withdrawal_limit_authority: CardRangeWithdrawalLimitAuthorityDto::Platform,
        limit_calendar: None,
        issuance_enabled: true,
        cms_operation_mode: CardRangeCmsOperationModeDto::Full,
        metadata: json!({}),
    };

    let card_range = NewCardRange::try_from(request)
        .expect("structural mapping should succeed before domain validation");
    let error = card_range
        .validate()
        .expect_err("PLATFORM authority requires a limit calendar");

    assert_eq!(
        error.to_string(),
        "PLATFORM withdrawal authority requires a limit calendar"
    );
}

#[test]
fn rejects_unknown_card_range_api_enum_value() {
    let error = serde_json::from_value::<CreateCardRangeRequest>(json!({
        "start_card_number": "6219861000000001",
        "end_card_number": "6219861000000999",
        "funding_mode": "SINGLE_PROVIDER",
        "withdrawal_limit_authority": "UNSUPPORTED",
        "limit_calendar": null,
        "issuance_enabled": true,
        "cms_operation_mode": "FULL"
    }))
    .expect_err("unknown enum values must not deserialize");

    assert!(error.to_string().contains("UNSUPPORTED"));
    assert_eq!(
        WurzburgResultCode::InvalidCardRangeBoundary.parts().1,
        "INVALID_CARD_RANGE_BOUNDARY"
    );
}

#[test]
fn rejects_invalid_withdrawal_authority_filter_with_specific_result_code() {
    let error = CardRangeListQuery::try_from(ListCardRangesQuery {
        status: None,
        funding_mode: None,
        withdrawal_limit_authority: Some("UNSUPPORTED".to_string()),
        cursor: None,
        limit: Some(50),
    })
    .expect_err("invalid authority filter should fail");

    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        error.body().error.code,
        "INVALID_WITHDRAWAL_LIMIT_AUTHORITY"
    );
}

#[test]
fn rejects_out_of_range_card_range_page_limit() {
    let error = CardRangeListQuery::try_from(ListCardRangesQuery {
        status: None,
        funding_mode: None,
        withdrawal_limit_authority: None,
        cursor: None,
        limit: Some(101),
    })
    .expect_err("page limit above the production bound should fail");

    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    assert_eq!(error.body().error.code, "INVALID_CARD_RANGE_FILTER");
}
