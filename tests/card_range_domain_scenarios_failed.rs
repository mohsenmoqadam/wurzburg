use wurzburg::domain::card_range::{
    CardNumberRange, CardRangeControlChange, CardRangeError, CmsOperationMode, LimitCalendar,
    LimitWindowMode, WeekStartDay, WithdrawalLimitAuthority, normalize_card_number,
    validate_authority_calendar,
};

fn tehran_calendar() -> LimitCalendar {
    LimitCalendar {
        timezone: "Asia/Tehran".to_string(),
        week_starts_on: WeekStartDay::Saturday,
        window_mode: LimitWindowMode::Calendar,
    }
}

#[test]
fn rejects_operational_control_change_without_audit_reason() {
    let error = CardRangeControlChange {
        issuance_enabled: false,
        cms_operation_mode: CmsOperationMode::Blocked,
        reason: "".to_string(),
    }
    .validate()
    .expect_err("audit reason is mandatory");
    assert_eq!(error, CardRangeError::InvalidReason);
}

#[test]
fn rejects_card_number_boundary_with_wrong_length() {
    let error = normalize_card_number("621986").expect_err("short card number should fail");

    assert_eq!(error, CardRangeError::InvalidCardNumber);
}

#[test]
fn rejects_card_number_boundary_starting_with_zero() {
    let error = normalize_card_number("0219861000000000").expect_err("leading zero should fail");

    assert_eq!(error, CardRangeError::InvalidCardNumber);
}

#[test]
fn rejects_card_number_range_when_start_is_after_end() {
    let error = CardNumberRange::new("6219861000000999", "6219861000000000")
        .expect_err("reversed range should fail");

    assert_eq!(error, CardRangeError::InvalidBoundaryOrder);
}

#[test]
fn rejects_platform_authority_without_calendar() {
    let error = validate_authority_calendar(WithdrawalLimitAuthority::Platform, None)
        .expect_err("platform authority requires calendar");

    assert_eq!(error, CardRangeError::PlatformAuthorityRequiresCalendar);
}

#[test]
fn rejects_cms_authority_with_calendar() {
    let calendar = tehran_calendar();
    let error = validate_authority_calendar(WithdrawalLimitAuthority::Cms, Some(&calendar))
        .expect_err("cms authority requires no calendar");

    assert_eq!(error, CardRangeError::CmsAuthorityRequiresNoCalendar);
}
