use uuid::Uuid;
use wurzburg::domain::card_range::{
    CardNumberRange, CmsOperationMode, FundingMode, LimitCalendar, LimitWindowMode, NewCardRange,
    WeekStartDay, WithdrawalLimitAuthority, normalize_card_number,
};

fn tehran_calendar() -> LimitCalendar {
    LimitCalendar {
        timezone: "Asia/Tehran".to_string(),
        week_starts_on: WeekStartDay::Saturday,
        window_mode: LimitWindowMode::Calendar,
    }
}

#[test]
fn accepts_platform_authority_range_with_calendar() {
    let range = NewCardRange {
        card_range_id: Uuid::new_v4(),
        numbers: CardNumberRange::new("6219861000000000", "6219861000000999").unwrap(),
        funding_mode: FundingMode::SingleProvider,
        withdrawal_limit_authority: WithdrawalLimitAuthority::Platform,
        limit_calendar: Some(tehran_calendar()),
        issuance_enabled: true,
        cms_operation_mode: CmsOperationMode::Full,
        metadata_json: serde_json::json!({}),
    };

    range.validate().expect("platform range should be valid");
}

#[test]
fn accepts_cms_authority_range_without_calendar() {
    let range = NewCardRange {
        card_range_id: Uuid::new_v4(),
        numbers: CardNumberRange::new("6219861000001000", "6219861000001999").unwrap(),
        funding_mode: FundingMode::MultiProvider,
        withdrawal_limit_authority: WithdrawalLimitAuthority::Cms,
        limit_calendar: None,
        issuance_enabled: true,
        cms_operation_mode: CmsOperationMode::Full,
        metadata_json: serde_json::json!({}),
    };

    range
        .validate()
        .expect("cms-authority range should be valid");
}

#[test]
fn normalizes_valid_card_number_boundary_without_luhn_validation() {
    let normalized = normalize_card_number("6219861000000000").unwrap();

    assert_eq!(normalized, "6219861000000000");
}

#[test]
fn detects_overlapping_card_number_ranges() {
    let first = CardNumberRange::new("6219861000000000", "6219861000000999").unwrap();
    let second = CardNumberRange::new("6219861000000500", "6219861000001999").unwrap();

    assert!(first.overlaps(&second));
}
