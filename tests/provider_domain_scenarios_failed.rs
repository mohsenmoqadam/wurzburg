use chrono::{NaiveTime, Utc};
use uuid::Uuid;
use wurzburg::domain::provider::{
    CreditGrantLimitMode, NewProvider, ProviderActiveWindow, ProviderOperationalControls,
    ProviderOperationalProfile, ProviderWeekday,
};

/// Scenario goal: overlapping weekly controls must be rejected before Oracle,
/// TigerBeetle, Kafka, or audit state can be changed.
#[test]
fn rejects_overlapping_provider_operational_windows() {
    let provider = NewProvider {
        kafka_access: None,
        provider_id: Uuid::new_v4(),
        legal_name: "Legal Provider".to_string(),
        trade_name: "Provider".to_string(),
        tax_id: None,
        registration_number: None,
        email_address: None,
        website_url: None,
        mailing_address: None,
        metadata: serde_json::json!({}),
        contacts: vec![],
        operational_profile: ProviderOperationalProfile {
            effective_at: Utc::now(),
            controls: ProviderOperationalControls {
                timezone: "Asia/Tehran".to_string(),
                user_onboarding_enabled: true,
                active_windows: vec![
                    window("08:00:00", "12:00:00"),
                    window("11:00:00", "13:00:00"),
                ],
                max_total_users: None,
                credit_grant_enabled: true,
                credit_grant_mode: CreditGrantLimitMode::FixedLimit,
                credit_grant_limit_amount_rials: 1_000_000,
                credit_return_enabled: true,
                new_assignment_enabled: true,
                same_pan_reprint_enabled: true,
                new_pan_replacement_enabled: true,
                attach_existing_multi_provider_card_enabled: true,
                event_delivery_enabled: true,
                event_delivery_disabled_reason: None,
            },
        },
    };

    assert_eq!(
        provider.validate_and_normalize().unwrap_err(),
        "operational windows must not overlap on the same day"
    );
}

fn window(start: &str, end: &str) -> ProviderActiveWindow {
    ProviderActiveWindow {
        days: vec![ProviderWeekday::Saturday],
        start_local_time: NaiveTime::parse_from_str(start, "%H:%M:%S").unwrap(),
        end_local_time: NaiveTime::parse_from_str(end, "%H:%M:%S").unwrap(),
    }
}
