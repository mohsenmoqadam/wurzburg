use chrono::Utc;
use uuid::Uuid;
use wurzburg::domain::provider::{
    CreditGrantLimitMode, NewProvider, ProviderAccountCategory, ProviderOperationalProfile,
};

/// Scenario goal: prove the minimal Provider command normalizes identity data,
/// accepts optional informational identifiers, and creates four stable,
/// category-specific account identities without storing balances in Oracle.
#[test]
fn validates_provider_identity_and_deterministic_account_set() {
    let provider_id = Uuid::new_v4();
    let provider = NewProvider {
        provider_id,
        legal_name: "  Legal Provider ۱۲۳ ".to_string(),
        trade_name: "Provider Brand".to_string(),
        tax_id: None,
        registration_number: None,
        email_address: None,
        website_url: None,
        mailing_address: None,
        metadata: serde_json::json!({}),
        contacts: vec![],
        operational_profile: ProviderOperationalProfile {
            effective_at: Utc::now(),
            timezone: "Asia/Tehran".to_string(),
            user_onboarding_enabled: true,
            active_windows: vec![],
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
    }
    .validate_and_normalize()
    .expect("provider should satisfy the contract");

    assert_eq!(provider.legal_name, "Legal Provider 123");
    let ids: std::collections::HashSet<_> = ProviderAccountCategory::ALL
        .map(|category| category.deterministic_account_id(provider_id))
        .into_iter()
        .collect();
    assert_eq!(ids.len(), 4);
}
