use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProviderEventType {
    ProviderStatusChanged,
    UserOnboarded,
    CardAssigned,
    CardReplaced,
    CreditGranted,
    CreditReturned,
    WithdrawalConfirmed,
    WithdrawalRolledBack,
    FeeCharged,
}

impl ProviderEventType {
    pub const ALL: [Self; 9] = [
        Self::ProviderStatusChanged,
        Self::UserOnboarded,
        Self::CardAssigned,
        Self::CardReplaced,
        Self::CreditGranted,
        Self::CreditReturned,
        Self::WithdrawalConfirmed,
        Self::WithdrawalRolledBack,
        Self::FeeCharged,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProviderStatusChanged => "PROVIDER_STATUS_CHANGED",
            Self::UserOnboarded => "USER_ONBOARDED",
            Self::CardAssigned => "CARD_ASSIGNED",
            Self::CardReplaced => "CARD_REPLACED",
            Self::CreditGranted => "CREDIT_GRANTED",
            Self::CreditReturned => "CREDIT_RETURNED",
            Self::WithdrawalConfirmed => "WITHDRAWAL_CONFIRMED",
            Self::WithdrawalRolledBack => "WITHDRAWAL_ROLLED_BACK",
            Self::FeeCharged => "FEE_CHARGED",
        }
    }

    pub fn parse_name(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|event_type| event_type.as_str() == value)
    }
}

#[cfg(test)]
mod tests {
    use super::ProviderEventType;

    const PUBLIC_SCHEMAS: [&str; 9] = [
        include_str!("../../contracts/provider-events/v1/provider_status_changed.schema.json"),
        include_str!("../../contracts/provider-events/v1/user_onboarded.schema.json"),
        include_str!("../../contracts/provider-events/v1/card_assigned.schema.json"),
        include_str!("../../contracts/provider-events/v1/card_replaced.schema.json"),
        include_str!("../../contracts/provider-events/v1/credit_granted.schema.json"),
        include_str!("../../contracts/provider-events/v1/credit_returned.schema.json"),
        include_str!("../../contracts/provider-events/v1/withdrawal_confirmed.schema.json"),
        include_str!("../../contracts/provider-events/v1/withdrawal_rolled_back.schema.json"),
        include_str!("../../contracts/provider-events/v1/fee_charged.schema.json"),
    ];

    #[test]
    fn provider_event_catalog_round_trips_only_public_allowlisted_types() {
        for event_type in ProviderEventType::ALL {
            assert_eq!(
                ProviderEventType::parse_name(event_type.as_str()),
                Some(event_type)
            );
        }
        assert_eq!(ProviderEventType::parse_name("INTERNAL_WAL_RECOVERY"), None);
    }

    #[test]
    fn every_public_event_has_a_safe_version_one_schema_artifact() {
        assert_eq!(PUBLIC_SCHEMAS.len(), ProviderEventType::ALL.len());
        for schema in PUBLIC_SCHEMAS {
            let document: serde_json::Value =
                serde_json::from_str(schema).expect("provider event schema must be valid JSON");
            assert_eq!(
                document["$schema"],
                "https://json-schema.org/draft/2020-12/schema"
            );
            assert!(!schema.contains("traceparent"));
            assert!(!schema.contains("tracestate"));
            assert!(!schema.contains("national_id"));
            assert!(!schema.contains("\"card_number\""));
        }
    }
}
