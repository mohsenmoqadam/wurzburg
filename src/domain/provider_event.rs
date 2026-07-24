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

    pub fn schema_file_name(self) -> &'static str {
        match self {
            Self::ProviderStatusChanged => "provider_status_changed",
            Self::UserOnboarded => "user_onboarded",
            Self::CardAssigned => "card_assigned",
            Self::CardReplaced => "card_replaced",
            Self::CreditGranted => "credit_granted",
            Self::CreditReturned => "credit_returned",
            Self::WithdrawalConfirmed => "withdrawal_confirmed",
            Self::WithdrawalRolledBack => "withdrawal_rolled_back",
            Self::FeeCharged => "fee_charged",
        }
    }

    pub fn schema_json(self, version: u16) -> Option<&'static str> {
        if version != 1 {
            return None;
        }
        Some(match self {
            Self::ProviderStatusChanged => {
                include_str!(
                    "../../contracts/provider-events/v1/provider_status_changed.schema.json"
                )
            }
            Self::UserOnboarded => {
                include_str!("../../contracts/provider-events/v1/user_onboarded.schema.json")
            }
            Self::CardAssigned => {
                include_str!("../../contracts/provider-events/v1/card_assigned.schema.json")
            }
            Self::CardReplaced => {
                include_str!("../../contracts/provider-events/v1/card_replaced.schema.json")
            }
            Self::CreditGranted => {
                include_str!("../../contracts/provider-events/v1/credit_granted.schema.json")
            }
            Self::CreditReturned => {
                include_str!("../../contracts/provider-events/v1/credit_returned.schema.json")
            }
            Self::WithdrawalConfirmed => {
                include_str!("../../contracts/provider-events/v1/withdrawal_confirmed.schema.json")
            }
            Self::WithdrawalRolledBack => {
                include_str!(
                    "../../contracts/provider-events/v1/withdrawal_rolled_back.schema.json"
                )
            }
            Self::FeeCharged => {
                include_str!("../../contracts/provider-events/v1/fee_charged.schema.json")
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::ProviderEventType;

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
        for event_type in ProviderEventType::ALL {
            let schema = event_type
                .schema_json(1)
                .expect("every public provider event must expose a v1 schema");
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
        assert!(
            ProviderEventType::ProviderStatusChanged
                .schema_json(2)
                .is_none()
        );
    }
}
