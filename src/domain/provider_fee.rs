use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const MAX_FEE_AMOUNT_RIALS: u64 = 9_007_199_254_740_991;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FeePayer {
    ProviderUser,
    Provider,
}

impl FeePayer {
    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::ProviderUser => "PROVIDER_USER",
            Self::Provider => "PROVIDER",
        }
    }

    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "PROVIDER_USER" => Some(Self::ProviderUser),
            "PROVIDER" => Some(Self::Provider),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeePolicy {
    pub rate_bps: u32,
    pub fixed_amount_rials: u64,
    pub fee_payer: FeePayer,
}

impl FeePolicy {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.rate_bps > 10_000 {
            return Err("fee rate must be between 0 and 10000 basis points");
        }
        if self.fixed_amount_rials > MAX_FEE_AMOUNT_RIALS {
            return Err("fixed fee exceeds the supported monetary range");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProviderFeeProfileStatus {
    Draft,
    Active,
    Superseded,
}

impl ProviderFeeProfileStatus {
    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::Draft => "DRAFT",
            Self::Active => "ACTIVE",
            Self::Superseded => "SUPERSEDED",
        }
    }

    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "DRAFT" => Some(Self::Draft),
            "ACTIVE" => Some(Self::Active),
            "SUPERSEDED" => Some(Self::Superseded),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderFeeProfile {
    pub provider_fee_profile_id: Uuid,
    pub provider_id: Uuid,
    pub fee_policy: FeePolicy,
    pub status: ProviderFeeProfileStatus,
    pub version: i64,
    pub superseded_by_profile_id: Option<Uuid>,
    pub publication_operation_id: Option<Uuid>,
    pub created_by_subject: String,
    pub updated_by_subject: String,
    pub change_reason: String,
    pub activated_at: Option<DateTime<Utc>>,
    pub superseded_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ProviderFeeProfile {
    pub fn replay_snapshot(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("provider fee profile must serialize")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DesiredProviderFeeProfile {
    pub fee_policy: FeePolicy,
    pub reason: String,
}

impl DesiredProviderFeeProfile {
    pub fn validate(&self) -> Result<(), &'static str> {
        self.fee_policy.validate()?;
        let reason = self.reason.trim();
        if reason.is_empty() || reason.len() > 1000 || reason.chars().any(char::is_control) {
            return Err("fee profile change reason is invalid");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderFeeMaterializationReceipt {
    pub receipt_event_id: Uuid,
    pub operation_id: Uuid,
    pub provider_id: Uuid,
    pub provider_fee_profile_id: Uuid,
    pub materialized_version: i64,
    pub runtime_key: String,
    pub materialized_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_fee_is_a_valid_explicit_policy() {
        let policy = FeePolicy {
            rate_bps: 0,
            fixed_amount_rials: 0,
            fee_payer: FeePayer::ProviderUser,
        };
        assert_eq!(policy.validate(), Ok(()));
    }

    #[test]
    fn rate_above_one_hundred_percent_is_rejected() {
        let policy = FeePolicy {
            rate_bps: 10_001,
            fixed_amount_rials: 0,
            fee_payer: FeePayer::Provider,
        };
        assert!(policy.validate().is_err());
    }
}
