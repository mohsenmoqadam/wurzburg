use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WithdrawalWindowLimit {
    /// `None` disables the amount limit for this window without disabling its
    /// independently configured count limit.
    pub max_amount: Option<u64>,
    /// `None` disables the count limit for this window without disabling its
    /// independently configured amount limit.
    pub max_count: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WithdrawalLimits {
    /// A missing transaction bound means that bound is not enforced.
    pub per_transaction_min_amount: Option<u64>,
    pub per_transaction_max_amount: Option<u64>,
    /// A missing window means neither amount nor count is limited for it.
    pub daily: Option<WithdrawalWindowLimit>,
    pub weekly: Option<WithdrawalWindowLimit>,
    pub monthly: Option<WithdrawalWindowLimit>,
    pub yearly: Option<WithdrawalWindowLimit>,
}

impl WithdrawalLimits {
    pub fn validate(&self) -> CardPolicyResult<()> {
        if let (Some(minimum), Some(maximum)) = (
            self.per_transaction_min_amount,
            self.per_transaction_max_amount,
        ) && minimum > maximum
        {
            return Err(CardPolicyError::InvalidTransactionBounds);
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CardPolicyTerms {
    pub withdrawal_limits: Option<WithdrawalLimits>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CardPolicyStatus {
    Draft,
    Active,
    Superseded,
}

impl CardPolicyStatus {
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
pub struct CardPolicyProfile {
    pub card_policy_profile_id: Uuid,
    pub card_range_id: Uuid,
    /// Flattening keeps the persisted idempotent snapshot identical to the
    /// public policy profile shape instead of introducing an internal `terms`
    /// wrapper during replay.
    #[serde(flatten)]
    pub terms: CardPolicyTerms,
    pub status: CardPolicyStatus,
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

impl CardPolicyProfile {
    pub fn replay_snapshot(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("CardPolicyProfile serialization must be stable")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DesiredCardPolicy {
    pub terms: CardPolicyTerms,
    pub reason: String,
}

/// Proof emitted by Wolfsburg after a complete CPOL value is durable in
/// Dragonfly. Every field participates in activation validation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyMaterializationReceipt {
    pub receipt_event_id: Uuid,
    pub operation_id: Uuid,
    pub card_range_id: Uuid,
    pub card_policy_profile_id: Uuid,
    pub materialized_version: i64,
    pub runtime_key: String,
    pub materialized_at: DateTime<Utc>,
}

impl DesiredCardPolicy {
    pub fn validate_for_platform(&self) -> CardPolicyResult<()> {
        let limits = self
            .terms
            .withdrawal_limits
            .as_ref()
            .ok_or(CardPolicyError::PlatformLimitsRequired)?;
        limits.validate()?;
        validate_reason(&self.reason)
    }

    pub fn validate_for_cms(&self) -> CardPolicyResult<()> {
        if self.terms.withdrawal_limits.is_some() {
            return Err(CardPolicyError::CmsLimitsMustBeNull);
        }
        validate_reason(&self.reason)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CardPolicyError {
    PlatformLimitsRequired,
    CmsLimitsMustBeNull,
    InvalidTransactionBounds,
    InvalidReason,
}

impl fmt::Display for CardPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PlatformLimitsRequired => {
                write!(formatter, "PLATFORM authority requires withdrawal_limits")
            }
            Self::CmsLimitsMustBeNull => {
                write!(
                    formatter,
                    "CMS authority requires withdrawal_limits to be null"
                )
            }
            Self::InvalidTransactionBounds => write!(
                formatter,
                "per-transaction minimum cannot exceed per-transaction maximum"
            ),
            Self::InvalidReason => write!(
                formatter,
                "policy change reason must be between 1 and 1000 characters"
            ),
        }
    }
}

fn validate_reason(reason: &str) -> CardPolicyResult<()> {
    let reason = reason.trim();
    if reason.is_empty() || reason.len() > 1000 || reason.chars().any(char::is_control) {
        return Err(CardPolicyError::InvalidReason);
    }
    Ok(())
}

pub type CardPolicyResult<T> = Result<T, CardPolicyError>;

#[cfg(test)]
mod tests {
    use super::{
        CardPolicyError, CardPolicyTerms, DesiredCardPolicy, WithdrawalLimits,
        WithdrawalWindowLimit,
    };

    fn platform_policy(limits: Option<WithdrawalLimits>) -> DesiredCardPolicy {
        DesiredCardPolicy {
            terms: CardPolicyTerms {
                withdrawal_limits: limits,
            },
            reason: "Initial range policy".to_string(),
        }
    }

    #[test]
    fn null_window_metrics_are_valid_and_independent() {
        let policy = platform_policy(Some(WithdrawalLimits {
            per_transaction_min_amount: None,
            per_transaction_max_amount: None,
            daily: Some(WithdrawalWindowLimit {
                max_amount: Some(1_000_000),
                max_count: None,
            }),
            weekly: Some(WithdrawalWindowLimit {
                max_amount: None,
                max_count: Some(12),
            }),
            monthly: Some(WithdrawalWindowLimit {
                max_amount: None,
                max_count: None,
            }),
            yearly: None,
        }));

        assert_eq!(policy.validate_for_platform(), Ok(()));
    }

    #[test]
    fn platform_and_cms_authorities_have_distinct_contracts() {
        let no_limits = platform_policy(None);
        assert_eq!(
            no_limits.validate_for_platform(),
            Err(CardPolicyError::PlatformLimitsRequired)
        );
        assert_eq!(no_limits.validate_for_cms(), Ok(()));
    }

    #[test]
    fn transaction_minimum_cannot_exceed_maximum() {
        let policy = platform_policy(Some(WithdrawalLimits {
            per_transaction_min_amount: Some(200),
            per_transaction_max_amount: Some(100),
            daily: None,
            weekly: None,
            monthly: None,
            yearly: None,
        }));

        assert_eq!(
            policy.validate_for_platform(),
            Err(CardPolicyError::InvalidTransactionBounds)
        );
    }
}
