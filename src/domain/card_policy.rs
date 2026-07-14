use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CardPolicyProfileStatus {
    Draft,
    Active,
    Superseded,
    Suspended,
}

impl CardPolicyProfileStatus {
    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::Draft => "DRAFT",
            Self::Active => "ACTIVE",
            Self::Superseded => "SUPERSEDED",
            Self::Suspended => "SUSPENDED",
        }
    }

    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "DRAFT" => Some(Self::Draft),
            "ACTIVE" => Some(Self::Active),
            "SUPERSEDED" => Some(Self::Superseded),
            "SUSPENDED" => Some(Self::Suspended),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CardRangePolicyAssignmentStatus {
    Active,
    Superseded,
}

impl CardRangePolicyAssignmentStatus {
    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::Active => "ACTIVE",
            Self::Superseded => "SUPERSEDED",
        }
    }

    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "ACTIVE" => Some(Self::Active),
            "SUPERSEDED" => Some(Self::Superseded),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CardPolicyProfile {
    pub id: Uuid,
    pub card_range_id: Uuid,
    pub profile: serde_json::Value,
    pub status: CardPolicyProfileStatus,
    pub version: i64,
    pub effective_at: DateTime<Utc>,
    pub superseded_by_profile_id: Option<Uuid>,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CardRangePolicyAssignment {
    pub card_range_id: Uuid,
    pub card_policy_profile_id: Uuid,
    pub status: CardRangePolicyAssignmentStatus,
    pub assigned_by: String,
    pub assigned_at: DateTime<Utc>,
    pub superseded_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CardRangePolicyDetails {
    pub profile: CardPolicyProfile,
    pub assignment: CardRangePolicyAssignment,
}

#[derive(Debug, Clone)]
pub struct NewCardRangePolicy {
    pub card_range_id: Uuid,
    pub profile_id: Uuid,
    pub profile: serde_json::Value,
    pub effective_at: DateTime<Utc>,
    pub actor_subject: String,
}
