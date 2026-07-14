use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FundingMode {
    SingleProvider,
    MultiProvider,
}

impl FundingMode {
    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::SingleProvider => "SINGLE_PROVIDER",
            Self::MultiProvider => "MULTI_PROVIDER",
        }
    }

    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "SINGLE_PROVIDER" => Some(Self::SingleProvider),
            "MULTI_PROVIDER" => Some(Self::MultiProvider),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CardRangeStatus {
    Draft,
    Active,
    Suspended,
}

impl CardRangeStatus {
    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::Draft => "DRAFT",
            Self::Active => "ACTIVE",
            Self::Suspended => "SUSPENDED",
        }
    }

    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "DRAFT" => Some(Self::Draft),
            "ACTIVE" => Some(Self::Active),
            "SUSPENDED" => Some(Self::Suspended),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CardRangeProviderStatus {
    Active,
    Suspended,
}

impl CardRangeProviderStatus {
    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::Active => "ACTIVE",
            Self::Suspended => "SUSPENDED",
        }
    }

    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "ACTIVE" => Some(Self::Active),
            "SUSPENDED" => Some(Self::Suspended),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CardRange {
    pub id: Uuid,
    pub start_card_number: String,
    pub end_card_number: String,
    pub funding_mode: FundingMode,
    pub status: CardRangeStatus,
    pub metadata: serde_json::Value,
    pub created_by: String,
    pub updated_by: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CardRangeProvider {
    pub card_range_id: Uuid,
    pub provider_id: Uuid,
    pub status: CardRangeProviderStatus,
    pub metadata: serde_json::Value,
    pub created_by: String,
    pub updated_by: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewCardRange {
    pub id: Uuid,
    pub start_card_number: String,
    pub end_card_number: String,
    pub funding_mode: FundingMode,
    pub metadata: serde_json::Value,
    pub actor_subject: String,
}
