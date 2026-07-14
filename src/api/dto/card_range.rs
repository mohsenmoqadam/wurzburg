use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::domain::card_range::{
    CardRange, CardRangeProvider, CardRangeProviderStatus, CardRangeStatus, FundingMode,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema)]
pub enum FundingModeDto {
    SingleProvider,
    MultiProvider,
}

impl From<FundingModeDto> for FundingMode {
    fn from(value: FundingModeDto) -> Self {
        match value {
            FundingModeDto::SingleProvider => Self::SingleProvider,
            FundingModeDto::MultiProvider => Self::MultiProvider,
        }
    }
}

impl From<FundingMode> for FundingModeDto {
    fn from(value: FundingMode) -> Self {
        match value {
            FundingMode::SingleProvider => Self::SingleProvider,
            FundingMode::MultiProvider => Self::MultiProvider,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema)]
pub enum CardRangeStatusDto {
    Draft,
    Active,
    Suspended,
}

impl From<CardRangeStatus> for CardRangeStatusDto {
    fn from(value: CardRangeStatus) -> Self {
        match value {
            CardRangeStatus::Draft => Self::Draft,
            CardRangeStatus::Active => Self::Active,
            CardRangeStatus::Suspended => Self::Suspended,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema)]
pub enum CardRangeProviderStatusDto {
    Active,
    Suspended,
}

impl From<CardRangeProviderStatus> for CardRangeProviderStatusDto {
    fn from(value: CardRangeProviderStatus) -> Self {
        match value {
            CardRangeProviderStatus::Active => Self::Active,
            CardRangeProviderStatus::Suspended => Self::Suspended,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateCardRangeRequest {
    pub start_card_number: String,
    pub end_card_number: String,
    pub funding_mode: FundingModeDto,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdateCardRangeRequest {
    pub metadata: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct AttachCardRangeProviderRequest {
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CardRangeResponse {
    pub card_range_id: Uuid,
    pub start_card_number: String,
    pub end_card_number: String,
    pub funding_mode: FundingModeDto,
    pub status: CardRangeStatusDto,
    pub metadata: serde_json::Value,
    #[schema(value_type = String, format = DateTime)]
    pub created_at: DateTime<Utc>,
    #[schema(value_type = String, format = DateTime)]
    pub updated_at: DateTime<Utc>,
}

impl From<CardRange> for CardRangeResponse {
    fn from(value: CardRange) -> Self {
        Self {
            card_range_id: value.id,
            start_card_number: value.start_card_number,
            end_card_number: value.end_card_number,
            funding_mode: value.funding_mode.into(),
            status: value.status.into(),
            metadata: value.metadata,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CardRangeProviderResponse {
    pub card_range_id: Uuid,
    pub provider_id: Uuid,
    pub status: CardRangeProviderStatusDto,
    pub metadata: serde_json::Value,
    #[schema(value_type = String, format = DateTime)]
    pub created_at: DateTime<Utc>,
    #[schema(value_type = String, format = DateTime)]
    pub updated_at: DateTime<Utc>,
}

impl From<CardRangeProvider> for CardRangeProviderResponse {
    fn from(value: CardRangeProvider) -> Self {
        Self {
            card_range_id: value.card_range_id,
            provider_id: value.provider_id,
            status: value.status.into(),
            metadata: value.metadata,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CardRangeListResponse {
    pub data: Vec<CardRangeResponse>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CardRangeProviderListResponse {
    pub data: Vec<CardRangeProviderResponse>,
}
