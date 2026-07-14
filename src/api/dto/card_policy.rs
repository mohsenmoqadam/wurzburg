use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::domain::card_policy::{
    CardPolicyProfileStatus, CardRangePolicyAssignmentStatus, CardRangePolicyDetails,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema)]
pub enum CardPolicyProfileStatusDto {
    Draft,
    Active,
    Superseded,
    Suspended,
}

impl From<CardPolicyProfileStatus> for CardPolicyProfileStatusDto {
    fn from(value: CardPolicyProfileStatus) -> Self {
        match value {
            CardPolicyProfileStatus::Draft => Self::Draft,
            CardPolicyProfileStatus::Active => Self::Active,
            CardPolicyProfileStatus::Superseded => Self::Superseded,
            CardPolicyProfileStatus::Suspended => Self::Suspended,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema)]
pub enum CardRangePolicyAssignmentStatusDto {
    Active,
    Superseded,
}

impl From<CardRangePolicyAssignmentStatus> for CardRangePolicyAssignmentStatusDto {
    fn from(value: CardRangePolicyAssignmentStatus) -> Self {
        match value {
            CardRangePolicyAssignmentStatus::Active => Self::Active,
            CardRangePolicyAssignmentStatus::Superseded => Self::Superseded,
        }
    }
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateCardRangePolicyRequest {
    pub profile: serde_json::Value,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CardRangePolicyResponse {
    pub card_range_id: Uuid,
    pub card_policy_profile_id: Uuid,
    pub profile: serde_json::Value,
    pub profile_status: CardPolicyProfileStatusDto,
    pub assignment_status: CardRangePolicyAssignmentStatusDto,
    pub version: i64,
    #[schema(value_type = String, format = DateTime)]
    pub effective_at: DateTime<Utc>,
    #[schema(value_type = String, format = DateTime)]
    pub assigned_at: DateTime<Utc>,
}

impl From<CardRangePolicyDetails> for CardRangePolicyResponse {
    fn from(value: CardRangePolicyDetails) -> Self {
        Self {
            card_range_id: value.assignment.card_range_id,
            card_policy_profile_id: value.profile.id,
            profile: value.profile.profile,
            profile_status: value.profile.status.into(),
            assignment_status: value.assignment.status.into(),
            version: value.profile.version,
            effective_at: value.profile.effective_at,
            assigned_at: value.assignment.assigned_at,
        }
    }
}
