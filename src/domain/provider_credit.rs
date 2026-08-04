use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CreditMovementType {
    Grant,
    ReturnFullBalance,
}

impl CreditMovementType {
    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::Grant => "GRANT",
            Self::ReturnFullBalance => "RETURN_FULL_BALANCE",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CreditMovementInitiator {
    Provider,
    Cardholder,
}

impl CreditMovementInitiator {
    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::Provider => "PROVIDER",
            Self::Cardholder => "CARDHOLDER",
        }
    }
}

#[derive(Debug, Clone)]
pub struct CreditAccountContext {
    pub provider_id: Uuid,
    pub user_id: Uuid,
    pub card_id: Uuid,
    pub card_number: String,
    pub masked_card_number: String,
    pub provider_user_id: Uuid,
    pub provider_customer_reference: String,
    pub provider_user_account_id: Uuid,
    pub provider_owned_account_id: Uuid,
    pub cms_settlement_account_id: Uuid,
    pub card_state_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderCreditBalance {
    pub provider_id: Uuid,
    pub user_id: Uuid,
    pub card_id: Uuid,
    pub currency: String,
    pub observed_remaining_amount_rials: u64,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreditMovementIntent {
    pub operation_id: Uuid,
    pub movement_id: Uuid,
    pub movement_type: CreditMovementType,
    pub initiated_by: CreditMovementInitiator,
    pub provider_id: Uuid,
    pub user_id: Uuid,
    pub card_id: Uuid,
    pub provider_user_id: Uuid,
    pub provider_user_account_id: Uuid,
    pub provider_owned_account_id: Uuid,
    pub amount_rials: u64,
    pub expected_remaining_amount_rials: Option<u64>,
    pub provider_reference: Option<String>,
    pub deterministic_transfer_id: Uuid,
    pub operational_profile_id: Uuid,
    pub reason: String,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreditMovementResult {
    pub movement_id: Uuid,
    pub operation_id: Uuid,
    pub movement_type: CreditMovementType,
    pub provider_id: Uuid,
    pub user_id: Uuid,
    pub card_id: Uuid,
    pub amount_rials: u64,
    pub provider_reference: Option<String>,
    pub command_status: String,
    pub event_publication_status: String,
    pub profile_materialization_status: String,
    pub created_at: DateTime<Utc>,
}
