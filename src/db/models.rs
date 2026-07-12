use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub enum AccountStatus {
    Active,
    Inactive,
    Suspended,
    Blocked,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub enum Currency {
    Irr,
    Usd,
    Eur,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub enum TransactionType {
    Credit,
    Debit,
    Transfer,
    Fee,
    Settlement,
    Refund,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub enum TransactionStatus {
    Pending,
    Success,
    Failed,
    Reversed,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Copy)]
pub enum PriorityStatus {
    ACTIVE,
    CONSUMED,
    EXPIRED,
    CANCELLED,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Copy)]
pub enum PriorityUsageType {
    SingleUse,
    MultiUse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Provider {
    pub id: Uuid,
    pub is_core: bool,
    pub legal_name: String,
    pub trade_name: String,
    pub tax_id: String,
    pub email_address: String,
    pub office_phone: String,
    pub website_url: Option<String>,
    pub mailing_address: String,
    pub alert_phone_numbers: Vec<String>,
    pub banner_image_id: Option<String>,
    pub profile_image_id: Option<String>,
    pub is_active: bool,
    pub fee_rate_bps: i32,
    pub fixed_fee_amount: i64,
    pub kafka_config: serde_json::Value,
    pub ledger_account_id: Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: Uuid,
    pub nid: String,
    pub internal_metadata: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserProvider {
    pub user_id: Uuid,
    pub provider_id: Uuid,
    pub external_metadata: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserAccount {
    pub id: Uuid,
    pub user_id: Uuid,
    pub provider_id: Uuid,
    pub ledger_account_id: Uuid,
    pub status: AccountStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UserProviderDetails {
    pub user_id: Uuid,
    pub nid: String,
    pub internal_metadata: serde_json::Value,
    pub external_metadata: serde_json::Value,
    pub user_ledger_account_id: Uuid,
    pub provider_ledger_account_id: Uuid,
    pub status: AccountStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UserAccountProviderInfo {
    pub provider_id: Uuid,
    pub provider_name: String,
    pub ledger_account_id: Uuid,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Transaction {
    pub id: Uuid,
    pub idempotency_key: String,
    pub transaction_type: TransactionType,
    pub amount: i64,
    pub currency: Option<Currency>,
    pub dr_account_id: Uuid,
    pub cr_account_id: Uuid,
    pub user_id: Option<Uuid>,
    pub provider_id: Option<Uuid>,
    pub status: TransactionStatus,
    pub parent_transaction_id: Option<Uuid>,
    pub external_reference_id: Option<String>,
    pub description: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub processed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UserPriorityConfig {
    pub id: Uuid,
    pub user_id: Uuid,
    pub idempotency_key: String,
    pub status: PriorityStatus,
    pub expires_at: Option<DateTime<Utc>>,
    pub is_deleted: bool,
    pub deleted_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UserPriorityItem {
    pub id: Uuid,
    pub config_id: Uuid,
    pub provider_id: Uuid,
    pub user_ledger_account_id: Uuid,
    pub provider_ledger_account_id: Uuid,
    pub priority_order: i32,
    pub usage_type: PriorityUsageType,
    pub max_amount: i64,
    pub fee_rate_bps: i32,
    pub fixed_fee_amount: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// A combined struct for returning a config with its items
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UserPriorityDetails {
    #[serde(flatten)]
    pub config: UserPriorityConfig,
    pub items: Vec<UserPriorityItem>,
}

// A struct to hold the necessary data for creating a priority item
#[derive(Debug)]
pub struct PriorityItemData {
    pub provider_id: Uuid,
    pub usage_type: crate::db::models::PriorityUsageType,
    pub max_amount: i64,
    pub user_ledger_account_id: Uuid,
    pub provider_ledger_account_id: Uuid,
}
