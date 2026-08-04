use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FinancialTransactionType {
    CreditGranted,
    CreditReturned,
    WithdrawalConfirmed,
    WithdrawalRolledBack,
    FeeCharged,
}

impl FinancialTransactionType {
    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::CreditGranted => "CREDIT_GRANTED",
            Self::CreditReturned => "CREDIT_RETURNED",
            Self::WithdrawalConfirmed => "WITHDRAWAL_CONFIRMED",
            Self::WithdrawalRolledBack => "WITHDRAWAL_ROLLED_BACK",
            Self::FeeCharged => "FEE_CHARGED",
        }
    }

    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "CREDIT_GRANTED" => Some(Self::CreditGranted),
            "CREDIT_RETURNED" => Some(Self::CreditReturned),
            "WITHDRAWAL_CONFIRMED" => Some(Self::WithdrawalConfirmed),
            "WITHDRAWAL_ROLLED_BACK" => Some(Self::WithdrawalRolledBack),
            "FEE_CHARGED" => Some(Self::FeeCharged),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FinancialAccountCategory {
    ProviderOwned,
    ProviderFee,
    CmsSettlement,
    PlatformFee,
    ProviderUser,
}

impl FinancialAccountCategory {
    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::ProviderOwned => "PROVIDER_OWNED",
            Self::ProviderFee => "PROVIDER_FEE",
            Self::CmsSettlement => "CMS_SETTLEMENT",
            Self::PlatformFee => "PLATFORM_FEE",
            Self::ProviderUser => "PROVIDER_USER",
        }
    }

    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "PROVIDER_OWNED" => Some(Self::ProviderOwned),
            "PROVIDER_FEE" => Some(Self::ProviderFee),
            "CMS_SETTLEMENT" => Some(Self::CmsSettlement),
            "PLATFORM_FEE" => Some(Self::PlatformFee),
            "PROVIDER_USER" => Some(Self::ProviderUser),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FinancialEntryDirection {
    Debit,
    Credit,
}

impl FinancialEntryDirection {
    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "DEBIT" => Some(Self::Debit),
            "CREDIT" => Some(Self::Credit),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinancialTransactionEntry {
    pub provider_id: Uuid,
    pub account_category: FinancialAccountCategory,
    pub direction: FinancialEntryDirection,
    pub entry_role: String,
    pub amount_rials: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinancialTransaction {
    pub transaction_id: Uuid,
    pub transaction_type: FinancialTransactionType,
    pub source_system: String,
    pub status: String,
    pub user_id: Uuid,
    pub card_id: Uuid,
    pub masked_card_number: String,
    pub amount_rials: String,
    pub currency: String,
    pub reference: Option<String>,
    pub original_transaction_id: Option<Uuid>,
    pub entries: Vec<FinancialTransactionEntry>,
    pub occurred_at: DateTime<Utc>,
    pub recorded_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinancialTransactionCursor {
    pub occurred_at: DateTime<Utc>,
    pub transaction_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionVisibility {
    Provider {
        provider_id: Uuid,
        user_id: Option<Uuid>,
        card_number: Option<String>,
        account_category: Option<FinancialAccountCategory>,
    },
    Cardholder {
        user_id: Uuid,
        card_number: Option<String>,
    },
    Platform {
        provider_id: Option<Uuid>,
        user_id: Option<Uuid>,
        card_number: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinancialTransactionQuery {
    pub visibility: TransactionVisibility,
    pub transaction_type: Option<FinancialTransactionType>,
    pub occurred_from: Option<DateTime<Utc>>,
    pub occurred_to: Option<DateTime<Utc>>,
    pub limit: u16,
    pub cursor: Option<FinancialTransactionCursor>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinancialTransactionPage {
    pub items: Vec<FinancialTransaction>,
    pub next_cursor: Option<FinancialTransactionCursor>,
}
