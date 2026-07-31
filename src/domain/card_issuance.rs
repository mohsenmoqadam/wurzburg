use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CardIssuanceBatchStatus {
    Creating,
    Ready,
    ProcessingResult,
    Completed,
    PartiallyCompleted,
    Failed,
}

impl CardIssuanceBatchStatus {
    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "CREATING" => Some(Self::Creating),
            "READY" => Some(Self::Ready),
            "PROCESSING_RESULT" => Some(Self::ProcessingResult),
            "COMPLETED" => Some(Self::Completed),
            "PARTIALLY_COMPLETED" => Some(Self::PartiallyCompleted),
            "FAILED" => Some(Self::Failed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CardIssuanceBatch {
    pub batch_id: Uuid,
    pub status: CardIssuanceBatchStatus,
    pub request_checksum_sha256: Option<String>,
    pub result_checksum_sha256: Option<String>,
    pub request_count: u32,
    pub issued_count: u32,
    pub rejected_count: u32,
    pub failed_count: u32,
    pub expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CardIssuanceBatchCursor {
    pub created_at: DateTime<Utc>,
    pub batch_id: Uuid,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CardIssuanceBatchPage {
    pub items: Vec<CardIssuanceBatch>,
    pub next_cursor: Option<CardIssuanceBatchCursor>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CardIssuanceBatchResultRow {
    pub issuance_request_id: Uuid,
    pub row_number: u32,
    pub status: String,
    pub result_code: Option<String>,
    pub result_message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CardIssuanceExportRow {
    pub issuance_request_id: Uuid,
    pub requested_at: DateTime<Utc>,
    pub card_range_id: Uuid,
    pub funding_mode: String,
    pub national_id: String,
    pub first_name: String,
    pub last_name: String,
    pub birth_date: Option<String>,
    pub mobile: String,
    pub delivery_province: String,
    pub delivery_city: String,
    pub delivery_address: String,
    pub postal_code: String,
    pub provider_request_count: u32,
}

#[derive(Debug, Clone)]
pub struct PreparedCardIssuanceBatch {
    pub batch: CardIssuanceBatch,
    pub object_key: String,
    pub rows: Vec<CardIssuanceExportRow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CardIssuanceResultStatus {
    Issued,
    Rejected,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CardIssuanceResultRow {
    pub issuance_request_id: Uuid,
    pub status: CardIssuanceResultStatus,
    pub card_number: Option<String>,
    pub issuer_reference: Option<String>,
    pub failure_code: Option<String>,
    pub failure_message: Option<String>,
    pub produced_at: Option<DateTime<Utc>>,
    pub dispatched_at: Option<DateTime<Utc>>,
    pub tracking_reference: Option<String>,
}

impl CardIssuanceResultRow {
    pub fn validate(&self) -> Result<(), &'static str> {
        match self.status {
            CardIssuanceResultStatus::Issued => {
                if self.card_number.as_deref().is_none_or(str::is_empty)
                    || self.issuer_reference.as_deref().is_none_or(str::is_empty)
                    || self.failure_code.is_some()
                    || self.failure_message.is_some()
                {
                    return Err("issued result requires card number and issuer reference only");
                }
            }
            CardIssuanceResultStatus::Rejected => {
                if self.card_number.is_some()
                    || self.failure_code.as_deref().is_none_or(str::is_empty)
                {
                    return Err(
                        "rejected result requires failure code and must not contain a card number",
                    );
                }
            }
        }
        Ok(())
    }
}
