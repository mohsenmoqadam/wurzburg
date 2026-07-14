use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct IdempotencyRecord {
    pub id: Uuid,
    pub operation_type: String,
    pub idempotency_key: String,
    pub request_hash: String,
    pub status: IdempotencyStatus,
    pub resource_type: Option<String>,
    pub resource_id: Option<Uuid>,
    pub response_snapshot: Option<serde_json::Value>,
    pub created_by_subject: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdempotencyStatus {
    InProgress,
    Completed,
    Failed,
    Conflict,
}

impl IdempotencyStatus {
    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::InProgress => "IN_PROGRESS",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
            Self::Conflict => "CONFLICT",
        }
    }

    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "IN_PROGRESS" => Some(Self::InProgress),
            "COMPLETED" => Some(Self::Completed),
            "FAILED" => Some(Self::Failed),
            "CONFLICT" => Some(Self::Conflict),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct NewIdempotencyRecord {
    pub id: Uuid,
    pub operation_type: String,
    pub idempotency_key: String,
    pub request_hash: String,
    pub created_by_subject: String,
}
