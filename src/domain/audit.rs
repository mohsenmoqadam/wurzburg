use std::net::IpAddr;

use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrustedAuditContext {
    pub actor_subject: String,
    pub actor_client_id: Option<String>,
    pub actor_provider_id: Option<Uuid>,
    pub actor_user_id: Option<Uuid>,
    pub actor_issuer: Option<String>,
    pub source_ip: Option<IpAddr>,
    pub correlation_id: String,
    pub request_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditAction {
    Insert,
    Update,
    Delete,
    StateTransition,
    SecretRead,
    SecretRotate,
}

impl AuditAction {
    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
            Self::StateTransition => "STATE_TRANSITION",
            Self::SecretRead => "SECRET_READ",
            Self::SecretRotate => "SECRET_ROTATE",
        }
    }

    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "INSERT" => Some(Self::Insert),
            "UPDATE" => Some(Self::Update),
            "DELETE" => Some(Self::Delete),
            "STATE_TRANSITION" => Some(Self::StateTransition),
            "SECRET_READ" => Some(Self::SecretRead),
            "SECRET_ROTATE" => Some(Self::SecretRotate),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewAuditLog {
    pub audit_log_id: Uuid,
    pub entity_type: String,
    pub entity_id: Uuid,
    pub action_type: AuditAction,
    pub reason: Option<String>,
    pub old_values: Option<serde_json::Value>,
    pub new_values: Option<serde_json::Value>,
    pub context: TrustedAuditContext,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AuditLogRecord {
    pub audit_log_id: Uuid,
    pub entity_type: String,
    pub entity_id: Uuid,
    pub action_type: AuditAction,
    pub reason: Option<String>,
    pub old_values: Option<serde_json::Value>,
    pub new_values: Option<serde_json::Value>,
    pub actor_subject: String,
    pub actor_client_id: Option<String>,
    pub actor_provider_id: Option<Uuid>,
    pub actor_user_id: Option<Uuid>,
    pub actor_issuer: Option<String>,
    pub source_ip: Option<String>,
    pub correlation_id: String,
    pub request_id: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditLogCursor {
    pub created_at: DateTime<Utc>,
    pub audit_log_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditLogQuery {
    pub entity_type: Option<String>,
    pub entity_id: Option<Uuid>,
    pub action_type: Option<AuditAction>,
    pub actor_subject: Option<String>,
    pub actor_client_id: Option<String>,
    pub actor_provider_id: Option<Uuid>,
    pub actor_user_id: Option<Uuid>,
    pub source_ip: Option<String>,
    pub correlation_id: Option<String>,
    pub request_id: Option<String>,
    pub created_from: Option<DateTime<Utc>>,
    pub created_to: Option<DateTime<Utc>>,
    pub limit: u32,
    pub cursor: Option<AuditLogCursor>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AuditLogPage {
    pub items: Vec<AuditLogRecord>,
    pub next_cursor: Option<AuditLogCursor>,
}

pub fn redact_audit_snapshot(mut value: serde_json::Value) -> serde_json::Value {
    redact_value(&mut value);
    value
}

fn redact_value(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            for (key, value) in object {
                if is_sensitive_audit_field(key) {
                    *value = serde_json::Value::String("[REDACTED]".to_string());
                } else {
                    redact_value(value);
                }
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                redact_value(value);
            }
        }
        _ => {}
    }
}

fn is_sensitive_audit_field(key: &str) -> bool {
    matches!(
        key.to_ascii_lowercase().as_str(),
        "pan"
            | "card_number"
            | "national_id"
            | "legal_name"
            | "trade_name"
            | "name"
            | "contact_name"
            | "contacts"
            | "email"
            | "email_address"
            | "phone"
            | "mobile"
            | "mailing_address"
            | "website_url"
            | "metadata"
            | "metadata_json"
            | "password"
            | "token"
            | "credential"
    )
}

#[cfg(test)]
mod tests {
    use super::{AuditAction, redact_audit_snapshot};

    #[test]
    fn audit_action_uses_constrained_database_values() {
        assert_eq!(AuditAction::Insert.as_db_value(), "INSERT");
        assert_eq!(AuditAction::Update.as_db_value(), "UPDATE");
        assert_eq!(AuditAction::Delete.as_db_value(), "DELETE");
        assert_eq!(
            AuditAction::StateTransition.as_db_value(),
            "STATE_TRANSITION"
        );
        assert_eq!(AuditAction::SecretRead.as_db_value(), "SECRET_READ");
        assert_eq!(AuditAction::SecretRotate.as_db_value(), "SECRET_ROTATE");
    }

    #[test]
    fn audit_snapshot_redaction_preserves_safe_evidence_only() {
        let redacted = redact_audit_snapshot(serde_json::json!({
            "provider_id": "safe-id",
            "legal_name": "sensitive",
            "nested": {"card_number": "6219860000000000", "status": "ACTIVE"},
            "metadata": {"anything": "unrestricted"}
        }));
        assert_eq!(redacted["provider_id"], "safe-id");
        assert_eq!(redacted["legal_name"], "[REDACTED]");
        assert_eq!(redacted["nested"]["card_number"], "[REDACTED]");
        assert_eq!(redacted["nested"]["status"], "ACTIVE");
        assert_eq!(redacted["metadata"], "[REDACTED]");
    }
}
