use std::net::IpAddr;

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

#[cfg(test)]
mod tests {
    use super::AuditAction;

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
}
