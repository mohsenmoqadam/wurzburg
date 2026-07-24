use std::sync::Arc;

use crate::{
    api::{
        auth::{TrustedActor, require_scope},
        error::ApiError,
    },
    db::oracle::OracleRepository,
    domain::audit::{AuditLogPage, AuditLogQuery},
};

#[derive(Clone)]
pub struct AuditLogService {
    repository: Arc<OracleRepository>,
}

impl AuditLogService {
    pub fn new(repository: Arc<OracleRepository>) -> Self {
        Self { repository }
    }

    #[tracing::instrument(skip(self, actor, query))]
    pub async fn list(
        &self,
        actor: &TrustedActor,
        query: AuditLogQuery,
    ) -> Result<AuditLogPage, ApiError> {
        require_scope(actor, "platform.audit:read")?;
        self.repository
            .list_audit_logs(query)
            .await
            .map_err(ApiError::from_database)
    }
}
