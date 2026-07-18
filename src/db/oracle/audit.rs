use crate::{
    db::{
        error::{DbError, DbResult},
        oracle::types::uuid_to_raw16,
    },
    domain::audit::NewAuditLog,
};

pub(crate) fn insert_audit_log(
    connection: &oracle::Connection,
    audit_log: NewAuditLog,
) -> DbResult<()> {
    let audit_log_id = uuid_to_raw16(audit_log.audit_log_id).to_vec();
    let entity_id = uuid_to_raw16(audit_log.entity_id).to_vec();
    let actor_provider_id = audit_log
        .context
        .actor_provider_id
        .map(|value| uuid_to_raw16(value).to_vec());
    let actor_user_id = audit_log
        .context
        .actor_user_id
        .map(|value| uuid_to_raw16(value).to_vec());
    let source_ip = audit_log.context.source_ip.map(|value| value.to_string());
    let old_values = audit_log.old_values.map(|value| value.to_string());
    let new_values = audit_log.new_values.map(|value| value.to_string());

    // Callers must pass redacted business facts only. This function deliberately
    // accepts an existing connection so audit evidence shares the command commit.
    connection
        .execute(
            audit_insert_sql(),
            &[
                &audit_log_id,
                &audit_log.entity_type,
                &entity_id,
                &audit_log.action_type.as_db_value(),
                &audit_log.reason,
                &old_values,
                &new_values,
                &audit_log.context.actor_subject,
                &audit_log.context.actor_client_id,
                &actor_provider_id,
                &actor_user_id,
                &audit_log.context.actor_issuer,
                &source_ip,
                &audit_log.context.correlation_id,
                &audit_log.context.request_id,
            ],
        )
        .map_err(|error| DbError::Query(format!("failed to insert audit log: {error}")))?;

    Ok(())
}

pub(crate) fn audit_insert_sql() -> &'static str {
    r#"
    INSERT INTO audit_logs (
        audit_log_id,
        entity_type,
        entity_id,
        action_type,
        reason,
        old_values,
        new_values,
        actor_subject,
        actor_client_id,
        actor_provider_id,
        actor_user_id,
        actor_issuer,
        source_ip,
        correlation_id,
        request_id
    )
    VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15)
    "#
}

#[cfg(test)]
mod tests {
    use super::audit_insert_sql;

    #[test]
    fn audit_insert_sql_persists_actor_and_network_context() {
        let sql = audit_insert_sql();

        assert!(sql.contains("actor_subject"));
        assert!(sql.contains("actor_client_id"));
        assert!(sql.contains("actor_provider_id"));
        assert!(sql.contains("actor_user_id"));
        assert!(sql.contains("actor_issuer"));
        assert!(sql.contains("source_ip"));
        assert!(sql.contains("correlation_id"));
        assert!(sql.contains("request_id"));
    }

    #[test]
    fn audit_insert_sql_persists_before_after_snapshots() {
        let sql = audit_insert_sql();

        assert!(sql.contains("old_values"));
        assert!(sql.contains("new_values"));
        assert!(sql.contains("reason"));
    }
}
