use crate::{
    db::{
        error::{DbError, DbResult},
        oracle::{
            OracleRepository,
            types::{raw16_to_uuid, uuid_to_raw16},
        },
    },
    domain::audit::{
        AuditAction, AuditLogCursor, AuditLogPage, AuditLogQuery, AuditLogRecord, NewAuditLog,
        redact_audit_snapshot,
    },
};

impl OracleRepository {
    #[tracing::instrument(skip(self, query), fields(db.system="oracle", db.operation.name="audit_logs.list"))]
    pub async fn list_audit_logs(&self, query: AuditLogQuery) -> DbResult<AuditLogPage> {
        self.pool
            .with_connection(move |connection| {
                let entity_id = query.entity_id.map(|value| uuid_to_raw16(value).to_vec());
                let actor_provider_id = query
                    .actor_provider_id
                    .map(|value| uuid_to_raw16(value).to_vec());
                let actor_user_id = query
                    .actor_user_id
                    .map(|value| uuid_to_raw16(value).to_vec());
                let action_type = query
                    .action_type
                    .map(|value| value.as_db_value().to_string());
                let created_from = query.created_from.map(format_oracle_utc);
                let created_to = query.created_to.map(format_oracle_utc);
                let cursor_created_at = query
                    .cursor
                    .as_ref()
                    .map(|cursor| format_oracle_utc(cursor.created_at));
                let cursor_id = query
                    .cursor
                    .as_ref()
                    .map(|cursor| uuid_to_raw16(cursor.audit_log_id).to_vec());
                let fetch_limit = i64::from(query.limit) + 1;
                let page_limit = query.limit as usize;
                let binds: &[(&str, &dyn oracle::sql_type::ToSql)] = &[
                    ("entity_type", &query.entity_type),
                    ("entity_id", &entity_id),
                    ("action_type", &action_type),
                    ("actor_subject", &query.actor_subject),
                    ("actor_client_id", &query.actor_client_id),
                    ("actor_provider_id", &actor_provider_id),
                    ("actor_user_id", &actor_user_id),
                    ("source_ip", &query.source_ip),
                    ("correlation_id", &query.correlation_id),
                    ("request_id", &query.request_id),
                    ("created_from", &created_from),
                    ("created_to", &created_to),
                    ("cursor_created_at", &cursor_created_at),
                    ("cursor_id", &cursor_id),
                    ("fetch_limit", &fetch_limit),
                ];
                let rows = connection
                    .query_named(audit_list_sql(), binds)
                    .map_err(|error| {
                        DbError::Query(format!("failed to list audit logs: {error}"))
                    })?;
                let mut items = Vec::new();
                for row in rows {
                    let row = row.map_err(|error| {
                        DbError::Query(format!("failed to read audit log row: {error}"))
                    })?;
                    items.push(map_audit_row(&row)?);
                }
                let has_next_page = items.len() > page_limit;
                if has_next_page {
                    items.truncate(page_limit);
                }
                let next_cursor = has_next_page.then(|| {
                    let item = items.last().expect("non-empty paginated audit page");
                    AuditLogCursor {
                        created_at: item.created_at,
                        audit_log_id: item.audit_log_id,
                    }
                });
                Ok(AuditLogPage { items, next_cursor })
            })
            .await
    }
}

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
    let old_values = audit_log
        .old_values
        .map(redact_audit_snapshot)
        .map(|value| value.to_string());
    let new_values = audit_log
        .new_values
        .map(redact_audit_snapshot)
        .map(|value| value.to_string());

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

fn audit_list_sql() -> &'static str {
    r#"
    SELECT audit_log_id, entity_type, entity_id, action_type, reason,
           JSON_SERIALIZE(old_values RETURNING CLOB),
           JSON_SERIALIZE(new_values RETURNING CLOB),
           actor_subject, actor_client_id, actor_provider_id, actor_user_id,
           actor_issuer, source_ip, correlation_id, request_id,
           TO_CHAR(SYS_EXTRACT_UTC(created_at),'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')
    FROM audit_logs
    WHERE (:entity_type IS NULL OR entity_type = :entity_type)
      AND (:entity_id IS NULL OR entity_id = :entity_id)
      AND (:action_type IS NULL OR action_type = :action_type)
      AND (:actor_subject IS NULL OR actor_subject = :actor_subject)
      AND (:actor_client_id IS NULL OR actor_client_id = :actor_client_id)
      AND (:actor_provider_id IS NULL OR actor_provider_id = :actor_provider_id)
      AND (:actor_user_id IS NULL OR actor_user_id = :actor_user_id)
      AND (:source_ip IS NULL OR source_ip = :source_ip)
      AND (:correlation_id IS NULL OR correlation_id = :correlation_id)
      AND (:request_id IS NULL OR request_id = :request_id)
      AND (:created_from IS NULL OR SYS_EXTRACT_UTC(created_at) >= TO_TIMESTAMP(:created_from, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'))
      AND (:created_to IS NULL OR SYS_EXTRACT_UTC(created_at) < TO_TIMESTAMP(:created_to, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'))
      AND (
          :cursor_created_at IS NULL
          OR SYS_EXTRACT_UTC(created_at) < TO_TIMESTAMP(:cursor_created_at, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')
          OR (
              SYS_EXTRACT_UTC(created_at) = TO_TIMESTAMP(:cursor_created_at, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')
              AND audit_log_id < :cursor_id
          )
      )
    ORDER BY created_at DESC, audit_log_id DESC
    FETCH FIRST :fetch_limit ROWS ONLY
    "#
}

fn map_audit_row(row: &oracle::Row) -> DbResult<AuditLogRecord> {
    let audit_log_id: Vec<u8> = row.get(0).map_err(read_error)?;
    let entity_id: Vec<u8> = row.get(2).map_err(read_error)?;
    let action_type: String = row.get(3).map_err(read_error)?;
    let old_values: Option<String> = row.get(5).map_err(read_error)?;
    let new_values: Option<String> = row.get(6).map_err(read_error)?;
    let actor_provider_id: Option<Vec<u8>> = row.get(9).map_err(read_error)?;
    let actor_user_id: Option<Vec<u8>> = row.get(10).map_err(read_error)?;
    let created_at: String = row.get(15).map_err(read_error)?;
    Ok(AuditLogRecord {
        audit_log_id: raw16_to_uuid(&audit_log_id)?,
        entity_type: row.get(1).map_err(read_error)?,
        entity_id: raw16_to_uuid(&entity_id)?,
        action_type: AuditAction::from_db_value(&action_type)
            .ok_or_else(|| DbError::Query("unknown audit action type".to_string()))?,
        reason: row.get(4).map_err(read_error)?,
        old_values: parse_optional_json(old_values)?,
        new_values: parse_optional_json(new_values)?,
        actor_subject: row.get(7).map_err(read_error)?,
        actor_client_id: row.get(8).map_err(read_error)?,
        actor_provider_id: actor_provider_id
            .as_deref()
            .map(raw16_to_uuid)
            .transpose()?,
        actor_user_id: actor_user_id.as_deref().map(raw16_to_uuid).transpose()?,
        actor_issuer: row.get(11).map_err(read_error)?,
        source_ip: row.get(12).map_err(read_error)?,
        correlation_id: row.get(13).map_err(read_error)?,
        request_id: row.get(14).map_err(read_error)?,
        created_at: parse_utc(&created_at)?,
    })
}

fn parse_optional_json(value: Option<String>) -> DbResult<Option<serde_json::Value>> {
    value
        .map(|value| {
            serde_json::from_str(&value)
                .map(redact_audit_snapshot)
                .map_err(|error| DbError::Query(format!("invalid audit JSON: {error}")))
        })
        .transpose()
}

fn parse_utc(value: &str) -> DbResult<chrono::DateTime<chrono::Utc>> {
    Ok(chrono::DateTime::parse_from_rfc3339(value)
        .map_err(|error| DbError::Query(format!("invalid audit timestamp: {error}")))?
        .with_timezone(&chrono::Utc))
}

fn format_oracle_utc(value: chrono::DateTime<chrono::Utc>) -> String {
    value.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

fn read_error(error: oracle::Error) -> DbError {
    DbError::Query(format!("failed to read Oracle audit row: {error}"))
}

#[cfg(test)]
mod tests {
    use super::{audit_insert_sql, audit_list_sql};

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

    #[test]
    fn audit_list_uses_allowlisted_bind_filters_and_keyset_order() {
        let sql = audit_list_sql();
        for bind in [
            ":entity_type",
            ":entity_id",
            ":action_type",
            ":actor_subject",
            ":actor_client_id",
            ":actor_provider_id",
            ":actor_user_id",
            ":source_ip",
            ":correlation_id",
            ":request_id",
            ":created_from",
            ":created_to",
        ] {
            assert!(sql.contains(bind));
        }
        assert!(sql.contains("audit_log_id < :cursor_id"));
        assert!(sql.contains("ORDER BY created_at DESC, audit_log_id DESC"));
        assert!(sql.contains("FETCH FIRST :fetch_limit ROWS ONLY"));
    }
}
