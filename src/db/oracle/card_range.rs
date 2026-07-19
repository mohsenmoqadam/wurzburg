use chrono::{DateTime, Utc};
use oracle::Row;
use uuid::Uuid;

use crate::{
    api::command::MutationCommandContext,
    db::{
        error::{DbError, DbResult},
        oracle::{
            OracleRepository,
            audit::insert_audit_log,
            idempotency::{
                complete_idempotency_record, fetch_idempotency_record, insert_idempotency_record,
            },
            types::{raw16_to_uuid, uuid_to_raw16},
        },
    },
    domain::{
        audit::{AuditAction, NewAuditLog},
        card_range::{
            CardNumberRange, CardRange, CardRangeListCursor, CardRangeListPage, CardRangeListQuery,
            CardRangeStatus, CmsOperationMode, FundingMode, LimitCalendar, LimitWindowMode,
            NewCardRange, WeekStartDay, WithdrawalLimitAuthority,
        },
        idempotency::IdempotencyStatus,
    },
};

#[derive(Debug, Clone, PartialEq)]
pub enum CreateCardRangePersistenceOutcome {
    Created(Box<CardRange>),
    Replayed(serde_json::Value),
    IdempotencyConflict,
    IdempotencyInProgress,
    IdempotencyInvalidState,
    Overlap,
}

impl OracleRepository {
    #[tracing::instrument(
        skip(self, command_context, card_range),
        fields(db.system = "oracle", db.operation.name = "card_ranges.create")
    )]
    pub async fn create_card_range_atomic(
        &self,
        command_context: MutationCommandContext,
        card_range: NewCardRange,
    ) -> DbResult<CreateCardRangePersistenceOutcome> {
        let operation_type = command_context.operation_type.clone();
        let idempotency_key = command_context.idempotency_key.as_str().to_string();
        let request_hash = command_context.request_hash.clone();
        let retry_operation_type = operation_type.clone();
        let retry_idempotency_key = idempotency_key.clone();
        let retry_request_hash = request_hash.clone();
        let pool = self.pool.clone();

        let result = self
            .pool
            .with_transaction("atomic card range creation", move |connection| {
                if let Some(existing) = traced_db_step("idempotency.lookup", || {
                    fetch_idempotency_record(connection, &operation_type, &idempotency_key)
                })? {
                    return classify_idempotency(&existing, &request_hash);
                }

                traced_db_step("card_ranges.lock_structure", || {
                    lock_card_range_structure(connection)
                })?;
                // A concurrent request may have completed while this command was
                // waiting for the structural lock. Re-read before evaluating
                // overlap so same-key retries replay the winner deterministically.
                if let Some(existing) = traced_db_step("idempotency.recheck", || {
                    fetch_idempotency_record(connection, &operation_type, &idempotency_key)
                })? {
                    return classify_idempotency(&existing, &request_hash);
                }
                if traced_db_step("card_ranges.check_overlap", || {
                    card_range_overlaps(connection, &card_range.numbers)
                })? {
                    return Ok(CreateCardRangePersistenceOutcome::Overlap);
                }

                traced_db_step("idempotency.insert", || {
                    insert_idempotency_record(connection, command_context.new_idempotency_record())
                })?;
                let created = traced_db_step("card_ranges.insert", || {
                    insert_card_range(connection, card_range, &command_context.actor.subject)
                })?;
                let snapshot = created.replay_snapshot();

                traced_db_step("audit_logs.insert", || {
                    insert_audit_log(
                        connection,
                        NewAuditLog {
                            audit_log_id: Uuid::new_v4(),
                            entity_type: "CARD_RANGE".to_string(),
                            entity_id: created.card_range_id,
                            action_type: AuditAction::Insert,
                            reason: Some("card range created".to_string()),
                            old_values: None,
                            new_values: Some(snapshot.clone()),
                            context: command_context.audit_context(),
                        },
                    )
                })?;
                traced_db_step("idempotency.complete", || {
                    complete_idempotency_record(
                        connection,
                        &operation_type,
                        &idempotency_key,
                        "card_range",
                        created.card_range_id,
                        snapshot,
                    )
                })?;

                Ok(CreateCardRangePersistenceOutcome::Created(Box::new(
                    created,
                )))
            })
            .await;

        match result {
            Err(DbError::Conflict(_)) => {
                pool.with_connection(move |connection| {
                    let existing = fetch_idempotency_record(
                        connection,
                        &retry_operation_type,
                        &retry_idempotency_key,
                    )?
                    .ok_or_else(|| {
                        DbError::Query(
                            "concurrent idempotency winner was not visible after conflict"
                                .to_string(),
                        )
                    })?;
                    classify_idempotency(&existing, &retry_request_hash)
                })
                .await
            }
            other => other,
        }
    }

    #[tracing::instrument(skip(self), fields(db.system = "oracle", db.operation.name = "card_ranges.get"))]
    pub async fn get_card_range(&self, card_range_id: Uuid) -> DbResult<Option<CardRange>> {
        self.pool
            .with_connection(
                move |connection| match fetch_card_range(connection, card_range_id) {
                    Ok(card_range) => Ok(Some(card_range)),
                    Err(DbError::Query(message)) if message.contains("ORA-01403") => Ok(None),
                    Err(error) => Err(error),
                },
            )
            .await
    }

    #[tracing::instrument(skip(self, query), fields(db.system = "oracle", db.operation.name = "card_ranges.list"))]
    pub async fn list_card_ranges(&self, query: CardRangeListQuery) -> DbResult<CardRangeListPage> {
        self.pool
            .with_connection(move |connection| {
                let status = query.status.map(|value| value.as_db_value().to_string());
                let funding_mode = query
                    .funding_mode
                    .map(|value| value.as_db_value().to_string());
                let authority = query
                    .withdrawal_limit_authority
                    .map(|value| value.as_db_value().to_string());
                let cursor_created_at = query
                    .cursor
                    .as_ref()
                    .map(|cursor| format_oracle_utc(cursor.created_at));
                let cursor_id = query
                    .cursor
                    .as_ref()
                    .map(|cursor| uuid_to_raw16(cursor.card_range_id).to_vec());
                let fetch_limit = i64::from(query.database_fetch_limit());
                let page_limit = usize::from(query.limit);
                let bind_params: &[(&str, &dyn oracle::sql_type::ToSql)] = &[
                    ("status", &status),
                    ("funding_mode", &funding_mode),
                    ("authority", &authority),
                    ("cursor_created_at", &cursor_created_at),
                    ("cursor_id", &cursor_id),
                    ("fetch_limit", &fetch_limit),
                ];

                let rows = connection
                    .query_named(card_range_list_sql(), bind_params)
                    .map_err(|error| {
                        DbError::Query(format!("failed to list card ranges: {error}"))
                    })?;

                let mut items = Vec::new();
                for row in rows {
                    let row = row.map_err(|error| {
                        DbError::Query(format!("failed to read listed card range row: {error}"))
                    })?;
                    items.push(map_card_range_row(&row)?);
                }

                let has_next_page = items.len() > page_limit;
                if has_next_page {
                    items.truncate(page_limit);
                }
                let next_cursor = if has_next_page {
                    items.last().map(|card_range| CardRangeListCursor {
                        created_at: card_range.created_at,
                        card_range_id: card_range.card_range_id,
                    })
                } else {
                    None
                };

                Ok(CardRangeListPage { items, next_cursor })
            })
            .await
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="card_range_providers.list", card_range_id=%card_range_id))]
    pub async fn list_card_range_providers(
        &self,
        card_range_id: Uuid,
    ) -> DbResult<Vec<crate::domain::card_range::CardRangeProviderEligibility>> {
        self.pool.with_connection(move |connection| {
            let rows = connection.query("SELECT provider_id,status FROM card_range_providers WHERE card_range_id=:1 ORDER BY created_at,provider_id", &[&uuid_to_raw16(card_range_id).to_vec()]).map_err(|error| DbError::Query(format!("failed to list range providers: {error}")))?;
            rows.map(|row| {
                let row = row.map_err(|error| DbError::Query(format!("failed to read range provider: {error}")))?;
                let raw: Vec<u8> = row.get(0).map_err(read_error)?;
                let status: String = row.get(1).map_err(read_error)?;
                Ok(crate::domain::card_range::CardRangeProviderEligibility { card_range_id, provider_id: raw16_to_uuid(&raw)?, status: crate::domain::card_range::CardRangeProviderStatus::from_db_value(&status).ok_or_else(|| DbError::Query("unknown range provider status".to_string()))? })
            }).collect()
        }).await
    }
}

fn traced_db_step<T>(
    operation_name: &'static str,
    operation: impl FnOnce() -> DbResult<T>,
) -> DbResult<T> {
    let span = tracing::info_span!(
        "oracle.command.step",
        db.system = "oracle",
        db.operation.name = operation_name
    );
    let _guard = span.enter();
    operation()
}

fn classify_idempotency(
    existing: &crate::domain::idempotency::IdempotencyRecord,
    request_hash: &str,
) -> DbResult<CreateCardRangePersistenceOutcome> {
    if existing.request_hash != request_hash {
        return Ok(CreateCardRangePersistenceOutcome::IdempotencyConflict);
    }

    Ok(match existing.status {
        IdempotencyStatus::Completed => existing
            .response_snapshot
            .clone()
            .map(CreateCardRangePersistenceOutcome::Replayed)
            .unwrap_or(CreateCardRangePersistenceOutcome::IdempotencyInvalidState),
        IdempotencyStatus::InProgress => CreateCardRangePersistenceOutcome::IdempotencyInProgress,
        IdempotencyStatus::Failed | IdempotencyStatus::Conflict => {
            CreateCardRangePersistenceOutcome::IdempotencyInvalidState
        }
    })
}

fn lock_card_range_structure(connection: &oracle::Connection) -> DbResult<()> {
    connection
        .query_row_as::<String>(card_range_lock_sql(), &[&"CARD_RANGE_STRUCTURE"])
        .map(|_| ())
        .map_err(|error| DbError::Query(format!("failed to lock card range structure: {error}")))
}

fn card_range_overlaps(
    connection: &oracle::Connection,
    numbers: &CardNumberRange,
) -> DbResult<bool> {
    let count = connection
        .query_row_as::<i64>(card_range_overlap_sql(), &[&numbers.end, &numbers.start])
        .map_err(|error| DbError::Query(format!("failed to check card range overlap: {error}")))?;
    Ok(count > 0)
}

fn insert_card_range(
    connection: &oracle::Connection,
    card_range: NewCardRange,
    actor_subject: &str,
) -> DbResult<CardRange> {
    let card_range_id = uuid_to_raw16(card_range.card_range_id).to_vec();
    let limit_calendar_json = card_range
        .limit_calendar
        .as_ref()
        .map(limit_calendar_to_json_string)
        .transpose()?;
    let metadata_json = card_range.metadata_json.to_string();
    let issuance_enabled = i32::from(card_range.issuance_enabled);

    connection
        .execute(
            card_range_insert_sql(),
            &[
                &card_range_id,
                &card_range.numbers.start,
                &card_range.numbers.end,
                &card_range.funding_mode.as_db_value(),
                &card_range.withdrawal_limit_authority.as_db_value(),
                &limit_calendar_json,
                &CardRangeStatus::Draft.as_db_value(),
                &issuance_enabled,
                &card_range.cms_operation_mode.as_db_value(),
                &1_i64,
                &metadata_json,
                &actor_subject,
                &actor_subject,
            ],
        )
        .map_err(|error| DbError::Query(format!("failed to insert card range: {error}")))?;

    fetch_card_range(connection, card_range.card_range_id)
}

pub(crate) fn card_range_lock_sql() -> &'static str {
    "SELECT lock_name FROM card_range_allocation_locks WHERE lock_name = :1 FOR UPDATE"
}

pub(crate) fn card_range_insert_sql() -> &'static str {
    r#"
    INSERT INTO card_ranges (
        card_range_id,
        start_card_number,
        end_card_number,
        funding_mode,
        withdrawal_limit_authority,
        limit_calendar_json,
        status,
        issuance_enabled,
        cms_operation_mode,
        operational_version,
        metadata_json,
        created_by_subject,
        updated_by_subject
    )
    VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13)
    "#
}

pub(crate) fn card_range_overlap_sql() -> &'static str {
    r#"
    SELECT COUNT(*)
    FROM card_ranges
    WHERE start_card_number <= :1
      AND end_card_number >= :2
    "#
}

pub(crate) fn card_range_select_sql() -> &'static str {
    r#"
    SELECT
        card_range_id,
        start_card_number,
        end_card_number,
        funding_mode,
        withdrawal_limit_authority,
        JSON_SERIALIZE(limit_calendar_json RETURNING CLOB) AS limit_calendar_json,
        status,
        issuance_enabled,
        cms_operation_mode,
        operational_version,
        materialized_operational_version,
        range_control_operation_id,
        JSON_SERIALIZE(metadata_json RETURNING CLOB) AS metadata_json,
        created_by_subject,
        updated_by_subject,
        TO_CHAR(SYS_EXTRACT_UTC(created_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS created_at,
        TO_CHAR(SYS_EXTRACT_UTC(updated_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS updated_at
    FROM card_ranges
    WHERE card_range_id = :1
    "#
}

pub(crate) fn card_range_list_sql() -> &'static str {
    r#"
    SELECT
        card_range_id,
        start_card_number,
        end_card_number,
        funding_mode,
        withdrawal_limit_authority,
        JSON_SERIALIZE(limit_calendar_json RETURNING CLOB) AS limit_calendar_json,
        status,
        issuance_enabled,
        cms_operation_mode,
        operational_version,
        materialized_operational_version,
        range_control_operation_id,
        JSON_SERIALIZE(metadata_json RETURNING CLOB) AS metadata_json,
        created_by_subject,
        updated_by_subject,
        TO_CHAR(SYS_EXTRACT_UTC(created_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS created_at,
        TO_CHAR(SYS_EXTRACT_UTC(updated_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS updated_at
    FROM card_ranges
    WHERE (:status IS NULL OR status = :status)
      AND (:funding_mode IS NULL OR funding_mode = :funding_mode)
      AND (:authority IS NULL OR withdrawal_limit_authority = :authority)
      AND (
          :cursor_created_at IS NULL
          OR SYS_EXTRACT_UTC(created_at) > TO_TIMESTAMP(:cursor_created_at, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')
          OR (
              SYS_EXTRACT_UTC(created_at) = TO_TIMESTAMP(:cursor_created_at, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')
              AND card_range_id > :cursor_id
          )
      )
    ORDER BY created_at ASC, card_range_id ASC
    FETCH NEXT :fetch_limit ROWS ONLY
    "#
}

pub(crate) fn fetch_card_range(
    connection: &oracle::Connection,
    card_range_id: Uuid,
) -> DbResult<CardRange> {
    let card_range_id = uuid_to_raw16(card_range_id).to_vec();
    let row = connection
        .query_row(card_range_select_sql(), &[&card_range_id])
        .map_err(|error| DbError::Query(format!("failed to fetch card range: {error}")))?;

    map_card_range_row(&row)
}

pub(crate) fn map_card_range_row(row: &Row) -> DbResult<CardRange> {
    let card_range_id: Vec<u8> = row.get(0).map_err(read_error)?;
    let funding_mode: String = row.get(3).map_err(read_error)?;
    let authority: String = row.get(4).map_err(read_error)?;
    let limit_calendar_json: Option<String> = row.get(5).map_err(read_error)?;
    let status: String = row.get(6).map_err(read_error)?;
    let cms_operation_mode: String = row.get(8).map_err(read_error)?;
    let range_control_operation_id: Option<Vec<u8>> = row.get(11).map_err(read_error)?;
    let metadata_json: String = row.get(12).map_err(read_error)?;
    let created_at: String = row.get(15).map_err(read_error)?;
    let updated_at: String = row.get(16).map_err(read_error)?;

    Ok(CardRange {
        card_range_id: raw16_to_uuid(&card_range_id)?,
        numbers: CardNumberRange {
            start: row.get(1).map_err(read_error)?,
            end: row.get(2).map_err(read_error)?,
        },
        funding_mode: FundingMode::from_db_value(&funding_mode)
            .ok_or_else(|| DbError::Query(format!("unknown funding mode `{funding_mode}`")))?,
        withdrawal_limit_authority: WithdrawalLimitAuthority::from_db_value(&authority)
            .ok_or_else(|| DbError::Query(format!("unknown withdrawal authority `{authority}`")))?,
        limit_calendar: limit_calendar_json
            .as_deref()
            .map(limit_calendar_from_json)
            .transpose()?,
        status: CardRangeStatus::from_db_value(&status)
            .ok_or_else(|| DbError::Query(format!("unknown card range status `{status}`")))?,
        issuance_enabled: row.get::<_, i32>(7).map_err(read_error)? == 1,
        cms_operation_mode: CmsOperationMode::from_db_value(&cms_operation_mode).ok_or_else(
            || DbError::Query(format!("unknown CMS operation mode `{cms_operation_mode}`")),
        )?,
        operational_version: row.get(9).map_err(read_error)?,
        materialized_operational_version: row.get(10).map_err(read_error)?,
        range_control_operation_id: range_control_operation_id
            .as_deref()
            .map(raw16_to_uuid)
            .transpose()?,
        metadata_json: serde_json::from_str(&metadata_json).map_err(|error| {
            DbError::Query(format!(
                "invalid card range metadata JSON in Oracle row: {error}"
            ))
        })?,
        created_by_subject: row.get(13).map_err(read_error)?,
        updated_by_subject: row.get(14).map_err(read_error)?,
        created_at: parse_utc(&created_at)?,
        updated_at: parse_utc(&updated_at)?,
    })
}

fn limit_calendar_to_json_string(calendar: &LimitCalendar) -> DbResult<String> {
    serde_json::to_string(&serde_json::json!({
        "timezone": calendar.timezone,
        "week_starts_on": week_start_to_json(calendar.week_starts_on),
        "window_mode": limit_window_mode_to_json(calendar.window_mode)
    }))
    .map_err(|error| DbError::Query(format!("failed to serialize limit calendar: {error}")))
}

fn limit_calendar_from_json(value: &str) -> DbResult<LimitCalendar> {
    let value: serde_json::Value = serde_json::from_str(value)
        .map_err(|error| DbError::Query(format!("invalid limit calendar JSON: {error}")))?;

    let timezone = value
        .get("timezone")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| DbError::Query("limit calendar timezone is missing".to_string()))?
        .to_string();
    let week_starts_on = match value
        .get("week_starts_on")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| DbError::Query("limit calendar week start is missing".to_string()))?
    {
        "Saturday" => WeekStartDay::Saturday,
        "Sunday" => WeekStartDay::Sunday,
        "Monday" => WeekStartDay::Monday,
        other => return Err(DbError::Query(format!("unknown week start day `{other}`"))),
    };
    let window_mode = match value
        .get("window_mode")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| DbError::Query("limit calendar window mode is missing".to_string()))?
    {
        "Calendar" => LimitWindowMode::Calendar,
        other => {
            return Err(DbError::Query(format!(
                "unknown limit window mode `{other}`"
            )));
        }
    };

    Ok(LimitCalendar {
        timezone,
        week_starts_on,
        window_mode,
    })
}

fn week_start_to_json(value: WeekStartDay) -> &'static str {
    match value {
        WeekStartDay::Saturday => "Saturday",
        WeekStartDay::Sunday => "Sunday",
        WeekStartDay::Monday => "Monday",
    }
}

fn limit_window_mode_to_json(value: LimitWindowMode) -> &'static str {
    match value {
        LimitWindowMode::Calendar => "Calendar",
    }
}

fn parse_utc(value: &str) -> DbResult<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)
        .map_err(|error| {
            DbError::Query(format!("invalid Oracle UTC timestamp `{value}`: {error}"))
        })?
        .with_timezone(&Utc))
}

fn format_oracle_utc(value: DateTime<Utc>) -> String {
    value.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

fn read_error(error: oracle::Error) -> DbError {
    DbError::Query(format!("failed to read Oracle card range row: {error}"))
}

#[cfg(test)]
mod tests {
    use super::{
        card_range_insert_sql, card_range_list_sql, card_range_lock_sql, card_range_overlap_sql,
        card_range_select_sql,
    };

    #[test]
    fn insert_sql_persists_range_structure_and_controls() {
        let sql = card_range_insert_sql();

        assert!(sql.contains("start_card_number"));
        assert!(sql.contains("end_card_number"));
        assert!(sql.contains("funding_mode"));
        assert!(sql.contains("withdrawal_limit_authority"));
        assert!(sql.contains("cms_operation_mode"));
    }

    #[test]
    fn overlap_sql_uses_inclusive_boundary_shape() {
        let sql = card_range_overlap_sql();

        assert!(sql.contains("start_card_number <= :1"));
        assert!(sql.contains("end_card_number >= :2"));
    }

    #[test]
    fn structural_lock_serializes_overlap_decisions() {
        let sql = card_range_lock_sql();

        assert!(sql.contains("card_range_allocation_locks"));
        assert!(sql.contains("FOR UPDATE"));
    }

    #[test]
    fn select_sql_serializes_json_columns() {
        let sql = card_range_select_sql();

        assert!(sql.contains("JSON_SERIALIZE(limit_calendar_json"));
        assert!(sql.contains("JSON_SERIALIZE(metadata_json"));
    }

    #[test]
    fn list_sql_uses_allowlisted_filters_and_keyset_cursor() {
        let sql = card_range_list_sql();

        assert!(sql.contains("(:status IS NULL OR status = :status)"));
        assert!(sql.contains("(:funding_mode IS NULL OR funding_mode = :funding_mode)"));
        assert!(sql.contains("(:authority IS NULL OR withdrawal_limit_authority = :authority)"));
        assert!(sql.contains("card_range_id > :cursor_id"));
        assert!(sql.contains("FETCH NEXT :fetch_limit ROWS ONLY"));
    }
}
