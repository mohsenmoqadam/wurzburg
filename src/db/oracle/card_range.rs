use async_trait::async_trait;
use chrono::{DateTime, Utc};
use oracle::Row;
use uuid::Uuid;

use crate::{
    db::{
        error::{DbError, DbResult},
        oracle::{
            OracleRepository,
            types::{raw16_to_uuid, uuid_to_raw16},
        },
        traits::CardRangeRepository,
    },
    domain::card_range::{
        CardNumberRange, CardRange, CardRangeStatus, CmsOperationMode, FundingMode, LimitCalendar,
        LimitWindowMode, NewCardRange, WeekStartDay, WithdrawalLimitAuthority,
    },
};

#[async_trait]
impl CardRangeRepository for OracleRepository {
    async fn create_card_range(
        &self,
        card_range: NewCardRange,
        actor_subject: String,
    ) -> DbResult<CardRange> {
        self.pool
            .with_transaction("card range creation", move |connection| {
                let card_range_id = uuid_to_raw16(card_range.card_range_id).to_vec();
                let limit_calendar_json = card_range
                    .limit_calendar
                    .as_ref()
                    .map(limit_calendar_to_json_string)
                    .transpose()?;
                let metadata_json = card_range.metadata_json.to_string();
                let issuance_enabled = if card_range.issuance_enabled {
                    1_i32
                } else {
                    0_i32
                };

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
                    .map_err(|error| {
                        DbError::Query(format!("failed to insert card range: {error}"))
                    })?;

                fetch_card_range(connection, card_range.card_range_id)
            })
            .await
    }

    async fn get_card_range(&self, card_range_id: Uuid) -> DbResult<Option<CardRange>> {
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

    async fn card_range_overlaps(&self, numbers: CardNumberRange) -> DbResult<bool> {
        self.pool
            .with_connection(move |connection| {
                let count = connection
                    .query_row_as::<i64>(card_range_overlap_sql(), &[&numbers.end, &numbers.start])
                    .map_err(|error| {
                        DbError::Query(format!("failed to check card range overlap: {error}"))
                    })?;

                Ok(count > 0)
            })
            .await
    }
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
        JSON_SERIALIZE(metadata_json RETURNING CLOB) AS metadata_json,
        created_by_subject,
        updated_by_subject,
        TO_CHAR(SYS_EXTRACT_UTC(created_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS created_at,
        TO_CHAR(SYS_EXTRACT_UTC(updated_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS updated_at
    FROM card_ranges
    WHERE card_range_id = :1
    "#
}

fn fetch_card_range(connection: &oracle::Connection, card_range_id: Uuid) -> DbResult<CardRange> {
    let card_range_id = uuid_to_raw16(card_range_id).to_vec();
    let row = connection
        .query_row(card_range_select_sql(), &[&card_range_id])
        .map_err(|error| DbError::Query(format!("failed to fetch card range: {error}")))?;

    map_card_range_row(&row)
}

fn map_card_range_row(row: &Row) -> DbResult<CardRange> {
    let card_range_id: Vec<u8> = row.get(0).map_err(read_error)?;
    let funding_mode: String = row.get(3).map_err(read_error)?;
    let authority: String = row.get(4).map_err(read_error)?;
    let limit_calendar_json: Option<String> = row.get(5).map_err(read_error)?;
    let status: String = row.get(6).map_err(read_error)?;
    let cms_operation_mode: String = row.get(8).map_err(read_error)?;
    let metadata_json: String = row.get(10).map_err(read_error)?;
    let created_at: String = row.get(13).map_err(read_error)?;
    let updated_at: String = row.get(14).map_err(read_error)?;

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
        metadata_json: serde_json::from_str(&metadata_json).map_err(|error| {
            DbError::Query(format!(
                "invalid card range metadata JSON in Oracle row: {error}"
            ))
        })?,
        created_by_subject: row.get(11).map_err(read_error)?,
        updated_by_subject: row.get(12).map_err(read_error)?,
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

fn read_error(error: oracle::Error) -> DbError {
    DbError::Query(format!("failed to read Oracle card range row: {error}"))
}

#[cfg(test)]
mod tests {
    use super::{card_range_insert_sql, card_range_overlap_sql, card_range_select_sql};

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
    fn select_sql_serializes_json_columns() {
        let sql = card_range_select_sql();

        assert!(sql.contains("JSON_SERIALIZE(limit_calendar_json"));
        assert!(sql.contains("JSON_SERIALIZE(metadata_json"));
    }
}
