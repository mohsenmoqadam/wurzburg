use anyhow::{Result, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use oracle::{ErrorKind, Row};
use uuid::Uuid;

use crate::{
    db::{
        oracle::{
            OracleRepository,
            types::{raw16_to_uuid, uuid_to_raw16},
        },
        traits::CardRangeRepository,
    },
    domain::card_range::{
        CardRange, CardRangeProvider, CardRangeProviderStatus, CardRangeStatus, FundingMode,
        NewCardRange,
    },
};

#[async_trait]
impl CardRangeRepository for OracleRepository {
    async fn create_card_range(&self, card_range: NewCardRange) -> Result<CardRange> {
        self.pool
            .with_transaction("card range creation", move |connection| {
                let id = uuid_to_raw16(card_range.id).to_vec();
                let metadata = card_range.metadata.to_string();
                lock_card_range_allocation(connection)?;

                if card_range_overlaps_on_connection(
                    connection,
                    &card_range.start_card_number,
                    &card_range.end_card_number,
                )? {
                    return Err(crate::db::error::DbError::Conflict(
                        "card range overlaps an existing range".to_string(),
                    ));
                }

                connection
                    .execute(
                        r#"
                        INSERT INTO card_ranges (
                            card_range_id, start_card_number, end_card_number,
                            funding_mode, status, metadata_json,
                            created_by, updated_by
                        )
                        VALUES (:1, :2, :3, :4, 'DRAFT', :5, :6, :6)
                        "#,
                        &[
                            &id,
                            &card_range.start_card_number,
                            &card_range.end_card_number,
                            &card_range.funding_mode.as_db_value(),
                            &metadata,
                            &card_range.actor_subject,
                        ],
                    )
                    .map_err(|error| {
                        crate::db::error::DbError::Query(format!(
                            "failed to insert card range: {error}"
                        ))
                    })?;

                let sql = card_range_select_sql("WHERE card_range_id = :1");
                let row = connection.query_row(&sql, &[&id]).map_err(|error| {
                    crate::db::error::DbError::Query(format!(
                        "failed to fetch inserted card range: {error}"
                    ))
                })?;

                let card_range = map_card_range_row(&row).map_err(|error| {
                    crate::db::error::DbError::Query(format!("invalid card range row: {error}"))
                })?;

                Ok(card_range)
            })
            .await
            .map_err(Into::into)
    }

    async fn get_card_range(&self, card_range_id: Uuid) -> Result<Option<CardRange>> {
        self.pool
            .with_transaction("card range status update", move |connection| {
                let id = uuid_to_raw16(card_range_id).to_vec();
                let sql = card_range_select_sql("WHERE card_range_id = :1");
                match connection.query_row(&sql, &[&id]) {
                    Ok(row) => map_card_range_row(&row).map(Some).map_err(|error| {
                        crate::db::error::DbError::Query(format!("invalid card range row: {error}"))
                    }),
                    Err(error) if error.kind() == ErrorKind::NoDataFound => Ok(None),
                    Err(error) => Err(crate::db::error::DbError::Query(format!(
                        "failed to fetch card range: {error}"
                    ))),
                }
            })
            .await
            .map_err(Into::into)
    }

    async fn list_card_ranges(&self) -> Result<Vec<CardRange>> {
        self.pool
            .with_connection(|connection| {
                let sql = card_range_select_sql("ORDER BY start_card_number ASC");
                let mut rows = connection.query(&sql, &[]).map_err(|error| {
                    crate::db::error::DbError::Query(format!("failed to list card ranges: {error}"))
                })?;

                let mut ranges = Vec::new();
                while let Some(row_result) = rows.next() {
                    let row = row_result.map_err(|error| {
                        crate::db::error::DbError::Query(format!(
                            "failed to read card range row: {error}"
                        ))
                    })?;
                    ranges.push(map_card_range_row(&row).map_err(|error| {
                        crate::db::error::DbError::Query(format!("invalid card range row: {error}"))
                    })?);
                }

                Ok(ranges)
            })
            .await
            .map_err(Into::into)
    }

    async fn card_range_overlaps(
        &self,
        start_card_number: &str,
        end_card_number: &str,
    ) -> Result<bool> {
        let start = start_card_number.to_owned();
        let end = end_card_number.to_owned();
        self.pool
            .with_connection(move |connection| {
                let count: i64 = connection
                    .query_row_as(
                        r#"
                        SELECT COUNT(*)
                        FROM card_ranges
                        WHERE start_card_number <= :1
                          AND end_card_number >= :2
                        "#,
                        &[&end, &start],
                    )
                    .map_err(|error| {
                        crate::db::error::DbError::Query(format!(
                            "failed to check card range overlap: {error}"
                        ))
                    })?;

                Ok(count > 0)
            })
            .await
            .map_err(Into::into)
    }

    async fn set_card_range_status(
        &self,
        card_range_id: Uuid,
        status: CardRangeStatus,
        actor_subject: String,
    ) -> Result<CardRange> {
        self.pool
            .with_connection(move |connection| {
                let id = uuid_to_raw16(card_range_id).to_vec();
                connection
                    .execute(
                        r#"
                        UPDATE card_ranges
                        SET status = :1,
                            updated_by = :2,
                            updated_at = SYSTIMESTAMP
                        WHERE card_range_id = :3
                        "#,
                        &[&status.as_db_value(), &actor_subject, &id],
                    )
                    .map_err(|error| {
                        crate::db::error::DbError::Query(format!(
                            "failed to update card range status: {error}"
                        ))
                    })?;

                let sql = card_range_select_sql("WHERE card_range_id = :1");
                let row = connection.query_row(&sql, &[&id]).map_err(|error| {
                    crate::db::error::DbError::Query(format!(
                        "failed to fetch updated card range: {error}"
                    ))
                })?;

                let card_range = map_card_range_row(&row).map_err(|error| {
                    crate::db::error::DbError::Query(format!("invalid card range row: {error}"))
                })?;

                Ok(card_range)
            })
            .await
            .map_err(Into::into)
    }

    async fn update_card_range_metadata(
        &self,
        card_range_id: Uuid,
        metadata: serde_json::Value,
        actor_subject: String,
    ) -> Result<CardRange> {
        self.pool
            .with_transaction("card range metadata update", move |connection| {
                let id = uuid_to_raw16(card_range_id).to_vec();
                let metadata = metadata.to_string();
                connection
                    .execute(
                        r#"
                        UPDATE card_ranges
                        SET metadata_json = :1,
                            updated_by = :2,
                            updated_at = SYSTIMESTAMP
                        WHERE card_range_id = :3
                        "#,
                        &[&metadata, &actor_subject, &id],
                    )
                    .map_err(|error| {
                        crate::db::error::DbError::Query(format!(
                            "failed to update card range metadata: {error}"
                        ))
                    })?;

                let sql = card_range_select_sql("WHERE card_range_id = :1");
                let row = connection.query_row(&sql, &[&id]).map_err(|error| {
                    crate::db::error::DbError::Query(format!(
                        "failed to fetch updated card range: {error}"
                    ))
                })?;

                let card_range = map_card_range_row(&row).map_err(|error| {
                    crate::db::error::DbError::Query(format!("invalid card range row: {error}"))
                })?;

                Ok(card_range)
            })
            .await
            .map_err(Into::into)
    }

    async fn upsert_card_range_provider(
        &self,
        card_range_id: Uuid,
        provider_id: Uuid,
        status: CardRangeProviderStatus,
        metadata: serde_json::Value,
        actor_subject: String,
    ) -> Result<CardRangeProvider> {
        self.pool
            .with_transaction("card range provider upsert", move |connection| {
                let range_id = uuid_to_raw16(card_range_id).to_vec();
                let provider_id_raw = uuid_to_raw16(provider_id).to_vec();
                let metadata = metadata.to_string();
                connection
                    .execute(
                        r#"
                        MERGE INTO card_range_providers target
                        USING (
                            SELECT :1 AS card_range_id, :2 AS provider_id FROM dual
                        ) source
                        ON (
                            target.card_range_id = source.card_range_id
                            AND target.provider_id = source.provider_id
                        )
                        WHEN MATCHED THEN UPDATE SET
                            status = :3,
                            metadata_json = :4,
                            updated_by = :5,
                            updated_at = SYSTIMESTAMP
                        WHEN NOT MATCHED THEN INSERT (
                            card_range_id, provider_id, status, metadata_json,
                            created_by, updated_by
                        )
                        VALUES (:1, :2, :3, :4, :5, :5)
                        "#,
                        &[
                            &range_id,
                            &provider_id_raw,
                            &status.as_db_value(),
                            &metadata,
                            &actor_subject,
                        ],
                    )
                    .map_err(|error| {
                        crate::db::error::DbError::Query(format!(
                            "failed to upsert card range provider: {error}"
                        ))
                    })?;

                let provider = fetch_card_range_provider(connection, &range_id, &provider_id_raw)?;
                Ok(provider)
            })
            .await
            .map_err(Into::into)
    }

    async fn get_card_range_provider(
        &self,
        card_range_id: Uuid,
        provider_id: Uuid,
    ) -> Result<Option<CardRangeProvider>> {
        self.pool
            .with_transaction("card range provider status update", move |connection| {
                let range_id = uuid_to_raw16(card_range_id).to_vec();
                let provider_id_raw = uuid_to_raw16(provider_id).to_vec();
                let sql = card_range_provider_select_sql(
                    "WHERE crp.card_range_id = :1 AND crp.provider_id = :2",
                );
                match connection.query_row(&sql, &[&range_id, &provider_id_raw]) {
                    Ok(row) => map_card_range_provider_row(&row)
                        .map(Some)
                        .map_err(|error| {
                            crate::db::error::DbError::Query(format!(
                                "invalid card range provider row: {error}"
                            ))
                        }),
                    Err(error) if error.kind() == ErrorKind::NoDataFound => Ok(None),
                    Err(error) => Err(crate::db::error::DbError::Query(format!(
                        "failed to fetch card range provider: {error}"
                    ))),
                }
            })
            .await
            .map_err(Into::into)
    }

    async fn set_card_range_provider_status(
        &self,
        card_range_id: Uuid,
        provider_id: Uuid,
        status: CardRangeProviderStatus,
        actor_subject: String,
    ) -> Result<CardRangeProvider> {
        self.pool
            .with_connection(move |connection| {
                let range_id = uuid_to_raw16(card_range_id).to_vec();
                let provider_id_raw = uuid_to_raw16(provider_id).to_vec();
                connection
                    .execute(
                        r#"
                        UPDATE card_range_providers
                        SET status = :1,
                            updated_by = :2,
                            updated_at = SYSTIMESTAMP
                        WHERE card_range_id = :3
                          AND provider_id = :4
                        "#,
                        &[
                            &status.as_db_value(),
                            &actor_subject,
                            &range_id,
                            &provider_id_raw,
                        ],
                    )
                    .map_err(|error| {
                        crate::db::error::DbError::Query(format!(
                            "failed to update card range provider status: {error}"
                        ))
                    })?;

                let provider = fetch_card_range_provider(connection, &range_id, &provider_id_raw)?;
                Ok(provider)
            })
            .await
            .map_err(Into::into)
    }

    async fn list_card_range_providers(
        &self,
        card_range_id: Uuid,
    ) -> Result<Vec<CardRangeProvider>> {
        self.pool
            .with_connection(move |connection| {
                let range_id = uuid_to_raw16(card_range_id).to_vec();
                let sql = card_range_provider_select_sql(
                    "WHERE crp.card_range_id = :1 ORDER BY crp.created_at ASC",
                );
                let mut rows = connection.query(&sql, &[&range_id]).map_err(|error| {
                    crate::db::error::DbError::Query(format!(
                        "failed to list card range providers: {error}"
                    ))
                })?;

                let mut providers = Vec::new();
                while let Some(row_result) = rows.next() {
                    let row = row_result.map_err(|error| {
                        crate::db::error::DbError::Query(format!(
                            "failed to read card range provider row: {error}"
                        ))
                    })?;
                    providers.push(map_card_range_provider_row(&row).map_err(|error| {
                        crate::db::error::DbError::Query(format!(
                            "invalid card range provider row: {error}"
                        ))
                    })?);
                }
                Ok(providers)
            })
            .await
            .map_err(Into::into)
    }

    async fn list_provider_card_ranges(&self, provider_id: Uuid) -> Result<Vec<CardRange>> {
        self.pool
            .with_connection(move |connection| {
                let provider_id = uuid_to_raw16(provider_id).to_vec();
                let sql = card_range_select_sql(
                    "JOIN card_range_providers crp ON crp.card_range_id = cr.card_range_id WHERE crp.provider_id = :1 ORDER BY cr.start_card_number ASC",
                );
                let mut rows = connection
                    .query(&sql, &[&provider_id])
                    .map_err(|error| crate::db::error::DbError::Query(format!(
                        "failed to list provider card ranges: {error}"
                    )))?;

                let mut ranges = Vec::new();
                while let Some(row_result) = rows.next() {
                    let row = row_result.map_err(|error| {
                        crate::db::error::DbError::Query(format!(
                            "failed to read provider card range row: {error}"
                        ))
                    })?;
                    ranges.push(map_card_range_row(&row).map_err(|error| {
                        crate::db::error::DbError::Query(format!("invalid card range row: {error}"))
                    })?);
                }
                Ok(ranges)
            })
            .await
            .map_err(Into::into)
    }

    async fn count_card_range_providers(
        &self,
        card_range_id: Uuid,
        status: CardRangeProviderStatus,
    ) -> Result<i64> {
        self.pool
            .with_connection(move |connection| {
                let range_id = uuid_to_raw16(card_range_id).to_vec();
                connection
                    .query_row_as(
                        r#"
                        SELECT COUNT(*)
                        FROM card_range_providers
                        WHERE card_range_id = :1
                          AND status = :2
                        "#,
                        &[&range_id, &status.as_db_value()],
                    )
                    .map_err(|error| {
                        crate::db::error::DbError::Query(format!(
                            "failed to count card range providers: {error}"
                        ))
                    })
            })
            .await
            .map_err(Into::into)
    }
}

fn lock_card_range_allocation(connection: &oracle::Connection) -> crate::db::error::DbResult<()> {
    connection
        .query_row(
            "SELECT lock_name FROM card_range_allocation_locks WHERE lock_name = 'CARD_RANGE_ALLOCATION' FOR UPDATE",
            &[],
        )
        .map(|_| ())
        .map_err(|error| {
            crate::db::error::DbError::Query(format!(
                "failed to lock card range allocation: {error}"
            ))
        })
}

fn card_range_overlaps_on_connection(
    connection: &oracle::Connection,
    start_card_number: &str,
    end_card_number: &str,
) -> crate::db::error::DbResult<bool> {
    let count: i64 = connection
        .query_row_as(
            r#"
            SELECT COUNT(*)
            FROM card_ranges
            WHERE start_card_number <= :1
              AND end_card_number >= :2
            "#,
            &[&end_card_number, &start_card_number],
        )
        .map_err(|error| {
            crate::db::error::DbError::Query(format!("failed to check card range overlap: {error}"))
        })?;

    Ok(count > 0)
}

fn card_range_select_sql(tail: &str) -> String {
    format!(
        r#"
        SELECT
            cr.card_range_id,
            cr.start_card_number,
            cr.end_card_number,
            cr.funding_mode,
            cr.status,
            JSON_SERIALIZE(cr.metadata_json RETURNING CLOB) AS metadata_json,
            cr.created_by,
            cr.updated_by,
            TO_CHAR(SYS_EXTRACT_UTC(cr.created_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS created_at,
            TO_CHAR(SYS_EXTRACT_UTC(cr.updated_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS updated_at
        FROM card_ranges cr
        {tail}
        "#
    )
}

fn card_range_provider_select_sql(tail: &str) -> String {
    format!(
        r#"
        SELECT
            crp.card_range_id,
            crp.provider_id,
            crp.status,
            JSON_SERIALIZE(crp.metadata_json RETURNING CLOB) AS metadata_json,
            crp.created_by,
            crp.updated_by,
            TO_CHAR(SYS_EXTRACT_UTC(crp.created_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS created_at,
            TO_CHAR(SYS_EXTRACT_UTC(crp.updated_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS updated_at
        FROM card_range_providers crp
        {tail}
        "#
    )
}

fn fetch_card_range_provider(
    connection: &oracle::Connection,
    card_range_id: &[u8],
    provider_id: &[u8],
) -> crate::db::error::DbResult<CardRangeProvider> {
    let sql =
        card_range_provider_select_sql("WHERE crp.card_range_id = :1 AND crp.provider_id = :2");
    let row = connection
        .query_row(&sql, &[&card_range_id, &provider_id])
        .map_err(|error| {
            crate::db::error::DbError::Query(format!(
                "failed to fetch card range provider: {error}"
            ))
        })?;

    map_card_range_provider_row(&row).map_err(|error| {
        crate::db::error::DbError::Query(format!("invalid card range provider row: {error}"))
    })
}

fn map_card_range_row(row: &Row) -> Result<CardRange> {
    let id: Vec<u8> = row.get(0)?;
    let funding_mode: String = row.get(3)?;
    let status: String = row.get(4)?;
    let metadata: String = row.get(5)?;
    let created_at: String = row.get(8)?;
    let updated_at: String = row.get(9)?;

    Ok(CardRange {
        id: raw16_to_uuid(&id)?,
        start_card_number: row.get(1)?,
        end_card_number: row.get(2)?,
        funding_mode: FundingMode::from_db_value(&funding_mode)
            .ok_or_else(|| anyhow!("unknown funding mode `{funding_mode}`"))?,
        status: CardRangeStatus::from_db_value(&status)
            .ok_or_else(|| anyhow!("unknown card range status `{status}`"))?,
        metadata: serde_json::from_str(&metadata)?,
        created_by: row.get(6)?,
        updated_by: row.get(7)?,
        created_at: parse_utc(&created_at)?,
        updated_at: parse_utc(&updated_at)?,
    })
}

fn map_card_range_provider_row(row: &Row) -> Result<CardRangeProvider> {
    let card_range_id: Vec<u8> = row.get(0)?;
    let provider_id: Vec<u8> = row.get(1)?;
    let status: String = row.get(2)?;
    let metadata: String = row.get(3)?;
    let created_at: String = row.get(6)?;
    let updated_at: String = row.get(7)?;

    Ok(CardRangeProvider {
        card_range_id: raw16_to_uuid(&card_range_id)?,
        provider_id: raw16_to_uuid(&provider_id)?,
        status: CardRangeProviderStatus::from_db_value(&status)
            .ok_or_else(|| anyhow!("unknown card range provider status `{status}`"))?,
        metadata: serde_json::from_str(&metadata)?,
        created_by: row.get(4)?,
        updated_by: row.get(5)?,
        created_at: parse_utc(&created_at)?,
        updated_at: parse_utc(&updated_at)?,
    })
}

fn parse_utc(value: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)?.with_timezone(&Utc))
}
