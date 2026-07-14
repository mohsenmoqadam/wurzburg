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
        traits::CardPolicyRepository,
    },
    domain::card_policy::{
        CardPolicyProfile, CardPolicyProfileStatus, CardRangePolicyAssignment,
        CardRangePolicyAssignmentStatus, CardRangePolicyDetails, NewCardRangePolicy,
    },
};

#[async_trait]
impl CardPolicyRepository for OracleRepository {
    async fn create_card_range_policy(
        &self,
        policy: NewCardRangePolicy,
    ) -> Result<CardRangePolicyDetails> {
        self.pool
            .with_transaction("card range policy replacement", move |connection| {
                let card_range_id = uuid_to_raw16(policy.card_range_id).to_vec();
                let profile_id = uuid_to_raw16(policy.profile_id).to_vec();
                let profile_json = policy.profile.to_string();
                let effective_at = policy
                    .effective_at
                    .format("%Y-%m-%dT%H:%M:%S%.6f%:z")
                    .to_string();

                lock_card_range(connection, &card_range_id)?;
                let next_version = next_policy_version(connection, &card_range_id)?;

                connection
                    .execute(
                        r#"
                        INSERT INTO card_policy_profiles (
                            card_policy_profile_id, card_range_id, profile_json, status,
                            version, effective_at, created_by
                        )
                        VALUES (
                            :1, :2, :3, 'ACTIVE', :4,
                            TO_TIMESTAMP_TZ(:5, 'YYYY-MM-DD"T"HH24:MI:SS.FF6TZH:TZM'),
                            :6
                        )
                        "#,
                        &[
                            &profile_id,
                            &card_range_id,
                            &profile_json,
                            &next_version,
                            &effective_at,
                            &policy.actor_subject,
                        ],
                    )
                    .map_err(|error| {
                        crate::db::error::DbError::Query(format!(
                            "failed to insert card policy profile: {error}"
                        ))
                    })?;

                connection
                    .execute(
                        r#"
                        UPDATE card_range_policy_assignments
                        SET status = 'SUPERSEDED',
                            superseded_at = SYSTIMESTAMP
                        WHERE card_range_id = :1
                          AND status = 'ACTIVE'
                        "#,
                        &[&card_range_id],
                    )
                    .map_err(|error| {
                        crate::db::error::DbError::Query(format!(
                            "failed to supersede active card range policy assignment: {error}"
                        ))
                    })?;

                connection
                    .execute(
                        r#"
                        UPDATE card_policy_profiles
                        SET status = 'SUPERSEDED',
                            superseded_by_profile_id = :1
                        WHERE card_range_id = :2
                          AND status = 'ACTIVE'
                          AND card_policy_profile_id <> :1
                        "#,
                        &[&profile_id, &card_range_id],
                    )
                    .map_err(|error| {
                        crate::db::error::DbError::Query(format!(
                            "failed to supersede previous card policy profile: {error}"
                        ))
                    })?;

                connection
                    .execute(
                        r#"
                        INSERT INTO card_range_policy_assignments (
                            card_range_id, card_policy_profile_id, status, assigned_by
                        )
                        VALUES (:1, :2, 'ACTIVE', :3)
                        "#,
                        &[&card_range_id, &profile_id, &policy.actor_subject],
                    )
                    .map_err(|error| {
                        crate::db::error::DbError::Query(format!(
                            "failed to insert card range policy assignment: {error}"
                        ))
                    })?;

                let details =
                    fetch_active_policy(connection, &card_range_id)?.ok_or_else(|| {
                        crate::db::error::DbError::Query(
                            "inserted card range policy was not found".to_string(),
                        )
                    })?;

                Ok(details)
            })
            .await
            .map_err(Into::into)
    }

    async fn get_active_card_range_policy(
        &self,
        card_range_id: Uuid,
    ) -> Result<Option<CardRangePolicyDetails>> {
        self.pool
            .with_connection(move |connection| {
                let card_range_id = uuid_to_raw16(card_range_id).to_vec();
                fetch_active_policy(connection, &card_range_id)
            })
            .await
            .map_err(Into::into)
    }
}

fn lock_card_range(
    connection: &oracle::Connection,
    card_range_id: &[u8],
) -> crate::db::error::DbResult<()> {
    connection
        .query_row(
            "SELECT card_range_id FROM card_ranges WHERE card_range_id = :1 FOR UPDATE",
            &[&card_range_id],
        )
        .map(|_| ())
        .map_err(|error| {
            crate::db::error::DbError::Query(format!(
                "failed to lock card range for policy update: {error}"
            ))
        })
}

fn next_policy_version(
    connection: &oracle::Connection,
    card_range_id: &[u8],
) -> crate::db::error::DbResult<i64> {
    connection
        .query_row_as(
            r#"
            SELECT COALESCE(MAX(version), 0) + 1
            FROM card_policy_profiles
            WHERE card_range_id = :1
            "#,
            &[&card_range_id],
        )
        .map_err(|error| {
            crate::db::error::DbError::Query(format!(
                "failed to compute next card range policy version: {error}"
            ))
        })
}

fn fetch_active_policy(
    connection: &oracle::Connection,
    card_range_id: &[u8],
) -> crate::db::error::DbResult<Option<CardRangePolicyDetails>> {
    match connection.query_row(policy_select_sql(), &[&card_range_id]) {
        Ok(row) => map_policy_details_row(&row).map(Some).map_err(|error| {
            crate::db::error::DbError::Query(format!("invalid card range policy row: {error}"))
        }),
        Err(error) if error.kind() == ErrorKind::NoDataFound => Ok(None),
        Err(error) => Err(crate::db::error::DbError::Query(format!(
            "failed to fetch active card range policy: {error}"
        ))),
    }
}

fn policy_select_sql() -> &'static str {
    r#"
    SELECT
        cpp.card_policy_profile_id,
        cpp.card_range_id,
        JSON_SERIALIZE(cpp.profile_json RETURNING CLOB) AS profile_json,
        cpp.status,
        cpp.version,
        TO_CHAR(SYS_EXTRACT_UTC(cpp.effective_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS effective_at,
        cpp.superseded_by_profile_id,
        cpp.created_by,
        TO_CHAR(SYS_EXTRACT_UTC(cpp.created_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS created_at,
        crpa.card_range_id,
        crpa.card_policy_profile_id,
        crpa.status,
        crpa.assigned_by,
        TO_CHAR(SYS_EXTRACT_UTC(crpa.assigned_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS assigned_at,
        TO_CHAR(SYS_EXTRACT_UTC(crpa.superseded_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') AS superseded_at
    FROM card_range_policy_assignments crpa
    JOIN card_policy_profiles cpp
      ON cpp.card_policy_profile_id = crpa.card_policy_profile_id
    WHERE crpa.card_range_id = :1
      AND crpa.status = 'ACTIVE'
    "#
}

fn map_policy_details_row(row: &Row) -> Result<CardRangePolicyDetails> {
    let profile_id: Vec<u8> = row.get(0)?;
    let profile_card_range_id: Vec<u8> = row.get(1)?;
    let profile_json: String = row.get(2)?;
    let profile_status: String = row.get(3)?;
    let effective_at: String = row.get(5)?;
    let superseded_by_profile_id: Option<Vec<u8>> = row.get(6)?;
    let created_at: String = row.get(8)?;

    let assignment_card_range_id: Vec<u8> = row.get(9)?;
    let assignment_profile_id: Vec<u8> = row.get(10)?;
    let assignment_status: String = row.get(11)?;
    let assigned_at: String = row.get(13)?;
    let superseded_at: Option<String> = row.get(14)?;

    Ok(CardRangePolicyDetails {
        profile: CardPolicyProfile {
            id: raw16_to_uuid(&profile_id)?,
            card_range_id: raw16_to_uuid(&profile_card_range_id)?,
            profile: serde_json::from_str(&profile_json)?,
            status: CardPolicyProfileStatus::from_db_value(&profile_status)
                .ok_or_else(|| anyhow!("unknown card policy profile status `{profile_status}`"))?,
            version: row.get(4)?,
            effective_at: parse_utc(&effective_at)?,
            superseded_by_profile_id: superseded_by_profile_id
                .as_deref()
                .map(raw16_to_uuid)
                .transpose()?,
            created_by: row.get(7)?,
            created_at: parse_utc(&created_at)?,
        },
        assignment: CardRangePolicyAssignment {
            card_range_id: raw16_to_uuid(&assignment_card_range_id)?,
            card_policy_profile_id: raw16_to_uuid(&assignment_profile_id)?,
            status: CardRangePolicyAssignmentStatus::from_db_value(&assignment_status).ok_or_else(
                || anyhow!("unknown card range policy assignment status `{assignment_status}`"),
            )?,
            assigned_by: row.get(12)?,
            assigned_at: parse_utc(&assigned_at)?,
            superseded_at: superseded_at.as_deref().map(parse_utc).transpose()?,
        },
    })
}

fn parse_utc(value: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)?.with_timezone(&Utc))
}
