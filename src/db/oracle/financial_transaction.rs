use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::{
    db::{
        error::{DbError, DbResult},
        oracle::{OracleRepository, types::raw16_to_uuid},
    },
    domain::{
        financial_transaction::{
            FinancialAccountCategory, FinancialEntryDirection, FinancialTransaction,
            FinancialTransactionCursor, FinancialTransactionEntry, FinancialTransactionPage,
            FinancialTransactionQuery, FinancialTransactionType, TransactionVisibility,
        },
        user_card::mask_card_number,
    },
};

impl OracleRepository {
    #[tracing::instrument(
        skip(self, query),
        fields(db.system = "oracle", db.operation.name = "financial_transactions.list")
    )]
    pub async fn list_financial_transactions(
        &self,
        query: FinancialTransactionQuery,
    ) -> DbResult<FinancialTransactionPage> {
        self.pool
            .with_connection(move |connection| list(connection, query))
            .await
    }
}

fn list(
    connection: &oracle::Connection,
    query: FinancialTransactionQuery,
) -> DbResult<FinancialTransactionPage> {
    let visibility = VisibilityBinds::from_visibility(&query.visibility);
    let transaction_type = query
        .transaction_type
        .map(|value| value.as_db_value().to_string());
    let occurred_from = query.occurred_from.map(format_oracle_utc);
    let occurred_to = query.occurred_to.map(format_oracle_utc);
    let cursor_occurred_at = query
        .cursor
        .as_ref()
        .map(|value| format_oracle_utc(value.occurred_at));
    let cursor_transaction_id = query
        .cursor
        .as_ref()
        .map(|value| value.transaction_id.as_bytes().to_vec());
    let fetch_limit = i64::from(query.limit) + 1;
    let binds: &[(&str, &dyn oracle::sql_type::ToSql)] = &[
        ("visibility_mode", &visibility.mode),
        ("provider_id", &visibility.provider_id),
        ("user_id", &visibility.user_id),
        ("card_number", &visibility.card_number),
        ("account_category", &visibility.account_category),
        ("transaction_type", &transaction_type),
        ("occurred_from", &occurred_from),
        ("occurred_to", &occurred_to),
        ("cursor_occurred_at", &cursor_occurred_at),
        ("cursor_transaction_id", &cursor_transaction_id),
        ("fetch_limit", &fetch_limit),
    ];
    let rows = connection
        .query_named(list_sql(), binds)
        .map_err(|error| query_error("failed to list financial transactions", error))?;

    let mut items: Vec<FinancialTransaction> = Vec::new();
    let mut has_next = false;
    for row in rows {
        let row = row.map_err(read_error)?;
        let transaction_id = row_uuid(&row, 0)?;
        if items.last().map(|item| item.transaction_id) != Some(transaction_id) {
            if items.len() == usize::from(query.limit) {
                has_next = true;
                break;
            }
            items.push(map_transaction(&row, transaction_id)?);
        }
        let entry = map_entry(&row)?;
        items
            .last_mut()
            .expect("a transaction is inserted before its entry")
            .entries
            .push(entry);
    }

    let next_cursor = has_next.then(|| {
        let item = items
            .last()
            .expect("a next transaction page follows a non-empty current page");
        FinancialTransactionCursor {
            occurred_at: item.occurred_at,
            transaction_id: item.transaction_id,
        }
    });
    Ok(FinancialTransactionPage { items, next_cursor })
}

struct VisibilityBinds {
    mode: String,
    provider_id: Option<Vec<u8>>,
    user_id: Option<Vec<u8>>,
    card_number: Option<String>,
    account_category: Option<String>,
}

impl VisibilityBinds {
    fn from_visibility(visibility: &TransactionVisibility) -> Self {
        match visibility {
            TransactionVisibility::Provider {
                provider_id,
                user_id,
                card_number,
                account_category,
            } => Self {
                mode: "PROVIDER".to_string(),
                provider_id: Some(provider_id.as_bytes().to_vec()),
                user_id: user_id.map(|value| value.as_bytes().to_vec()),
                card_number: card_number.clone(),
                account_category: account_category.map(|value| value.as_db_value().to_string()),
            },
            TransactionVisibility::Cardholder {
                user_id,
                card_number,
            } => Self {
                mode: "CARDHOLDER".to_string(),
                provider_id: None,
                user_id: Some(user_id.as_bytes().to_vec()),
                card_number: card_number.clone(),
                account_category: None,
            },
            TransactionVisibility::Platform {
                provider_id,
                user_id,
                card_number,
            } => Self {
                mode: "PLATFORM".to_string(),
                provider_id: provider_id.map(|value| value.as_bytes().to_vec()),
                user_id: user_id.map(|value| value.as_bytes().to_vec()),
                card_number: card_number.clone(),
                account_category: None,
            },
        }
    }
}

fn map_transaction(row: &oracle::Row, transaction_id: Uuid) -> DbResult<FinancialTransaction> {
    let transaction_type: String = row.get(1).map_err(read_error)?;
    let original_transaction_id: Option<Vec<u8>> = row.get(10).map_err(read_error)?;
    let card_number: String = row.get(6).map_err(read_error)?;
    Ok(FinancialTransaction {
        transaction_id,
        transaction_type: FinancialTransactionType::from_db_value(&transaction_type)
            .ok_or_else(|| DbError::Query("unknown financial transaction type".to_string()))?,
        source_system: row.get(2).map_err(read_error)?,
        status: row.get(3).map_err(read_error)?,
        user_id: row_uuid(row, 4)?,
        card_id: row_uuid(row, 5)?,
        masked_card_number: mask_card_number(&card_number),
        amount_rials: row.get(7).map_err(read_error)?,
        currency: row.get(8).map_err(read_error)?,
        reference: row.get(9).map_err(read_error)?,
        original_transaction_id: original_transaction_id
            .as_deref()
            .map(raw16_to_uuid)
            .transpose()?,
        entries: Vec::new(),
        occurred_at: parse_time(row.get::<_, String>(11).map_err(read_error)?)?,
        recorded_at: parse_time(row.get::<_, String>(12).map_err(read_error)?)?,
    })
}

fn map_entry(row: &oracle::Row) -> DbResult<FinancialTransactionEntry> {
    let account_category: String = row.get(14).map_err(read_error)?;
    let direction: String = row.get(15).map_err(read_error)?;
    Ok(FinancialTransactionEntry {
        provider_id: row_uuid(row, 13)?,
        account_category: FinancialAccountCategory::from_db_value(&account_category)
            .ok_or_else(|| DbError::Query("unknown financial account category".to_string()))?,
        direction: FinancialEntryDirection::from_db_value(&direction)
            .ok_or_else(|| DbError::Query("unknown financial entry direction".to_string()))?,
        entry_role: row.get(16).map_err(read_error)?,
        amount_rials: row.get(17).map_err(read_error)?,
    })
}

fn row_uuid(row: &oracle::Row, index: usize) -> DbResult<Uuid> {
    let value: Vec<u8> = row.get(index).map_err(read_error)?;
    raw16_to_uuid(&value)
}

fn parse_time(value: String) -> DbResult<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(&value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|error| {
            DbError::Query(format!("invalid financial transaction timestamp: {error}"))
        })
}

fn format_oracle_utc(value: DateTime<Utc>) -> String {
    value.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

fn read_error(error: oracle::Error) -> DbError {
    DbError::Query(format!("failed to map financial transaction row: {error}"))
}

fn query_error(context: &str, error: oracle::Error) -> DbError {
    DbError::Query(format!("{context}: {error}"))
}

fn list_sql() -> &'static str {
    r#"
    WITH selected_transactions AS (
        SELECT t.transaction_id, t.transaction_type, t.source_system, t.status,
               t.user_id, t.card_id, c.card_number,
               TO_CHAR(t.amount_rials, 'TM9', 'NLS_NUMERIC_CHARACTERS=''.,''') amount_rials,
               t.currency,
               CASE WHEN :visibility_mode = 'CARDHOLDER' THEN NULL ELSE t.external_reference END external_reference,
               t.original_transaction_id, t.occurred_at, t.recorded_at
          FROM financial_transactions t
          JOIN cards c ON c.card_id = t.card_id
         WHERE (
             (:visibility_mode = 'PROVIDER' AND EXISTS (
                 SELECT 1 FROM financial_transaction_entries ve
                  WHERE ve.transaction_id = t.transaction_id
                    AND ve.provider_id = :provider_id
                    AND (:account_category IS NULL OR ve.account_category = :account_category)
             ))
             OR (:visibility_mode = 'CARDHOLDER' AND t.user_id = :user_id)
             OR (:visibility_mode = 'PLATFORM' AND (
                 :provider_id IS NULL OR EXISTS (
                     SELECT 1 FROM financial_transaction_entries ve
                      WHERE ve.transaction_id = t.transaction_id AND ve.provider_id = :provider_id
                 )
             ))
         )
           AND (:user_id IS NULL OR t.user_id = :user_id)
           AND (:card_number IS NULL OR c.card_number = :card_number)
           AND (:transaction_type IS NULL OR t.transaction_type = :transaction_type)
           AND (:occurred_from IS NULL OR SYS_EXTRACT_UTC(t.occurred_at) >= TO_TIMESTAMP(:occurred_from, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'))
           AND (:occurred_to IS NULL OR SYS_EXTRACT_UTC(t.occurred_at) < TO_TIMESTAMP(:occurred_to, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'))
           AND (
               :cursor_occurred_at IS NULL
               OR SYS_EXTRACT_UTC(t.occurred_at) < TO_TIMESTAMP(:cursor_occurred_at, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')
               OR (
                   SYS_EXTRACT_UTC(t.occurred_at) = TO_TIMESTAMP(:cursor_occurred_at, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')
                   AND t.transaction_id < :cursor_transaction_id
               )
           )
         ORDER BY t.occurred_at DESC, t.transaction_id DESC
         FETCH FIRST :fetch_limit ROWS ONLY
    )
    SELECT st.transaction_id, st.transaction_type, st.source_system, st.status,
           st.user_id, st.card_id, st.card_number, st.amount_rials, st.currency,
           st.external_reference, st.original_transaction_id,
           TO_CHAR(SYS_EXTRACT_UTC(st.occurred_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'),
           TO_CHAR(SYS_EXTRACT_UTC(st.recorded_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'),
           e.provider_id, e.account_category, e.direction, e.entry_role,
           TO_CHAR(e.amount_rials, 'TM9', 'NLS_NUMERIC_CHARACTERS=''.,''') entry_amount_rials
      FROM selected_transactions st
      JOIN financial_transaction_entries e ON e.transaction_id = st.transaction_id
     WHERE :visibility_mode <> 'PROVIDER'
        OR (e.provider_id = :provider_id
            AND (:account_category IS NULL OR e.account_category = :account_category))
     ORDER BY st.occurred_at DESC, st.transaction_id DESC, e.entry_sequence ASC
    "#
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_uses_keyset_pagination_and_entry_level_provider_visibility() {
        let sql = list_sql();
        assert!(sql.contains("t.transaction_id < :cursor_transaction_id"));
        assert!(sql.contains("e.provider_id = :provider_id"));
        assert!(sql.contains("FETCH FIRST :fetch_limit ROWS ONLY"));
        assert!(!sql.contains("OFFSET"));
    }
}
