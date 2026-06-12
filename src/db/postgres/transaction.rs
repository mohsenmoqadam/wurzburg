// src/db/postgres/transaction.rs
use async_trait::async_trait;
use uuid::Uuid;
use anyhow::Result;

use crate::db::traits::TransactionRepository;
use crate::db::postgres::PgRepository;
use crate::db::models::Transaction;

#[async_trait]
impl TransactionRepository for PgRepository {
    #[tracing::instrument(skip(self))]
    async fn get_transaction_by_idempotency_key(&self, key: &str) -> Result<Option<Transaction>> {
        let tx = sqlx::query_as::<_, Transaction>(
            "SELECT * FROM transactions WHERE idempotency_key = $1"
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;

        Ok(tx)
    }
    
    #[tracing::instrument(skip(self))]
    async fn execute_credit_transfer(
        &self,
        idempotency_key: String,
        amount: i64,
        provider_id: Uuid,
        user_id: Uuid,
        provider_ledger_id: Uuid,
        user_ledger_id: Uuid,
        actor_id: Uuid,
        description: Option<String>,
    ) -> Result<Transaction> {
        let mut tx = self.pool.begin().await?;

        sqlx::query("SELECT set_config('app.current_user_id', $1, true)")
            .bind(actor_id.to_string())
            .execute(&mut *tx)
            .await?;

        let transaction_id = Uuid::new_v4();
        let transaction = sqlx::query_as::<_, Transaction>(
            r#"
            INSERT INTO transactions (
                id, idempotency_key, transaction_type, amount,
                dr_account_id, cr_account_id, provider_id, user_id,
                status, description, processed_at
            )
            VALUES ($1, $2, 'CREDIT'::transaction_type_enum, $3, $4, $5, $6, $7, 'SUCCESS'::transaction_status_enum, $8, NOW())
            RETURNING *
            "#
        )
        .bind(transaction_id)
        .bind(&idempotency_key)
        .bind(amount)
        .bind(provider_ledger_id)
        .bind(user_ledger_id)
        .bind(provider_id)
        .bind(user_id)
        .bind(&description)
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(transaction)
    }

    #[tracing::instrument(skip(self))]
    async fn execute_debit_transfer(
        &self,
        idempotency_key: String,
        amount: i64,
        provider_id: Uuid,
        user_id: Uuid,
        provider_ledger_id: Uuid,
        user_ledger_id: Uuid,
        actor_id: Uuid,
        description: Option<String>,
    ) -> Result<Transaction> {
        let mut tx = self.pool.begin().await?;

        sqlx::query("SELECT set_config('app.current_user_id', $1, true)")
            .bind(actor_id.to_string())
            .execute(&mut *tx)
            .await?;

        let transaction_id = Uuid::new_v4();
        let transaction = sqlx::query_as::<_, Transaction>(
            r#"
            INSERT INTO transactions (
                id, idempotency_key, transaction_type, amount,
                dr_account_id, cr_account_id, provider_id, user_id,
                status, description, processed_at
            )
            VALUES ($1, $2, 'DEBIT'::transaction_type_enum, $3, $4, $5, $6, $7, 'SUCCESS'::transaction_status_enum, $8, NOW())
            RETURNING *
            "#
        )
        .bind(transaction_id)
        .bind(&idempotency_key)
        .bind(amount)
        .bind(user_ledger_id)
        .bind(provider_ledger_id)
        .bind(provider_id)
        .bind(user_id)
        .bind(&description)
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(transaction)
    }

    #[tracing::instrument(skip(self))]
    async fn get_provider_transactions(
        &self,
        provider_id: Uuid,
        limit: i64,
        offset: i64,
    ) -> Result<(Vec<crate::db::models::Transaction>, i64), sqlx::Error> {
        let count_row: (i64,) = sqlx::query_as(
            "SELECT count(*) FROM transactions WHERE provider_id = $1"
        )
        .bind(provider_id)
        .fetch_one(&self.pool)
        .await?;

        let transactions = sqlx::query_as!(
            crate::db::models::Transaction,
            r#"
            SELECT 
                id, idempotency_key, transaction_type as "transaction_type: _", 
                amount, currency as "currency: _", dr_account_id, cr_account_id, 
                user_id, provider_id, status as "status: _", parent_transaction_id, 
                external_reference_id, description, metadata, created_at, processed_at 
            FROM transactions 
            WHERE provider_id = $1 
            ORDER BY created_at DESC LIMIT $2 OFFSET $3
            "#,
            provider_id,
            limit,
            offset
        )
        .fetch_all(&self.pool)
        .await?;

        Ok((transactions, count_row.0))
    }

    #[tracing::instrument(skip(self))]
    async fn get_user_transactions(
        &self,
        provider_id: Uuid,
        user_id: Uuid,
        limit: i64,
        offset: i64,
    ) -> Result<(Vec<crate::db::models::Transaction>, i64), sqlx::Error> {
        let count_row: (i64,) = sqlx::query_as(
            "SELECT count(*) FROM transactions WHERE provider_id = $1 AND user_id = $2"
        )
        .bind(provider_id)
        .bind(user_id)
        .fetch_one(&self.pool)
        .await?;

        let transactions = sqlx::query_as!(
            crate::db::models::Transaction,
            r#"
            SELECT 
                id, idempotency_key, transaction_type as "transaction_type: _", 
                amount, currency as "currency: _", dr_account_id, cr_account_id, 
                user_id, provider_id, status as "status: _", parent_transaction_id, 
                external_reference_id, description, metadata, created_at, processed_at 
            FROM transactions 
            WHERE provider_id = $1 AND user_id = $2 
            ORDER BY created_at DESC LIMIT $3 OFFSET $4
            "#,
            provider_id,
            user_id,
            limit,
            offset
        )
        .fetch_all(&self.pool)
        .await?;

        Ok((transactions, count_row.0))
    }
}
