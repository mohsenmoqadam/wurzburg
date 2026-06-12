// src/db/postgres/user.rs
use async_trait::async_trait;
use uuid::Uuid;
use anyhow::Result;
use sqlx::types::Json;

use crate::db::traits::UserRepository;
use crate::db::postgres::PgRepository;
use crate::db::models::{Provider, User, UserAccount, UserAccountProviderInfo, UserProviderDetails};

#[async_trait]
impl UserRepository for PgRepository {
    #[tracing::instrument(skip(self))]
    async fn create_or_link_user(
        &self,
        nid: String,
        provider_id: Uuid,
        internal_metadata: serde_json::Value,
        external_metadata: serde_json::Value,
        ledger_account_id: Uuid,
        actor_id: Uuid,
    ) -> Result<(User, UserAccount)> {
        let mut tx = self.pool.begin().await?;

        // Set the actor ID for the audit triggers
        sqlx::query("SELECT set_config('app.current_user_id', $1, true)")
            .bind(actor_id.to_string())
            .execute(&mut *tx)
            .await?;

        // 1. Find or create the user by NID
        let user_opt = sqlx::query_as::<_, User>(
            "SELECT * FROM users WHERE nid = $1"
        )
        .bind(&nid)
        .fetch_optional(&mut *tx)
        .await?;

        let user = match user_opt {
            Some(existing_user) => existing_user,
            None => {
                let new_user_id = Uuid::new_v4();
                sqlx::query_as::<_, User>(
                    r#"
                    INSERT INTO users (id, nid, internal_metadata) 
                    VALUES ($1, $2, $3) 
                    RETURNING *
                    "#
                )
                .bind(new_user_id)
                .bind(&nid)
                .bind(Json(internal_metadata))
                .fetch_one(&mut *tx)
                .await?
            }
        };

        // 2. Link the user to the provider (Upsert metadata if relation exists)
        sqlx::query(
            r#"
            INSERT INTO user_providers (user_id, provider_id, external_metadata) 
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id, provider_id) 
            DO UPDATE SET external_metadata = EXCLUDED.external_metadata, updated_at = NOW()
            "#
        )
        .bind(user.id)
        .bind(provider_id)
        .bind(Json(external_metadata))
        .execute(&mut *tx)
        .await?;

        // 3. Find or create the user account for this provider
        let account_opt = sqlx::query_as::<_, UserAccount>(
            "SELECT * FROM user_accounts WHERE user_id = $1 AND provider_id = $2"
        )
        .bind(user.id)
        .bind(provider_id)
        .fetch_optional(&mut *tx)
        .await?;

        let account = match account_opt {
            Some(existing_account) => existing_account,
            None => {
                let new_account_id = Uuid::new_v4();
                sqlx::query_as::<_, UserAccount>(
                    r#"
                    INSERT INTO user_accounts (id, user_id, provider_id, ledger_account_id) 
                    VALUES ($1, $2, $3, $4) 
                    RETURNING *
                    "#
                )
                .bind(new_account_id)
                .bind(user.id)
                .bind(provider_id)
                .bind(ledger_account_id)
                .fetch_one(&mut *tx)
                .await?
            }
        };

        tx.commit().await?;

        Ok((user, account))
    }

    #[tracing::instrument(skip(self))]
    async fn get_user_provider_details(
        &self,
        user_id: Uuid,
        provider_id: Uuid,
    ) -> Result<Option<UserProviderDetails>> {
        let details = sqlx::query_as!(
            UserProviderDetails,
            r#"
            SELECT
                u.id as user_id,
                u.nid as "nid!",
                u.internal_metadata,
                up.external_metadata,
                ua.ledger_account_id as user_ledger_account_id,
                p.ledger_account_id as provider_ledger_account_id,
                ua.status as "status!: _",
                u.created_at,
                u.updated_at
            FROM users u
            JOIN user_providers up ON u.id = up.user_id
            JOIN user_accounts ua ON u.id = ua.user_id
            JOIN providers p ON p.id = up.provider_id
            WHERE u.id = $1
              AND up.provider_id = $2
              AND ua.provider_id = $2
            "#,
            user_id,
            provider_id
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(details)
    }

    #[tracing::instrument(skip(self))]
    async fn get_user_providers(&self, user_id: Uuid) -> Result<Vec<Provider>> {
        let providers = sqlx::query_as::<_, Provider>(
            r#"
            SELECT p.* 
            FROM providers p
            JOIN user_providers up ON p.id = up.provider_id
            WHERE up.user_id = $1
            "#
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(providers)
    }

    #[tracing::instrument(skip(self))]
    async fn get_user_accounts_provider_info(&self, user_id: Uuid) -> Result<Vec<UserAccountProviderInfo>> {
        let rows = sqlx::query_as!(
            UserAccountProviderInfo,
            r#"
            SELECT 
                p.id as provider_id, 
                p.trade_name as provider_name, 
                ua.ledger_account_id
            FROM user_accounts ua
            JOIN providers p ON ua.provider_id = p.id
            WHERE ua.user_id = $1
            "#,
            user_id
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }
}
