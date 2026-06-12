// src/db/postgres/priority.rs
use async_trait::async_trait;
use uuid::Uuid;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};

use crate::db::traits::{PriorityRepository};
use crate::db::postgres::PgRepository;
use crate::db::models::{UserPriorityConfig, UserPriorityItem, UserPriorityDetails, PriorityItemData};

const ITEMS_JOIN_QUERY: &str = r#"
    SELECT 
        upi.id, 
        upi.config_id, 
        upi.provider_id, 
        upi.priority_order, 
        upi.usage_type, 
        upi.max_amount, 
        upi.created_at, 
        upi.updated_at,
        upi.ledger_account_id AS user_ledger_account_id,
        p.ledger_account_id AS provider_ledger_account_id,
        p.fee_rate_bps,
        p.fixed_fee_amount
    FROM user_priority_items upi
    JOIN providers p ON upi.provider_id = p.id
    WHERE upi.config_id = $1 AND upi.is_deleted = FALSE
    ORDER BY upi.priority_order ASC
"#;

#[async_trait]
impl PriorityRepository for PgRepository {
    #[tracing::instrument(skip(self))]
    async fn get_priority_config_by_idempotency_key(&self, key: &str) -> Result<Option<UserPriorityDetails>> {
        let config = sqlx::query_as::<_, UserPriorityConfig>(
            "SELECT * FROM user_priority_configs WHERE idempotency_key = $1 AND is_deleted = FALSE"
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(config) = config {
            let items = sqlx::query_as::<_, UserPriorityItem>(ITEMS_JOIN_QUERY)
            .bind(config.id)
            .fetch_all(&self.pool)
            .await?;

            Ok(Some(UserPriorityDetails { config, items }))
        } else {
            Ok(None)
        }
    }
    
    #[tracing::instrument(skip(self))]
    async fn create_priority_config(
        &self,
        idempotency_key: String,
        user_id: Uuid,
        items: Vec<PriorityItemData>,
        expires_at: Option<DateTime<Utc>>,
        actor_id: Uuid,
    ) -> Result<UserPriorityDetails> {
        let mut tx = self.pool.begin().await.context("Failed to begin transaction")?;

        // Set actor for audit trigger
        sqlx::query("SELECT set_config('app.current_user_id', $1, true)")
            .bind(actor_id.to_string())
            .execute(&mut *tx)
            .await
            .context("Failed to set actor_id for audit log")?;

        // Step 1: Cancel any existing active configuration for this user
        sqlx::query(
            "UPDATE user_priority_configs SET status = 'CANCELLED', updated_at = NOW() WHERE user_id = $1 AND status = 'ACTIVE' AND is_deleted = FALSE"
        )
        .bind(user_id)
        .execute(&mut *tx)
        .await
        .context("Failed to cancel existing active priority config")?;

        // Step 2: Create the new priority config (master record)
        let new_config = sqlx::query_as::<_, UserPriorityConfig>(
            r#"
            INSERT INTO user_priority_configs (user_id, idempotency_key, status, expires_at)
            VALUES ($1, $2, 'ACTIVE', $3)
            RETURNING *
            "#
        )
        .bind(user_id)
        .bind(idempotency_key)
        .bind(expires_at)
        .fetch_one(&mut *tx)
        .await
        .context("Failed to create new priority config")?;

        // Step 3: Create the priority items (WITHOUT ledger_account_id)
        for (index, item_data) in items.into_iter().enumerate() {
            let priority_order = (index + 1) as i32;

            sqlx::query(
                r#"
                INSERT INTO user_priority_items (
                    config_id, provider_id, ledger_account_id, priority_order, usage_type, max_amount
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                "#
            )
            .bind(new_config.id)
            .bind(item_data.provider_id)
            .bind(item_data.user_ledger_account_id)
            .bind(priority_order)
            .bind(item_data.usage_type)
            .bind(item_data.max_amount)
            .execute(&mut *tx)
            .await
            .context(format!("Failed to create priority item for provider {}", item_data.provider_id))?;
        }

        // Step 4: Fetch created items dynamically with Ledger IDs via JOIN
        let created_items = sqlx::query_as::<_, UserPriorityItem>(ITEMS_JOIN_QUERY)
            .bind(new_config.id)
            .fetch_all(&mut *tx)
            .await
            .context("Failed to fetch created priority items with ledger IDs")?;

        tx.commit().await.context("Failed to commit transaction")?;

        Ok(UserPriorityDetails {
            config: new_config,
            items: created_items,
        })
    }

    #[tracing::instrument(skip(self))]
    async fn get_active_priority_config(&self, user_id: Uuid) -> Result<Option<UserPriorityDetails>> {
        let config = sqlx::query_as::<_, UserPriorityConfig>(
            "SELECT * FROM user_priority_configs WHERE user_id = $1 AND status = 'ACTIVE' AND is_deleted = FALSE"
        )
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(config) = config {
            let items = sqlx::query_as::<_, UserPriorityItem>(ITEMS_JOIN_QUERY)
            .bind(config.id)
            .fetch_all(&self.pool)
            .await?;
            
            Ok(Some(UserPriorityDetails { config, items }))
        } else {
            Ok(None)
        }
    }

    #[tracing::instrument(skip(self))]
    async fn cancel_active_priority_config(&self, user_id: Uuid, actor_id: Uuid) -> Result<Option<UserPriorityDetails>> {
        let mut tx = self.pool.begin().await?;

        sqlx::query("SELECT set_config('app.current_user_id', $1, true)")
            .bind(actor_id.to_string())
            .execute(&mut *tx)
            .await?;

        let updated_config = sqlx::query_as::<_, UserPriorityConfig>(
            r#"
            UPDATE user_priority_configs
            SET status = 'CANCELLED', updated_at = NOW()
            WHERE user_id = $1 AND status = 'ACTIVE' AND is_deleted = FALSE
            RETURNING *
            "#
        )
        .bind(user_id)
        .fetch_optional(&mut *tx)
        .await?;

        if let Some(config) = updated_config {
             let items = sqlx::query_as::<_, UserPriorityItem>(ITEMS_JOIN_QUERY)
            .bind(config.id)
            .fetch_all(&mut *tx)
            .await?;

            tx.commit().await?;

            Ok(Some(UserPriorityDetails { config, items }))
        } else {
            tx.rollback().await?;
            Ok(None)
        }
    }

    #[tracing::instrument(skip(self))]
    async fn soft_delete_priority_config(&self, config_id: Uuid) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        sqlx::query!(
            r#"UPDATE user_priority_items SET is_deleted = TRUE, deleted_at = NOW() WHERE config_id = $1"#,
            config_id
        )
        .execute(&mut *tx)
        .await?;

        sqlx::query!(
            r#"UPDATE user_priority_configs SET is_deleted = TRUE, deleted_at = NOW() WHERE id = $1"#,
            config_id
        )
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn get_all_priority_configs(&self, user_id: Uuid) -> Result<Vec<UserPriorityDetails>> {
        let configs = sqlx::query_as::<_, UserPriorityConfig>(
            "SELECT * FROM user_priority_configs WHERE user_id = $1 AND is_deleted = FALSE ORDER BY created_at DESC"
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;

        let mut details_list = Vec::with_capacity(configs.len());

        for config in configs {
            let items = sqlx::query_as::<_, UserPriorityItem>(ITEMS_JOIN_QUERY)
            .bind(config.id)
            .fetch_all(&self.pool)
            .await?;

            details_list.push(UserPriorityDetails { config, items });
        }

        Ok(details_list)
    }
}

/*
// src/db/postgres/priority.rs
use async_trait::async_trait;
use uuid::Uuid;
use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};

use crate::db::traits::{PriorityRepository};
use crate::db::postgres::PgRepository;
use crate::db::models::{UserPriorityConfig, UserPriorityItem, UserPriorityDetails, PriorityItemData};

#[async_trait]
impl PriorityRepository for PgRepository {
    #[tracing::instrument(skip(self))]
    async fn get_priority_config_by_idempotency_key(&self, key: &str) -> Result<Option<UserPriorityDetails>> {
        let config = sqlx::query_as::<_, UserPriorityConfig>(
            "SELECT * FROM user_priority_configs WHERE idempotency_key = $1 AND is_deleted = FALSE"
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(config) = config {
            let items = sqlx::query_as::<_, UserPriorityItem>(
                "SELECT * FROM user_priority_items WHERE config_id = $1 AND is_deleted = FALSE ORDER BY priority_order ASC"
            )
            .bind(config.id)
            .fetch_all(&self.pool)
            .await?;

            Ok(Some(UserPriorityDetails { config, items }))
        } else {
            Ok(None)
        }
    }
    
    #[tracing::instrument(skip(self))]
    async fn create_priority_config(
        &self,
        idempotency_key: String,
        user_id: Uuid,
        items: Vec<PriorityItemData>,
        expires_at: Option<DateTime<Utc>>,
        actor_id: Uuid,
    ) -> Result<UserPriorityDetails> {
        let mut tx = self.pool.begin().await.context("Failed to begin transaction")?;

        // Set actor for audit trigger
        sqlx::query("SELECT set_config('app.current_user_id', $1, true)")
            .bind(actor_id.to_string())
            .execute(&mut *tx)
            .await
            .context("Failed to set actor_id for audit log")?;

        // Step 1: Cancel any existing active configuration for this user
        sqlx::query(
            "UPDATE user_priority_configs SET status = 'CANCELLED', updated_at = NOW() WHERE user_id = $1 AND status = 'ACTIVE' AND is_deleted = FALSE"
        )
        .bind(user_id)
        .execute(&mut *tx)
        .await
        .context("Failed to cancel existing active priority config")?;

        // Step 2: Create the new priority config (master record)
        let new_config = sqlx::query_as::<_, UserPriorityConfig>(
            r#"
            INSERT INTO user_priority_configs (user_id, idempotency_key, status, expires_at)
            VALUES ($1, $2, 'ACTIVE', $3)
            RETURNING *
            "#
        )
        .bind(user_id)
        .bind(idempotency_key)
        .bind(expires_at)
        .fetch_one(&mut *tx)
        .await
        .context("Failed to create new priority config (possibly duplicate idempotency_key)")?;

        // Step 3: Create the priority items (detail records)
        let mut created_items = Vec::with_capacity(items.len());
        for (index, item_data) in items.into_iter().enumerate() {
            let priority_order = (index + 1) as i32;

            // Fetch the corresponding ledger_account_id for the user/provider pair
            let account: (Uuid,) = sqlx::query_as(
                "SELECT ledger_account_id FROM user_accounts WHERE user_id = $1 AND provider_id = $2"
            )
            .bind(user_id)
            .bind(item_data.provider_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| anyhow!("User account not found for user_id {} and provider_id {}", user_id, item_data.provider_id))?;

            let ledger_account_id = account.0;

            let new_item = sqlx::query_as::<_, UserPriorityItem>(
                r#"
                INSERT INTO user_priority_items (
                    config_id, provider_id, ledger_account_id, priority_order,
                    usage_type, max_amount
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING *
                "#
            )
            .bind(new_config.id)
            .bind(item_data.provider_id)
            .bind(ledger_account_id)
            .bind(priority_order)
            .bind(item_data.usage_type)
            .bind(item_data.max_amount)
            .fetch_one(&mut *tx)
            .await
            .context(format!("Failed to create priority item for provider {}", item_data.provider_id))?;

            created_items.push(new_item);
        }

        tx.commit().await.context("Failed to commit transaction")?;

        Ok(UserPriorityDetails {
            config: new_config,
            items: created_items,
        })
    }

    #[tracing::instrument(skip(self))]
    async fn get_active_priority_config(&self, user_id: Uuid) -> Result<Option<UserPriorityDetails>> {
        // Find the active config first
        let config = sqlx::query_as::<_, UserPriorityConfig>(
            "SELECT * FROM user_priority_configs WHERE user_id = $1 AND status = 'ACTIVE' AND is_deleted = FALSE"
        )
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(config) = config {
            // If found, fetch its items
            let items = sqlx::query_as::<_, UserPriorityItem>(
                "SELECT * FROM user_priority_items WHERE config_id = $1 AND is_deleted = FALSE ORDER BY priority_order ASC"
            )
            .bind(config.id)
            .fetch_all(&self.pool)
            .await?;
            
            Ok(Some(UserPriorityDetails { config, items }))
        } else {
            Ok(None)
        }
    }

    #[tracing::instrument(skip(self))]
    async fn cancel_active_priority_config(&self, user_id: Uuid, actor_id: Uuid) -> Result<Option<UserPriorityDetails>> {
        let mut tx = self.pool.begin().await?;

        sqlx::query("SELECT set_config('app.current_user_id', $1, true)")
            .bind(actor_id.to_string())
            .execute(&mut *tx)
            .await?;

        // Find and update the active config atomically
        let updated_config = sqlx::query_as::<_, UserPriorityConfig>(
            r#"
            UPDATE user_priority_configs
            SET status = 'CANCELLED', updated_at = NOW()
            WHERE user_id = $1 AND status = 'ACTIVE' AND is_deleted = FALSE
            RETURNING *
            "#
        )
        .bind(user_id)
        .fetch_optional(&mut *tx)
        .await?;

        if let Some(config) = updated_config {
             let items = sqlx::query_as::<_, UserPriorityItem>(
                "SELECT * FROM user_priority_items WHERE config_id = $1 AND is_deleted = FALSE ORDER BY priority_order ASC"
            )
            .bind(config.id)
            .fetch_all(&mut *tx)
            .await?;

            tx.commit().await?;

            Ok(Some(UserPriorityDetails { config, items }))
        } else {
            tx.rollback().await?;
            Ok(None)
        }
    }

    #[tracing::instrument(skip(self))]
    async fn soft_delete_priority_config(&self, config_id: Uuid) -> Result<()> {
    let mut tx = self.pool.begin().await?;

    // 1. Soft delete items
    sqlx::query!(
        r#"UPDATE user_priority_items SET is_deleted = TRUE, deleted_at = NOW() WHERE config_id = $1"#,
        config_id
    )
    .execute(&mut *tx)
    .await?;

    // 2. Soft delete config
    sqlx::query!(
        r#"UPDATE user_priority_configs SET is_deleted = TRUE, deleted_at = NOW() WHERE id = $1"#,
        config_id
    )
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    Ok(())
}

    #[tracing::instrument(skip(self))]
    async fn get_all_priority_configs(&self, user_id: Uuid) -> Result<Vec<UserPriorityDetails>> {
        let configs = sqlx::query_as::<_, UserPriorityConfig>(
            "SELECT * FROM user_priority_configs WHERE user_id = $1 AND is_deleted = FALSE ORDER BY created_at DESC"
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;

        let mut details_list = Vec::with_capacity(configs.len());

        for config in configs {
            let items = sqlx::query_as::<_, UserPriorityItem>(
                "SELECT * FROM user_priority_items WHERE config_id = $1 AND is_deleted = FALSE ORDER BY priority_order ASC"
            )
            .bind(config.id)
            .fetch_all(&self.pool)
            .await?;

            details_list.push(UserPriorityDetails { config, items });
        }

        Ok(details_list)
    }
}
*/
