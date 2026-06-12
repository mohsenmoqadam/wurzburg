// src/db/postgres/provider.rs
use async_trait::async_trait;
use uuid::Uuid;
use anyhow::Result;

use crate::db::traits::ProviderRepository;
use crate::db::postgres::PgRepository;
use crate::db::models::Provider;

#[async_trait]
impl ProviderRepository for PgRepository {
    #[tracing::instrument(skip(self))]
    async fn get_provider_by_id(&self, id: Uuid) -> Result<Option<Provider>> {
        let provider = sqlx::query_as::<_, Provider>(
            "SELECT * FROM providers WHERE id = $1"
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(provider)
    }

    #[tracing::instrument(skip(self))]
    async fn create_provider(&self, provider: Provider, actor_id: Uuid) -> Result<Provider> {
        let mut tx = self.pool.begin().await?;

        // Set the actor ID for the audit trigger using set_config
        sqlx::query("SELECT set_config('app.current_user_id', $1, true)")
            .bind(actor_id.to_string()) 
            .execute(&mut *tx)
            .await?;

        let inserted_provider = sqlx::query_as::<_, Provider>(
            r#"
            INSERT INTO providers (
                id, is_core, legal_name, trade_name, tax_id, email_address, office_phone,
                website_url, mailing_address, alert_phone_numbers,
                banner_image_id, profile_image_id, is_active,
                fee_rate_bps, fixed_fee_amount, kafka_config,
                ledger_account_id,
                created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
            RETURNING *
            "#
        )
        .bind(provider.id)
        .bind(provider.is_core)
        .bind(provider.legal_name)
        .bind(provider.trade_name)
        .bind(provider.tax_id)
        .bind(provider.email_address)
        .bind(provider.office_phone)
        .bind(provider.website_url)
        .bind(provider.mailing_address)
        .bind(provider.alert_phone_numbers)
        .bind(provider.banner_image_id)
        .bind(provider.profile_image_id)
        .bind(provider.is_active)
        .bind(provider.fee_rate_bps)
        .bind(provider.fixed_fee_amount)
        .bind(provider.kafka_config)
        .bind(provider.ledger_account_id)
        .bind(provider.created_at)
        .bind(provider.updated_at)
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(inserted_provider)
    }
}
