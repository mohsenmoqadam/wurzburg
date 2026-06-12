// src/db/traits.rs
use async_trait::async_trait;
use uuid::Uuid;
use anyhow::Result;

use crate::db::models::{PriorityItemData, Provider, Transaction, User, UserAccount, UserAccountProviderInfo, UserPriorityDetails, UserProviderDetails};

#[async_trait]
pub trait ProviderRepository: Send + Sync {
    async fn get_provider_by_id(&self, id: Uuid) -> Result<Option<Provider>>;
    async fn create_provider(&self, provider: Provider, actor_id: Uuid) -> Result<Provider>;
}

#[async_trait]
pub trait UserRepository: Send + Sync {
    // Finds existing user by NID or creates a new one, links to provider, and creates account
    async fn create_or_link_user(
        &self,
        nid: String,
        provider_id: Uuid,
        internal_metadata: serde_json::Value,
        external_metadata: serde_json::Value,
        ledger_account_id: Uuid,
        actor_id: Uuid,
    ) -> Result<(User, UserAccount)>;

    async fn get_user_provider_details(
        &self,
        user_id: Uuid,
        provider_id: Uuid,
    ) -> Result<Option<UserProviderDetails>>;

    async fn get_user_providers(&self, user_id: Uuid) -> Result<Vec<Provider>>;

    async fn get_user_accounts_provider_info(&self, user_id: Uuid) -> Result<Vec<UserAccountProviderInfo>>;
}

#[async_trait]
pub trait TransactionRepository: Send + Sync {
    // Check for an existing transaction by idempotency key
    async fn get_transaction_by_idempotency_key(&self, key: &str) -> Result<Option<Transaction>>;
    
    // Atomically insert a transaction and update cached balances
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
    ) -> Result<Transaction>;

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
    ) -> Result<Transaction>;

    async fn get_provider_transactions(
        &self,
        provider_id: Uuid,
        limit: i64,
        offset: i64,
    ) -> Result<(Vec<crate::db::models::Transaction>, i64), sqlx::Error>;

    async fn get_user_transactions(
        &self,
        provider_id: Uuid,
        user_id: Uuid,
        limit: i64,
        offset: i64,
    ) -> Result<(Vec<crate::db::models::Transaction>, i64), sqlx::Error>;
}

#[async_trait]
pub trait PriorityRepository: Send + Sync {
    async fn get_priority_config_by_idempotency_key(&self, key: &str) -> Result<Option<UserPriorityDetails>>;
    /// Creates a new priority configuration for a user.
    /// This is a transactional operation that will:
    /// 1. Cancel any existing 'ACTIVE' configuration for the user.
    /// 2. Create a new 'user_priority_configs' record.
    /// 3. Create all associated 'user_priority_items' records.
    async fn create_priority_config(
        &self,
        idempotency_key: String,
        user_id: Uuid,
        items: Vec<PriorityItemData>,
        expires_at: Option<chrono::DateTime<chrono::Utc>>,
        actor_id: Uuid,
    ) -> Result<UserPriorityDetails>;

    /// Fetches the currently active priority configuration and its items for a given user.
    async fn get_active_priority_config(&self, user_id: Uuid) -> Result<Option<UserPriorityDetails>>;

    /// Cancels the currently active priority configuration for a user by setting its status to 'CANCELLED'.
    async fn cancel_active_priority_config(&self, user_id: Uuid, actor_id: Uuid) -> Result<Option<UserPriorityDetails>>;

    /// Hard deletes a priority configuration and its items.
    /// Used strictly for saga compensation (rollback) if Redis fails after DB insertion.
    async fn soft_delete_priority_config(&self, config_id: Uuid) -> Result<()>;

    /// Fetches all priority configurations (active, consumed, cancelled, expired) for a user, excluding soft-deleted ones.
    async fn get_all_priority_configs(&self, user_id: Uuid) -> Result<Vec<UserPriorityDetails>>;
}

pub trait AppRepository: ProviderRepository + UserRepository + TransactionRepository + PriorityRepository {}
