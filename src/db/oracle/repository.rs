use anyhow::Result;
use async_trait::async_trait;
use uuid::Uuid;

use crate::db::{
    models::{
        PriorityItemData, Provider, Transaction, User, UserAccount, UserAccountProviderInfo,
        UserPriorityDetails, UserProviderDetails,
    },
    oracle::OraclePool,
    traits::{
        AppRepository, PriorityRepository, ProviderRepository, TransactionRepository,
        UserRepository,
    },
};

#[derive(Clone)]
pub struct OracleRepository {
    pub pool: OraclePool,
}

impl OracleRepository {
    pub fn new(pool: OraclePool) -> Self {
        Self { pool }
    }

    fn not_implemented<T>(&self, operation: &str) -> Result<T> {
        anyhow::bail!(
            "Oracle repository operation `{}` is not implemented yet; use Oracle health/migration foundation only",
            operation
        )
    }
}

#[async_trait]
impl ProviderRepository for OracleRepository {
    async fn get_provider_by_id(&self, _id: Uuid) -> Result<Option<Provider>> {
        self.not_implemented("get_provider_by_id")
    }

    async fn create_provider(&self, _provider: Provider, _actor_id: Uuid) -> Result<Provider> {
        self.not_implemented("create_provider")
    }
}

#[async_trait]
impl UserRepository for OracleRepository {
    async fn create_or_link_user(
        &self,
        _nid: String,
        _provider_id: Uuid,
        _internal_metadata: serde_json::Value,
        _external_metadata: serde_json::Value,
        _ledger_account_id: Uuid,
        _actor_id: Uuid,
    ) -> Result<(User, UserAccount)> {
        self.not_implemented("create_or_link_user")
    }

    async fn get_user_provider_details(
        &self,
        _user_id: Uuid,
        _provider_id: Uuid,
    ) -> Result<Option<UserProviderDetails>> {
        self.not_implemented("get_user_provider_details")
    }

    async fn get_user_providers(&self, _user_id: Uuid) -> Result<Vec<Provider>> {
        self.not_implemented("get_user_providers")
    }

    async fn get_user_accounts_provider_info(
        &self,
        _user_id: Uuid,
    ) -> Result<Vec<UserAccountProviderInfo>> {
        self.not_implemented("get_user_accounts_provider_info")
    }
}

#[async_trait]
impl TransactionRepository for OracleRepository {
    async fn get_transaction_by_idempotency_key(&self, _key: &str) -> Result<Option<Transaction>> {
        self.not_implemented("get_transaction_by_idempotency_key")
    }

    async fn execute_credit_transfer(
        &self,
        _idempotency_key: String,
        _amount: i64,
        _provider_id: Uuid,
        _user_id: Uuid,
        _provider_ledger_id: Uuid,
        _user_ledger_id: Uuid,
        _actor_id: Uuid,
        _description: Option<String>,
    ) -> Result<Transaction> {
        self.not_implemented("execute_credit_transfer")
    }

    async fn execute_debit_transfer(
        &self,
        _idempotency_key: String,
        _amount: i64,
        _provider_id: Uuid,
        _user_id: Uuid,
        _provider_ledger_id: Uuid,
        _user_ledger_id: Uuid,
        _actor_id: Uuid,
        _description: Option<String>,
    ) -> Result<Transaction> {
        self.not_implemented("execute_debit_transfer")
    }

    async fn get_provider_transactions(
        &self,
        _provider_id: Uuid,
        _limit: i64,
        _offset: i64,
    ) -> Result<(Vec<Transaction>, i64)> {
        self.not_implemented("get_provider_transactions")
    }

    async fn get_user_transactions(
        &self,
        _provider_id: Uuid,
        _user_id: Uuid,
        _limit: i64,
        _offset: i64,
    ) -> Result<(Vec<Transaction>, i64)> {
        self.not_implemented("get_user_transactions")
    }
}

#[async_trait]
impl PriorityRepository for OracleRepository {
    async fn get_priority_config_by_idempotency_key(
        &self,
        _key: &str,
    ) -> Result<Option<UserPriorityDetails>> {
        self.not_implemented("get_priority_config_by_idempotency_key")
    }

    async fn create_priority_config(
        &self,
        _idempotency_key: String,
        _user_id: Uuid,
        _items: Vec<PriorityItemData>,
        _expires_at: Option<chrono::DateTime<chrono::Utc>>,
        _actor_id: Uuid,
    ) -> Result<UserPriorityDetails> {
        self.not_implemented("create_priority_config")
    }

    async fn get_active_priority_config(
        &self,
        _user_id: Uuid,
    ) -> Result<Option<UserPriorityDetails>> {
        self.not_implemented("get_active_priority_config")
    }

    async fn cancel_active_priority_config(
        &self,
        _user_id: Uuid,
        _actor_id: Uuid,
    ) -> Result<Option<UserPriorityDetails>> {
        self.not_implemented("cancel_active_priority_config")
    }

    async fn soft_delete_priority_config(&self, _config_id: Uuid) -> Result<()> {
        self.not_implemented("soft_delete_priority_config")
    }

    async fn get_all_priority_configs(&self, _user_id: Uuid) -> Result<Vec<UserPriorityDetails>> {
        self.not_implemented("get_all_priority_configs")
    }
}

impl AppRepository for OracleRepository {}
