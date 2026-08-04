use std::sync::Arc;

use uuid::Uuid;

use crate::{
    api::{auth::TrustedActor, error::ApiError, result_codes::WurzburgResultCode},
    db::oracle::OracleRepository,
    domain::financial_transaction::{
        FinancialTransactionPage, FinancialTransactionQuery, TransactionVisibility,
    },
};

#[derive(Clone)]
pub struct FinancialTransactionService {
    repository: Arc<OracleRepository>,
}

impl FinancialTransactionService {
    pub fn new(repository: Arc<OracleRepository>) -> Self {
        Self { repository }
    }

    #[tracing::instrument(skip(self, actor, query), fields(transaction.visibility = "provider", provider.id = %provider_id))]
    pub async fn list_provider(
        &self,
        actor: &TrustedActor,
        provider_id: Uuid,
        query: FinancialTransactionQuery,
    ) -> Result<FinancialTransactionPage, ApiError> {
        require_exact_scope(actor, "provider.transactions:read")?;
        if !actor.has_role("wurzburg_platform_admin") && actor.provider_id != Some(provider_id) {
            return Err(ApiError::new(WurzburgResultCode::ProviderScopeMismatch));
        }
        if !matches!(
            query.visibility,
            TransactionVisibility::Provider {
                provider_id: value,
                ..
            } if value == provider_id
        ) {
            return Err(ApiError::new(WurzburgResultCode::InvalidTransactionQuery));
        }
        self.list(query).await
    }

    #[tracing::instrument(skip(self, actor, query), fields(transaction.visibility = "cardholder", user.id = %user_id))]
    pub async fn list_cardholder(
        &self,
        actor: &TrustedActor,
        user_id: Uuid,
        query: FinancialTransactionQuery,
    ) -> Result<FinancialTransactionPage, ApiError> {
        require_exact_scope(actor, "card.transactions:read")?;
        if !actor.has_role("wurzburg_cardholder") || actor.user_id != Some(user_id) {
            return Err(ApiError::new(WurzburgResultCode::CardholderScopeMismatch));
        }
        if !matches!(
            query.visibility,
            TransactionVisibility::Cardholder { user_id: value, .. } if value == user_id
        ) {
            return Err(ApiError::new(WurzburgResultCode::InvalidTransactionQuery));
        }
        self.list(query).await
    }

    #[tracing::instrument(skip(self, actor, query), fields(transaction.visibility = "platform"))]
    pub async fn list_platform(
        &self,
        actor: &TrustedActor,
        query: FinancialTransactionQuery,
    ) -> Result<FinancialTransactionPage, ApiError> {
        let authorized = (actor.has_role("wurzburg_platform_admin")
            && actor.has_scope("platform.transactions:read"))
            || (actor.has_role("wurzburg_support") && actor.has_scope("support.transactions:read"))
            || (actor.has_role("wurzburg_reporting")
                && actor.has_scope("reports.transactions:read"));
        if !authorized {
            return Err(ApiError::with_details(
                WurzburgResultCode::MissingRequiredScope,
                serde_json::json!({
                    "required_scope": "platform.transactions:read | support.transactions:read | reports.transactions:read"
                }),
            ));
        }
        if !matches!(query.visibility, TransactionVisibility::Platform { .. }) {
            return Err(ApiError::new(WurzburgResultCode::InvalidTransactionQuery));
        }
        self.list(query).await
    }

    async fn list(
        &self,
        query: FinancialTransactionQuery,
    ) -> Result<FinancialTransactionPage, ApiError> {
        if query.limit == 0 || query.limit > 200 {
            return Err(ApiError::new(WurzburgResultCode::InvalidTransactionQuery));
        }
        if query
            .occurred_from
            .zip(query.occurred_to)
            .is_some_and(|(from, to)| from >= to)
        {
            return Err(ApiError::new(WurzburgResultCode::InvalidTransactionQuery));
        }
        self.repository
            .list_financial_transactions(query)
            .await
            .map_err(ApiError::from_database)
    }
}

fn require_exact_scope(actor: &TrustedActor, scope: &'static str) -> Result<(), ApiError> {
    if actor.has_scope(scope) {
        Ok(())
    } else {
        Err(ApiError::with_details(
            WurzburgResultCode::MissingRequiredScope,
            serde_json::json!({ "required_scope": scope }),
        ))
    }
}
