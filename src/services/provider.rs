use std::sync::Arc;

use tigerbeetle_rustclient_tests_snapshot::AccountFlags;
use uuid::Uuid;

use crate::{
    api::{
        auth::{TrustedActor, require_scope},
        command::MutationCommandContext,
        error::ApiError,
        result_codes::WurzburgResultCode,
    },
    config::TigerBeetleConfig,
    db::oracle::{CreateProviderPersistenceOutcome, OracleRepository},
    domain::provider::{
        NewProvider, Provider, ProviderLedgerAccountBalance, ProviderListPage, ProviderListQuery,
    },
    ledger::{LedgerAccount, LedgerClient, TigerBeetleError},
    security::provider_kafka_cipher::ProviderKafkaCredentialFactory,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderProvisioningDisposition {
    Ready,
    Pending,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CreateProviderOutcome {
    Created {
        provider: Box<Provider>,
        provisioning: ProviderProvisioningDisposition,
    },
    Replayed(serde_json::Value),
}

#[derive(Clone)]
pub struct ProviderService {
    repository: Arc<OracleRepository>,
    ledger_client: LedgerClient,
    ledger_config: TigerBeetleConfig,
    kafka_credentials: Option<Arc<ProviderKafkaCredentialFactory>>,
}

impl ProviderService {
    pub fn new(
        repository: Arc<OracleRepository>,
        ledger_client: LedgerClient,
        ledger_config: TigerBeetleConfig,
        kafka_credentials: Option<Arc<ProviderKafkaCredentialFactory>>,
    ) -> Self {
        Self {
            repository,
            ledger_client,
            ledger_config,
            kafka_credentials,
        }
    }

    #[tracing::instrument(skip(self, context, provider), fields(provider_id=%provider.provider_id))]
    pub async fn create_provider(
        &self,
        context: &MutationCommandContext,
        provider: NewProvider,
    ) -> Result<CreateProviderOutcome, ApiError> {
        require_scope(&context.actor, "platform.providers:write")?;
        let mut provider = provider.validate_and_normalize().map_err(|message| {
            ApiError::with_message(WurzburgResultCode::InvalidProviderContract, message)
        })?;
        if let Some(factory) = &self.kafka_credentials {
            provider.kafka_access =
                Some(factory.prepare(provider.provider_id).map_err(|error| {
                    tracing::error!(
                        error.kind = error.diagnostic_kind(),
                        "failed to prepare encrypted Provider Kafka credentials"
                    );
                    ApiError::new(WurzburgResultCode::SystemError)
                })?);
        }

        let outcome = self
            .repository
            .create_provider_atomic(context.clone(), provider)
            .await
            .map_err(ApiError::from_database)?;

        let created = match outcome {
            CreateProviderPersistenceOutcome::Created(provider) => provider,
            CreateProviderPersistenceOutcome::Replayed(snapshot) => {
                return Ok(CreateProviderOutcome::Replayed(snapshot));
            }
            CreateProviderPersistenceOutcome::IdempotencyConflict => {
                return Err(ApiError::new(WurzburgResultCode::IdempotencyKeyConflict));
            }
            CreateProviderPersistenceOutcome::IdempotencyInProgress => {
                return Err(ApiError::new(WurzburgResultCode::IdempotencyInProgress));
            }
            CreateProviderPersistenceOutcome::IdempotencyInvalidState => {
                return Err(ApiError::new(WurzburgResultCode::IdempotencyError));
            }
        };

        match self.provision_core_accounts(created.provider_id).await {
            Ok(()) => {
                let ready = self
                    .repository
                    .mark_provider_ready(created.provider_id)
                    .await
                    .map_err(ApiError::from_database)?;
                Ok(CreateProviderOutcome::Created {
                    provider: Box::new(ready),
                    provisioning: ProviderProvisioningDisposition::Ready,
                })
            }
            Err(error) => {
                tracing::warn!(
                    provider_id = %created.provider_id,
                    error.kind = error.diagnostic_kind(),
                    outcome.uncertain = error.outcome_is_uncertain(),
                    "provider core provisioning will continue through recovery"
                );
                Ok(CreateProviderOutcome::Created {
                    provider: created,
                    provisioning: ProviderProvisioningDisposition::Pending,
                })
            }
        }
    }

    #[tracing::instrument(skip(self, actor), fields(provider_id=%provider_id))]
    pub async fn get_provider(
        &self,
        actor: &TrustedActor,
        provider_id: Uuid,
    ) -> Result<Provider, ApiError> {
        require_scope(actor, "platform.providers:read")?;
        self.repository
            .get_provider(provider_id)
            .await
            .map_err(ApiError::from_database)?
            .ok_or_else(|| ApiError::new(WurzburgResultCode::ProviderNotFound))
    }

    #[tracing::instrument(skip(self, actor, query))]
    pub async fn list_providers(
        &self,
        actor: &TrustedActor,
        query: ProviderListQuery,
    ) -> Result<ProviderListPage, ApiError> {
        require_scope(actor, "platform.providers:read")?;
        self.repository
            .list_providers(query)
            .await
            .map_err(ApiError::from_database)
    }

    #[tracing::instrument(skip(self, actor), fields(provider_id=%provider_id))]
    pub async fn get_provider_ledger(
        &self,
        actor: &TrustedActor,
        provider_id: Uuid,
    ) -> Result<Vec<ProviderLedgerAccountBalance>, ApiError> {
        require_scope(actor, "platform.providers:read")?;
        if self
            .repository
            .get_provider(provider_id)
            .await
            .map_err(ApiError::from_database)?
            .is_none()
        {
            return Err(ApiError::new(WurzburgResultCode::ProviderNotFound));
        }
        let mappings = self
            .repository
            .get_provider_ledger_mappings(provider_id)
            .await
            .map_err(ApiError::from_database)?;
        if mappings.len() != 4 {
            tracing::error!(
                provider_id = %provider_id,
                account.count = mappings.len(),
                "provider ledger mapping set is incomplete"
            );
            return Err(ApiError::new(WurzburgResultCode::ProviderLedgerUnavailable));
        }
        let ids = mappings
            .iter()
            .map(|mapping| mapping.tigerbeetle_account_id.as_u128())
            .collect();
        let accounts = self
            .ledger_client
            .lookup_accounts(ids)
            .await
            .map_err(|error| {
                tracing::error!(
                    provider_id = %provider_id,
                    error.kind = error.diagnostic_kind(),
                    "TigerBeetle provider ledger lookup failed"
                );
                ApiError::new(WurzburgResultCode::ProviderLedgerUnavailable)
            })?;
        if accounts.len() != 4 {
            tracing::error!(
                provider_id = %provider_id,
                account.count = accounts.len(),
                "TigerBeetle returned an incomplete provider ledger account set"
            );
            return Err(ApiError::new(WurzburgResultCode::ProviderLedgerUnavailable));
        }

        mappings
            .into_iter()
            .map(|mapping| {
                let account_id = mapping.tigerbeetle_account_id.as_u128();
                let account = accounts
                    .iter()
                    .find(|account| account.id == account_id)
                    .ok_or_else(|| ApiError::new(WurzburgResultCode::ProviderLedgerUnavailable))?;
                if account.user_data_128 != provider_id.as_u128()
                    || account.ledger != self.ledger_config.ledger_id
                    || account.code != self.ledger_config.provider_account_code(mapping.category)
                {
                    tracing::error!(
                        provider_id = %provider_id,
                        account.category = mapping.category.as_db_value(),
                        "TigerBeetle provider ledger account violates its configured contract"
                    );
                    return Err(ApiError::new(WurzburgResultCode::ProviderLedgerUnavailable));
                }
                let effective_credits = account
                    .credits_posted
                    .checked_add(account.credits_pending)
                    .ok_or_else(|| ApiError::new(WurzburgResultCode::ProviderLedgerUnavailable))?;
                let effective_debits = account
                    .debits_posted
                    .checked_add(account.debits_pending)
                    .ok_or_else(|| ApiError::new(WurzburgResultCode::ProviderLedgerUnavailable))?;
                Ok(ProviderLedgerAccountBalance {
                    account_category: mapping.category,
                    tigerbeetle_account_id: mapping.tigerbeetle_account_id,
                    debits_posted: account.debits_posted.to_string(),
                    credits_posted: account.credits_posted.to_string(),
                    debits_pending: account.debits_pending.to_string(),
                    credits_pending: account.credits_pending.to_string(),
                    posted_balance: signed_difference(
                        account.credits_posted,
                        account.debits_posted,
                    ),
                    effective_balance: signed_difference(effective_credits, effective_debits),
                    status: mapping.status,
                })
            })
            .collect()
    }

    #[tracing::instrument(skip(self), fields(provider_id=%provider_id))]
    pub async fn recover_provider_core(&self, provider_id: Uuid) -> Result<(), TigerBeetleError> {
        self.provision_core_accounts(provider_id).await?;
        self.repository
            .mark_provider_ready(provider_id)
            .await
            .map_err(|_| TigerBeetleError::ClientFailure {
                operation: "finalize_provider_provisioning",
            })?;
        Ok(())
    }

    #[tracing::instrument(skip(self, context), fields(provider_id=%provider_id, card_range_id=%card_range_id))]
    pub async fn assign_card_range(
        &self,
        context: &MutationCommandContext,
        provider_id: Uuid,
        card_range_id: Uuid,
        reason: String,
    ) -> Result<crate::db::oracle::ProviderRangeAssignmentOutcome, ApiError> {
        require_scope(&context.actor, "platform.providers:write")?;
        let reason = reason.trim().to_string();
        if reason.is_empty() || reason.len() > 1000 || reason.chars().any(char::is_control) {
            return Err(ApiError::new(WurzburgResultCode::InvalidProviderContract));
        }
        let outcome = self
            .repository
            .assign_provider_range_atomic(context.clone(), provider_id, card_range_id, reason)
            .await
            .map_err(ApiError::from_database)?;
        use crate::db::oracle::ProviderRangeAssignmentOutcome;
        match outcome {
            ProviderRangeAssignmentOutcome::ProviderNotFound => {
                Err(ApiError::new(WurzburgResultCode::ProviderNotFound))
            }
            ProviderRangeAssignmentOutcome::ProviderNotActive => {
                Err(ApiError::new(WurzburgResultCode::ProviderNotActive))
            }
            ProviderRangeAssignmentOutcome::RangeNotFound => {
                Err(ApiError::new(WurzburgResultCode::CardRangeNotFound))
            }
            ProviderRangeAssignmentOutcome::PolicyMissing => Err(ApiError::new(
                WurzburgResultCode::CardRangePrerequisitesMissing,
            )),
            ProviderRangeAssignmentOutcome::FeeProfileMissing => Err(ApiError::new(
                WurzburgResultCode::ProviderFeeProfileRequired,
            )),
            ProviderRangeAssignmentOutcome::SingleProviderOccupied => {
                Err(ApiError::new(WurzburgResultCode::ProviderRangeOccupied))
            }
            ProviderRangeAssignmentOutcome::PublicationPending => Err(ApiError::new(
                WurzburgResultCode::RangeControlPublicationPending,
            )),
            ProviderRangeAssignmentOutcome::IdempotencyConflict => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyKeyConflict))
            }
            ProviderRangeAssignmentOutcome::IdempotencyInProgress => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyInProgress))
            }
            ProviderRangeAssignmentOutcome::IdempotencyInvalidState => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyError))
            }
            applied => Ok(applied),
        }
    }

    #[tracing::instrument(skip(self, context), fields(provider_id=%provider_id, provider.target_status=target.as_db_value()))]
    pub async fn transition_provider(
        &self,
        context: &MutationCommandContext,
        provider_id: Uuid,
        target: crate::domain::provider::ProviderStatus,
        reason: String,
    ) -> Result<crate::db::oracle::ProviderLifecycleOutcome, ApiError> {
        require_scope(&context.actor, "platform.providers:write")?;
        let reason = reason.trim().to_string();
        if reason.is_empty() || reason.len() > 1000 || reason.chars().any(char::is_control) {
            return Err(ApiError::new(WurzburgResultCode::InvalidProviderContract));
        }
        let outcome = self
            .repository
            .transition_provider_atomic(context.clone(), provider_id, target, reason)
            .await
            .map_err(ApiError::from_database)?;
        use crate::db::oracle::ProviderLifecycleOutcome;
        match outcome {
            ProviderLifecycleOutcome::NotFound => {
                Err(ApiError::new(WurzburgResultCode::ProviderNotFound))
            }
            ProviderLifecycleOutcome::InvalidTransition => {
                Err(ApiError::new(WurzburgResultCode::ProviderInvalidTransition))
            }
            ProviderLifecycleOutcome::PrerequisitesMissing => Err(ApiError::new(
                WurzburgResultCode::ProviderProvisioningPending,
            )),
            ProviderLifecycleOutcome::IdempotencyConflict => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyKeyConflict))
            }
            ProviderLifecycleOutcome::IdempotencyInProgress => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyInProgress))
            }
            ProviderLifecycleOutcome::IdempotencyInvalidState => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyError))
            }
            applied => Ok(applied),
        }
    }

    async fn provision_core_accounts(&self, provider_id: Uuid) -> Result<(), TigerBeetleError> {
        let mappings = self
            .repository
            .get_provider_ledger_mappings(provider_id)
            .await
            .map_err(|_| TigerBeetleError::ClientFailure {
                operation: "load_account_mappings",
            })?;
        if mappings.len() != 4 {
            return Err(TigerBeetleError::ClientFailure {
                operation: "validate_account_mappings",
            });
        }

        for mapping in mappings {
            let expected = LedgerAccount {
                id: mapping.tigerbeetle_account_id.as_u128(),
                debits_pending: 0,
                debits_posted: 0,
                credits_pending: 0,
                credits_posted: 0,
                user_data_128: provider_id.as_u128(),
                user_data_64: 0,
                user_data_32: 0,
                reserved: 0,
                ledger: self.ledger_config.ledger_id,
                code: self.ledger_config.provider_account_code(mapping.category),
                flags: AccountFlags::History.bits(),
                timestamp: 0,
            };

            let create_result = self.ledger_client.create_account(expected.clone()).await;
            if let Err(error) = &create_result {
                tracing::warn!(
                    provider_id = %provider_id,
                    account.category = mapping.category.as_db_value(),
                    error.kind = error.diagnostic_kind(),
                    "TigerBeetle account creation requires verification"
                );
            }

            let accounts = self.ledger_client.lookup_account(expected.id).await?;
            let account = accounts.first().ok_or(TigerBeetleError::ClientFailure {
                operation: "verify_provider_account",
            })?;
            if account.id != expected.id
                || account.ledger != expected.ledger
                || account.code != expected.code
                || account.flags != expected.flags
                || account.user_data_128 != expected.user_data_128
            {
                return Err(TigerBeetleError::ClientFailure {
                    operation: "verify_provider_account_contract",
                });
            }
        }
        Ok(())
    }
}

fn signed_difference(credits: u128, debits: u128) -> String {
    if credits >= debits {
        (credits - debits).to_string()
    } else {
        format!("-{}", debits - credits)
    }
}

#[cfg(test)]
mod balance_tests {
    use super::signed_difference;

    #[test]
    fn signed_difference_preserves_full_u128_range() {
        assert_eq!(signed_difference(150, 100), "50");
        assert_eq!(signed_difference(100, 150), "-50");
        assert_eq!(signed_difference(u128::MAX, 0), u128::MAX.to_string());
    }
}
