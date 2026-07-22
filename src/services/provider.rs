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
    domain::provider::{NewProvider, Provider},
    tigerbeetle::{AppAccount, AppTbClient, TigerBeetleError},
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
    tb_client: AppTbClient,
    tb_config: TigerBeetleConfig,
}

impl ProviderService {
    pub fn new(
        repository: Arc<OracleRepository>,
        tb_client: AppTbClient,
        tb_config: TigerBeetleConfig,
    ) -> Self {
        Self {
            repository,
            tb_client,
            tb_config,
        }
    }

    #[tracing::instrument(skip(self, context, provider), fields(provider_id=%provider.provider_id))]
    pub async fn create_provider(
        &self,
        context: &MutationCommandContext,
        provider: NewProvider,
    ) -> Result<CreateProviderOutcome, ApiError> {
        require_scope(&context.actor, "platform.providers:write")?;
        let provider = provider.validate_and_normalize().map_err(|message| {
            ApiError::with_message(WurzburgResultCode::InvalidProviderContract, message)
        })?;

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
            ProviderLifecycleOutcome::PublicationPending => Err(ApiError::new(
                WurzburgResultCode::RangeControlPublicationPending,
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
            let expected = AppAccount {
                id: mapping.tigerbeetle_account_id.as_u128(),
                debits_pending: 0,
                debits_posted: 0,
                credits_pending: 0,
                credits_posted: 0,
                user_data_128: provider_id.as_u128(),
                user_data_64: 0,
                user_data_32: 0,
                reserved: 0,
                ledger: self.tb_config.ledger_id,
                code: self.tb_config.provider_account_code(mapping.category),
                flags: AccountFlags::History.bits(),
                timestamp: 0,
            };

            let create_result = self.tb_client.create_account(expected.clone()).await;
            if let Err(error) = &create_result {
                tracing::warn!(
                    provider_id = %provider_id,
                    account.category = mapping.category.as_db_value(),
                    error.kind = error.diagnostic_kind(),
                    "TigerBeetle account creation requires verification"
                );
            }

            let accounts = self.tb_client.lookup_account(expected.id).await?;
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
