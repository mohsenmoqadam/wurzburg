use std::sync::Arc;

use uuid::Uuid;

use crate::{
    api::{
        auth::{TrustedActor, require_scope},
        command::MutationCommandContext,
        error::ApiError,
        result_codes::WurzburgResultCode,
    },
    db::oracle::{
        OracleRepository, SetProviderFeeProfilePersistenceOutcome, SetProviderFeeProfileResult,
    },
    domain::provider_fee::{DesiredProviderFeeProfile, ProviderFeeProfile},
};

#[derive(Debug, Clone, PartialEq)]
pub enum SetProviderFeeProfileOutcome {
    Applied(Box<SetProviderFeeProfileResult>),
    Replayed(serde_json::Value),
}

#[derive(Clone)]
pub struct ProviderFeeService {
    repository: Arc<OracleRepository>,
}

impl ProviderFeeService {
    pub fn new(repository: Arc<OracleRepository>) -> Self {
        Self { repository }
    }

    #[tracing::instrument(skip(self, context, desired), fields(provider_id=%provider_id))]
    pub async fn set_profile(
        &self,
        context: &MutationCommandContext,
        provider_id: Uuid,
        desired: DesiredProviderFeeProfile,
    ) -> Result<SetProviderFeeProfileOutcome, ApiError> {
        require_scope(&context.actor, "platform.fee_profiles:write")?;
        desired.validate().map_err(|message| {
            ApiError::with_message(
                WurzburgResultCode::ProviderFeeProfileContractInvalid,
                message.to_string(),
            )
        })?;
        match self
            .repository
            .set_provider_fee_profile_atomic(context.clone(), provider_id, desired)
            .await
            .map_err(ApiError::from_database)?
        {
            SetProviderFeeProfilePersistenceOutcome::Applied(value) => {
                Ok(SetProviderFeeProfileOutcome::Applied(value))
            }
            SetProviderFeeProfilePersistenceOutcome::Replayed(value) => {
                Ok(SetProviderFeeProfileOutcome::Replayed(value))
            }
            SetProviderFeeProfilePersistenceOutcome::ProviderNotFound => {
                Err(ApiError::new(WurzburgResultCode::ProviderNotFound))
            }
            SetProviderFeeProfilePersistenceOutcome::ContractInvalid(message) => {
                Err(ApiError::with_message(
                    WurzburgResultCode::ProviderFeeProfileContractInvalid,
                    message,
                ))
            }
            SetProviderFeeProfilePersistenceOutcome::DraftFrozen => Err(ApiError::new(
                WurzburgResultCode::ProviderFeeProfileDraftFrozen,
            )),
            SetProviderFeeProfilePersistenceOutcome::IdempotencyConflict => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyKeyConflict))
            }
            SetProviderFeeProfilePersistenceOutcome::IdempotencyInProgress => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyInProgress))
            }
            SetProviderFeeProfilePersistenceOutcome::IdempotencyInvalidState => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyError))
            }
        }
    }

    #[tracing::instrument(skip(self, actor), fields(provider_id=%provider_id))]
    pub async fn get_current(
        &self,
        actor: &TrustedActor,
        provider_id: Uuid,
    ) -> Result<ProviderFeeProfile, ApiError> {
        require_scope(actor, "platform.fee_profiles:read")?;
        self.ensure_provider_exists(provider_id).await?;
        self.repository
            .get_current_provider_fee_profile(provider_id)
            .await
            .map_err(ApiError::from_database)?
            .ok_or_else(|| ApiError::new(WurzburgResultCode::ProviderFeeProfileNotFound))
    }

    #[tracing::instrument(skip(self, actor), fields(provider_id=%provider_id, fee_profile_id=%profile_id))]
    pub async fn get(
        &self,
        actor: &TrustedActor,
        provider_id: Uuid,
        profile_id: Uuid,
    ) -> Result<ProviderFeeProfile, ApiError> {
        require_scope(actor, "platform.fee_profiles:read")?;
        self.ensure_provider_exists(provider_id).await?;
        self.repository
            .get_provider_fee_profile(provider_id, profile_id)
            .await
            .map_err(ApiError::from_database)?
            .ok_or_else(|| ApiError::new(WurzburgResultCode::ProviderFeeProfileNotFound))
    }

    #[tracing::instrument(skip(self, actor), fields(provider_id=%provider_id, limit))]
    pub async fn list(
        &self,
        actor: &TrustedActor,
        provider_id: Uuid,
        before_version: Option<i64>,
        limit: u16,
    ) -> Result<Vec<ProviderFeeProfile>, ApiError> {
        require_scope(actor, "platform.fee_profiles:read")?;
        if limit == 0 || limit > 100 || before_version.is_some_and(|value| value <= 0) {
            return Err(ApiError::new(WurzburgResultCode::InvalidProviderFilter));
        }
        self.ensure_provider_exists(provider_id).await?;
        self.repository
            .list_provider_fee_profiles(provider_id, before_version, limit)
            .await
            .map_err(ApiError::from_database)
    }

    async fn ensure_provider_exists(&self, provider_id: Uuid) -> Result<(), ApiError> {
        if self
            .repository
            .get_provider(provider_id)
            .await
            .map_err(ApiError::from_database)?
            .is_none()
        {
            return Err(ApiError::new(WurzburgResultCode::ProviderNotFound));
        }
        Ok(())
    }
}
