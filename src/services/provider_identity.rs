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
        OracleRepository, ProviderContactMutationOutcome, ProviderIdentityMutationOutcome,
    },
    domain::provider::{
        ProviderContact, ProviderContactListPage, ProviderContactListQuery, ProviderContactStatus,
        ProviderContactUpdate, ProviderIdentityUpdate, normalize_reason,
    },
};

#[derive(Clone)]
pub struct ProviderIdentityService {
    repository: Arc<OracleRepository>,
}

impl ProviderIdentityService {
    pub fn new(repository: Arc<OracleRepository>) -> Self {
        Self { repository }
    }

    #[tracing::instrument(skip(self, context, update), fields(provider_id=%provider_id))]
    pub async fn update_identity(
        &self,
        context: &MutationCommandContext,
        provider_id: Uuid,
        update: ProviderIdentityUpdate,
    ) -> Result<ProviderIdentityMutationOutcome, ApiError> {
        require_scope(&context.actor, "platform.providers:write")?;
        let update = update.validate_and_normalize().map_err(|message| {
            ApiError::with_message(WurzburgResultCode::InvalidProviderIdentityContract, message)
        })?;
        let outcome = self
            .repository
            .update_provider_identity_atomic(context.clone(), provider_id, update)
            .await
            .map_err(ApiError::from_database)?;
        map_identity_outcome(outcome)
    }

    #[tracing::instrument(skip(self, context, contact, reason), fields(provider_id=%provider_id))]
    pub async fn create_contact(
        &self,
        context: &MutationCommandContext,
        provider_id: Uuid,
        mut contact: ProviderContact,
        reason: String,
    ) -> Result<ProviderContactMutationOutcome, ApiError> {
        require_scope(&context.actor, "platform.providers:write")?;
        contact.validate_and_normalize().map_err(|message| {
            ApiError::with_message(WurzburgResultCode::InvalidProviderContactContract, message)
        })?;
        let reason =
            normalize_reason(reason, "provider contact creation reason").map_err(|message| {
                ApiError::with_message(WurzburgResultCode::InvalidProviderContactContract, message)
            })?;
        let outcome = self
            .repository
            .create_provider_contact_atomic(context.clone(), provider_id, contact, reason)
            .await
            .map_err(ApiError::from_database)?;
        map_contact_outcome(outcome)
    }

    #[tracing::instrument(skip(self, context, update), fields(provider_id=%provider_id, provider_contact_id=%contact_id))]
    pub async fn update_contact(
        &self,
        context: &MutationCommandContext,
        provider_id: Uuid,
        contact_id: Uuid,
        update: ProviderContactUpdate,
    ) -> Result<ProviderContactMutationOutcome, ApiError> {
        require_scope(&context.actor, "platform.providers:write")?;
        let update = update.validate_and_normalize().map_err(|message| {
            ApiError::with_message(WurzburgResultCode::InvalidProviderContactContract, message)
        })?;
        let outcome = self
            .repository
            .update_provider_contact_atomic(context.clone(), provider_id, contact_id, update)
            .await
            .map_err(ApiError::from_database)?;
        map_contact_outcome(outcome)
    }

    #[tracing::instrument(skip(self, context, reason), fields(provider_id=%provider_id, provider_contact_id=%contact_id, target_status=target.as_db_value()))]
    pub async fn transition_contact(
        &self,
        context: &MutationCommandContext,
        provider_id: Uuid,
        contact_id: Uuid,
        target: ProviderContactStatus,
        reason: String,
    ) -> Result<ProviderContactMutationOutcome, ApiError> {
        require_scope(&context.actor, "platform.providers:write")?;
        let reason =
            normalize_reason(reason, "provider contact transition reason").map_err(|message| {
                ApiError::with_message(WurzburgResultCode::InvalidProviderContactContract, message)
            })?;
        let outcome = self
            .repository
            .transition_provider_contact_atomic(
                context.clone(),
                provider_id,
                contact_id,
                target,
                reason,
            )
            .await
            .map_err(ApiError::from_database)?;
        map_contact_outcome(outcome)
    }

    #[tracing::instrument(skip(self, actor, query), fields(provider_id=%provider_id))]
    pub async fn list_contacts(
        &self,
        actor: &TrustedActor,
        provider_id: Uuid,
        query: ProviderContactListQuery,
    ) -> Result<ProviderContactListPage, ApiError> {
        require_scope(actor, "platform.providers:read")?;
        if query.limit == 0 || query.limit > 100 {
            return Err(ApiError::new(
                WurzburgResultCode::InvalidProviderContactFilter,
            ));
        }
        self.repository
            .list_provider_contacts(provider_id, query)
            .await
            .map_err(ApiError::from_database)?
            .ok_or_else(|| ApiError::new(WurzburgResultCode::ProviderNotFound))
    }
}

fn map_identity_outcome(
    outcome: ProviderIdentityMutationOutcome,
) -> Result<ProviderIdentityMutationOutcome, ApiError> {
    match outcome {
        ProviderIdentityMutationOutcome::ProviderNotFound => {
            Err(ApiError::new(WurzburgResultCode::ProviderNotFound))
        }
        ProviderIdentityMutationOutcome::IdempotencyConflict => {
            Err(ApiError::new(WurzburgResultCode::IdempotencyKeyConflict))
        }
        ProviderIdentityMutationOutcome::IdempotencyInProgress => {
            Err(ApiError::new(WurzburgResultCode::IdempotencyInProgress))
        }
        ProviderIdentityMutationOutcome::IdempotencyInvalidState => {
            Err(ApiError::new(WurzburgResultCode::IdempotencyError))
        }
        success => Ok(success),
    }
}

fn map_contact_outcome(
    outcome: ProviderContactMutationOutcome,
) -> Result<ProviderContactMutationOutcome, ApiError> {
    match outcome {
        ProviderContactMutationOutcome::ProviderNotFound => {
            Err(ApiError::new(WurzburgResultCode::ProviderNotFound))
        }
        ProviderContactMutationOutcome::ContactNotFound => {
            Err(ApiError::new(WurzburgResultCode::ProviderContactNotFound))
        }
        ProviderContactMutationOutcome::ContractInvalid => Err(ApiError::new(
            WurzburgResultCode::InvalidProviderContactContract,
        )),
        ProviderContactMutationOutcome::InvalidState => Err(ApiError::new(
            WurzburgResultCode::ProviderContactInvalidState,
        )),
        ProviderContactMutationOutcome::IdempotencyConflict => {
            Err(ApiError::new(WurzburgResultCode::IdempotencyKeyConflict))
        }
        ProviderContactMutationOutcome::IdempotencyInProgress => {
            Err(ApiError::new(WurzburgResultCode::IdempotencyInProgress))
        }
        ProviderContactMutationOutcome::IdempotencyInvalidState => {
            Err(ApiError::new(WurzburgResultCode::IdempotencyError))
        }
        success => Ok(success),
    }
}
