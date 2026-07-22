use std::{collections::BTreeSet, sync::Arc};

use uuid::Uuid;

use crate::{
    api::{
        auth::{TrustedActor, require_scope},
        command::MutationCommandContext,
        error::ApiError,
        result_codes::WurzburgResultCode,
    },
    db::oracle::{
        OracleRepository, ProviderEventSubscriptionRecord, ProviderEventSubscriptionSet,
        ProviderEventSubscriptionUpdateOutcome,
    },
    domain::provider_event::ProviderEventType,
};

pub enum ProviderEventSubscriptionCommandResult {
    Applied(serde_json::Value),
    Replayed(serde_json::Value),
}

#[derive(Clone)]
pub struct ProviderEventSubscriptionService {
    repository: Arc<OracleRepository>,
}

impl ProviderEventSubscriptionService {
    pub fn new(repository: Arc<OracleRepository>) -> Self {
        Self { repository }
    }

    #[tracing::instrument(skip(self, actor), fields(provider_id=%provider_id, provider.events.view=view))]
    pub async fn get(
        &self,
        actor: &TrustedActor,
        provider_id: Uuid,
        view: &'static str,
    ) -> Result<ProviderEventSubscriptionSet, ApiError> {
        match view {
            "admin" => require_scope(actor, "platform.provider_events:read")?,
            "provider" => {
                require_scope(actor, "provider.events:read")?;
                if actor
                    .provider_id
                    .is_some_and(|actor_provider_id| actor_provider_id != provider_id)
                {
                    return Err(ApiError::new(WurzburgResultCode::ProviderScopeMismatch));
                }
            }
            _ => return Err(ApiError::new(WurzburgResultCode::SystemError)),
        }
        self.repository
            .get_provider_event_subscriptions(provider_id)
            .await
            .map_err(ApiError::from_database)?
            .ok_or_else(|| ApiError::new(WurzburgResultCode::ProviderNotFound))
    }

    #[tracing::instrument(skip(self, context, subscriptions, reason), fields(provider_id=%provider_id))]
    pub async fn replace(
        &self,
        context: &MutationCommandContext,
        provider_id: Uuid,
        expected_version: u64,
        subscriptions: Vec<ProviderEventSubscriptionRecord>,
        reason: String,
    ) -> Result<ProviderEventSubscriptionCommandResult, ApiError> {
        require_scope(&context.actor, "platform.provider_events:write")?;
        let reason = reason.trim().to_string();
        if reason.is_empty() || reason.len() > 1000 || reason.chars().any(char::is_control) {
            return Err(ApiError::new(
                WurzburgResultCode::ProviderEventSubscriptionInvalid,
            ));
        }
        let event_types = subscriptions
            .iter()
            .map(|subscription| subscription.event_type)
            .collect::<BTreeSet<_>>();
        let expected_types = ProviderEventType::ALL.into_iter().collect::<BTreeSet<_>>();
        if event_types != expected_types || subscriptions.len() != expected_types.len() {
            return Err(ApiError::new(
                WurzburgResultCode::ProviderEventSubscriptionInvalid,
            ));
        }
        match self
            .repository
            .replace_provider_event_subscriptions_atomic(
                context.clone(),
                provider_id,
                expected_version,
                subscriptions,
                reason,
            )
            .await
            .map_err(ApiError::from_database)?
        {
            ProviderEventSubscriptionUpdateOutcome::Applied(snapshot) => {
                Ok(ProviderEventSubscriptionCommandResult::Applied(snapshot))
            }
            ProviderEventSubscriptionUpdateOutcome::Replayed(snapshot) => {
                Ok(ProviderEventSubscriptionCommandResult::Replayed(snapshot))
            }
            ProviderEventSubscriptionUpdateOutcome::ProviderNotFound => {
                Err(ApiError::new(WurzburgResultCode::ProviderNotFound))
            }
            ProviderEventSubscriptionUpdateOutcome::VersionConflict => Err(ApiError::new(
                WurzburgResultCode::ProviderEventSubscriptionVersionConflict,
            )),
            ProviderEventSubscriptionUpdateOutcome::IdempotencyConflict => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyKeyConflict))
            }
            ProviderEventSubscriptionUpdateOutcome::IdempotencyInProgress => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyInProgress))
            }
            ProviderEventSubscriptionUpdateOutcome::IdempotencyInvalidState => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyError))
            }
        }
    }
}
