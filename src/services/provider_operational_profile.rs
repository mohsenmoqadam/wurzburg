use std::{sync::Arc, time::Duration};

use tokio::sync::watch;
use uuid::Uuid;

use crate::{
    api::{
        auth::{TrustedActor, require_scope},
        command::MutationCommandContext,
        error::ApiError,
        result_codes::WurzburgResultCode,
    },
    config::ProviderOperationalProfileSchedulerConfig,
    db::oracle::{
        CancelProviderOperationalProfilePersistenceOutcome, OracleRepository,
        SetProviderOperationalProfilePersistenceOutcome, SetProviderOperationalProfileResult,
    },
    domain::provider::{DesiredProviderOperationalProfile, ProviderOperationalProfileRecord},
};

#[derive(Debug, Clone, PartialEq)]
pub enum SetProviderOperationalProfileOutcome {
    Applied(Box<SetProviderOperationalProfileResult>),
    Replayed(serde_json::Value),
}

#[derive(Debug, Clone, PartialEq)]
pub enum CancelProviderOperationalProfileOutcome {
    Applied(Box<ProviderOperationalProfileRecord>),
    Replayed(serde_json::Value),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProviderOperationalProfilePage {
    pub items: Vec<ProviderOperationalProfileRecord>,
    pub next_before_version: Option<i64>,
}

#[derive(Clone)]
pub struct ProviderOperationalProfileService {
    repository: Arc<OracleRepository>,
}

impl ProviderOperationalProfileService {
    pub fn new(repository: Arc<OracleRepository>) -> Self {
        Self { repository }
    }

    #[tracing::instrument(skip(self, context, desired), fields(provider_id=%provider_id))]
    pub async fn set_profile(
        &self,
        context: &MutationCommandContext,
        provider_id: Uuid,
        desired: DesiredProviderOperationalProfile,
    ) -> Result<SetProviderOperationalProfileOutcome, ApiError> {
        require_scope(&context.actor, "platform.providers:write")?;
        let desired = desired.validate_and_normalize().map_err(|message| {
            ApiError::with_message(
                WurzburgResultCode::ProviderOperationalProfileContractInvalid,
                message.to_string(),
            )
        })?;
        match self
            .repository
            .set_provider_operational_profile_atomic(context.clone(), provider_id, desired)
            .await
            .map_err(ApiError::from_database)?
        {
            SetProviderOperationalProfilePersistenceOutcome::Applied(value) => {
                Ok(SetProviderOperationalProfileOutcome::Applied(value))
            }
            SetProviderOperationalProfilePersistenceOutcome::Replayed(value) => {
                Ok(SetProviderOperationalProfileOutcome::Replayed(value))
            }
            SetProviderOperationalProfilePersistenceOutcome::ProviderNotFound => {
                Err(ApiError::new(WurzburgResultCode::ProviderNotFound))
            }
            SetProviderOperationalProfilePersistenceOutcome::ProviderInactive => Err(
                ApiError::new(WurzburgResultCode::ProviderOperationalProfileInvalidState),
            ),
            SetProviderOperationalProfilePersistenceOutcome::IdempotencyConflict => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyKeyConflict))
            }
            SetProviderOperationalProfilePersistenceOutcome::IdempotencyInProgress => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyInProgress))
            }
            SetProviderOperationalProfilePersistenceOutcome::IdempotencyInvalidState => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyError))
            }
        }
    }

    #[tracing::instrument(skip(self, context, reason), fields(provider_id=%provider_id, provider_operational_profile_id=%profile_id))]
    pub async fn cancel_scheduled(
        &self,
        context: &MutationCommandContext,
        provider_id: Uuid,
        profile_id: Uuid,
        reason: String,
    ) -> Result<CancelProviderOperationalProfileOutcome, ApiError> {
        require_scope(&context.actor, "platform.providers:write")?;
        let reason = reason.trim().to_string();
        if reason.is_empty() || reason.chars().count() > 1000 {
            return Err(ApiError::with_message(
                WurzburgResultCode::ProviderOperationalProfileContractInvalid,
                "operational profile cancellation reason must contain 1 to 1000 characters",
            ));
        }
        match self
            .repository
            .cancel_provider_operational_profile_atomic(
                context.clone(),
                provider_id,
                profile_id,
                reason,
            )
            .await
            .map_err(ApiError::from_database)?
        {
            CancelProviderOperationalProfilePersistenceOutcome::Applied(value) => {
                Ok(CancelProviderOperationalProfileOutcome::Applied(value))
            }
            CancelProviderOperationalProfilePersistenceOutcome::Replayed(value) => {
                Ok(CancelProviderOperationalProfileOutcome::Replayed(value))
            }
            CancelProviderOperationalProfilePersistenceOutcome::ProviderNotFound => {
                Err(ApiError::new(WurzburgResultCode::ProviderNotFound))
            }
            CancelProviderOperationalProfilePersistenceOutcome::ProfileNotFound => Err(
                ApiError::new(WurzburgResultCode::ProviderOperationalProfileNotFound),
            ),
            CancelProviderOperationalProfilePersistenceOutcome::InvalidState => Err(ApiError::new(
                WurzburgResultCode::ProviderOperationalProfileInvalidState,
            )),
            CancelProviderOperationalProfilePersistenceOutcome::IdempotencyConflict => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyKeyConflict))
            }
            CancelProviderOperationalProfilePersistenceOutcome::IdempotencyInProgress => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyInProgress))
            }
            CancelProviderOperationalProfilePersistenceOutcome::IdempotencyInvalidState => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyError))
            }
        }
    }

    #[tracing::instrument(skip(self, actor), fields(provider_id=%provider_id))]
    pub async fn get_current(
        &self,
        actor: &TrustedActor,
        provider_id: Uuid,
    ) -> Result<ProviderOperationalProfileRecord, ApiError> {
        require_scope(actor, "platform.providers:read")?;
        self.repository
            .get_current_provider_operational_profile(provider_id)
            .await
            .map_err(ApiError::from_database)?
            .ok_or_else(|| ApiError::new(WurzburgResultCode::ProviderOperationalProfileNotFound))
    }

    #[tracing::instrument(skip(self, actor), fields(provider_id=%provider_id, limit))]
    pub async fn list(
        &self,
        actor: &TrustedActor,
        provider_id: Uuid,
        before_version: Option<i64>,
        limit: u16,
    ) -> Result<ProviderOperationalProfilePage, ApiError> {
        require_scope(actor, "platform.providers:read")?;
        if limit == 0 || limit > 100 || before_version.is_some_and(|value| value <= 0) {
            return Err(ApiError::new(WurzburgResultCode::InvalidProviderFilter));
        }
        let mut items = self
            .repository
            .list_provider_operational_profiles(provider_id, before_version, limit + 1)
            .await
            .map_err(ApiError::from_database)?
            .ok_or_else(|| ApiError::new(WurzburgResultCode::ProviderNotFound))?;
        let has_next = items.len() > usize::from(limit);
        if has_next {
            items.truncate(usize::from(limit));
        }
        let next_before_version = has_next.then(|| {
            items
                .last()
                .expect("non-empty operational profile page")
                .version
        });
        Ok(ProviderOperationalProfilePage {
            items,
            next_before_version,
        })
    }
}

pub struct ProviderOperationalProfileSchedulerHandle {
    shutdown: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl ProviderOperationalProfileSchedulerHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        let _ = self.task.await;
    }
}

pub fn start_provider_operational_profile_scheduler(
    repository: Arc<OracleRepository>,
    config: ProviderOperationalProfileSchedulerConfig,
) -> Option<ProviderOperationalProfileSchedulerHandle> {
    if !config.enabled {
        tracing::info!(
            worker.name = "provider-operational-profile-scheduler",
            "provider operational profile scheduler disabled"
        );
        return None;
    }
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(run_scheduler(repository, config, receiver));
    Some(ProviderOperationalProfileSchedulerHandle { shutdown, task })
}

async fn run_scheduler(
    repository: Arc<OracleRepository>,
    config: ProviderOperationalProfileSchedulerConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        if *shutdown.borrow() {
            break;
        }
        let span = tracing::info_span!(
            "provider.operational_profiles.promote_due",
            worker.name = "provider-operational-profile-scheduler",
            batch_size = config.batch_size
        );
        let result = repository
            .promote_due_provider_operational_profiles(config.batch_size)
            .instrument(span)
            .await;
        match result {
            Ok(promoted) if promoted > 0 => tracing::info!(
                promoted.count = promoted,
                "promoted due provider operational profiles"
            ),
            Ok(_) => {}
            Err(error) if !*shutdown.borrow() => tracing::error!(
                error.kind = error.diagnostic_kind(),
                "failed to promote due provider operational profiles"
            ),
            Err(_) => break,
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(config.poll_interval_ms)) => {},
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break; }
            }
        }
    }
    tracing::info!(
        worker.name = "provider-operational-profile-scheduler",
        "provider operational profile scheduler stopped"
    );
}

use tracing::Instrument;
