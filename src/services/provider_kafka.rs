use std::{sync::Arc, time::Duration};

use rand::RngExt;
use tokio::sync::watch;
use tracing::Instrument;

use crate::{
    api::{
        auth::{TrustedActor, require_scope},
        command::MutationCommandContext,
        error::ApiError,
        result_codes::WurzburgResultCode,
    },
    config::ProviderKafkaAccessConfig,
    db::oracle::{
        OracleRepository, ProviderKafkaAccessRecord, ProviderKafkaCommandAction,
        ProviderKafkaCommandOutcome, ProviderKafkaCredentialReadOutcome, ProviderKafkaJobType,
        ProviderKafkaRetryDecision,
    },
    domain::audit::TrustedAuditContext,
    messaging::{KafkaAdminError, MessageBrokerAdmin, ProviderKafkaAccessSpec},
    security::provider_kafka_cipher::{CredentialCipherError, ProviderKafkaCredentialFactory},
};

pub struct ProviderKafkaCredentialBundle {
    pub provider_id: uuid::Uuid,
    pub topic_name: String,
    pub bootstrap_servers: Vec<String>,
    pub security_protocol: String,
    pub sasl_mechanism: String,
    pub username: String,
    pub consumer_group: String,
    pub password: crate::security::provider_kafka_cipher::SecretBytes,
    pub security_cert: Option<String>,
    pub credential_version: u64,
    pub credential_status: String,
}

pub struct ProviderKafkaAccessStatus {
    pub provider_id: uuid::Uuid,
    pub access_status: String,
    pub active_credential_version: Option<u64>,
    pub candidate_credential_version: Option<u64>,
    pub latest_operation: Option<crate::db::oracle::ProviderKafkaJobStatusRecord>,
}

pub enum ProviderKafkaCommandResult {
    Accepted(serde_json::Value),
    Replayed(serde_json::Value),
}

pub struct ProviderKafkaProvisioningHandle {
    shutdown: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl ProviderKafkaProvisioningHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        let _ = self.task.await;
    }
}

pub fn start_provider_kafka_provisioning_worker(
    repository: Arc<OracleRepository>,
    service: ProviderKafkaService,
    config: ProviderKafkaAccessConfig,
) -> Option<ProviderKafkaProvisioningHandle> {
    if !config.enabled {
        tracing::info!(
            worker.name = "provider-kafka-access-provisioning",
            "Provider Kafka provisioning worker disabled"
        );
        return None;
    }
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(run_worker(repository, service, config, receiver));
    Some(ProviderKafkaProvisioningHandle { shutdown, task })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderKafkaProvisioningError {
    Persistence,
    Cipher(CredentialCipherError),
    Kafka(KafkaAdminError),
}

impl ProviderKafkaProvisioningError {
    pub fn diagnostic_kind(self) -> &'static str {
        match self {
            Self::Persistence => "persistence",
            Self::Cipher(error) => error.diagnostic_kind(),
            Self::Kafka(error) => error.diagnostic_kind(),
        }
    }
}

async fn run_worker(
    repository: Arc<OracleRepository>,
    service: ProviderKafkaService,
    config: ProviderKafkaAccessConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        if *shutdown.borrow() {
            break;
        }
        match repository
            .claim_provider_kafka_jobs(
                config.worker_id.clone(),
                config.batch_size,
                config.lease_duration_ms,
            )
            .await
        {
            Ok(jobs) if jobs.is_empty() => {
                tokio::select! {
                    _ = tokio::time::sleep(config.poll_interval()) => {},
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { break; }
                    }
                }
            }
            Ok(jobs) => {
                for job in jobs {
                    let span = tracing::info_span!(
                        "provider.kafka.provisioning.recover",
                        provider_id = %job.provider_id,
                        provisioning.job_id = %job.job_id,
                        provisioning.job.type = ?job.job_type,
                        retry.count = job.attempt_count.saturating_add(1)
                    );
                    async {
                        match service.execute(job.provider_id, job.job_type).await {
                            Ok(()) => {
                                if let Err(error) = repository
                                    .complete_provider_kafka_job(
                                        job.job_id,
                                        job.provider_id,
                                        job.job_type,
                                        config.worker_id.clone(),
                                    )
                                    .await
                                {
                                    tracing::error!(
                                        error.kind = error.diagnostic_kind(),
                                        "failed to finalize Provider Kafka provisioning"
                                    );
                                }
                            }
                            Err(error) => {
                                let attempt = job.attempt_count.saturating_add(1);
                                let exhausted = attempt >= config.max_attempts;
                                let backoff = retry_backoff(&config, attempt);
                                tracing::warn!(
                                    error.kind = error.diagnostic_kind(),
                                    error = ?error,
                                    retry.attempt = attempt,
                                    retry.exhausted = exhausted,
                                    retry.backoff_ms = backoff.as_millis() as u64,
                                    "Provider Kafka provisioning boundary failed"
                                );
                                if let Err(db_error) = repository
                                    .retry_or_fail_provider_kafka_job(
                                        job.job_id,
                                        job.provider_id,
                                        job.job_type,
                                        config.worker_id.clone(),
                                        ProviderKafkaRetryDecision {
                                            backoff_ms: backoff.as_millis() as u64,
                                            exhausted,
                                            error_code: error.diagnostic_kind(),
                                        },
                                    )
                                    .await
                                {
                                    tracing::error!(
                                        error.kind = db_error.diagnostic_kind(),
                                        "failed to persist Provider Kafka provisioning outcome"
                                    );
                                }
                            }
                        }
                    }
                    .instrument(span)
                    .await;
                }
            }
            Err(error) => {
                if *shutdown.borrow() {
                    break;
                }
                tracing::error!(
                    error.kind = error.diagnostic_kind(),
                    "failed to claim Provider Kafka provisioning jobs"
                );
                tokio::select! {
                    _ = tokio::time::sleep(config.poll_interval()) => {},
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { break; }
                    }
                }
            }
        }
    }
    tracing::info!(
        worker.name = "provider-kafka-access-provisioning",
        "Provider Kafka provisioning worker stopped"
    );
}

fn retry_backoff(config: &ProviderKafkaAccessConfig, attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(31);
    let base = config
        .initial_backoff_ms
        .saturating_mul(1_u64 << exponent)
        .min(config.max_backoff_ms);
    let jitter = rand::rng().random_range(0..=(base / 5).max(1));
    Duration::from_millis(base.saturating_add(jitter).min(config.max_backoff_ms))
}

#[cfg(test)]
mod tests {
    use super::retry_backoff;
    use crate::config::Settings;

    #[test]
    fn provider_kafka_retry_backoff_is_bounded() {
        let settings = Settings::new().expect("settings should load");
        for attempt in [1, 2, 10, u32::MAX] {
            assert!(
                retry_backoff(&settings.provider_kafka_access, attempt).as_millis()
                    <= u128::from(settings.provider_kafka_access.max_backoff_ms)
            );
        }
    }
}

#[derive(Clone)]
pub struct ProviderKafkaService {
    repository: Arc<OracleRepository>,
    admin: MessageBrokerAdmin,
    credentials: Arc<ProviderKafkaCredentialFactory>,
    scram_iterations: i32,
}

impl ProviderKafkaService {
    pub fn new(
        repository: Arc<OracleRepository>,
        admin: MessageBrokerAdmin,
        credentials: Arc<ProviderKafkaCredentialFactory>,
        scram_iterations: i32,
    ) -> Self {
        Self {
            repository,
            admin,
            credentials,
            scram_iterations,
        }
    }

    #[tracing::instrument(skip(self), fields(provider_id=%provider_id))]
    pub async fn execute(
        &self,
        provider_id: uuid::Uuid,
        job_type: ProviderKafkaJobType,
    ) -> Result<(), ProviderKafkaProvisioningError> {
        match job_type {
            ProviderKafkaJobType::Provision | ProviderKafkaJobType::Rotate => {
                self.provision_or_rotate(provider_id).await
            }
            ProviderKafkaJobType::Suspend => self.revoke(provider_id).await,
            ProviderKafkaJobType::Resume => self.resume(provider_id).await,
        }
    }

    #[tracing::instrument(skip(self, actor, audit_context), fields(provider_id=%provider_id))]
    pub async fn read_credentials(
        &self,
        actor: &TrustedActor,
        audit_context: TrustedAuditContext,
        provider_id: uuid::Uuid,
    ) -> Result<ProviderKafkaCredentialBundle, ApiError> {
        require_scope(actor, "provider.kafka_credentials:read")?;
        if actor
            .provider_id
            .is_some_and(|actor_provider_id| actor_provider_id != provider_id)
        {
            return Err(ApiError::new(WurzburgResultCode::ProviderScopeMismatch));
        }
        let access = match self
            .repository
            .read_active_provider_kafka_credential(provider_id, audit_context)
            .await
            .map_err(ApiError::from_database)?
        {
            ProviderKafkaCredentialReadOutcome::NotConfigured => {
                return Err(ApiError::new(
                    WurzburgResultCode::ProviderKafkaAccessNotFound,
                ));
            }
            ProviderKafkaCredentialReadOutcome::NotReady => {
                return Err(ApiError::new(
                    WurzburgResultCode::ProviderKafkaCredentialNotAvailable,
                ));
            }
            ProviderKafkaCredentialReadOutcome::Available(access) => *access,
        };
        let credential_version = access.credential_version.ok_or_else(|| {
            ApiError::new(WurzburgResultCode::ProviderKafkaCredentialNotAvailable)
        })?;
        let password = self
            .credentials
            .cipher()
            .decrypt(
                provider_id,
                credential_version,
                access
                    .encryption_key_version
                    .as_deref()
                    .ok_or_else(|| ApiError::new(WurzburgResultCode::SystemError))?,
                access
                    .password_ciphertext
                    .as_deref()
                    .ok_or_else(|| ApiError::new(WurzburgResultCode::SystemError))?,
            )
            .map_err(|error| {
                tracing::error!(
                    error.kind = error.diagnostic_kind(),
                    "failed to decrypt active Provider Kafka credential"
                );
                ApiError::new(WurzburgResultCode::SystemError)
            })?;
        Ok(ProviderKafkaCredentialBundle {
            provider_id,
            topic_name: access.topic_name,
            bootstrap_servers: access.bootstrap_servers,
            security_protocol: access.security_protocol,
            sasl_mechanism: access.sasl_mechanism,
            username: access.username,
            consumer_group: access.consumer_group,
            password,
            security_cert: self.credentials.security_cert().map(ToString::to_string),
            credential_version,
            credential_status: access.credential_status,
        })
    }

    #[tracing::instrument(skip(self, actor), fields(provider_id=%provider_id))]
    pub async fn read_status(
        &self,
        actor: &TrustedActor,
        provider_id: uuid::Uuid,
    ) -> Result<ProviderKafkaAccessStatus, ApiError> {
        require_scope(actor, "provider.kafka_credentials:read")?;
        if actor
            .provider_id
            .is_some_and(|actor_provider_id| actor_provider_id != provider_id)
        {
            return Err(ApiError::new(WurzburgResultCode::ProviderScopeMismatch));
        }
        let status = self
            .repository
            .get_provider_kafka_status(provider_id)
            .await
            .map_err(ApiError::from_database)?
            .ok_or_else(|| ApiError::new(WurzburgResultCode::ProviderKafkaAccessNotFound))?;
        Ok(ProviderKafkaAccessStatus {
            provider_id: status.provider_id,
            access_status: status.access_status,
            active_credential_version: status.active_credential_version,
            candidate_credential_version: status.candidate_credential_version,
            latest_operation: status.latest_operation,
        })
    }

    #[tracing::instrument(skip(self, context, reason), fields(provider_id=%provider_id, provider.kafka.action=?action))]
    pub async fn command_access(
        &self,
        context: &MutationCommandContext,
        provider_id: uuid::Uuid,
        action: ProviderKafkaCommandAction,
        reason: String,
    ) -> Result<ProviderKafkaCommandResult, ApiError> {
        require_scope(&context.actor, "provider.kafka_credentials:rotate")?;
        if context
            .actor
            .provider_id
            .is_some_and(|actor_provider_id| actor_provider_id != provider_id)
        {
            return Err(ApiError::new(WurzburgResultCode::ProviderScopeMismatch));
        }
        let reason = reason.trim().to_string();
        if reason.is_empty() || reason.len() > 1000 || reason.chars().any(char::is_control) {
            return Err(ApiError::new(WurzburgResultCode::InvalidProviderContract));
        }
        let credential = if matches!(
            action,
            ProviderKafkaCommandAction::Provision | ProviderKafkaCommandAction::Rotate
        ) {
            let version = self
                .repository
                .next_provider_kafka_credential_version(provider_id)
                .await
                .map_err(ApiError::from_database)?;
            Some(
                self.credentials
                    .prepare_credential(provider_id, version)
                    .map_err(|error| {
                        tracing::error!(
                            error.kind = error.diagnostic_kind(),
                            "failed to prepare Provider Kafka credential candidate"
                        );
                        ApiError::new(WurzburgResultCode::SystemError)
                    })?,
            )
        } else {
            None
        };
        match self
            .repository
            .command_provider_kafka_access_atomic(
                context.clone(),
                provider_id,
                action,
                credential,
                reason,
            )
            .await
            .map_err(ApiError::from_database)?
        {
            ProviderKafkaCommandOutcome::Applied(snapshot) => {
                Ok(ProviderKafkaCommandResult::Accepted(snapshot))
            }
            ProviderKafkaCommandOutcome::Replayed(snapshot) => {
                Ok(ProviderKafkaCommandResult::Replayed(snapshot))
            }
            ProviderKafkaCommandOutcome::NotConfigured => Err(ApiError::new(
                WurzburgResultCode::ProviderKafkaAccessNotFound,
            )),
            ProviderKafkaCommandOutcome::InvalidState => Err(ApiError::new(
                WurzburgResultCode::ProviderKafkaInvalidTransition,
            )),
            ProviderKafkaCommandOutcome::VersionConflict => Err(ApiError::new(
                WurzburgResultCode::ProviderKafkaCredentialVersionConflict,
            )),
            ProviderKafkaCommandOutcome::IdempotencyConflict => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyKeyConflict))
            }
            ProviderKafkaCommandOutcome::IdempotencyInProgress => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyInProgress))
            }
            ProviderKafkaCommandOutcome::IdempotencyInvalidState => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyError))
            }
        }
    }

    async fn provision_or_rotate(
        &self,
        provider_id: uuid::Uuid,
    ) -> Result<(), ProviderKafkaProvisioningError> {
        let access = self
            .repository
            .get_provider_kafka_candidate(provider_id)
            .await
            .map_err(|_| ProviderKafkaProvisioningError::Persistence)?
            .ok_or(ProviderKafkaProvisioningError::Persistence)?;
        let credential_version = access
            .credential_version
            .ok_or(ProviderKafkaProvisioningError::Persistence)?;
        let key_version = access
            .encryption_key_version
            .as_deref()
            .ok_or(ProviderKafkaProvisioningError::Persistence)?;
        let ciphertext = access
            .password_ciphertext
            .as_deref()
            .ok_or(ProviderKafkaProvisioningError::Persistence)?;
        let secret = self
            .credentials
            .cipher()
            .decrypt(provider_id, credential_version, key_version, ciphertext)
            .map_err(ProviderKafkaProvisioningError::Cipher)?;
        self.admin
            .provision_provider_access(
                access_spec(&access),
                secret.expose().to_vec(),
                self.scram_iterations,
            )
            .await
            .map_err(ProviderKafkaProvisioningError::Kafka)
    }

    async fn revoke(&self, provider_id: uuid::Uuid) -> Result<(), ProviderKafkaProvisioningError> {
        let access = self
            .repository
            .get_active_provider_kafka_access(provider_id)
            .await
            .map_err(|_| ProviderKafkaProvisioningError::Persistence)?
            .ok_or(ProviderKafkaProvisioningError::Persistence)?;
        self.admin
            .revoke_provider_access(access_spec(&access))
            .await
            .map_err(ProviderKafkaProvisioningError::Kafka)
    }

    async fn resume(&self, provider_id: uuid::Uuid) -> Result<(), ProviderKafkaProvisioningError> {
        let access = self
            .repository
            .get_active_provider_kafka_access(provider_id)
            .await
            .map_err(|_| ProviderKafkaProvisioningError::Persistence)?
            .ok_or(ProviderKafkaProvisioningError::Persistence)?;
        let credential_version = access
            .credential_version
            .ok_or(ProviderKafkaProvisioningError::Persistence)?;
        let secret = self
            .credentials
            .cipher()
            .decrypt(
                provider_id,
                credential_version,
                access
                    .encryption_key_version
                    .as_deref()
                    .ok_or(ProviderKafkaProvisioningError::Persistence)?,
                access
                    .password_ciphertext
                    .as_deref()
                    .ok_or(ProviderKafkaProvisioningError::Persistence)?,
            )
            .map_err(ProviderKafkaProvisioningError::Cipher)?;
        self.admin
            .provision_provider_access(
                access_spec(&access),
                secret.expose().to_vec(),
                self.scram_iterations,
            )
            .await
            .map_err(ProviderKafkaProvisioningError::Kafka)
    }
}

fn access_spec(access: &ProviderKafkaAccessRecord) -> ProviderKafkaAccessSpec {
    ProviderKafkaAccessSpec {
        topic_name: access.topic_name.clone(),
        username: access.username.clone(),
        consumer_group: access.consumer_group.clone(),
    }
}
