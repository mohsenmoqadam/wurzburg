use std::{sync::Arc, time::Duration};

use rand::RngExt;
use tokio::{sync::watch, task::JoinHandle};

use crate::{
    config::WalRecoveryConfig,
    db::oracle::{OracleRepository, WalRecoveryWork},
    services::provider_credit::ProviderCreditService,
    services::provider_user::ProviderUserService,
};

pub struct WalRecoveryHandle {
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl WalRecoveryHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        let _ = self.task.await;
    }
}

pub fn start_wal_recovery_worker(
    repository: Arc<OracleRepository>,
    provider_users: ProviderUserService,
    provider_credits: ProviderCreditService,
    config: WalRecoveryConfig,
) -> Option<WalRecoveryHandle> {
    if !config.enabled {
        return None;
    }
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(run(
        repository,
        provider_users,
        provider_credits,
        config,
        receiver,
    ));
    Some(WalRecoveryHandle { shutdown, task })
}

async fn run(
    repository: Arc<OracleRepository>,
    provider_users: ProviderUserService,
    provider_credits: ProviderCreditService,
    config: WalRecoveryConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        if *shutdown.borrow() {
            break;
        }
        match repository
            .claim_recoverable_wal(
                config.worker_id.clone(),
                config.batch_size,
                config.lease_duration_ms,
                config.stale_after_ms,
            )
            .await
        {
            Ok(jobs) if jobs.is_empty() => wait(&config, &mut shutdown).await,
            Ok(jobs) => {
                for job in jobs {
                    if *shutdown.borrow() {
                        break;
                    }
                    let span = tracing::info_span!(
                        "operation_wal.recover",
                        operation.id = %job.operation_id,
                        operation.type = %job.operation_type,
                        aggregate.id = %job.aggregate_id,
                        retry.count = job.attempt_count.saturating_add(1),
                    );
                    let result = match repository.load_wal_recovery_work(job.clone()).await {
                        Ok(Some(work)) => {
                            match &work {
                                WalRecoveryWork::ProviderUser { context, .. }
                                | WalRecoveryWork::CardIssuance { context, .. }
                                | WalRecoveryWork::ProviderCredit { context, .. } => {
                                    context.trace.link_to_span(&span);
                                }
                            }
                            process(&repository, &provider_users, &provider_credits, work)
                                .instrument(span)
                                .await
                        }
                        Ok(None) => Err("RECOVERY_INTENT_UNAVAILABLE"),
                        Err(error) => {
                            tracing::error!(
                                parent: &span,
                                error.kind = error.diagnostic_kind(),
                                "failed to load operation WAL recovery intent"
                            );
                            Err("RECOVERY_INTENT_LOAD_FAILED")
                        }
                    };
                    if let Err(error_code) = result {
                        let attempt = job.attempt_count.saturating_add(1);
                        let exhausted = attempt >= config.max_attempts;
                        if let Err(error) = repository
                            .reschedule_wal_recovery(
                                job.operation_id,
                                config.worker_id.clone(),
                                retry_backoff(&config, attempt).as_millis() as u64,
                                exhausted,
                                error_code,
                            )
                            .await
                        {
                            tracing::error!(
                                operation.id = %job.operation_id,
                                error.kind = error.diagnostic_kind(),
                                "failed to persist operation WAL recovery outcome"
                            );
                        }
                    }
                }
            }
            Err(error) => {
                tracing::error!(
                    error.kind = error.diagnostic_kind(),
                    "failed to claim operation WAL recovery work"
                );
                wait(&config, &mut shutdown).await;
            }
        }
        if let Err(error) = repository
            .recover_ready_issuance_batches(config.batch_size)
            .await
        {
            tracing::error!(
                error.kind = error.diagnostic_kind(),
                "failed to recover ready card-issuance batches"
            );
        }
    }
}

async fn process(
    repository: &OracleRepository,
    provider_users: &ProviderUserService,
    provider_credits: &ProviderCreditService,
    work: WalRecoveryWork,
) -> Result<(), &'static str> {
    match work {
        WalRecoveryWork::ProviderUser { context, intent } => {
            provider_users
                .provision_accounts(&intent)
                .await
                .map_err(|_| "TIGERBEETLE_PROVISIONING_FAILED")?;
            repository
                .finalize_existing_card_enrollment_atomic(context, intent)
                .await
                .map_err(|_| "PROVIDER_USER_FINALIZATION_FAILED")?;
        }
        WalRecoveryWork::CardIssuance {
            context,
            intent,
            finalization,
        } => {
            provider_users
                .provision_issued_card(&intent)
                .await
                .map_err(|_| "TIGERBEETLE_PROVISIONING_FAILED")?;
            repository
                .finalize_issued_card_atomic(&context, &intent, &finalization.into_result())
                .await
                .map_err(|_| "CARD_ISSUANCE_FINALIZATION_FAILED")?;
        }
        WalRecoveryWork::ProviderCredit {
            context,
            intent,
            card_number,
        } => {
            provider_credits
                .recover(context, intent, card_number)
                .await
                .map_err(|_| "PROVIDER_CREDIT_RECOVERY_FAILED")?;
        }
    }
    Ok(())
}

async fn wait(config: &WalRecoveryConfig, shutdown: &mut watch::Receiver<bool>) {
    tokio::select! {
        _ = tokio::time::sleep(config.poll_interval()) => {},
        _ = shutdown.changed() => {}
    }
}

fn retry_backoff(config: &WalRecoveryConfig, attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(31);
    let base = config
        .initial_backoff_ms
        .saturating_mul(1_u64 << exponent)
        .min(config.max_backoff_ms);
    let jitter = rand::rng().random_range(0..=(base / 5).max(1));
    Duration::from_millis(base.saturating_add(jitter).min(config.max_backoff_ms))
}

use tracing::Instrument;
