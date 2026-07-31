use std::{sync::Arc, time::Duration};

use rand::RngExt;
use tokio::sync::watch;

use crate::{
    config::ProviderCoreProvisioningConfig, db::oracle::OracleRepository,
    services::provider::ProviderService,
};

pub struct ProviderProvisioningHandle {
    shutdown: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl ProviderProvisioningHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        let _ = self.task.await;
    }
}

pub fn start_provider_provisioning_worker(
    repository: Arc<OracleRepository>,
    service: ProviderService,
    config: ProviderCoreProvisioningConfig,
) -> Option<ProviderProvisioningHandle> {
    if !config.enabled {
        tracing::info!(
            worker.name = "provider-core-provisioning",
            "provider provisioning worker disabled"
        );
        return None;
    }
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(run_worker(repository, service, config, receiver));
    Some(ProviderProvisioningHandle { shutdown, task })
}

async fn run_worker(
    repository: Arc<OracleRepository>,
    service: ProviderService,
    config: ProviderCoreProvisioningConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        if *shutdown.borrow() {
            break;
        }
        match repository
            .claim_provider_core_jobs(
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
                        "provider.provisioning.recover",
                        provider_id = %job.provider_id,
                        provisioning.job_id = %job.job_id,
                        retry.count = job.attempt_count.saturating_add(1)
                    );
                    let _guard = span.enter();
                    if let Err(error) = service.recover_provider_core(job.provider_id).await {
                        let attempt = job.attempt_count.saturating_add(1);
                        let exhausted = attempt >= config.max_attempts;
                        let backoff = retry_backoff(&config, attempt);
                        if let Err(db_error) = repository
                            .retry_or_fail_provider_core_job(
                                job.job_id,
                                job.provider_id,
                                config.worker_id.clone(),
                                backoff.as_millis() as u64,
                                exhausted,
                                error.diagnostic_kind(),
                            )
                            .await
                        {
                            tracing::error!(
                                error.kind = db_error.diagnostic_kind(),
                                "failed to persist provider provisioning recovery outcome"
                            );
                        }
                    }
                }
            }
            Err(error) => {
                if *shutdown.borrow() {
                    break;
                }
                tracing::error!(
                    error.kind = error.diagnostic_kind(),
                    "failed to claim provider provisioning jobs"
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
        worker.name = "provider-core-provisioning",
        "provider provisioning worker stopped"
    );
}

fn retry_backoff(config: &ProviderCoreProvisioningConfig, attempt: u32) -> Duration {
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
    fn provider_core_provisioning_backoff_is_bounded() {
        let settings = Settings::new().expect("settings should load");
        for attempt in [1, 2, 10, u32::MAX] {
            assert!(
                retry_backoff(&settings.provider_core_provisioning, attempt).as_millis()
                    <= u128::from(settings.provider_core_provisioning.max_backoff_ms)
            );
        }
    }
}
