use std::sync::Arc;

use tokio::{sync::watch, task::JoinHandle};

use crate::{
    config::CardProfileLockConfig, db::oracle::OracleRepository,
    runtime_profiles::CardProfileLockManager,
};

pub struct CardProfileCoordinatorHandle {
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl CardProfileCoordinatorHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        let _ = self.task.await;
    }
}

pub fn start_card_profile_coordinator(
    repository: Arc<OracleRepository>,
    locks: Arc<CardProfileLockManager>,
    config: CardProfileLockConfig,
) -> Option<CardProfileCoordinatorHandle> {
    if !config.coordinator_enabled {
        return None;
    }
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(run(repository, locks, config, receiver));
    Some(CardProfileCoordinatorHandle { shutdown, task })
}

#[tracing::instrument(skip(repository, locks, config, shutdown), fields(worker.name="card_profile_coordinator"))]
async fn run(
    repository: Arc<OracleRepository>,
    locks: Arc<CardProfileLockManager>,
    config: CardProfileLockConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        if *shutdown.borrow() {
            break;
        }
        match repository
            .list_pending_card_profile_operations(config.coordinator_batch_size)
            .await
        {
            Ok(pending) => {
                for operation in pending {
                    if *shutdown.borrow() {
                        break;
                    }
                    match locks
                        .ensure_pending(&operation.card_number, operation.operation_id)
                        .await
                    {
                        Ok(true) => tracing::debug!(
                            card.id = %operation.card_id,
                            operation.id = %operation.operation_id,
                            "pending card profile lock is owned and stale CP is absent"
                        ),
                        Ok(false) => tracing::warn!(
                            card.id = %operation.card_id,
                            operation.id = %operation.operation_id,
                            "pending card profile is temporarily owned by another operation"
                        ),
                        Err(error) => tracing::error!(
                            card.id = %operation.card_id,
                            operation.id = %operation.operation_id,
                            error.kind = error.diagnostic_kind(),
                            "failed to coordinate pending card profile"
                        ),
                    }
                }
            }
            Err(error) => tracing::error!(
                error.kind = error.diagnostic_kind(),
                "failed to load pending card profile operations"
            ),
        }
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break; }
            }
            () = tokio::time::sleep(config.renew_interval()) => {}
        }
    }
}
