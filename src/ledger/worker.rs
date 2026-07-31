use anyhow::Result;
use std::sync::{Arc, Mutex};
use tigerbeetle_rustclient_tests_snapshot::Client as TbClient;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval};

use super::commands::LedgerCommand;
use super::operations::{
    process_accounts, process_lookup_accounts, process_lookup_transfers, process_transfers,
};
use crate::config::Settings;
use crate::ledger::TigerBeetleError;
use crate::ledger::models::LedgerAccountBalance;

#[derive(Clone)]
pub struct LedgerWorkerHandle {
    shutdown: watch::Sender<bool>,
    task: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl LedgerWorkerHandle {
    pub async fn shutdown(&self) {
        let _ = self.shutdown.send(true);
        let task = self
            .task
            .lock()
            .expect("TigerBeetle worker handle lock poisoned")
            .take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }
}

/// Spawn the owned background worker that batches all TigerBeetle operations.
pub fn start_ledger_worker(
    config: Arc<Settings>,
    receiver: mpsc::Receiver<LedgerCommand>,
) -> LedgerWorkerHandle {
    let (shutdown, shutdown_receiver) = watch::channel(false);
    let task = tokio::spawn(async move {
        if let Err(error) = run_ledger_worker(config, receiver, shutdown_receiver).await {
            tracing::error!(error = ?error, "TigerBeetle worker failed");
        }
    });
    LedgerWorkerHandle {
        shutdown,
        task: Arc::new(Mutex::new(Some(task))),
    }
}

async fn run_ledger_worker(
    config: Arc<Settings>,
    mut receiver: mpsc::Receiver<LedgerCommand>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let addresses = config.tigerbeetle.replica_addresses.join(",");

    let client = TbClient::new(config.tigerbeetle.cluster_id as u128, &addresses)
        .map_err(|e| anyhow::anyhow!("Failed to init TB client: {:?}", e))?;

    let client = Arc::new(client);
    let batch_max_size = config.tigerbeetle.batch_max_size;
    let timeout = config.tigerbeetle.batch_timeout();

    // Buffers for transfers
    let mut transfer_batch = Vec::with_capacity(batch_max_size);
    let mut transfer_responders = Vec::with_capacity(batch_max_size);

    // Buffers for accounts
    let mut account_batch = Vec::with_capacity(batch_max_size);
    let mut account_responders = Vec::with_capacity(batch_max_size);

    // Buffers for account lookups
    let mut lookup_acc_batch = Vec::with_capacity(batch_max_size);
    let mut lookup_acc_responders = Vec::with_capacity(batch_max_size);

    // Buffers for transfer lookups
    let mut lookup_tx_batch = Vec::with_capacity(batch_max_size);
    let mut lookup_tx_responders = Vec::with_capacity(batch_max_size);

    let mut timer = interval(timeout);
    timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                cmd = receiver.recv() => {
                    match cmd {
                        Some(LedgerCommand::CreateTransfer { transfer, responder }) => {
                            transfer_batch.push(transfer);
                            transfer_responders.push(responder);
                            if transfer_batch.len() >= batch_max_size {
                                process_transfers(&client, &mut transfer_batch, &mut transfer_responders).await;
                            }
                        }
                        Some(LedgerCommand::CreateAccount { account, responder }) => {
                            account_batch.push(account);
                            account_responders.push(responder);
                            if account_batch.len() >= batch_max_size {
                                process_accounts(&client, &mut account_batch, &mut account_responders).await;
                            }
                        }
                        Some(LedgerCommand::LookupAccount { id, responder }) => {
                            lookup_acc_batch.push(id);
                            lookup_acc_responders.push(responder);
                            if lookup_acc_batch.len() >= batch_max_size {
                                process_lookup_accounts(&client, &mut lookup_acc_batch, &mut lookup_acc_responders).await;
                            }
                        }
                        Some(LedgerCommand::LookupAccounts { ids, response }) => {
                            let result = client
                                .lookup_accounts(&ids)
                                .await
                                .map_err(|_| TigerBeetleError::ClientFailure { operation: "lookup_accounts" });

                            let _ = response.send(result);
                        }
                        Some(LedgerCommand::LookupTransfer { id, responder }) => {
                            lookup_tx_batch.push(id);
                            lookup_tx_responders.push(responder);
                            if lookup_tx_batch.len() >= batch_max_size {
                                process_lookup_transfers(&client, &mut lookup_tx_batch, &mut lookup_tx_responders).await;
                            }
                        }
                        Some(LedgerCommand::GetAccountBalances { ids, responder }) => {
                            let result = match client.lookup_accounts(&ids).await {
                                Ok(accounts) => {
                                    let balances = accounts.into_iter().map(|acc| {
                                        LedgerAccountBalance {
                                            account_id: acc.id,
                                            posted_balance: (acc.credits_posted as i128) - (acc.debits_posted as i128),
                                            pending_balance: (acc.credits_pending as i128) - (acc.debits_pending as i128),
                                        }
                                    }).collect();
                                    Ok(balances)
                                }
                                Err(_) => Err(TigerBeetleError::ClientFailure { operation: "lookup_accounts" }),
                            };
                            let _ = responder.send(result);
                        }
                        None => {
                            tracing::info!("TigerBeetle worker channel closed. Exiting worker loop.");
                            break;
                        }
                    }
                }
                _ = timer.tick() => {
                    // Flush pending batches upon timer tick regardless of size
                    if !transfer_batch.is_empty() {
                        process_transfers(&client, &mut transfer_batch, &mut transfer_responders).await;
                    }
                    if !account_batch.is_empty() {
                        process_accounts(&client, &mut account_batch, &mut account_responders).await;
                    }
                    if !lookup_acc_batch.is_empty() {
                        process_lookup_accounts(&client, &mut lookup_acc_batch, &mut lookup_acc_responders).await;
                    }
                    if !lookup_tx_batch.is_empty() {
                        process_lookup_transfers(&client, &mut lookup_tx_batch, &mut lookup_tx_responders).await;
                    }
                }
        }
    }

    // Requests already accepted into a batch remain part of the graceful
    // drain. The process-level deadline still bounds an unavailable cluster.
    if !transfer_batch.is_empty() {
        process_transfers(&client, &mut transfer_batch, &mut transfer_responders).await;
    }
    if !account_batch.is_empty() {
        process_accounts(&client, &mut account_batch, &mut account_responders).await;
    }
    if !lookup_acc_batch.is_empty() {
        process_lookup_accounts(&client, &mut lookup_acc_batch, &mut lookup_acc_responders).await;
    }
    if !lookup_tx_batch.is_empty() {
        process_lookup_transfers(&client, &mut lookup_tx_batch, &mut lookup_tx_responders).await;
    }
    tracing::info!(
        worker.name = "tigerbeetle-batch",
        "TigerBeetle worker stopped"
    );

    Ok(())
}
