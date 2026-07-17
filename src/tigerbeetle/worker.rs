use anyhow::Result;
use std::sync::Arc;
use tigerbeetle_rustclient_tests_snapshot::Client as TbClient;
use tokio::sync::mpsc;
use tokio::time::{MissedTickBehavior, interval};

use super::commands::TbCommand;
use super::operations::{
    process_accounts, process_lookup_accounts, process_lookup_transfers, process_transfers,
};
use crate::config::Settings;
use crate::tigerbeetle::models::AppAccountBalance;

/// Spawns the background worker to handle batched requests for all TigerBeetle operations.
pub async fn start_tb_worker(
    config: Arc<Settings>,
    mut receiver: mpsc::Receiver<TbCommand>,
) -> Result<()> {
    let addresses = config.tigerbeetle.replica_addresses.join(",");

    let client = TbClient::new(config.tigerbeetle.cluster_id as u128, &addresses)
        .map_err(|e| anyhow::anyhow!("Failed to init TB client: {:?}", e))?;

    let client = Arc::new(client);
    let batch_max_size = config.tigerbeetle.batch_max_size;
    let timeout = config.tigerbeetle.batch_timeout();

    tokio::spawn(async move {
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
                cmd = receiver.recv() => {
                    match cmd {
                        Some(TbCommand::CreateTransfer { transfer, responder }) => {
                            transfer_batch.push(transfer);
                            transfer_responders.push(responder);
                            if transfer_batch.len() >= batch_max_size {
                                process_transfers(&client, &mut transfer_batch, &mut transfer_responders).await;
                            }
                        }
                        Some(TbCommand::CreateAccount { account, responder }) => {
                            account_batch.push(account);
                            account_responders.push(responder);
                            if account_batch.len() >= batch_max_size {
                                process_accounts(&client, &mut account_batch, &mut account_responders).await;
                            }
                        }
                        Some(TbCommand::LookupAccount { id, responder }) => {
                            lookup_acc_batch.push(id);
                            lookup_acc_responders.push(responder);
                            if lookup_acc_batch.len() >= batch_max_size {
                                process_lookup_accounts(&client, &mut lookup_acc_batch, &mut lookup_acc_responders).await;
                            }
                        }
                        Some(TbCommand::LookupAccounts { ids, response }) => {
                            let result = client
                                .lookup_accounts(&ids)
                                .await
                                .map_err(|e| e.to_string());

                            let _ = response.send(result);
                        }
                        Some(TbCommand::LookupTransfer { id, responder }) => {
                            lookup_tx_batch.push(id);
                            lookup_tx_responders.push(responder);
                            if lookup_tx_batch.len() >= batch_max_size {
                                process_lookup_transfers(&client, &mut lookup_tx_batch, &mut lookup_tx_responders).await;
                            }
                        }
                        Some(TbCommand::GetAccountBalances { ids, responder }) => {
                            let result = match client.lookup_accounts(&ids).await {
                                Ok(accounts) => {
                                    let balances = accounts.into_iter().map(|acc| {
                                        AppAccountBalance {
                                            account_id: acc.id,
                                            posted_balance: (acc.credits_posted as i128) - (acc.debits_posted as i128),
                                            pending_balance: (acc.credits_pending as i128) - (acc.debits_pending as i128),
                                        }
                                    }).collect();
                                    Ok(balances)
                                }
                                Err(e) => Err(e.to_string()),
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
    });

    Ok(())
}
