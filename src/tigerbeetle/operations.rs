use super::mapper;
use super::models::{AppAccount, AppCreateAccountsResult, AppCreateTransfersResult, AppTransfer};
use std::sync::Arc;
use tigerbeetle_rustclient_tests_snapshot::Client as TbClient;
use tokio::sync::oneshot;

/// Executes the batch operation for creating transfers, maps models, and notifies responders.
pub async fn process_transfers(
    client: &Arc<TbClient>,
    batch: &mut Vec<AppTransfer>,
    responders: &mut Vec<oneshot::Sender<Result<Vec<AppCreateTransfersResult>, String>>>,
) {
    let tb_batch: Vec<_> = batch.iter().map(mapper::to_tb_transfer).collect();

    let results = match client.create_transfers(&tb_batch).await {
        Ok(tb_results) => Ok(tb_results
            .iter()
            .map(mapper::from_tb_transfer_result)
            .collect()),
        Err(e) => Err(format!("TigerBeetle create_transfers error: {:?}", e)),
    };

    for responder in responders.drain(..) {
        let _ = responder.send(results.clone());
    }
    batch.clear();
}

/// Executes the batch operation for creating accounts, maps models, and notifies responders.
pub async fn process_accounts(
    client: &Arc<TbClient>,
    batch: &mut Vec<AppAccount>,
    responders: &mut Vec<oneshot::Sender<Result<Vec<AppCreateAccountsResult>, String>>>,
) {
    let tb_batch: Vec<_> = batch.iter().map(mapper::to_tb_account).collect();

    let results = match client.create_accounts(&tb_batch).await {
        Ok(tb_results) => Ok(tb_results
            .iter()
            .map(mapper::from_tb_account_result)
            .collect()),
        Err(e) => Err(format!("TigerBeetle create_accounts error: {:?}", e)),
    };

    for responder in responders.drain(..) {
        let _ = responder.send(results.clone());
    }
    batch.clear();
}

/// Executes the batch operation for looking up accounts, maps models, and notifies responders.
pub async fn process_lookup_accounts(
    client: &Arc<TbClient>,
    batch: &mut Vec<u128>,
    responders: &mut Vec<oneshot::Sender<Result<Vec<AppAccount>, String>>>,
) {
    let results = match client.lookup_accounts(batch).await {
        Ok(tb_results) => Ok(tb_results.iter().map(mapper::from_tb_account).collect()),
        Err(e) => Err(format!("TigerBeetle lookup_accounts error: {:?}", e)),
    };

    for responder in responders.drain(..) {
        let _ = responder.send(results.clone());
    }
    batch.clear();
}

/// Executes the batch operation for looking up transfers, maps models, and notifies responders.
pub async fn process_lookup_transfers(
    client: &Arc<TbClient>,
    batch: &mut Vec<u128>,
    responders: &mut Vec<oneshot::Sender<Result<Vec<AppTransfer>, String>>>,
) {
    let results = match client.lookup_transfers(batch).await {
        Ok(tb_results) => Ok(tb_results.iter().map(mapper::from_tb_transfer).collect()),
        Err(e) => Err(format!("TigerBeetle lookup_transfers error: {:?}", e)),
    };

    for responder in responders.drain(..) {
        let _ = responder.send(results.clone());
    }
    batch.clear();
}
