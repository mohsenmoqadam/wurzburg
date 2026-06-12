use anyhow::Result;
use tigerbeetle_rustclient_tests_snapshot::Account;
use tokio::sync::{mpsc, oneshot};
use crate::config::Settings;
use crate::tigerbeetle::models::AppAccountBalance;
use super::commands::TbCommand;
use super::models::{
    AppAccount, AppCreateAccountsResult, AppCreateTransfersResult, AppTransfer,
};

#[derive(Clone)]
pub struct AppTbClient {
    pub sender: mpsc::Sender<TbCommand>,
}

impl AppTbClient {
    /// Initializes the MPSC channel for batching commands to the background worker.
    pub fn new(config: &Settings) -> Result<(Self, mpsc::Receiver<TbCommand>)> {
        let (sender, receiver) = mpsc::channel(config.tigerbeetle.channel_capacity);
        Ok((Self { sender }, receiver))
    }

    /// Submits a transfer to be batched and awaits its result.
    #[tracing::instrument(skip(self))]
    pub async fn create_transfer(
        &self,
        transfer: AppTransfer,
    ) -> Result<Vec<AppCreateTransfersResult>, String> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.sender
            .send(TbCommand::CreateTransfer { transfer, responder: resp_tx })
            .await
            .map_err(|_| "Failed to send transfer to worker".to_string())?;

        resp_rx.await.map_err(|_| "Worker dropped response channel".to_string())?
    }

    /// Submits an account to be batched and awaits its result.
    #[tracing::instrument(skip(self))]
    pub async fn create_account(
        &self,
        account: AppAccount,
    ) -> Result<Vec<AppCreateAccountsResult>, String> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.sender
            .send(TbCommand::CreateAccount { account, responder: resp_tx })
            .await
            .map_err(|_| "Failed to send account to worker".to_string())?;

        resp_rx.await.map_err(|_| "Worker dropped response channel".to_string())?
    }

    /// Looks up a single account by ID (batched internally) and awaits its data.
    #[tracing::instrument(skip(self))]
    pub async fn lookup_account(&self, id: u128) -> Result<Vec<AppAccount>, String> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.sender
            .send(TbCommand::LookupAccount { id, responder: resp_tx })
            .await
            .map_err(|_| "Failed to send lookup account request to worker".to_string())?;

        resp_rx.await.map_err(|_| "Worker dropped response channel".to_string())?
    }

    /// Look up multiple accounts in a single batch request to TigerBeetle
    #[tracing::instrument(skip(self))]
    pub async fn lookup_accounts(&self, ids: Vec<u128>) -> Result<Vec<Account>, String> {
        let (tx, rx) = oneshot::channel();
        
        self.sender
            .send(TbCommand::LookupAccounts {
                ids,
                response: tx,
            })
            .await
            .map_err(|_| "Failed to send LookupAccounts command to TB worker".to_string())?;

        rx.await.map_err(|_| "Worker dropped the oneshot channel".to_string())?
    }
    
    /// Looks up a single transfer by ID (batched internally) and awaits its data.
    #[tracing::instrument(skip(self))]
    pub async fn lookup_transfer(&self, id: u128) -> Result<Vec<AppTransfer>, String> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.sender
            .send(TbCommand::LookupTransfer { id, responder: resp_tx })
            .await
            .map_err(|_| "Failed to send lookup transfer request to worker".to_string())?;

        resp_rx.await.map_err(|_| "Worker dropped response channel".to_string())?
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_account_balances(&self, ids: Vec<u128>) -> Result<Vec<AppAccountBalance>, String> {
        let (tx, rx) = oneshot::channel();

        self.sender
            .send(TbCommand::GetAccountBalances {
                ids,
                responder: tx,
            })
            .await
            .map_err(|_| "Failed to send GetAccountBalances command to TB worker".to_string())?;

        rx.await.map_err(|_| "Worker dropped the oneshot channel".to_string())?
    }
}
