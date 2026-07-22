use super::commands::TbCommand;
use super::error::{TigerBeetleError, TigerBeetleResult};
use super::models::{AppAccount, AppCreateAccountsResult, AppCreateTransfersResult, AppTransfer};
use crate::config::Settings;
use crate::tigerbeetle::models::AppAccountBalance;
use anyhow::Result;
use tigerbeetle_rustclient_tests_snapshot::Account;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

#[derive(Clone)]
pub struct AppTbClient {
    sender: mpsc::Sender<TbCommand>,
    operation_timeout: std::time::Duration,
}

impl AppTbClient {
    /// Initializes the MPSC channel for batching commands to the background worker.
    pub fn new(config: &Settings) -> Result<(Self, mpsc::Receiver<TbCommand>)> {
        let (sender, receiver) = mpsc::channel(config.tigerbeetle.channel_capacity);
        Ok((
            Self {
                sender,
                operation_timeout: config.tigerbeetle.operation_timeout(),
            },
            receiver,
        ))
    }

    /// Submits a transfer to be batched and awaits its result.
    #[tracing::instrument(skip(self))]
    pub async fn create_transfer(
        &self,
        transfer: AppTransfer,
    ) -> TigerBeetleResult<Vec<AppCreateTransfersResult>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.send(TbCommand::CreateTransfer {
            transfer,
            responder: resp_tx,
        })
        .await?;
        self.receive(resp_rx).await
    }

    /// Submits an account to be batched and awaits its result.
    #[tracing::instrument(skip(self))]
    pub async fn create_account(
        &self,
        account: AppAccount,
    ) -> TigerBeetleResult<Vec<AppCreateAccountsResult>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.send(TbCommand::CreateAccount {
            account,
            responder: resp_tx,
        })
        .await?;
        self.receive(resp_rx).await
    }

    /// Looks up a single account by ID (batched internally) and awaits its data.
    #[tracing::instrument(skip(self))]
    pub async fn lookup_account(&self, id: u128) -> TigerBeetleResult<Vec<AppAccount>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.send(TbCommand::LookupAccount {
            id,
            responder: resp_tx,
        })
        .await?;
        self.receive(resp_rx).await
    }

    /// Look up multiple accounts in a single batch request to TigerBeetle
    #[tracing::instrument(skip(self))]
    pub async fn lookup_accounts(&self, ids: Vec<u128>) -> TigerBeetleResult<Vec<Account>> {
        let (tx, rx) = oneshot::channel();

        self.send(TbCommand::LookupAccounts { ids, response: tx })
            .await?;
        self.receive(rx).await
    }

    /// Looks up a single transfer by ID (batched internally) and awaits its data.
    #[tracing::instrument(skip(self))]
    pub async fn lookup_transfer(&self, id: u128) -> TigerBeetleResult<Vec<AppTransfer>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.send(TbCommand::LookupTransfer {
            id,
            responder: resp_tx,
        })
        .await?;
        self.receive(resp_rx).await
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_account_balances(
        &self,
        ids: Vec<u128>,
    ) -> TigerBeetleResult<Vec<AppAccountBalance>> {
        let (tx, rx) = oneshot::channel();

        self.send(TbCommand::GetAccountBalances { ids, responder: tx })
            .await?;
        self.receive(rx).await
    }

    async fn send(&self, command: TbCommand) -> TigerBeetleResult<()> {
        timeout(self.operation_timeout, self.sender.send(command))
            .await
            .map_err(|_| TigerBeetleError::QueueUnavailable)?
            .map_err(|_| TigerBeetleError::QueueUnavailable)
    }

    async fn receive<T>(
        &self,
        receiver: oneshot::Receiver<TigerBeetleResult<T>>,
    ) -> TigerBeetleResult<T> {
        timeout(self.operation_timeout, receiver)
            .await
            .map_err(|_| TigerBeetleError::DeadlineExceeded)?
            .map_err(|_| TigerBeetleError::WorkerUnavailable)?
    }
}
