use super::commands::LedgerCommand;
use super::error::{TigerBeetleError, TigerBeetleResult};
use super::models::{
    LedgerAccount, LedgerCreateAccountsResult, LedgerCreateTransfersResult, LedgerTransfer,
};
use crate::config::Settings;
use crate::ledger::models::LedgerAccountBalance;
use anyhow::Result;
use tigerbeetle_rustclient_tests_snapshot::Account;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

#[derive(Clone)]
pub struct LedgerClient {
    sender: mpsc::Sender<LedgerCommand>,
    operation_timeout: std::time::Duration,
}

impl LedgerClient {
    /// Initializes the MPSC channel for batching commands to the background worker.
    pub fn new(config: &Settings) -> Result<(Self, mpsc::Receiver<LedgerCommand>)> {
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
        transfer: LedgerTransfer,
    ) -> TigerBeetleResult<Vec<LedgerCreateTransfersResult>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.send(LedgerCommand::CreateTransfer {
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
        account: LedgerAccount,
    ) -> TigerBeetleResult<Vec<LedgerCreateAccountsResult>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.send(LedgerCommand::CreateAccount {
            account,
            responder: resp_tx,
        })
        .await?;
        self.receive(resp_rx).await
    }

    /// Looks up a single account by ID (batched internally) and awaits its data.
    #[tracing::instrument(skip(self))]
    pub async fn lookup_account(&self, id: u128) -> TigerBeetleResult<Vec<LedgerAccount>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.send(LedgerCommand::LookupAccount {
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

        self.send(LedgerCommand::LookupAccounts { ids, response: tx })
            .await?;
        self.receive(rx).await
    }

    /// Looks up a single transfer by ID (batched internally) and awaits its data.
    #[tracing::instrument(skip(self))]
    pub async fn lookup_transfer(&self, id: u128) -> TigerBeetleResult<Vec<LedgerTransfer>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.send(LedgerCommand::LookupTransfer {
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
    ) -> TigerBeetleResult<Vec<LedgerAccountBalance>> {
        let (tx, rx) = oneshot::channel();

        self.send(LedgerCommand::GetAccountBalances { ids, responder: tx })
            .await?;
        self.receive(rx).await
    }

    async fn send(&self, command: LedgerCommand) -> TigerBeetleResult<()> {
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
