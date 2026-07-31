use crate::ledger::models::LedgerAccountBalance;
use tigerbeetle_rustclient_tests_snapshot::Account;
use tokio::sync::oneshot;

use super::error::TigerBeetleResult;
use super::models::{
    LedgerAccount, LedgerCreateAccountsResult, LedgerCreateTransfersResult, LedgerTransfer,
};

/// Commands routed to the background worker for batch processing.
pub enum LedgerCommand {
    CreateAccount {
        account: LedgerAccount,
        responder: oneshot::Sender<TigerBeetleResult<Vec<LedgerCreateAccountsResult>>>,
    },
    CreateTransfer {
        transfer: LedgerTransfer,
        responder: oneshot::Sender<TigerBeetleResult<Vec<LedgerCreateTransfersResult>>>,
    },
    LookupAccount {
        id: u128,
        responder: oneshot::Sender<TigerBeetleResult<Vec<LedgerAccount>>>,
    },
    LookupAccounts {
        ids: Vec<u128>,
        response: oneshot::Sender<TigerBeetleResult<Vec<Account>>>,
    },
    LookupTransfer {
        id: u128,
        responder: oneshot::Sender<TigerBeetleResult<Vec<LedgerTransfer>>>,
    },
    GetAccountBalances {
        ids: Vec<u128>,
        responder: oneshot::Sender<TigerBeetleResult<Vec<LedgerAccountBalance>>>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ledger::models::{LedgerAccount, LedgerTransfer};
    use tokio::sync::{mpsc, oneshot};

    /// Tests the routing of a `CreateAccount` command.
    ///
    /// This test ensures that a `LedgerCommand::CreateAccount` can be successfully sent
    /// through an MPSC channel, received, and a response can be sent back via
    /// the oneshot channel provided in the command.
    #[tokio::test]
    async fn test_ledger_create_account_routing() {
        let (tx, mut rx) = mpsc::channel::<LedgerCommand>(32);
        let (resp_tx, resp_rx) = oneshot::channel();

        let account = LedgerAccount {
            id: 1,
            debits_pending: 0,
            debits_posted: 0,
            credits_pending: 0,
            credits_posted: 0,
            user_data_128: 0,
            user_data_64: 0,
            user_data_32: 0,
            reserved: 0,
            ledger: 1,
            code: 718,
            flags: 0,
            timestamp: 0,
        };

        // Spawn a task to send the command
        tokio::spawn(async move {
            tx.send(LedgerCommand::CreateAccount {
                account,
                responder: resp_tx,
            })
            .await
            .unwrap();
        });

        // Receive the command and verify its contents
        if let Some(LedgerCommand::CreateAccount {
            account: received_acc,
            responder,
        }) = rx.recv().await
        {
            assert_eq!(received_acc.id, 1);
            // Send a successful response back
            let _ = responder.send(Ok(vec![]));
        } else {
            panic!("Expected to receive a CreateAccount command");
        }

        // Ensure the response was received successfully
        assert!(
            resp_rx
                .await
                .expect("Responder channel failed to receive")
                .is_ok()
        );
    }

    /// Tests the routing of a `CreateTransfer` command.
    ///
    /// This test verifies that a `LedgerCommand::CreateTransfer` is correctly sent
    /// through the MPSC channel, received by the consumer, and that a response
    /// can be successfully returned.
    #[tokio::test]
    async fn test_ledger_create_transfer_routing() {
        let (tx, mut rx) = mpsc::channel::<LedgerCommand>(32);
        let (resp_tx, resp_rx) = oneshot::channel();

        let transfer = LedgerTransfer {
            id: 1,
            debit_account_id: 2,
            credit_account_id: 3,
            amount: 100,
            pending_id: 0,
            user_data_128: 0,
            user_data_64: 0,
            user_data_32: 0,
            timeout: 0,
            ledger: 1,
            code: 1,
            flags: 0,
            timestamp: 0,
        };

        tokio::spawn(async move {
            tx.send(LedgerCommand::CreateTransfer {
                transfer,
                responder: resp_tx,
            })
            .await
            .unwrap();
        });

        if let Some(LedgerCommand::CreateTransfer {
            transfer: received_tf,
            responder,
        }) = rx.recv().await
        {
            assert_eq!(received_tf.id, 1);
            let _ = responder.send(Ok(vec![]));
        } else {
            panic!("Expected to receive a CreateTransfer command");
        }

        assert!(
            resp_rx
                .await
                .expect("Responder channel failed to receive")
                .is_ok()
        );
    }

    /// Tests the routing of a `LookupAccount` command.
    ///
    /// This test ensures that a `LedgerCommand::LookupAccount` with a specific ID is
    /// properly sent and received, and that a response can be returned.
    #[tokio::test]
    async fn test_ledger_lookup_account_routing() {
        let (tx, mut rx) = mpsc::channel::<LedgerCommand>(32);
        let (resp_tx, resp_rx) = oneshot::channel();

        tokio::spawn(async move {
            tx.send(LedgerCommand::LookupAccount {
                id: 100,
                responder: resp_tx,
            })
            .await
            .unwrap();
        });

        if let Some(LedgerCommand::LookupAccount { id, responder }) = rx.recv().await {
            assert_eq!(id, 100);
            let _ = responder.send(Ok(vec![]));
        } else {
            panic!("Expected to receive a LookupAccount command");
        }

        assert!(
            resp_rx
                .await
                .expect("Responder channel failed to receive")
                .is_ok()
        );
    }

    /// Tests the routing of a `LookupTransfer` command.
    ///
    /// This test verifies that a `LedgerCommand::LookupTransfer` with a specific ID is
    /// correctly sent through the channel and a response is successfully received.
    #[tokio::test]
    async fn test_ledger_lookup_transfer_routing() {
        let (tx, mut rx) = mpsc::channel::<LedgerCommand>(32);
        let (resp_tx, resp_rx) = oneshot::channel();

        tokio::spawn(async move {
            tx.send(LedgerCommand::LookupTransfer {
                id: 200,
                responder: resp_tx,
            })
            .await
            .unwrap();
        });

        if let Some(LedgerCommand::LookupTransfer { id, responder }) = rx.recv().await {
            assert_eq!(id, 200);
            let _ = responder.send(Ok(vec![]));
        } else {
            panic!("Expected to receive a LookupTransfer command");
        }

        assert!(
            resp_rx
                .await
                .expect("Responder channel failed to receive")
                .is_ok()
        );
    }
}
