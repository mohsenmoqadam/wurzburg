use tigerbeetle_rustclient_tests_snapshot::{
    Account as TbAccount, AccountFlags, CreateAccountsResult, CreateTransfersResult,
    Transfer as TbTransfer, TransferFlags,
};

use super::models::{AppAccount, AppCreateAccountsResult, AppCreateTransfersResult, AppTransfer};

pub fn to_tb_account(app: &AppAccount) -> TbAccount {
    TbAccount {
        id: app.id,
        debits_pending: app.debits_pending,
        debits_posted: app.debits_posted,
        credits_pending: app.credits_pending,
        credits_posted: app.credits_posted,
        user_data_128: app.user_data_128,
        user_data_64: app.user_data_64,
        user_data_32: app.user_data_32,
        reserved: Default::default(),
        ledger: app.ledger,
        code: app.code,
        flags: AccountFlags::from_bits_retain(app.flags),
        timestamp: app.timestamp,
    }
}

pub fn to_tb_transfer(app: &AppTransfer) -> TbTransfer {
    TbTransfer {
        id: app.id,
        debit_account_id: app.debit_account_id,
        credit_account_id: app.credit_account_id,
        amount: app.amount,
        pending_id: app.pending_id,
        user_data_128: app.user_data_128,
        user_data_64: app.user_data_64,
        user_data_32: app.user_data_32,
        timeout: app.timeout,
        ledger: app.ledger,
        code: app.code,
        flags: TransferFlags::from_bits_retain(app.flags),
        timestamp: app.timestamp,
    }
}

pub fn from_tb_account(tb: &TbAccount) -> AppAccount {
    AppAccount {
        id: tb.id,
        debits_pending: tb.debits_pending,
        debits_posted: tb.debits_posted,
        credits_pending: tb.credits_pending,
        credits_posted: tb.credits_posted,
        user_data_128: tb.user_data_128,
        user_data_64: tb.user_data_64,
        user_data_32: tb.user_data_32,
        reserved: 0,
        ledger: tb.ledger,
        code: tb.code,
        flags: tb.flags.bits(),
        timestamp: tb.timestamp,
    }
}

pub fn from_tb_transfer(tb: &TbTransfer) -> AppTransfer {
    AppTransfer {
        id: tb.id,
        debit_account_id: tb.debit_account_id,
        credit_account_id: tb.credit_account_id,
        amount: tb.amount,
        pending_id: tb.pending_id,
        user_data_128: tb.user_data_128,
        user_data_64: tb.user_data_64,
        user_data_32: tb.user_data_32,
        timeout: tb.timeout,
        ledger: tb.ledger,
        code: tb.code,
        flags: tb.flags.bits(),
        timestamp: tb.timestamp,
    }
}

pub fn from_tb_account_result(tb: &CreateAccountsResult) -> AppCreateAccountsResult {
    AppCreateAccountsResult {
        index: tb.index as u32,
        result: tb.result as u32,
    }
}

pub fn from_tb_transfer_result(tb: &CreateTransfersResult) -> AppCreateTransfersResult {
    AppCreateTransfersResult {
        index: tb.index as u32,
        result: tb.result as u32,
    }
}
