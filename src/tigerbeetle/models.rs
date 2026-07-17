/// Independent domain model for an Account.
#[derive(Debug, Clone)]
pub struct AppAccount {
    pub id: u128,
    pub debits_pending: u128,
    pub debits_posted: u128,
    pub credits_pending: u128,
    pub credits_posted: u128,
    pub user_data_128: u128,
    pub user_data_64: u64,
    pub user_data_32: u32,
    pub reserved: u32,
    pub ledger: u32,
    pub code: u16,
    pub flags: u16,
    pub timestamp: u64,
}

/// Independent domain model for a Transfer.
#[derive(Debug, Clone)]
pub struct AppTransfer {
    pub id: u128,
    pub debit_account_id: u128,
    pub credit_account_id: u128,
    pub amount: u128,
    pub pending_id: u128,
    pub user_data_128: u128,
    pub user_data_64: u64,
    pub user_data_32: u32,
    pub timeout: u32,
    pub ledger: u32,
    pub code: u16,
    pub flags: u16,
    pub timestamp: u64,
}

#[derive(Debug, Clone)]
pub struct AppCreateAccountsResult {
    pub index: u32,
    pub result: u32,
}

#[derive(Debug, Clone)]
pub struct AppCreateTransfersResult {
    pub index: u32,
    pub result: u32,
}

#[derive(Debug, Clone)]
pub struct AppAccountBalance {
    pub account_id: u128,
    pub posted_balance: i128,
    pub pending_balance: i128,
}
