use std::{error::Error, fmt, sync::Arc};

use chrono::Utc;
use sha2::{Digest, Sha256};
use tigerbeetle_rustclient_tests_snapshot::{Account, AccountFlags};
use uuid::Uuid;

use crate::{
    api::command::MutationCommandContext,
    config::TigerBeetleConfig,
    db::{
        error::DbError,
        oracle::{BeginCreditMovementOutcome, CreditAccountContextOutcome, OracleRepository},
    },
    domain::{
        provider::{CreditGrantLimitMode, ProviderOperationalProfileRecord},
        provider_credit::{
            CreditAccountContext, CreditMovementInitiator, CreditMovementIntent,
            CreditMovementResult, CreditMovementType, ProviderCreditBalance,
        },
    },
    ledger::{LedgerClient, LedgerTransfer, TigerBeetleError},
    runtime_profiles::{
        CardProfileLockError, CardProfileLockManager, CardProfileLockOutcome, ProviderCreditLock,
        ProviderCreditLockError, ProviderCreditLockManager, ProviderCreditLockOutcome,
    },
};

const MAX_MONEY_RIALS: u64 = 9_007_199_254_740_991;

#[derive(Debug, Clone)]
pub struct GrantCreditCommand {
    pub user_id: Uuid,
    pub card_number: String,
    pub amount_rials: u64,
    pub provider_reference: String,
    pub reason: String,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct ReturnCreditCommand {
    pub user_id: Uuid,
    pub card_number: String,
    pub expected_remaining_amount_rials: u64,
    pub provider_reference: Option<String>,
    pub initiated_by: CreditMovementInitiator,
    pub reason: String,
    pub metadata: serde_json::Value,
}

struct MovementDetails {
    movement_type: CreditMovementType,
    initiated_by: CreditMovementInitiator,
    amount_rials: u64,
    expected_remaining_amount_rials: Option<u64>,
    provider_reference: Option<String>,
    reason: String,
    metadata: serde_json::Value,
}

#[derive(Debug, Clone)]
pub enum ProviderCreditCommandOutcome {
    Applied(CreditMovementResult),
    Replayed(CreditMovementResult),
    ProviderNotFound,
    ProviderNotActive,
    RelationshipNotFound,
    RelationshipNotActive,
    CardNotFound,
    CardNotActive,
    FundingSourceNotActive,
    GrantDisabled,
    ReturnDisabled,
    ExposureLimitExceeded,
    ExpectedBalanceChanged,
    NoRemainingCredit,
    ProviderCreditLocked,
    CardProfileLocked,
    ProviderReferenceConflict,
    IdempotencyConflict,
    IdempotencyInProgress,
    RecoveryRequired,
}

#[derive(Debug)]
pub enum ProviderCreditServiceError {
    Database(DbError),
    Ledger(TigerBeetleError),
    CardRuntime(CardProfileLockError),
    ProviderRuntime(ProviderCreditLockError),
    LedgerContract,
}

impl fmt::Display for ProviderCreditServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Database(_) => "database",
            Self::Ledger(_) => "ledger",
            Self::CardRuntime(_) | Self::ProviderRuntime(_) => "runtime_profile",
            Self::LedgerContract => "ledger_contract",
        })
    }
}
impl Error for ProviderCreditServiceError {}
impl From<DbError> for ProviderCreditServiceError {
    fn from(value: DbError) -> Self {
        Self::Database(value)
    }
}
impl From<TigerBeetleError> for ProviderCreditServiceError {
    fn from(value: TigerBeetleError) -> Self {
        Self::Ledger(value)
    }
}
impl From<CardProfileLockError> for ProviderCreditServiceError {
    fn from(value: CardProfileLockError) -> Self {
        Self::CardRuntime(value)
    }
}
impl From<ProviderCreditLockError> for ProviderCreditServiceError {
    fn from(value: ProviderCreditLockError) -> Self {
        Self::ProviderRuntime(value)
    }
}

#[derive(Clone)]
pub struct ProviderCreditService {
    repository: Arc<OracleRepository>,
    ledger: LedgerClient,
    ledger_config: TigerBeetleConfig,
    card_locks: Arc<CardProfileLockManager>,
    provider_locks: Arc<ProviderCreditLockManager>,
}

impl ProviderCreditService {
    pub fn new(
        repository: Arc<OracleRepository>,
        ledger: LedgerClient,
        ledger_config: TigerBeetleConfig,
        card_locks: Arc<CardProfileLockManager>,
        provider_locks: Arc<ProviderCreditLockManager>,
    ) -> Self {
        Self {
            repository,
            ledger,
            ledger_config,
            card_locks,
            provider_locks,
        }
    }

    #[tracing::instrument(skip(self, card_number), fields(provider.id=%provider_id, user.id=%user_id))]
    pub async fn read_balance(
        &self,
        provider_id: Uuid,
        user_id: Uuid,
        card_number: String,
    ) -> Result<
        Result<ProviderCreditBalance, ProviderCreditCommandOutcome>,
        ProviderCreditServiceError,
    > {
        let account_context = match self
            .repository
            .resolve_credit_account_context(provider_id, user_id, card_number)
            .await?
        {
            CreditAccountContextOutcome::Found(value) => value,
            other => return Ok(Err(map_context_outcome(other))),
        };
        let account = self
            .lookup_account(account_context.provider_user_account_id)
            .await?;
        self.validate_user_account(&account, &account_context)?;
        let remaining = spendable_balance(&account)?;
        Ok(Ok(ProviderCreditBalance {
            provider_id,
            user_id,
            card_id: account_context.card_id,
            currency: "IRR".to_string(),
            observed_remaining_amount_rials: remaining,
            observed_at: Utc::now(),
        }))
    }

    #[tracing::instrument(skip(self, context, command), fields(operation.type="provider_credit.grant", provider.id=%provider_id, user.id=%command.user_id))]
    pub async fn grant(
        &self,
        context: &MutationCommandContext,
        provider_id: Uuid,
        command: GrantCreditCommand,
    ) -> Result<ProviderCreditCommandOutcome, ProviderCreditServiceError> {
        if let Some(replay) = self.repository.find_credit_movement_replay(context).await? {
            return Ok(map_begin_outcome(replay));
        }
        let Some(profile) = self.current_profile(provider_id).await? else {
            return Ok(ProviderCreditCommandOutcome::ProviderNotActive);
        };
        if !profile.controls.credit_grant_enabled {
            return Ok(ProviderCreditCommandOutcome::GrantDisabled);
        }
        let operation_id = Uuid::new_v4();
        let provider_lock = match self
            .provider_locks
            .acquire(provider_id, operation_id)
            .await?
        {
            ProviderCreditLockOutcome::Acquired(lock) => lock,
            ProviderCreditLockOutcome::Busy => {
                return Ok(ProviderCreditCommandOutcome::ProviderCreditLocked);
            }
        };
        let card_lock_outcome = match self
            .card_locks
            .acquire(&command.card_number, operation_id)
            .await
        {
            Ok(value) => value,
            Err(error) => {
                let _ = self.provider_locks.release(&provider_lock).await;
                return Err(error.into());
            }
        };
        let card_lock = match card_lock_outcome {
            CardProfileLockOutcome::Acquired(lock) => lock,
            CardProfileLockOutcome::Busy => {
                let _ = self.provider_locks.release(&provider_lock).await;
                return Ok(ProviderCreditCommandOutcome::CardProfileLocked);
            }
        };
        let resolved_context = match self
            .repository
            .resolve_credit_account_context(
                provider_id,
                command.user_id,
                command.card_number.clone(),
            )
            .await
        {
            Ok(value) => value,
            Err(error) => {
                let _ = self.card_locks.release(&card_lock).await;
                let _ = self.provider_locks.release(&provider_lock).await;
                return Err(error.into());
            }
        };
        let account_context = match resolved_context {
            CreditAccountContextOutcome::Found(value) => value,
            other => {
                let _ = self.card_locks.release(&card_lock).await;
                let _ = self.provider_locks.release(&provider_lock).await;
                return Ok(map_context_outcome(other));
            }
        };
        let accounts = match self
            .lookup_accounts(&[
                account_context.provider_user_account_id,
                account_context.provider_owned_account_id,
                account_context.cms_settlement_account_id,
            ])
            .await
        {
            Ok(value) => value,
            Err(error) => {
                let _ = self.card_locks.release(&card_lock).await;
                let _ = self.provider_locks.release(&provider_lock).await;
                return Err(error);
            }
        };
        let contract = self
            .validate_user_account(&accounts[0], &account_context)
            .and_then(|_| {
                self.validate_provider_account(
                    &accounts[1],
                    account_context.provider_id,
                    self.ledger_config.provider_owned_account_code,
                )
            })
            .and_then(|_| {
                self.validate_provider_account(
                    &accounts[2],
                    account_context.provider_id,
                    self.ledger_config.cms_settlement_account_code,
                )
            });
        if let Err(error) = contract {
            let _ = self.card_locks.release(&card_lock).await;
            let _ = self.provider_locks.release(&provider_lock).await;
            return Err(error);
        }
        let within_limit =
            match grant_within_limit(&profile, &accounts[1], &accounts[2], command.amount_rials) {
                Ok(value) => value,
                Err(error) => {
                    let _ = self.card_locks.release(&card_lock).await;
                    let _ = self.provider_locks.release(&provider_lock).await;
                    return Err(error);
                }
            };
        if !within_limit {
            let _ = self.card_locks.release(&card_lock).await;
            let _ = self.provider_locks.release(&provider_lock).await;
            return Ok(ProviderCreditCommandOutcome::ExposureLimitExceeded);
        }
        let intent = movement_intent(
            operation_id,
            &account_context,
            profile.provider_operational_profile_id,
            MovementDetails {
                movement_type: CreditMovementType::Grant,
                initiated_by: CreditMovementInitiator::Provider,
                amount_rials: command.amount_rials,
                expected_remaining_amount_rials: None,
                provider_reference: Some(command.provider_reference),
                reason: command.reason,
                metadata: command.metadata,
            },
        );
        let begin = match self
            .repository
            .begin_credit_movement_atomic(
                context.clone(),
                intent.clone(),
                command.card_number.clone(),
            )
            .await
        {
            Ok(value) => value,
            Err(error) => {
                let _ = self.card_locks.release(&card_lock).await;
                let _ = self.provider_locks.release(&provider_lock).await;
                return Err(error.into());
            }
        };
        let BeginCreditMovementOutcome::Prepared(intent) = begin else {
            let _ = self.card_locks.release(&card_lock).await;
            let _ = self.provider_locks.release(&provider_lock).await;
            return Ok(map_begin_outcome(begin));
        };
        if self
            .card_locks
            .invalidate_profile(&card_lock)
            .await
            .is_err()
        {
            return self
                .transition_to_recovery(
                    intent.operation_id,
                    "DRAGONFLY_INVALIDATION_FAILED",
                    &provider_lock,
                )
                .await;
        }
        if let Err(error) = self
            .repository
            .mark_credit_movement_external_in_flight(intent.operation_id)
            .await
        {
            let _ = self.provider_locks.release(&provider_lock).await;
            return Err(error.into());
        }
        if !self
            .renew_movement_locks(provider_id, intent.operation_id, &command.card_number)
            .await
            .unwrap_or(false)
        {
            return self
                .transition_to_recovery(
                    intent.operation_id,
                    "DRAGONFLY_LOCK_RENEWAL_FAILED",
                    &provider_lock,
                )
                .await;
        }
        let transfer = transfer_for(&intent, &self.ledger_config);
        if self.create_and_verify_transfer(&transfer).await.is_err() {
            return self
                .transition_to_recovery(
                    intent.operation_id,
                    "TIGERBEETLE_TRANSFER_UNCERTAIN",
                    &provider_lock,
                )
                .await;
        }
        let remaining = match self
            .lookup_account(intent.provider_user_account_id)
            .await
            .and_then(|account| spendable_balance(&account))
        {
            Ok(value) => value,
            Err(_) => {
                return self
                    .transition_to_recovery(
                        intent.operation_id,
                        "TIGERBEETLE_BALANCE_UNCERTAIN",
                        &provider_lock,
                    )
                    .await;
            }
        };
        let result = match self
            .repository
            .finalize_credit_movement_atomic(
                context.durable(),
                intent.clone(),
                remaining,
                Utc::now(),
            )
            .await
        {
            Ok(result) => result,
            Err(error) => {
                tracing::error!(operation.id=%intent.operation_id, error.kind=error.diagnostic_kind(), "credit grant finalization requires recovery");
                return self
                    .transition_to_recovery(
                        intent.operation_id,
                        "ORACLE_FINALIZATION_FAILED",
                        &provider_lock,
                    )
                    .await;
            }
        };
        let _ = self.provider_locks.release(&provider_lock).await;
        Ok(ProviderCreditCommandOutcome::Applied(result))
    }

    #[tracing::instrument(skip(self, context, command), fields(operation.type="provider_credit.return", provider.id=%provider_id, user.id=%command.user_id))]
    pub async fn return_full_balance(
        &self,
        context: &MutationCommandContext,
        provider_id: Uuid,
        command: ReturnCreditCommand,
    ) -> Result<ProviderCreditCommandOutcome, ProviderCreditServiceError> {
        if let Some(replay) = self.repository.find_credit_movement_replay(context).await? {
            return Ok(map_begin_outcome(replay));
        }
        let Some(profile) = self.current_profile(provider_id).await? else {
            return Ok(ProviderCreditCommandOutcome::ProviderNotActive);
        };
        if !profile.controls.credit_return_enabled {
            return Ok(ProviderCreditCommandOutcome::ReturnDisabled);
        }
        let operation_id = Uuid::new_v4();
        let provider_lock = match self
            .provider_locks
            .acquire(provider_id, operation_id)
            .await?
        {
            ProviderCreditLockOutcome::Acquired(lock) => lock,
            ProviderCreditLockOutcome::Busy => {
                return Ok(ProviderCreditCommandOutcome::ProviderCreditLocked);
            }
        };
        let card_lock_outcome = match self
            .card_locks
            .acquire(&command.card_number, operation_id)
            .await
        {
            Ok(value) => value,
            Err(error) => {
                let _ = self.provider_locks.release(&provider_lock).await;
                return Err(error.into());
            }
        };
        let card_lock = match card_lock_outcome {
            CardProfileLockOutcome::Acquired(lock) => lock,
            CardProfileLockOutcome::Busy => {
                let _ = self.provider_locks.release(&provider_lock).await;
                return Ok(ProviderCreditCommandOutcome::CardProfileLocked);
            }
        };
        let resolved_context = match self
            .repository
            .resolve_credit_account_context(
                provider_id,
                command.user_id,
                command.card_number.clone(),
            )
            .await
        {
            Ok(value) => value,
            Err(error) => {
                let _ = self.card_locks.release(&card_lock).await;
                let _ = self.provider_locks.release(&provider_lock).await;
                return Err(error.into());
            }
        };
        let account_context = match resolved_context {
            CreditAccountContextOutcome::Found(value) => value,
            other => {
                let _ = self.card_locks.release(&card_lock).await;
                let _ = self.provider_locks.release(&provider_lock).await;
                return Ok(map_context_outcome(other));
            }
        };
        let accounts = match self
            .lookup_accounts(&[
                account_context.provider_user_account_id,
                account_context.provider_owned_account_id,
            ])
            .await
        {
            Ok(value) => value,
            Err(error) => {
                let _ = self.card_locks.release(&card_lock).await;
                let _ = self.provider_locks.release(&provider_lock).await;
                return Err(error);
            }
        };
        let contract = self
            .validate_user_account(&accounts[0], &account_context)
            .and_then(|_| {
                self.validate_provider_account(
                    &accounts[1],
                    account_context.provider_id,
                    self.ledger_config.provider_owned_account_code,
                )
            })
            .and_then(|_| spendable_balance(&accounts[0]));
        let remaining = match contract {
            Ok(value) => value,
            Err(error) => {
                let _ = self.card_locks.release(&card_lock).await;
                let _ = self.provider_locks.release(&provider_lock).await;
                return Err(error);
            }
        };
        if remaining == 0 {
            let _ = self.card_locks.release(&card_lock).await;
            let _ = self.provider_locks.release(&provider_lock).await;
            return Ok(ProviderCreditCommandOutcome::NoRemainingCredit);
        }
        if remaining != command.expected_remaining_amount_rials {
            let _ = self.card_locks.release(&card_lock).await;
            let _ = self.provider_locks.release(&provider_lock).await;
            return Ok(ProviderCreditCommandOutcome::ExpectedBalanceChanged);
        }
        let intent = movement_intent(
            operation_id,
            &account_context,
            profile.provider_operational_profile_id,
            MovementDetails {
                movement_type: CreditMovementType::ReturnFullBalance,
                initiated_by: command.initiated_by,
                amount_rials: remaining,
                expected_remaining_amount_rials: Some(remaining),
                provider_reference: command.provider_reference,
                reason: command.reason,
                metadata: command.metadata,
            },
        );
        let begin = match self
            .repository
            .begin_credit_movement_atomic(
                context.clone(),
                intent.clone(),
                command.card_number.clone(),
            )
            .await
        {
            Ok(value) => value,
            Err(error) => {
                let _ = self.card_locks.release(&card_lock).await;
                let _ = self.provider_locks.release(&provider_lock).await;
                return Err(error.into());
            }
        };
        let BeginCreditMovementOutcome::Prepared(intent) = begin else {
            let _ = self.card_locks.release(&card_lock).await;
            let _ = self.provider_locks.release(&provider_lock).await;
            return Ok(map_begin_outcome(begin));
        };
        if self
            .card_locks
            .invalidate_profile(&card_lock)
            .await
            .is_err()
        {
            return self
                .transition_to_recovery(
                    intent.operation_id,
                    "DRAGONFLY_INVALIDATION_FAILED",
                    &provider_lock,
                )
                .await;
        }
        if let Err(error) = self
            .repository
            .mark_credit_movement_external_in_flight(intent.operation_id)
            .await
        {
            let _ = self.provider_locks.release(&provider_lock).await;
            return Err(error.into());
        }
        if !self
            .renew_movement_locks(provider_id, intent.operation_id, &command.card_number)
            .await
            .unwrap_or(false)
        {
            return self
                .transition_to_recovery(
                    intent.operation_id,
                    "DRAGONFLY_LOCK_RENEWAL_FAILED",
                    &provider_lock,
                )
                .await;
        }
        if self
            .create_and_verify_transfer(&transfer_for(&intent, &self.ledger_config))
            .await
            .is_err()
        {
            return self
                .transition_to_recovery(
                    intent.operation_id,
                    "TIGERBEETLE_TRANSFER_UNCERTAIN",
                    &provider_lock,
                )
                .await;
        }
        let observed = match self
            .lookup_account(intent.provider_user_account_id)
            .await
            .and_then(|account| spendable_balance(&account))
        {
            Ok(value) => value,
            Err(_) => {
                return self
                    .transition_to_recovery(
                        intent.operation_id,
                        "TIGERBEETLE_BALANCE_UNCERTAIN",
                        &provider_lock,
                    )
                    .await;
            }
        };
        let result = match self
            .repository
            .finalize_credit_movement_atomic(
                context.durable(),
                intent.clone(),
                observed,
                Utc::now(),
            )
            .await
        {
            Ok(result) => result,
            Err(error) => {
                tracing::error!(operation.id=%intent.operation_id, error.kind=error.diagnostic_kind(), "credit return finalization requires recovery");
                return self
                    .transition_to_recovery(
                        intent.operation_id,
                        "ORACLE_FINALIZATION_FAILED",
                        &provider_lock,
                    )
                    .await;
            }
        };
        let _ = self.provider_locks.release(&provider_lock).await;
        Ok(ProviderCreditCommandOutcome::Applied(result))
    }

    pub(crate) async fn recover(
        &self,
        context: crate::api::command::DurableMutationContext,
        intent: CreditMovementIntent,
        card_number: String,
    ) -> Result<(), ProviderCreditServiceError> {
        let Some(provider_lock) = self
            .provider_locks
            .ensure(intent.provider_id, intent.operation_id)
            .await?
        else {
            return Err(ProviderCreditServiceError::LedgerContract);
        };
        let result = async {
            if !self
                .card_locks
                .ensure_pending(&card_number, intent.operation_id)
                .await?
            {
                return Err(ProviderCreditServiceError::LedgerContract);
            }
            let accounts = self
                .lookup_accounts(&[
                    intent.provider_user_account_id,
                    intent.provider_owned_account_id,
                ])
                .await?;
            self.validate_intent_user_account(&accounts[0], &intent)?;
            self.validate_provider_account(
                &accounts[1],
                intent.provider_id,
                self.ledger_config.provider_owned_account_code,
            )?;
            let transfer = transfer_for(&intent, &self.ledger_config);
            self.create_and_verify_transfer(&transfer).await?;
            let remaining =
                spendable_balance(&self.lookup_account(intent.provider_user_account_id).await?)?;
            self.repository
                .finalize_credit_movement_atomic(context, intent, remaining, Utc::now())
                .await?;
            Ok(())
        }
        .await;
        let _ = self.provider_locks.release(&provider_lock).await;
        result
    }

    async fn current_profile(
        &self,
        provider_id: Uuid,
    ) -> Result<Option<ProviderOperationalProfileRecord>, ProviderCreditServiceError> {
        Ok(self
            .repository
            .get_current_provider_operational_profile(provider_id)
            .await?)
    }

    async fn lookup_account(
        &self,
        account_id: Uuid,
    ) -> Result<Account, ProviderCreditServiceError> {
        let mut accounts = self
            .ledger
            .lookup_accounts(vec![account_id.as_u128()])
            .await?;
        if accounts.len() != 1 || accounts[0].id != account_id.as_u128() {
            return Err(ProviderCreditServiceError::LedgerContract);
        }
        Ok(accounts.remove(0))
    }

    async fn lookup_accounts(
        &self,
        ids: &[Uuid],
    ) -> Result<Vec<Account>, ProviderCreditServiceError> {
        let accounts = self
            .ledger
            .lookup_accounts(ids.iter().map(|value| value.as_u128()).collect())
            .await?;
        ids.iter()
            .map(|id| {
                accounts
                    .iter()
                    .find(|account| account.id == id.as_u128())
                    .cloned()
                    .ok_or(ProviderCreditServiceError::LedgerContract)
            })
            .collect()
    }

    fn validate_user_account(
        &self,
        account: &Account,
        context: &CreditAccountContext,
    ) -> Result<(), ProviderCreditServiceError> {
        let expected_flags =
            (AccountFlags::History | AccountFlags::DebitsMustNotExceedCredits).bits();
        if account.id != context.provider_user_account_id.as_u128()
            || account.user_data_128 != context.provider_user_id.as_u128()
            || account.ledger != self.ledger_config.ledger_id
            || account.code != self.ledger_config.user_account_code
            || account.flags.bits() != expected_flags
        {
            return Err(ProviderCreditServiceError::LedgerContract);
        }
        Ok(())
    }

    fn validate_intent_user_account(
        &self,
        account: &Account,
        intent: &CreditMovementIntent,
    ) -> Result<(), ProviderCreditServiceError> {
        let expected_flags =
            (AccountFlags::History | AccountFlags::DebitsMustNotExceedCredits).bits();
        if account.id != intent.provider_user_account_id.as_u128()
            || account.user_data_128 != intent.provider_user_id.as_u128()
            || account.ledger != self.ledger_config.ledger_id
            || account.code != self.ledger_config.user_account_code
            || account.flags.bits() != expected_flags
        {
            return Err(ProviderCreditServiceError::LedgerContract);
        }
        Ok(())
    }

    fn validate_provider_account(
        &self,
        account: &Account,
        provider_id: Uuid,
        expected_code: u16,
    ) -> Result<(), ProviderCreditServiceError> {
        if account.user_data_128 != provider_id.as_u128()
            || account.ledger != self.ledger_config.ledger_id
            || account.code != expected_code
            || account.flags.bits() != AccountFlags::History.bits()
        {
            return Err(ProviderCreditServiceError::LedgerContract);
        }
        Ok(())
    }

    async fn create_and_verify_transfer(
        &self,
        transfer: &LedgerTransfer,
    ) -> Result<(), ProviderCreditServiceError> {
        let _ = self.ledger.create_transfer(transfer.clone()).await;
        let found = self.ledger.lookup_transfer(transfer.id).await?;
        if found.len() != 1 || !same_transfer(&found[0], transfer) {
            return Err(ProviderCreditServiceError::LedgerContract);
        }
        Ok(())
    }

    async fn mark_recovery(
        &self,
        operation_id: Uuid,
        code: &'static str,
    ) -> Result<(), ProviderCreditServiceError> {
        self.repository
            .mark_credit_movement_recovery_required(operation_id, code)
            .await?;
        Ok(())
    }

    async fn renew_movement_locks(
        &self,
        provider_id: Uuid,
        operation_id: Uuid,
        card_number: &str,
    ) -> Result<bool, ProviderCreditServiceError> {
        let provider_owned = self
            .provider_locks
            .ensure(provider_id, operation_id)
            .await?
            .is_some();
        let card_owned = self
            .card_locks
            .ensure_pending(card_number, operation_id)
            .await?;
        Ok(provider_owned && card_owned)
    }

    async fn transition_to_recovery(
        &self,
        operation_id: Uuid,
        code: &'static str,
        provider_lock: &ProviderCreditLock,
    ) -> Result<ProviderCreditCommandOutcome, ProviderCreditServiceError> {
        let marked = self.mark_recovery(operation_id, code).await;
        let _ = self.provider_locks.release(provider_lock).await;
        marked?;
        Ok(ProviderCreditCommandOutcome::RecoveryRequired)
    }
}

fn movement_intent(
    operation_id: Uuid,
    context: &CreditAccountContext,
    operational_profile_id: Uuid,
    details: MovementDetails,
) -> CreditMovementIntent {
    CreditMovementIntent {
        operation_id,
        movement_id: Uuid::new_v4(),
        movement_type: details.movement_type,
        initiated_by: details.initiated_by,
        provider_id: context.provider_id,
        user_id: context.user_id,
        card_id: context.card_id,
        provider_user_id: context.provider_user_id,
        provider_user_account_id: context.provider_user_account_id,
        provider_owned_account_id: context.provider_owned_account_id,
        amount_rials: details.amount_rials,
        expected_remaining_amount_rials: details.expected_remaining_amount_rials,
        provider_reference: details.provider_reference,
        deterministic_transfer_id: deterministic_transfer_id(operation_id),
        operational_profile_id,
        reason: details.reason,
        metadata: details.metadata,
    }
}

fn transfer_for(intent: &CreditMovementIntent, config: &TigerBeetleConfig) -> LedgerTransfer {
    let (debit, credit) = match intent.movement_type {
        CreditMovementType::Grant => (
            intent.provider_owned_account_id,
            intent.provider_user_account_id,
        ),
        CreditMovementType::ReturnFullBalance => (
            intent.provider_user_account_id,
            intent.provider_owned_account_id,
        ),
    };
    LedgerTransfer {
        id: intent.deterministic_transfer_id.as_u128(),
        debit_account_id: debit.as_u128(),
        credit_account_id: credit.as_u128(),
        amount: u128::from(intent.amount_rials),
        pending_id: 0,
        user_data_128: intent.movement_id.as_u128(),
        user_data_64: 0,
        user_data_32: 0,
        timeout: 0,
        ledger: config.ledger_id,
        code: config.transfer_code,
        flags: 0,
        timestamp: 0,
    }
}

fn deterministic_transfer_id(operation_id: Uuid) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"wurzburg.provider.credit.transfer.v1");
    hasher.update(operation_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn same_transfer(actual: &LedgerTransfer, expected: &LedgerTransfer) -> bool {
    actual.id == expected.id
        && actual.debit_account_id == expected.debit_account_id
        && actual.credit_account_id == expected.credit_account_id
        && actual.amount == expected.amount
        && actual.pending_id == expected.pending_id
        && actual.user_data_128 == expected.user_data_128
        && actual.user_data_64 == expected.user_data_64
        && actual.user_data_32 == expected.user_data_32
        && actual.timeout == expected.timeout
        && actual.ledger == expected.ledger
        && actual.code == expected.code
        && actual.flags == expected.flags
}

fn spendable_balance(account: &Account) -> Result<u64, ProviderCreditServiceError> {
    let credits = account.credits_posted;
    let debits = account
        .debits_posted
        .checked_add(account.debits_pending)
        .ok_or(ProviderCreditServiceError::LedgerContract)?;
    let remaining = credits
        .checked_sub(debits)
        .ok_or(ProviderCreditServiceError::LedgerContract)?;
    let value = u64::try_from(remaining).map_err(|_| ProviderCreditServiceError::LedgerContract)?;
    if value > MAX_MONEY_RIALS {
        return Err(ProviderCreditServiceError::LedgerContract);
    }
    Ok(value)
}

fn grant_within_limit(
    profile: &ProviderOperationalProfileRecord,
    provider_owned: &Account,
    cms_settlement: &Account,
    amount: u64,
) -> Result<bool, ProviderCreditServiceError> {
    let provider_debits = provider_owned
        .debits_posted
        .checked_add(provider_owned.debits_pending)
        .ok_or(ProviderCreditServiceError::LedgerContract)?;
    let provider_debt = provider_debits.saturating_sub(provider_owned.credits_posted);
    let cms_credits = cms_settlement
        .credits_posted
        .checked_add(cms_settlement.credits_pending)
        .ok_or(ProviderCreditServiceError::LedgerContract)?;
    let cms_debt = cms_credits.saturating_sub(cms_settlement.debits_posted);
    let projected_provider_debt = provider_debt
        .checked_add(u128::from(amount))
        .ok_or(ProviderCreditServiceError::LedgerContract)?;
    let limit = u128::from(profile.controls.credit_grant_limit_amount_rials);
    Ok(match profile.controls.credit_grant_mode {
        CreditGrantLimitMode::FixedLimit => projected_provider_debt <= limit,
        // Reaching the CMS debt threshold closes further granting; the grant
        // itself does not change this settlement account.
        CreditGrantLimitMode::CmsDebtLimit => cms_debt < limit,
        CreditGrantLimitMode::OutstandingCreditLimit => {
            projected_provider_debt.saturating_sub(cms_debt) <= limit
        }
    })
}

fn map_context_outcome(value: CreditAccountContextOutcome) -> ProviderCreditCommandOutcome {
    match value {
        CreditAccountContextOutcome::Found(_) => unreachable!(),
        CreditAccountContextOutcome::ProviderNotFound => {
            ProviderCreditCommandOutcome::ProviderNotFound
        }
        CreditAccountContextOutcome::ProviderNotActive => {
            ProviderCreditCommandOutcome::ProviderNotActive
        }
        CreditAccountContextOutcome::RelationshipNotFound => {
            ProviderCreditCommandOutcome::RelationshipNotFound
        }
        CreditAccountContextOutcome::RelationshipNotActive => {
            ProviderCreditCommandOutcome::RelationshipNotActive
        }
        CreditAccountContextOutcome::CardNotFound => ProviderCreditCommandOutcome::CardNotFound,
        CreditAccountContextOutcome::CardNotActive => ProviderCreditCommandOutcome::CardNotActive,
        CreditAccountContextOutcome::FundingSourceNotActive => {
            ProviderCreditCommandOutcome::FundingSourceNotActive
        }
    }
}

fn map_begin_outcome(value: BeginCreditMovementOutcome) -> ProviderCreditCommandOutcome {
    match value {
        BeginCreditMovementOutcome::Prepared(_) => unreachable!(),
        BeginCreditMovementOutcome::Replayed(result) => {
            ProviderCreditCommandOutcome::Replayed(result)
        }
        BeginCreditMovementOutcome::IdempotencyConflict => {
            ProviderCreditCommandOutcome::IdempotencyConflict
        }
        BeginCreditMovementOutcome::IdempotencyInProgress => {
            ProviderCreditCommandOutcome::IdempotencyInProgress
        }
        BeginCreditMovementOutcome::ProviderReferenceConflict => {
            ProviderCreditCommandOutcome::ProviderReferenceConflict
        }
        BeginCreditMovementOutcome::ContextChanged => {
            ProviderCreditCommandOutcome::FundingSourceNotActive
        }
        BeginCreditMovementOutcome::OperationalProfileChanged => {
            ProviderCreditCommandOutcome::ProviderNotActive
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use tigerbeetle_rustclient_tests_snapshot::Account;
    use uuid::Uuid;

    use super::{grant_within_limit, spendable_balance};
    use crate::domain::provider::{
        CreditGrantLimitMode, ProviderOperationalControls, ProviderOperationalProfileRecord,
        ProviderOperationalProfileStatus,
    };

    #[test]
    fn spendable_credit_reserves_pending_debits_but_not_pending_credits() {
        let account = account_with_balances(25, 100, 20, 30);
        assert_eq!(spendable_balance(&account).unwrap(), 45);
    }

    #[test]
    fn spendable_credit_rejects_an_overdrawn_contract() {
        let account = account_with_balances(20, 10, 0, 15);
        assert!(spendable_balance(&account).is_err());
    }

    #[test]
    fn fixed_limit_includes_posted_and_pending_provider_debits() {
        let profile = profile(CreditGrantLimitMode::FixedLimit, 100);
        let provider = account_with_balances(80, 10, 0, 10);
        let cms = account_with_balances(0, 0, 0, 0);
        assert!(grant_within_limit(&profile, &provider, &cms, 20).unwrap());
        assert!(!grant_within_limit(&profile, &provider, &cms, 21).unwrap());
    }

    #[test]
    fn cms_debt_limit_closes_grants_at_the_threshold() {
        let profile = profile(CreditGrantLimitMode::CmsDebtLimit, 100);
        let provider = account_with_balances(0, 0, 0, 0);
        let below = account_with_balances(0, 90, 0, 0);
        let at_limit = account_with_balances(0, 100, 0, 0);
        assert!(grant_within_limit(&profile, &provider, &below, 1).unwrap());
        assert!(!grant_within_limit(&profile, &provider, &at_limit, 1).unwrap());
    }

    #[test]
    fn outstanding_limit_offsets_provider_distribution_by_cms_settlement() {
        let profile = profile(CreditGrantLimitMode::OutstandingCreditLimit, 50);
        let provider = account_with_balances(80, 0, 0, 0);
        let cms = account_with_balances(0, 50, 0, 0);
        assert!(grant_within_limit(&profile, &provider, &cms, 20).unwrap());
        assert!(!grant_within_limit(&profile, &provider, &cms, 21).unwrap());
    }

    fn account_with_balances(
        debits_posted: u128,
        credits_posted: u128,
        credits_pending: u128,
        debits_pending: u128,
    ) -> Account {
        Account {
            debits_posted,
            credits_posted,
            credits_pending,
            debits_pending,
            ..Account::default()
        }
    }

    fn profile(mode: CreditGrantLimitMode, limit: u64) -> ProviderOperationalProfileRecord {
        let now = Utc::now();
        ProviderOperationalProfileRecord {
            provider_operational_profile_id: Uuid::new_v4(),
            provider_id: Uuid::new_v4(),
            status: ProviderOperationalProfileStatus::Active,
            version: 1,
            effective_at: now,
            controls: ProviderOperationalControls {
                timezone: "Asia/Tehran".to_string(),
                user_onboarding_enabled: true,
                active_windows: vec![],
                max_total_users: None,
                credit_grant_enabled: true,
                credit_grant_mode: mode,
                credit_grant_limit_amount_rials: limit,
                credit_return_enabled: true,
                new_assignment_enabled: true,
                same_pan_reprint_enabled: true,
                new_pan_replacement_enabled: true,
                attach_existing_multi_provider_card_enabled: true,
                event_delivery_enabled: true,
                event_delivery_disabled_reason: None,
            },
            superseded_by_profile_id: None,
            created_by_subject: "test".to_string(),
            updated_by_subject: "test".to_string(),
            change_reason: "test".to_string(),
            activated_at: Some(now),
            superseded_at: None,
            cancelled_at: None,
            created_at: now,
            updated_at: now,
        }
    }
}
