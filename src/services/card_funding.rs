use std::{error::Error, fmt, sync::Arc};

use uuid::Uuid;

use crate::{
    api::command::MutationCommandContext,
    db::{
        error::DbError,
        oracle::{FundingOrderCommand, FundingOrderPersistenceOutcome, OracleRepository},
    },
    domain::user_card::FundingOrderSource,
    messaging::contract::InternalEventHeaders,
    runtime_profiles::{CardProfileLockError, CardProfileLockManager, CardProfileLockOutcome},
};

#[derive(Debug)]
pub enum CardFundingServiceError {
    Database(DbError),
    RuntimeProfile(CardProfileLockError),
}

impl fmt::Display for CardFundingServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Database(_) => "database",
            Self::RuntimeProfile(_) => "runtime_profile",
        })
    }
}
impl Error for CardFundingServiceError {}
impl From<DbError> for CardFundingServiceError {
    fn from(value: DbError) -> Self {
        Self::Database(value)
    }
}
impl From<CardProfileLockError> for CardFundingServiceError {
    fn from(value: CardProfileLockError) -> Self {
        Self::RuntimeProfile(value)
    }
}

pub struct CardFundingService {
    repository: Arc<OracleRepository>,
    locks: Arc<CardProfileLockManager>,
}

impl CardFundingService {
    pub fn new(repository: Arc<OracleRepository>, locks: Arc<CardProfileLockManager>) -> Self {
        Self { repository, locks }
    }

    #[tracing::instrument(skip(self, context, card_number, sources), fields(operation.type="card_funding_order.update"))]
    pub async fn update_order(
        &self,
        context: &MutationCommandContext,
        card_number: &str,
        expected_state_version: i64,
        expected_owner: Option<Uuid>,
        sources: Vec<FundingOrderSource>,
        reason: String,
    ) -> Result<FundingOrderPersistenceOutcome, CardFundingServiceError> {
        if let Some(replay) = self.repository.find_funding_order_replay(context).await? {
            return Ok(replay);
        }
        let operation_id = Uuid::new_v4();
        let lock = match self.locks.acquire(card_number, operation_id).await? {
            CardProfileLockOutcome::Acquired(lock) => lock,
            CardProfileLockOutcome::Busy => {
                return Ok(FundingOrderPersistenceOutcome::CardProfileLocked);
            }
        };
        let headers = InternalEventHeaders::from_current_span(
            context.request.correlation_id.clone(),
            context.request.request_id.to_string(),
        );
        let outcome = self
            .repository
            .update_card_funding_order_atomic(
                context,
                FundingOrderCommand {
                    card_number: card_number.to_string(),
                    expected_state_version,
                    expected_owner,
                    sources,
                    reason,
                    operation_id,
                    headers,
                },
            )
            .await;
        match outcome {
            Ok(FundingOrderPersistenceOutcome::Applied(result)) => {
                self.locks.invalidate_profile(&lock).await?;
                Ok(FundingOrderPersistenceOutcome::Applied(result))
            }
            Ok(FundingOrderPersistenceOutcome::Replayed(result)) => {
                let _ = self.locks.release(&lock).await?;
                Ok(FundingOrderPersistenceOutcome::Replayed(result))
            }
            Ok(other) => {
                let _ = self.locks.release(&lock).await?;
                Ok(other)
            }
            Err(error) => Err(CardFundingServiceError::Database(error)),
        }
    }
}
