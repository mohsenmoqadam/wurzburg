use std::sync::Arc;

use uuid::Uuid;

use crate::{
    api::{
        auth::{TrustedActor, require_scope},
        command::MutationCommandContext,
        error::ApiError,
        result_codes::WurzburgResultCode,
    },
    db::oracle::{CreateCardRangePersistenceOutcome, OracleRepository},
    domain::card_range::{CardRange, CardRangeListPage, CardRangeListQuery, NewCardRange},
};

#[derive(Debug, Clone, PartialEq)]
pub enum CreateCardRangeOutcome {
    Created(CardRange),
    Replayed(serde_json::Value),
}

#[derive(Clone)]
pub struct CardRangeService {
    repository: Arc<OracleRepository>,
}

impl CardRangeService {
    pub fn new(repository: Arc<OracleRepository>) -> Self {
        Self { repository }
    }

    #[tracing::instrument(skip(self, command_context, card_range))]
    pub async fn create_card_range(
        &self,
        command_context: &MutationCommandContext,
        card_range: NewCardRange,
    ) -> Result<CreateCardRangeOutcome, ApiError> {
        require_scope(&command_context.actor, "platform.card_ranges:write")?;

        card_range.validate().map_err(|error| {
            ApiError::with_message(
                WurzburgResultCode::InvalidCardRangeBoundary,
                error.to_string(),
            )
        })?;

        let outcome = self
            .repository
            .create_card_range_atomic(command_context.clone(), card_range)
            .await
            .map_err(ApiError::from_database)?;

        match outcome {
            CreateCardRangePersistenceOutcome::Created(created) => {
                Ok(CreateCardRangeOutcome::Created(created))
            }
            CreateCardRangePersistenceOutcome::Replayed(snapshot) => {
                Ok(CreateCardRangeOutcome::Replayed(snapshot))
            }
            CreateCardRangePersistenceOutcome::IdempotencyConflict => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyKeyConflict))
            }
            CreateCardRangePersistenceOutcome::IdempotencyInProgress => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyInProgress))
            }
            CreateCardRangePersistenceOutcome::IdempotencyInvalidState => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyError))
            }
            CreateCardRangePersistenceOutcome::Overlap => {
                Err(ApiError::new(WurzburgResultCode::CardRangeOverlap))
            }
        }
    }

    #[tracing::instrument(skip(self, actor), fields(card_range_id = %card_range_id))]
    pub async fn get_card_range(
        &self,
        actor: &TrustedActor,
        card_range_id: Uuid,
    ) -> Result<CardRange, ApiError> {
        require_scope(actor, "platform.card_ranges:read")?;

        self.repository
            .get_card_range(card_range_id)
            .await
            .map_err(ApiError::from_database)?
            .ok_or_else(|| ApiError::new(WurzburgResultCode::CardRangeNotFound))
    }

    #[tracing::instrument(skip(self, actor, query), fields(limit = query.limit))]
    pub async fn list_card_ranges(
        &self,
        actor: &TrustedActor,
        query: CardRangeListQuery,
    ) -> Result<CardRangeListPage, ApiError> {
        require_scope(actor, "platform.card_ranges:read")?;

        self.repository
            .list_card_ranges(query)
            .await
            .map_err(ApiError::from_database)
    }
}
