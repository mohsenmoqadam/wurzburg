use std::sync::Arc;

use uuid::Uuid;

use crate::{
    api::{
        auth::{TrustedActor, require_scope},
        command::MutationCommandContext,
        error::ApiError,
        result_codes::WurzburgResultCode,
    },
    db::oracle::{
        CardRangeMutation, CardRangeMutationPersistenceOutcome, CardRangeMutationResult,
        CreateCardRangePersistenceOutcome, OracleRepository,
    },
    domain::card_range::{CardRange, CardRangeListPage, CardRangeListQuery, NewCardRange},
};

#[derive(Debug, Clone, PartialEq)]
pub enum CreateCardRangeOutcome {
    Created(Box<CardRange>),
    Replayed(serde_json::Value),
}

#[derive(Debug, Clone, PartialEq)]
pub enum CardRangeMutationServiceOutcome {
    Applied(Box<CardRangeMutationResult>),
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

    #[tracing::instrument(skip(self, context, mutation), fields(card_range_id = %card_range_id))]
    pub async fn mutate_card_range(
        &self,
        context: &MutationCommandContext,
        card_range_id: Uuid,
        mutation: CardRangeMutation,
    ) -> Result<CardRangeMutationServiceOutcome, ApiError> {
        require_scope(&context.actor, "platform.card_ranges:write")?;
        let outcome = self
            .repository
            .mutate_card_range_atomic(context.clone(), card_range_id, mutation)
            .await
            .map_err(ApiError::from_database)?;
        match outcome {
            CardRangeMutationPersistenceOutcome::Applied(result) => {
                Ok(CardRangeMutationServiceOutcome::Applied(result))
            }
            CardRangeMutationPersistenceOutcome::Replayed(value) => {
                Ok(CardRangeMutationServiceOutcome::Replayed(value))
            }
            CardRangeMutationPersistenceOutcome::NotFound => {
                Err(ApiError::new(WurzburgResultCode::CardRangeNotFound))
            }
            CardRangeMutationPersistenceOutcome::InvalidTransition => Err(ApiError::new(
                WurzburgResultCode::CardRangeInvalidTransition,
            )),
            CardRangeMutationPersistenceOutcome::Immutable => {
                Err(ApiError::new(WurzburgResultCode::CardRangeImmutableField))
            }
            CardRangeMutationPersistenceOutcome::PrerequisitesMissing => Err(ApiError::new(
                WurzburgResultCode::CardRangePrerequisitesMissing,
            )),
            CardRangeMutationPersistenceOutcome::FeeProfilesMissing => Err(ApiError::with_details(
                WurzburgResultCode::CardRangePrerequisitesMissing,
                serde_json::json!({"missing_prerequisite": "PROVIDER_FEE_PROFILE"}),
            )),
            CardRangeMutationPersistenceOutcome::PublicationPending => Err(ApiError::new(
                WurzburgResultCode::RangeControlPublicationPending,
            )),
            CardRangeMutationPersistenceOutcome::Overlap => {
                Err(ApiError::new(WurzburgResultCode::CardRangeOverlap))
            }
            CardRangeMutationPersistenceOutcome::ContractInvalid(message) => Err(
                ApiError::with_message(WurzburgResultCode::InvalidCardRangeBoundary, message),
            ),
            CardRangeMutationPersistenceOutcome::IdempotencyConflict => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyKeyConflict))
            }
            CardRangeMutationPersistenceOutcome::IdempotencyInProgress => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyInProgress))
            }
            CardRangeMutationPersistenceOutcome::IdempotencyInvalidState => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyError))
            }
        }
    }

    #[tracing::instrument(skip(self, actor), fields(card_range_id = %card_range_id, actor.subject = %actor.subject))]
    pub async fn list_provider_eligibility(
        &self,
        actor: &TrustedActor,
        card_range_id: Uuid,
    ) -> Result<Vec<crate::domain::card_range::CardRangeProviderEligibility>, ApiError> {
        require_scope(actor, "platform.card_ranges:read")?;
        if self
            .repository
            .get_card_range(card_range_id)
            .await
            .map_err(ApiError::from_database)?
            .is_none()
        {
            return Err(ApiError::new(WurzburgResultCode::CardRangeNotFound));
        }
        self.repository
            .list_card_range_providers(card_range_id)
            .await
            .map_err(ApiError::from_database)
    }

    #[tracing::instrument(skip(self, actor), fields(operation.id = %operation_id, actor.subject = %actor.subject))]
    pub async fn get_operation(
        &self,
        actor: &TrustedActor,
        operation_id: Uuid,
    ) -> Result<crate::db::oracle::IntegrationOperationView, ApiError> {
        require_scope(actor, "platform.card_ranges:read")?;
        self.repository
            .get_integration_operation(operation_id)
            .await
            .map_err(ApiError::from_database)?
            .ok_or_else(|| ApiError::new(WurzburgResultCode::IntegrationOperationNotFound))
    }
}
