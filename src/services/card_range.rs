use async_trait::async_trait;
use uuid::Uuid;

use crate::{
    api::{
        auth::require_scope,
        command::{IdempotencyStart, IdempotencyStarter, MutationCommandContext},
        error::ApiError,
        result_codes::WurzburgResultCode,
    },
    db::traits::{AuditRepository, CardRangeRepository, IdempotencyRepository},
    domain::{
        audit::{AuditAction, NewAuditLog},
        card_range::{CardRange, NewCardRange},
    },
};

#[derive(Debug, Clone, PartialEq)]
pub enum CreateCardRangeOutcome {
    Created(CardRange),
    Replayed(serde_json::Value),
}

#[async_trait]
pub trait CardRangeServiceRepository:
    CardRangeRepository + AuditRepository + IdempotencyRepository + IdempotencyStarter + Sync
{
}

impl<T> CardRangeServiceRepository for T where
    T: CardRangeRepository + AuditRepository + IdempotencyRepository + IdempotencyStarter + Sync
{
}

#[derive(Clone)]
pub struct CardRangeService<R> {
    repository: R,
}

impl<R> CardRangeService<R>
where
    R: CardRangeServiceRepository,
{
    pub fn new(repository: R) -> Self {
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

        match self
            .repository
            .start_idempotent_mutation(command_context)
            .await?
        {
            IdempotencyStart::Replay(snapshot) => {
                return Ok(CreateCardRangeOutcome::Replayed(snapshot));
            }
            IdempotencyStart::Execute => {}
        }

        if self
            .repository
            .card_range_overlaps(card_range.numbers.clone())
            .await
            .map_err(|error| {
                ApiError::with_message(WurzburgResultCode::SystemError, error.to_string())
            })?
        {
            return Err(ApiError::new(WurzburgResultCode::CardRangeOverlap));
        }

        let created = self
            .repository
            .create_card_range(card_range, command_context.actor.subject.clone())
            .await
            .map_err(|error| {
                ApiError::with_message(WurzburgResultCode::SystemError, error.to_string())
            })?;

        // The audit row is written before idempotency completion so a replayable
        // success response is never recorded without immutable business evidence.
        self.repository
            .insert_audit_log(NewAuditLog {
                audit_log_id: Uuid::new_v4(),
                entity_type: "CARD_RANGE".to_string(),
                entity_id: created.card_range_id,
                action_type: AuditAction::Insert,
                reason: Some("card range created".to_string()),
                old_values: None,
                new_values: Some(created.replay_snapshot()),
                context: command_context.audit_context(),
            })
            .await
            .map_err(|error| {
                ApiError::with_message(WurzburgResultCode::SystemError, error.to_string())
            })?;

        self.repository
            .complete_idempotency_record(
                &command_context.operation_type,
                command_context.idempotency_key.as_str(),
                "card_range",
                created.card_range_id,
                created.replay_snapshot(),
            )
            .await
            .map_err(|error| {
                ApiError::with_message(WurzburgResultCode::IdempotencyError, error.to_string())
            })?;

        Ok(CreateCardRangeOutcome::Created(created))
    }
}
