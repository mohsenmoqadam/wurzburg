use std::sync::Arc;

use uuid::Uuid;

use crate::{
    api::{
        auth::{TrustedActor, require_scope},
        command::MutationCommandContext,
        error::ApiError,
        result_codes::WurzburgResultCode,
    },
    db::oracle::{OracleRepository, SetCardPolicyPersistenceOutcome, SetCardPolicyResult},
    domain::{
        card_policy::{CardPolicyProfile, DesiredCardPolicy},
        card_range::{FundingMode, LimitCalendar, WithdrawalLimitAuthority},
    },
};

#[derive(Debug, Clone, PartialEq)]
pub struct CardPolicyView {
    pub profile: CardPolicyProfile,
    pub funding_mode: FundingMode,
    pub withdrawal_limit_authority: WithdrawalLimitAuthority,
    pub limit_calendar: Option<LimitCalendar>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetCardPolicyOutcome {
    Applied(Box<SetCardPolicyResult>),
    Replayed(serde_json::Value),
}

#[derive(Clone)]
pub struct CardPolicyService {
    repository: Arc<OracleRepository>,
}

impl CardPolicyService {
    pub fn new(repository: Arc<OracleRepository>) -> Self {
        Self { repository }
    }

    #[tracing::instrument(skip(self, command_context, desired_policy), fields(card_range_id = %card_range_id))]
    pub async fn set_policy(
        &self,
        command_context: &MutationCommandContext,
        card_range_id: Uuid,
        desired_policy: DesiredCardPolicy,
    ) -> Result<SetCardPolicyOutcome, ApiError> {
        require_scope(&command_context.actor, "platform.policies:write")?;
        let card_range = self
            .repository
            .get_card_range(card_range_id)
            .await
            .map_err(ApiError::from_database)?
            .ok_or_else(|| ApiError::new(WurzburgResultCode::CardRangeNotFound))?;

        match card_range.withdrawal_limit_authority {
            WithdrawalLimitAuthority::Platform => desired_policy.validate_for_platform(),
            WithdrawalLimitAuthority::Cms => desired_policy.validate_for_cms(),
        }
        .map_err(|error| {
            ApiError::with_message(
                WurzburgResultCode::CardPolicyContractInvalid,
                error.to_string(),
            )
        })?;

        let outcome = self
            .repository
            .set_card_policy_atomic(command_context.clone(), card_range_id, desired_policy)
            .await
            .map_err(ApiError::from_database)?;

        match outcome {
            SetCardPolicyPersistenceOutcome::Applied(result) => {
                Ok(SetCardPolicyOutcome::Applied(result))
            }
            SetCardPolicyPersistenceOutcome::Replayed(snapshot) => {
                Ok(SetCardPolicyOutcome::Replayed(snapshot))
            }
            SetCardPolicyPersistenceOutcome::RangeNotFound => {
                Err(ApiError::new(WurzburgResultCode::CardRangeNotFound))
            }
            SetCardPolicyPersistenceOutcome::ContractInvalid(message) => Err(
                ApiError::with_message(WurzburgResultCode::CardPolicyContractInvalid, message),
            ),
            SetCardPolicyPersistenceOutcome::DraftFrozen => {
                Err(ApiError::new(WurzburgResultCode::PolicyDraftFrozen))
            }
            SetCardPolicyPersistenceOutcome::IdempotencyConflict => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyKeyConflict))
            }
            SetCardPolicyPersistenceOutcome::IdempotencyInProgress => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyInProgress))
            }
            SetCardPolicyPersistenceOutcome::IdempotencyInvalidState => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyError))
            }
        }
    }

    #[tracing::instrument(skip(self, actor), fields(card_range_id = %card_range_id))]
    pub async fn get_current_policy(
        &self,
        actor: &TrustedActor,
        card_range_id: Uuid,
    ) -> Result<CardPolicyView, ApiError> {
        require_scope(actor, "platform.policies:read")?;
        let profile = self
            .repository
            .get_current_card_policy(card_range_id)
            .await
            .map_err(ApiError::from_database)?
            .ok_or_else(|| ApiError::new(WurzburgResultCode::CardPolicyNotFound))?;
        self.compose_view(card_range_id, profile).await
    }

    #[tracing::instrument(skip(self, actor), fields(card_range_id = %card_range_id, policy_id = %policy_id))]
    pub async fn get_policy(
        &self,
        actor: &TrustedActor,
        card_range_id: Uuid,
        policy_id: Uuid,
    ) -> Result<CardPolicyView, ApiError> {
        require_scope(actor, "platform.policies:read")?;
        let profile = self
            .repository
            .get_card_policy(card_range_id, policy_id)
            .await
            .map_err(ApiError::from_database)?
            .ok_or_else(|| ApiError::new(WurzburgResultCode::CardPolicyNotFound))?;
        self.compose_view(card_range_id, profile).await
    }

    #[tracing::instrument(skip(self, actor), fields(card_range_id = %card_range_id, limit))]
    pub async fn list_policies(
        &self,
        actor: &TrustedActor,
        card_range_id: Uuid,
        before_version: Option<i64>,
        limit: u16,
    ) -> Result<Vec<CardPolicyView>, ApiError> {
        require_scope(actor, "platform.policies:read")?;
        if limit == 0 || limit > 100 || before_version.is_some_and(|version| version <= 0) {
            return Err(ApiError::new(WurzburgResultCode::InvalidCardRangeFilter));
        }
        let profiles = self
            .repository
            .list_card_policies(card_range_id, before_version, limit)
            .await
            .map_err(ApiError::from_database)?;
        let card_range = self
            .repository
            .get_card_range(card_range_id)
            .await
            .map_err(ApiError::from_database)?
            .ok_or_else(|| ApiError::new(WurzburgResultCode::CardRangeNotFound))?;
        Ok(profiles
            .into_iter()
            .map(|profile| CardPolicyView {
                profile,
                funding_mode: card_range.funding_mode,
                withdrawal_limit_authority: card_range.withdrawal_limit_authority,
                limit_calendar: card_range.limit_calendar.clone(),
            })
            .collect())
    }

    async fn compose_view(
        &self,
        card_range_id: Uuid,
        profile: CardPolicyProfile,
    ) -> Result<CardPolicyView, ApiError> {
        let card_range = self
            .repository
            .get_card_range(card_range_id)
            .await
            .map_err(ApiError::from_database)?
            .ok_or_else(|| ApiError::new(WurzburgResultCode::CardRangeNotFound))?;
        Ok(CardPolicyView {
            profile,
            funding_mode: card_range.funding_mode,
            withdrawal_limit_authority: card_range.withdrawal_limit_authority,
            limit_calendar: card_range.limit_calendar,
        })
    }
}
