use std::sync::Arc;

use anyhow::Error;
use chrono::Utc;
use uuid::Uuid;

use crate::{
    db::traits::AppRepository,
    domain::card_policy::{CardRangePolicyDetails, NewCardRangePolicy},
};

pub type CardPolicyServiceResult<T> = Result<T, CardPolicyServiceError>;

#[derive(Debug)]
pub enum CardPolicyServiceError {
    CardRangeNotFound,
    InvalidPolicyProfile,
    Repository(Error),
}

impl std::fmt::Display for CardPolicyServiceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CardRangeNotFound => write!(formatter, "card range not found"),
            Self::InvalidPolicyProfile => {
                write!(formatter, "card policy profile must be a JSON object")
            }
            Self::Repository(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for CardPolicyServiceError {}

impl From<Error> for CardPolicyServiceError {
    fn from(error: Error) -> Self {
        Self::Repository(error)
    }
}

#[derive(Clone)]
pub struct CardPolicyService {
    repository: Arc<dyn AppRepository>,
}

impl CardPolicyService {
    pub fn new(repository: Arc<dyn AppRepository>) -> Self {
        Self { repository }
    }

    pub async fn create_or_replace_range_policy(
        &self,
        card_range_id: Uuid,
        profile: serde_json::Value,
        actor_subject: String,
    ) -> CardPolicyServiceResult<CardRangePolicyDetails> {
        if !profile.is_object() {
            return Err(CardPolicyServiceError::InvalidPolicyProfile);
        }

        self.repository
            .get_card_range(card_range_id)
            .await?
            .ok_or(CardPolicyServiceError::CardRangeNotFound)?;

        self.repository
            .create_card_range_policy(NewCardRangePolicy {
                card_range_id,
                profile_id: Uuid::new_v4(),
                profile,
                effective_at: Utc::now(),
                actor_subject,
            })
            .await
            .map_err(Into::into)
    }

    pub async fn get_active_range_policy(
        &self,
        card_range_id: Uuid,
    ) -> CardPolicyServiceResult<Option<CardRangePolicyDetails>> {
        self.repository
            .get_active_card_range_policy(card_range_id)
            .await
            .map_err(Into::into)
    }
}
