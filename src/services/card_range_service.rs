use std::sync::Arc;

use anyhow::Error;
use uuid::Uuid;

use crate::{
    db::error::DbError,
    db::traits::AppRepository,
    domain::card_range::{
        CardRange, CardRangeProvider, CardRangeProviderStatus, CardRangeStatus, FundingMode,
        NewCardRange,
    },
};

pub type CardRangeServiceResult<T> = Result<T, CardRangeServiceError>;

#[derive(Debug)]
pub enum CardRangeServiceError {
    CardRangeNotFound,
    CardRangeProviderNotFound,
    CardRangeOverlap,
    InvalidCardNumber(&'static str),
    InvalidRangeOrder(&'static str),
    InvalidMetadata,
    SingleProviderActivationRequiresExactlyOneProvider,
    MultiProviderActivationRequiresAtLeastOneProvider,
    SingleProviderAllowsOnlyOneActiveProvider,
    Repository(Error),
}

impl std::fmt::Display for CardRangeServiceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CardRangeNotFound => write!(formatter, "card range not found"),
            Self::CardRangeProviderNotFound => write!(formatter, "card range provider not found"),
            Self::CardRangeOverlap => write!(formatter, "card range overlaps an existing range"),
            Self::InvalidCardNumber(message) => write!(formatter, "{message}"),
            Self::InvalidRangeOrder(message) => write!(formatter, "{message}"),
            Self::InvalidMetadata => write!(formatter, "metadata must be a JSON object"),
            Self::SingleProviderActivationRequiresExactlyOneProvider => write!(
                formatter,
                "single-provider ranges require exactly one active provider before activation"
            ),
            Self::MultiProviderActivationRequiresAtLeastOneProvider => write!(
                formatter,
                "multi-provider ranges require at least one active provider before activation"
            ),
            Self::SingleProviderAllowsOnlyOneActiveProvider => {
                write!(
                    formatter,
                    "single-provider ranges can only have one active provider"
                )
            }
            Self::Repository(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for CardRangeServiceError {}

impl From<Error> for CardRangeServiceError {
    fn from(error: Error) -> Self {
        if let Some(DbError::Conflict(message)) = error.downcast_ref::<DbError>() {
            if message.contains("card range overlaps") {
                return Self::CardRangeOverlap;
            }
        }

        Self::Repository(error)
    }
}

#[derive(Clone)]
pub struct CardRangeService {
    repository: Arc<dyn AppRepository>,
}

impl CardRangeService {
    pub fn new(repository: Arc<dyn AppRepository>) -> Self {
        Self { repository }
    }

    pub async fn create_card_range(
        &self,
        start_card_number: String,
        end_card_number: String,
        funding_mode: FundingMode,
        metadata: serde_json::Value,
        actor_subject: String,
    ) -> CardRangeServiceResult<CardRange> {
        let start = normalize_card_number(&start_card_number)?;
        let end = normalize_card_number(&end_card_number)?;
        validate_range_order(&start, &end)?;
        validate_metadata_object(&metadata)?;

        if self.repository.card_range_overlaps(&start, &end).await? {
            return Err(CardRangeServiceError::CardRangeOverlap);
        }

        self.repository
            .create_card_range(NewCardRange {
                id: Uuid::new_v4(),
                start_card_number: start,
                end_card_number: end,
                funding_mode,
                metadata,
                actor_subject,
            })
            .await
            .map_err(Into::into)
    }

    pub async fn get_card_range(
        &self,
        card_range_id: Uuid,
    ) -> CardRangeServiceResult<Option<CardRange>> {
        self.repository
            .get_card_range(card_range_id)
            .await
            .map_err(Into::into)
    }

    pub async fn list_card_ranges(&self) -> CardRangeServiceResult<Vec<CardRange>> {
        self.repository.list_card_ranges().await.map_err(Into::into)
    }

    pub async fn update_card_range_metadata(
        &self,
        card_range_id: Uuid,
        metadata: serde_json::Value,
        actor_subject: String,
    ) -> CardRangeServiceResult<CardRange> {
        validate_metadata_object(&metadata)?;
        self.ensure_card_range_exists(card_range_id).await?;

        self.repository
            .update_card_range_metadata(card_range_id, metadata, actor_subject)
            .await
            .map_err(Into::into)
    }

    pub async fn activate_card_range(
        &self,
        card_range_id: Uuid,
        actor_subject: String,
    ) -> CardRangeServiceResult<CardRange> {
        let card_range = self
            .repository
            .get_card_range(card_range_id)
            .await?
            .ok_or(CardRangeServiceError::CardRangeNotFound)?;

        let active_provider_count = self
            .repository
            .count_card_range_providers(card_range_id, CardRangeProviderStatus::Active)
            .await?;

        match card_range.funding_mode {
            FundingMode::SingleProvider if active_provider_count != 1 => {
                return Err(
                    CardRangeServiceError::SingleProviderActivationRequiresExactlyOneProvider,
                );
            }
            FundingMode::MultiProvider if active_provider_count < 1 => {
                return Err(
                    CardRangeServiceError::MultiProviderActivationRequiresAtLeastOneProvider,
                );
            }
            _ => {}
        }

        self.repository
            .set_card_range_status(card_range_id, CardRangeStatus::Active, actor_subject)
            .await
            .map_err(Into::into)
    }

    pub async fn suspend_card_range(
        &self,
        card_range_id: Uuid,
        actor_subject: String,
    ) -> CardRangeServiceResult<CardRange> {
        self.ensure_card_range_exists(card_range_id).await?;

        self.repository
            .set_card_range_status(card_range_id, CardRangeStatus::Suspended, actor_subject)
            .await
            .map_err(Into::into)
    }

    pub async fn attach_provider(
        &self,
        card_range_id: Uuid,
        provider_id: Uuid,
        metadata: serde_json::Value,
        actor_subject: String,
    ) -> CardRangeServiceResult<CardRangeProvider> {
        validate_metadata_object(&metadata)?;

        let card_range = self
            .repository
            .get_card_range(card_range_id)
            .await?
            .ok_or(CardRangeServiceError::CardRangeNotFound)?;

        if matches!(card_range.funding_mode, FundingMode::SingleProvider) {
            let active_provider_count = self
                .repository
                .count_card_range_providers(card_range_id, CardRangeProviderStatus::Active)
                .await?;
            let existing = self
                .repository
                .get_card_range_provider(card_range_id, provider_id)
                .await?;

            if active_provider_count >= 1 && existing.is_none() {
                return Err(CardRangeServiceError::SingleProviderAllowsOnlyOneActiveProvider);
            }
        }

        self.repository
            .upsert_card_range_provider(
                card_range_id,
                provider_id,
                CardRangeProviderStatus::Active,
                metadata,
                actor_subject,
            )
            .await
            .map_err(Into::into)
    }

    pub async fn suspend_provider(
        &self,
        card_range_id: Uuid,
        provider_id: Uuid,
        actor_subject: String,
    ) -> CardRangeServiceResult<CardRangeProvider> {
        self.ensure_card_range_exists(card_range_id).await?;
        self.repository
            .get_card_range_provider(card_range_id, provider_id)
            .await?
            .ok_or(CardRangeServiceError::CardRangeProviderNotFound)?;

        self.repository
            .set_card_range_provider_status(
                card_range_id,
                provider_id,
                CardRangeProviderStatus::Suspended,
                actor_subject,
            )
            .await
            .map_err(Into::into)
    }

    pub async fn list_range_providers(
        &self,
        card_range_id: Uuid,
    ) -> CardRangeServiceResult<Vec<CardRangeProvider>> {
        self.ensure_card_range_exists(card_range_id).await?;

        self.repository
            .list_card_range_providers(card_range_id)
            .await
            .map_err(Into::into)
    }

    pub async fn list_provider_ranges(
        &self,
        provider_id: Uuid,
    ) -> CardRangeServiceResult<Vec<CardRange>> {
        self.repository
            .list_provider_card_ranges(provider_id)
            .await
            .map_err(Into::into)
    }

    async fn ensure_card_range_exists(&self, card_range_id: Uuid) -> CardRangeServiceResult<()> {
        self.repository
            .get_card_range(card_range_id)
            .await?
            .ok_or(CardRangeServiceError::CardRangeNotFound)?;
        Ok(())
    }
}

pub fn normalize_card_number(value: &str) -> CardRangeServiceResult<String> {
    let normalized: String = value.chars().filter(|ch| !ch.is_whitespace()).collect();

    if normalized.len() < 12 || normalized.len() > 19 {
        return Err(CardRangeServiceError::InvalidCardNumber(
            "card number must contain 12 to 19 digits",
        ));
    }

    if !normalized.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(CardRangeServiceError::InvalidCardNumber(
            "card number must contain only digits",
        ));
    }

    Ok(normalized)
}

fn validate_range_order(start: &str, end: &str) -> CardRangeServiceResult<()> {
    if start.len() != end.len() {
        return Err(CardRangeServiceError::InvalidRangeOrder(
            "start and end card numbers must have the same length",
        ));
    }

    if start > end {
        return Err(CardRangeServiceError::InvalidRangeOrder(
            "start card number must be less than or equal to end card number",
        ));
    }

    Ok(())
}

fn validate_metadata_object(metadata: &serde_json::Value) -> CardRangeServiceResult<()> {
    if metadata.is_object() {
        Ok(())
    } else {
        Err(CardRangeServiceError::InvalidMetadata)
    }
}
