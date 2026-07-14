// src/db/traits.rs
use anyhow::Result;
use async_trait::async_trait;
use uuid::Uuid;

use crate::domain::{
    card_policy::{CardRangePolicyDetails, NewCardRangePolicy},
    card_range::{
        CardRange, CardRangeProvider, CardRangeProviderStatus, CardRangeStatus, NewCardRange,
    },
    idempotency::{IdempotencyRecord, NewIdempotencyRecord},
};

#[async_trait]
pub trait CardRangeRepository: Send + Sync {
    async fn create_card_range(&self, card_range: NewCardRange) -> Result<CardRange>;
    async fn get_card_range(&self, card_range_id: Uuid) -> Result<Option<CardRange>>;
    async fn list_card_ranges(&self) -> Result<Vec<CardRange>>;
    async fn card_range_overlaps(
        &self,
        start_card_number: &str,
        end_card_number: &str,
    ) -> Result<bool>;
    async fn set_card_range_status(
        &self,
        card_range_id: Uuid,
        status: CardRangeStatus,
        actor_subject: String,
    ) -> Result<CardRange>;
    async fn update_card_range_metadata(
        &self,
        card_range_id: Uuid,
        metadata: serde_json::Value,
        actor_subject: String,
    ) -> Result<CardRange>;

    async fn upsert_card_range_provider(
        &self,
        card_range_id: Uuid,
        provider_id: Uuid,
        status: CardRangeProviderStatus,
        metadata: serde_json::Value,
        actor_subject: String,
    ) -> Result<CardRangeProvider>;
    async fn get_card_range_provider(
        &self,
        card_range_id: Uuid,
        provider_id: Uuid,
    ) -> Result<Option<CardRangeProvider>>;
    async fn set_card_range_provider_status(
        &self,
        card_range_id: Uuid,
        provider_id: Uuid,
        status: CardRangeProviderStatus,
        actor_subject: String,
    ) -> Result<CardRangeProvider>;
    async fn list_card_range_providers(
        &self,
        card_range_id: Uuid,
    ) -> Result<Vec<CardRangeProvider>>;
    async fn list_provider_card_ranges(&self, provider_id: Uuid) -> Result<Vec<CardRange>>;
    async fn count_card_range_providers(
        &self,
        card_range_id: Uuid,
        status: CardRangeProviderStatus,
    ) -> Result<i64>;
}

#[async_trait]
pub trait CardPolicyRepository: Send + Sync {
    async fn create_card_range_policy(
        &self,
        policy: NewCardRangePolicy,
    ) -> Result<CardRangePolicyDetails>;
    async fn get_active_card_range_policy(
        &self,
        card_range_id: Uuid,
    ) -> Result<Option<CardRangePolicyDetails>>;
}

#[async_trait]
pub trait IdempotencyRepository: Send + Sync {
    async fn get_idempotency_record(
        &self,
        operation_type: &str,
        idempotency_key: &str,
    ) -> Result<Option<IdempotencyRecord>>;
    async fn create_idempotency_record(&self, record: NewIdempotencyRecord) -> Result<()>;
    async fn complete_idempotency_record(
        &self,
        operation_type: &str,
        idempotency_key: &str,
        resource_type: &str,
        resource_id: Uuid,
        response_snapshot: serde_json::Value,
    ) -> Result<()>;
}

pub trait AppRepository:
    CardRangeRepository + CardPolicyRepository + IdempotencyRepository
{
}
