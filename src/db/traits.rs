use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::domain::{
    audit::NewAuditLog,
    card_range::{CardNumberRange, CardRange, CardRangeListPage, CardRangeListQuery, NewCardRange},
    idempotency::{IdempotencyRecord, NewIdempotencyRecord},
};

#[async_trait]
pub trait IdempotencyRepository: Send + Sync {
    async fn get_idempotency_record(
        &self,
        operation_type: &str,
        idempotency_key: &str,
    ) -> crate::db::error::DbResult<Option<IdempotencyRecord>>;

    async fn create_idempotency_record(
        &self,
        record: NewIdempotencyRecord,
    ) -> crate::db::error::DbResult<()>;

    async fn complete_idempotency_record(
        &self,
        operation_type: &str,
        idempotency_key: &str,
        resource_type: &str,
        resource_id: Uuid,
        response_snapshot: serde_json::Value,
    ) -> crate::db::error::DbResult<()>;
}

#[async_trait]
pub trait AuditRepository: Send + Sync {
    async fn insert_audit_log(&self, audit_log: NewAuditLog) -> crate::db::error::DbResult<()>;
}

#[async_trait]
pub trait CardRangeRepository: Send + Sync {
    async fn create_card_range(
        &self,
        card_range: NewCardRange,
        actor_subject: String,
    ) -> crate::db::error::DbResult<CardRange>;

    async fn get_card_range(
        &self,
        card_range_id: Uuid,
    ) -> crate::db::error::DbResult<Option<CardRange>>;

    async fn list_card_ranges(
        &self,
        query: CardRangeListQuery,
    ) -> crate::db::error::DbResult<CardRangeListPage>;

    async fn card_range_overlaps(
        &self,
        numbers: CardNumberRange,
    ) -> crate::db::error::DbResult<bool>;
}

pub trait AppRepository: IdempotencyRepository + AuditRepository + CardRangeRepository {}

#[async_trait]
impl<T> IdempotencyRepository for Arc<T>
where
    T: IdempotencyRepository + ?Sized,
{
    async fn get_idempotency_record(
        &self,
        operation_type: &str,
        idempotency_key: &str,
    ) -> crate::db::error::DbResult<Option<IdempotencyRecord>> {
        (**self)
            .get_idempotency_record(operation_type, idempotency_key)
            .await
    }

    async fn create_idempotency_record(
        &self,
        record: NewIdempotencyRecord,
    ) -> crate::db::error::DbResult<()> {
        (**self).create_idempotency_record(record).await
    }

    async fn complete_idempotency_record(
        &self,
        operation_type: &str,
        idempotency_key: &str,
        resource_type: &str,
        resource_id: Uuid,
        response_snapshot: serde_json::Value,
    ) -> crate::db::error::DbResult<()> {
        (**self)
            .complete_idempotency_record(
                operation_type,
                idempotency_key,
                resource_type,
                resource_id,
                response_snapshot,
            )
            .await
    }
}

#[async_trait]
impl<T> AuditRepository for Arc<T>
where
    T: AuditRepository + ?Sized,
{
    async fn insert_audit_log(&self, audit_log: NewAuditLog) -> crate::db::error::DbResult<()> {
        (**self).insert_audit_log(audit_log).await
    }
}

#[async_trait]
impl<T> CardRangeRepository for Arc<T>
where
    T: CardRangeRepository + ?Sized,
{
    async fn create_card_range(
        &self,
        card_range: NewCardRange,
        actor_subject: String,
    ) -> crate::db::error::DbResult<CardRange> {
        (**self).create_card_range(card_range, actor_subject).await
    }

    async fn get_card_range(
        &self,
        card_range_id: Uuid,
    ) -> crate::db::error::DbResult<Option<CardRange>> {
        (**self).get_card_range(card_range_id).await
    }

    async fn list_card_ranges(
        &self,
        query: CardRangeListQuery,
    ) -> crate::db::error::DbResult<CardRangeListPage> {
        (**self).list_card_ranges(query).await
    }

    async fn card_range_overlaps(
        &self,
        numbers: CardNumberRange,
    ) -> crate::db::error::DbResult<bool> {
        (**self).card_range_overlaps(numbers).await
    }
}
