use async_trait::async_trait;
use uuid::Uuid;

use crate::{
    api::{
        auth::TrustedActor, error::ApiError, idempotency::IdempotencyKey,
        request_context::TrustedRequestContext, result_codes::WurzburgResultCode,
    },
    db::traits::IdempotencyRepository,
    domain::{
        audit::TrustedAuditContext,
        idempotency::{IdempotencyRecord, IdempotencyStatus, NewIdempotencyRecord},
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationCommandContext {
    pub operation_type: String,
    pub actor: TrustedActor,
    pub request: TrustedRequestContext,
    pub idempotency_key: IdempotencyKey,
    pub request_hash: String,
}

impl MutationCommandContext {
    pub fn audit_context(&self) -> TrustedAuditContext {
        TrustedAuditContext {
            actor_subject: self.actor.subject.clone(),
            actor_client_id: Some(self.actor.client_id.clone()),
            actor_provider_id: self.actor.provider_id,
            actor_user_id: self.actor.user_id,
            actor_issuer: Some(self.actor.issuer.clone()),
            source_ip: Some(self.request.client_ip),
            correlation_id: self.request.correlation_id.clone(),
            request_id: self.request.request_id.to_string(),
        }
    }

    pub fn new_idempotency_record(&self) -> NewIdempotencyRecord {
        NewIdempotencyRecord {
            idempotency_record_id: Uuid::new_v4(),
            operation_type: self.operation_type.clone(),
            idempotency_key: self.idempotency_key.as_str().to_string(),
            request_hash: self.request_hash.clone(),
            created_by_subject: self.actor.subject.clone(),
            created_by_client_id: Some(self.actor.client_id.clone()),
            actor_provider_id: self.actor.provider_id,
            actor_user_id: self.actor.user_id,
            correlation_id: self.request.correlation_id.clone(),
            request_id: self.request.request_id.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum IdempotencyStart {
    Execute,
    Replay(serde_json::Value),
}

#[async_trait]
pub trait IdempotencyStarter {
    async fn start_idempotent_mutation(
        &self,
        context: &MutationCommandContext,
    ) -> Result<IdempotencyStart, ApiError>;
}

#[async_trait]
impl<T> IdempotencyStarter for T
where
    T: IdempotencyRepository + Sync,
{
    async fn start_idempotent_mutation(
        &self,
        context: &MutationCommandContext,
    ) -> Result<IdempotencyStart, ApiError> {
        let existing = self
            .get_idempotency_record(&context.operation_type, context.idempotency_key.as_str())
            .await
            .map_err(|error| {
                ApiError::with_message(WurzburgResultCode::IdempotencyError, error.to_string())
            })?;

        let Some(existing) = existing else {
            self.create_idempotency_record(context.new_idempotency_record())
                .await
                .map_err(|error| {
                    ApiError::with_message(WurzburgResultCode::IdempotencyError, error.to_string())
                })?;
            return Ok(IdempotencyStart::Execute);
        };

        classify_existing_idempotency_record(&existing, &context.request_hash)
    }
}

pub fn classify_existing_idempotency_record(
    existing: &IdempotencyRecord,
    request_hash: &str,
) -> Result<IdempotencyStart, ApiError> {
    if existing.request_hash != request_hash {
        return Err(ApiError::new(WurzburgResultCode::IdempotencyKeyConflict));
    }

    match existing.status {
        IdempotencyStatus::Completed => {
            let snapshot = existing
                .response_snapshot
                .clone()
                .ok_or_else(|| ApiError::new(WurzburgResultCode::IdempotencyError))?;
            Ok(IdempotencyStart::Replay(snapshot))
        }
        IdempotencyStatus::InProgress => {
            Err(ApiError::new(WurzburgResultCode::IdempotencyInProgress))
        }
        IdempotencyStatus::Failed | IdempotencyStatus::Conflict => {
            Err(ApiError::new(WurzburgResultCode::IdempotencyError))
        }
    }
}
