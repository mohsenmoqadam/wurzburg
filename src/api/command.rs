use uuid::Uuid;

use crate::{
    api::{
        auth::TrustedActor, idempotency::IdempotencyKey, request_context::TrustedRequestContext,
    },
    domain::{audit::TrustedAuditContext, idempotency::NewIdempotencyRecord},
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
        trusted_audit_context(&self.actor, &self.request)
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

pub fn trusted_audit_context(
    actor: &TrustedActor,
    request: &TrustedRequestContext,
) -> TrustedAuditContext {
    TrustedAuditContext {
        actor_subject: actor.subject.clone(),
        actor_client_id: Some(actor.client_id.clone()),
        actor_provider_id: actor.provider_id,
        actor_user_id: actor.user_id,
        actor_issuer: Some(actor.issuer.clone()),
        source_ip: Some(request.client_ip),
        correlation_id: request.correlation_id.clone(),
        request_id: request.request_id.to_string(),
    }
}
