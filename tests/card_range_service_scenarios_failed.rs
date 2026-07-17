use std::{
    net::IpAddr,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use uuid::Uuid;
use wurzburg::{
    api::{
        auth::{TrustedActor, VerifiedActorClaims},
        command::MutationCommandContext,
        idempotency::IdempotencyKey,
        request_context::{BackendToken, TrustedRequestContext},
    },
    config::BackendTokenTransport,
    db::{
        error::DbResult,
        traits::{AuditRepository, CardRangeRepository, IdempotencyRepository},
    },
    domain::{
        audit::NewAuditLog,
        card_range::{
            CardNumberRange, CardRange, CmsOperationMode, FundingMode, NewCardRange,
            WithdrawalLimitAuthority,
        },
        idempotency::{IdempotencyRecord, NewIdempotencyRecord},
    },
    services::card_range::CardRangeService,
};

#[derive(Default, Clone)]
struct FailureRepository {
    overlap: Arc<Mutex<bool>>,
}

#[async_trait]
impl CardRangeRepository for FailureRepository {
    async fn create_card_range(
        &self,
        _card_range: NewCardRange,
        _actor_subject: String,
    ) -> DbResult<CardRange> {
        panic!("create_card_range should not be reached in this failed scenario")
    }

    async fn get_card_range(&self, _card_range_id: Uuid) -> DbResult<Option<CardRange>> {
        Ok(None)
    }

    async fn card_range_overlaps(&self, _numbers: CardNumberRange) -> DbResult<bool> {
        Ok(*self.overlap.lock().unwrap())
    }
}

#[async_trait]
impl IdempotencyRepository for FailureRepository {
    async fn get_idempotency_record(
        &self,
        _operation_type: &str,
        _idempotency_key: &str,
    ) -> DbResult<Option<IdempotencyRecord>> {
        Ok(None)
    }

    async fn create_idempotency_record(&self, _record: NewIdempotencyRecord) -> DbResult<()> {
        Ok(())
    }

    async fn complete_idempotency_record(
        &self,
        _operation_type: &str,
        _idempotency_key: &str,
        _resource_type: &str,
        _resource_id: Uuid,
        _response_snapshot: serde_json::Value,
    ) -> DbResult<()> {
        Ok(())
    }
}

#[async_trait]
impl AuditRepository for FailureRepository {
    async fn insert_audit_log(&self, _audit_log: NewAuditLog) -> DbResult<()> {
        Ok(())
    }
}

fn command_context(scopes: Vec<&str>) -> MutationCommandContext {
    MutationCommandContext {
        operation_type: "card_ranges.create".to_string(),
        actor: TrustedActor::from_verified_claims(VerifiedActorClaims {
            issuer: "https://wso2.example.test".to_string(),
            subject: "admin@example.test".to_string(),
            client_id: "admin-ui".to_string(),
            roles: vec!["wurzburg_platform_admin".to_string()],
            scopes: scopes.into_iter().map(ToOwned::to_owned).collect(),
            provider_id: None,
            user_id: None,
        })
        .unwrap(),
        request: TrustedRequestContext {
            correlation_id: "corr-001".to_string(),
            request_id: Uuid::parse_str("018f9e64-1b5f-7cc1-a3cf-2a519179f801").unwrap(),
            client_ip: "203.0.113.10".parse::<IpAddr>().unwrap(),
            gateway_id: "gw-prod-a".to_string(),
            backend_token: BackendToken::from_verified_transport(
                BackendTokenTransport::AuthorizationBearer,
                "secret.jwt.value",
            )
            .unwrap(),
        },
        idempotency_key: IdempotencyKey::from_validated("idem-card-range-create").unwrap(),
        request_hash: "hash-card-range-create".to_string(),
    }
}

fn cms_range_with_invalid_calendar() -> NewCardRange {
    NewCardRange {
        card_range_id: Uuid::new_v4(),
        numbers: CardNumberRange::new("6219861000000000", "6219861000000999").unwrap(),
        funding_mode: FundingMode::SingleProvider,
        withdrawal_limit_authority: WithdrawalLimitAuthority::Cms,
        limit_calendar: Some(wurzburg::domain::card_range::LimitCalendar {
            timezone: "Asia/Tehran".to_string(),
            week_starts_on: wurzburg::domain::card_range::WeekStartDay::Saturday,
            window_mode: wurzburg::domain::card_range::LimitWindowMode::Calendar,
        }),
        issuance_enabled: true,
        cms_operation_mode: CmsOperationMode::Full,
        metadata_json: serde_json::json!({}),
    }
}

fn valid_range() -> NewCardRange {
    NewCardRange {
        card_range_id: Uuid::new_v4(),
        numbers: CardNumberRange::new("6219861000000000", "6219861000000999").unwrap(),
        funding_mode: FundingMode::SingleProvider,
        withdrawal_limit_authority: WithdrawalLimitAuthority::Cms,
        limit_calendar: None,
        issuance_enabled: true,
        cms_operation_mode: CmsOperationMode::Full,
        metadata_json: serde_json::json!({}),
    }
}

#[tokio::test]
async fn rejects_create_without_platform_write_scope() {
    let service = CardRangeService::new(FailureRepository::default());

    let error = service
        .create_card_range(
            &command_context(vec!["platform.card_ranges:read"]),
            valid_range(),
        )
        .await
        .expect_err("missing write scope should fail");

    assert_eq!(error.body().error.code, "MISSING_REQUIRED_SCOPE");
}

#[tokio::test]
async fn rejects_invalid_authority_calendar_before_persistence() {
    let service = CardRangeService::new(FailureRepository::default());

    let error = service
        .create_card_range(
            &command_context(vec!["platform.card_ranges:write"]),
            cms_range_with_invalid_calendar(),
        )
        .await
        .expect_err("invalid authority/calendar pairing should fail");

    assert_eq!(error.body().error.code, "INVALID_CARD_RANGE_BOUNDARY");
}

#[tokio::test]
async fn rejects_overlapping_card_range() {
    let repository = FailureRepository::default();
    *repository.overlap.lock().unwrap() = true;
    let service = CardRangeService::new(repository);

    let error = service
        .create_card_range(
            &command_context(vec!["platform.card_ranges:write"]),
            valid_range(),
        )
        .await
        .expect_err("overlap should fail");

    assert_eq!(error.body().error.code, "CARD_RANGE_OVERLAP");
}
