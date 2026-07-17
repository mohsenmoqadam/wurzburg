use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use chrono::Utc;
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
            CardNumberRange, CardRange, CardRangeListCursor, CardRangeListPage, CardRangeListQuery,
            CardRangeStatus, CmsOperationMode, FundingMode, LimitCalendar, LimitWindowMode,
            NewCardRange, WeekStartDay, WithdrawalLimitAuthority,
        },
        idempotency::{IdempotencyRecord, IdempotencyStatus, NewIdempotencyRecord},
    },
    services::card_range::{CardRangeService, CreateCardRangeOutcome},
};

#[derive(Default, Clone)]
struct MemoryCardRangeRepository {
    state: Arc<Mutex<MemoryState>>,
}

#[derive(Default)]
struct MemoryState {
    ranges: HashMap<Uuid, CardRange>,
    idempotency: HashMap<(String, String), IdempotencyRecord>,
    audit_logs: Vec<NewAuditLog>,
    overlap: bool,
    completed_idempotency_count: usize,
}

#[async_trait]
impl CardRangeRepository for MemoryCardRangeRepository {
    async fn create_card_range(
        &self,
        card_range: NewCardRange,
        actor_subject: String,
    ) -> DbResult<CardRange> {
        let now = Utc::now();
        let created = CardRange {
            card_range_id: card_range.card_range_id,
            numbers: card_range.numbers,
            funding_mode: card_range.funding_mode,
            withdrawal_limit_authority: card_range.withdrawal_limit_authority,
            limit_calendar: card_range.limit_calendar,
            status: CardRangeStatus::Draft,
            issuance_enabled: card_range.issuance_enabled,
            cms_operation_mode: card_range.cms_operation_mode,
            operational_version: 1,
            metadata_json: card_range.metadata_json,
            created_by_subject: actor_subject.clone(),
            updated_by_subject: actor_subject,
            created_at: now,
            updated_at: now,
        };

        self.state
            .lock()
            .unwrap()
            .ranges
            .insert(created.card_range_id, created.clone());
        Ok(created)
    }

    async fn get_card_range(&self, card_range_id: Uuid) -> DbResult<Option<CardRange>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .ranges
            .get(&card_range_id)
            .cloned())
    }

    async fn list_card_ranges(&self, query: CardRangeListQuery) -> DbResult<CardRangeListPage> {
        let mut items = self
            .state
            .lock()
            .unwrap()
            .ranges
            .values()
            .filter(|range| {
                query.status.is_none_or(|status| range.status == status)
                    && query
                        .funding_mode
                        .is_none_or(|funding_mode| range.funding_mode == funding_mode)
                    && query
                        .withdrawal_limit_authority
                        .is_none_or(|authority| range.withdrawal_limit_authority == authority)
                    && query.cursor.as_ref().is_none_or(|cursor| {
                        range.created_at > cursor.created_at
                            || (range.created_at == cursor.created_at
                                && range.card_range_id > cursor.card_range_id)
                    })
            })
            .cloned()
            .collect::<Vec<_>>();
        items.sort_by_key(|range| (range.created_at, range.card_range_id));

        let has_next_page = items.len() > usize::from(query.limit);
        if has_next_page {
            items.truncate(usize::from(query.limit));
        }
        let next_cursor = if has_next_page {
            items.last().map(|range| CardRangeListCursor {
                created_at: range.created_at,
                card_range_id: range.card_range_id,
            })
        } else {
            None
        };

        Ok(CardRangeListPage { items, next_cursor })
    }

    async fn card_range_overlaps(&self, _numbers: CardNumberRange) -> DbResult<bool> {
        Ok(self.state.lock().unwrap().overlap)
    }
}

#[async_trait]
impl IdempotencyRepository for MemoryCardRangeRepository {
    async fn get_idempotency_record(
        &self,
        operation_type: &str,
        idempotency_key: &str,
    ) -> DbResult<Option<IdempotencyRecord>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .idempotency
            .get(&(operation_type.to_string(), idempotency_key.to_string()))
            .cloned())
    }

    async fn create_idempotency_record(&self, record: NewIdempotencyRecord) -> DbResult<()> {
        let now = Utc::now();
        self.state.lock().unwrap().idempotency.insert(
            (
                record.operation_type.clone(),
                record.idempotency_key.clone(),
            ),
            IdempotencyRecord {
                idempotency_record_id: record.idempotency_record_id,
                operation_type: record.operation_type,
                idempotency_key: record.idempotency_key,
                request_hash: record.request_hash,
                status: IdempotencyStatus::InProgress,
                resource_type: None,
                resource_id: None,
                response_snapshot: None,
                error_snapshot: None,
                created_by_subject: record.created_by_subject,
                created_by_client_id: record.created_by_client_id,
                actor_provider_id: record.actor_provider_id,
                actor_user_id: record.actor_user_id,
                correlation_id: record.correlation_id,
                request_id: record.request_id,
                created_at: now,
                updated_at: now,
                completed_at: None,
            },
        );
        Ok(())
    }

    async fn complete_idempotency_record(
        &self,
        operation_type: &str,
        idempotency_key: &str,
        resource_type: &str,
        resource_id: Uuid,
        response_snapshot: serde_json::Value,
    ) -> DbResult<()> {
        let mut state = self.state.lock().unwrap();
        let record = state
            .idempotency
            .get_mut(&(operation_type.to_string(), idempotency_key.to_string()))
            .expect("created idempotency record should exist");
        record.status = IdempotencyStatus::Completed;
        record.resource_type = Some(resource_type.to_string());
        record.resource_id = Some(resource_id);
        record.response_snapshot = Some(response_snapshot);
        state.completed_idempotency_count += 1;
        Ok(())
    }
}

#[async_trait]
impl AuditRepository for MemoryCardRangeRepository {
    async fn insert_audit_log(&self, audit_log: NewAuditLog) -> DbResult<()> {
        self.state.lock().unwrap().audit_logs.push(audit_log);
        Ok(())
    }
}

fn tehran_calendar() -> LimitCalendar {
    LimitCalendar {
        timezone: "Asia/Tehran".to_string(),
        week_starts_on: WeekStartDay::Saturday,
        window_mode: LimitWindowMode::Calendar,
    }
}

fn new_card_range() -> NewCardRange {
    NewCardRange {
        card_range_id: Uuid::new_v4(),
        numbers: CardNumberRange::new("6219861000000000", "6219861000000999").unwrap(),
        funding_mode: FundingMode::SingleProvider,
        withdrawal_limit_authority: WithdrawalLimitAuthority::Platform,
        limit_calendar: Some(tehran_calendar()),
        issuance_enabled: true,
        cms_operation_mode: CmsOperationMode::Full,
        metadata_json: serde_json::json!({ "scenario": "success" }),
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

#[tokio::test]
async fn creates_card_range_with_audit_and_completed_idempotency() {
    let repository = MemoryCardRangeRepository::default();
    let service = CardRangeService::new(repository.clone());
    let context = command_context(vec!["platform.card_ranges:write"]);

    let outcome = service
        .create_card_range(&context, new_card_range())
        .await
        .expect("card range creation should succeed");

    let CreateCardRangeOutcome::Created(created) = outcome else {
        panic!("expected created outcome");
    };
    let state = repository.state.lock().unwrap();

    assert!(state.ranges.contains_key(&created.card_range_id));
    assert_eq!(state.audit_logs.len(), 1);
    assert_eq!(state.audit_logs[0].entity_id, created.card_range_id);
    assert_eq!(state.completed_idempotency_count, 1);
}

#[tokio::test]
async fn replays_completed_idempotency_without_new_audit_or_insert() {
    let repository = MemoryCardRangeRepository::default();
    let context = command_context(vec!["platform.card_ranges:write"]);
    let replay_snapshot = serde_json::json!({ "card_range_id": "range-001" });
    let now = Utc::now();
    repository.state.lock().unwrap().idempotency.insert(
        (
            context.operation_type.clone(),
            context.idempotency_key.as_str().to_string(),
        ),
        IdempotencyRecord {
            idempotency_record_id: Uuid::new_v4(),
            operation_type: context.operation_type.clone(),
            idempotency_key: context.idempotency_key.as_str().to_string(),
            request_hash: context.request_hash.clone(),
            status: IdempotencyStatus::Completed,
            resource_type: Some("card_range".to_string()),
            resource_id: Some(Uuid::new_v4()),
            response_snapshot: Some(replay_snapshot.clone()),
            error_snapshot: None,
            created_by_subject: context.actor.subject.clone(),
            created_by_client_id: Some(context.actor.client_id.clone()),
            actor_provider_id: None,
            actor_user_id: None,
            correlation_id: context.request.correlation_id.clone(),
            request_id: context.request.request_id.to_string(),
            created_at: now,
            updated_at: now,
            completed_at: Some(now),
        },
    );

    let service = CardRangeService::new(repository.clone());
    let outcome = service
        .create_card_range(&context, new_card_range())
        .await
        .expect("completed idempotency should replay");

    assert_eq!(outcome, CreateCardRangeOutcome::Replayed(replay_snapshot));
    assert!(repository.state.lock().unwrap().audit_logs.is_empty());
}

#[tokio::test]
async fn reads_existing_card_range_with_read_scope() {
    let repository = MemoryCardRangeRepository::default();
    let service = CardRangeService::new(repository.clone());
    let context = command_context(vec![
        "platform.card_ranges:write",
        "platform.card_ranges:read",
    ]);
    let CreateCardRangeOutcome::Created(created) = service
        .create_card_range(&context, new_card_range())
        .await
        .expect("card range creation should succeed")
    else {
        panic!("expected created outcome");
    };

    let found = service
        .get_card_range(&context.actor, created.card_range_id)
        .await
        .expect("read scope should allow fetching the card range");

    assert_eq!(found.card_range_id, created.card_range_id);
}

#[tokio::test]
async fn lists_card_ranges_with_bounded_filters_and_cursor() {
    let repository = MemoryCardRangeRepository::default();
    let service = CardRangeService::new(repository.clone());
    for _ in 0..3 {
        let mut context = command_context(vec![
            "platform.card_ranges:write",
            "platform.card_ranges:read",
        ]);
        context.idempotency_key =
            IdempotencyKey::from_validated(Uuid::new_v4().to_string()).unwrap();
        context.request_hash = Uuid::new_v4().to_string();
        service
            .create_card_range(&context, new_card_range())
            .await
            .expect("card range creation should succeed");
    }
    let context = command_context(vec!["platform.card_ranges:read"]);

    let first_page = service
        .list_card_ranges(
            &context.actor,
            CardRangeListQuery::new(
                Some(CardRangeStatus::Draft),
                Some(FundingMode::SingleProvider),
                Some(WithdrawalLimitAuthority::Platform),
                None,
                Some(2),
            )
            .expect("valid list query"),
        )
        .await
        .expect("list should succeed");

    assert_eq!(first_page.items.len(), 2);
    assert!(first_page.next_cursor.is_some());

    let second_page = service
        .list_card_ranges(
            &context.actor,
            CardRangeListQuery::new(
                Some(CardRangeStatus::Draft),
                Some(FundingMode::SingleProvider),
                Some(WithdrawalLimitAuthority::Platform),
                first_page.next_cursor,
                Some(2),
            )
            .expect("valid list query"),
        )
        .await
        .expect("second list page should succeed");

    assert_eq!(second_page.items.len(), 1);
    assert!(second_page.next_cursor.is_none());
}
