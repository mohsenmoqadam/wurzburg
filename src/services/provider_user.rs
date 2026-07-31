use std::sync::Arc;

use chrono::{Datelike, Timelike, Utc};
use tigerbeetle_rustclient_tests_snapshot::AccountFlags;
use uuid::Uuid;

use crate::{
    api::{
        auth::require_scope, command::MutationCommandContext, error::ApiError,
        result_codes::WurzburgResultCode,
    },
    config::TigerBeetleConfig,
    db::oracle::{
        EnrollProviderUserPersistenceOutcome, ExistingCardProvisioningIntent, OracleRepository,
    },
    domain::{
        provider::{ProviderOperationalProfileRecord, ProviderWeekday},
        user_card::{NewProviderUserEnrollment, PolicyUsageAccountCategory, ProviderUserView},
    },
    tigerbeetle::{AppAccount, AppTbClient, TigerBeetleError},
};

#[derive(Debug, Clone)]
pub enum EnrollProviderUserOutcome {
    Activated(Box<ProviderUserView>),
    IssuancePending(Box<ProviderUserView>),
    Replayed(serde_json::Value),
}

#[derive(Clone)]
pub struct ProviderUserService {
    repository: Arc<OracleRepository>,
    tb_client: AppTbClient,
    tb_config: TigerBeetleConfig,
}

impl ProviderUserService {
    pub fn new(
        repository: Arc<OracleRepository>,
        tb_client: AppTbClient,
        tb_config: TigerBeetleConfig,
    ) -> Self {
        Self {
            repository,
            tb_client,
            tb_config,
        }
    }

    pub async fn list(
        &self,
        actor: &crate::api::auth::TrustedActor,
        provider_id: Uuid,
        status: Option<crate::domain::user_card::ProviderUserStatus>,
        cursor: Option<crate::domain::user_card::ProviderUserCursor>,
        limit: u16,
    ) -> Result<crate::domain::user_card::ProviderUserPage, ApiError> {
        require_scope(actor, "provider.users:read")?;
        enforce_provider_scope(actor, provider_id)?;
        if limit == 0 || limit > 200 {
            return Err(ApiError::new(
                WurzburgResultCode::ProviderUserContractInvalid,
            ));
        }
        self.repository
            .list_provider_users(provider_id, status, cursor, limit)
            .await
            .map_err(ApiError::from_database)
    }

    pub async fn get(
        &self,
        actor: &crate::api::auth::TrustedActor,
        provider_id: Uuid,
        user_id: Uuid,
    ) -> Result<crate::domain::user_card::ProviderUserRecord, ApiError> {
        require_scope(actor, "provider.users:read")?;
        enforce_provider_scope(actor, provider_id)?;
        self.repository
            .get_provider_user_record(provider_id, user_id)
            .await
            .map_err(ApiError::from_database)?
            .ok_or_else(|| ApiError::new(WurzburgResultCode::ProviderUserNotFound))
    }

    pub async fn list_user_cards(
        &self,
        actor: &crate::api::auth::TrustedActor,
        user_id: Uuid,
    ) -> Result<Vec<crate::domain::user_card::UserCardSummary>, ApiError> {
        let provider_filter = user_relationship_read_filter(actor, user_id, "provider.cards:read")?;
        self.repository
            .list_user_cards(user_id, provider_filter)
            .await
            .map_err(ApiError::from_database)
    }

    pub async fn list_user_providers(
        &self,
        actor: &crate::api::auth::TrustedActor,
        user_id: Uuid,
    ) -> Result<Vec<crate::domain::user_card::UserProviderSummary>, ApiError> {
        let provider_filter = user_relationship_read_filter(actor, user_id, "provider.users:read")?;
        self.repository
            .list_user_providers(user_id, provider_filter)
            .await
            .map_err(ApiError::from_database)
    }

    #[tracing::instrument(skip(self, context, enrollment), fields(provider_id=%provider_id))]
    pub async fn enroll(
        &self,
        context: &MutationCommandContext,
        provider_id: Uuid,
        enrollment: NewProviderUserEnrollment,
    ) -> Result<EnrollProviderUserOutcome, ApiError> {
        require_scope(&context.actor, "provider.users:write")?;
        if context
            .actor
            .provider_id
            .is_some_and(|actor_provider_id| actor_provider_id != provider_id)
        {
            return Err(ApiError::new(WurzburgResultCode::ProviderScopeMismatch));
        }
        let enrollment = enrollment.validate_and_normalize().map_err(|message| {
            ApiError::with_message(WurzburgResultCode::ProviderUserContractInvalid, message)
        })?;
        let profile = self
            .repository
            .get_current_provider_operational_profile(provider_id)
            .await
            .map_err(ApiError::from_database)?
            .ok_or_else(|| ApiError::new(WurzburgResultCode::ProviderOperationalProfileNotFound))?;
        enforce_onboarding_controls(&profile)?;

        let outcome = self
            .repository
            .prepare_provider_user_enrollment_atomic(
                context.clone(),
                provider_id,
                profile.provider_operational_profile_id,
                profile.controls.max_total_users,
                profile.controls.attach_existing_multi_provider_card_enabled,
                enrollment,
            )
            .await
            .map_err(ApiError::from_database)?;

        match outcome {
            EnrollProviderUserPersistenceOutcome::ExistingCardPrepared(intent) => {
                if let Err(error) = self.provision_accounts(&intent).await {
                    tracing::error!(
                        provider_id=%provider_id,
                        provider_user_id=%intent.provider_user_id,
                        operation_id=%intent.operation_id,
                        error.kind=error.diagnostic_kind(),
                        outcome.uncertain=error.outcome_is_uncertain(),
                        "provider-user TigerBeetle provisioning requires recovery"
                    );
                    self.repository
                        .mark_provider_user_provisioning_failed(
                            intent.provider_user_id,
                            intent.operation_id,
                            "TIGERBEETLE_PROVISIONING_UNCERTAIN",
                        )
                        .await
                        .map_err(ApiError::from_database)?;
                    return Err(ApiError::new(
                        WurzburgResultCode::ProviderUserRecoveryRequired,
                    ));
                }
                let view = self
                    .repository
                    .finalize_existing_card_enrollment_atomic(context.clone(), *intent)
                    .await
                    .map_err(ApiError::from_database)?;
                Ok(EnrollProviderUserOutcome::Activated(Box::new(view)))
            }
            EnrollProviderUserPersistenceOutcome::IssuancePending(view) => {
                Ok(EnrollProviderUserOutcome::IssuancePending(view))
            }
            EnrollProviderUserPersistenceOutcome::Replayed(value) => {
                Ok(EnrollProviderUserOutcome::Replayed(value))
            }
            EnrollProviderUserPersistenceOutcome::ProviderNotFound => {
                Err(ApiError::new(WurzburgResultCode::ProviderNotFound))
            }
            EnrollProviderUserPersistenceOutcome::ProviderNotActive => {
                Err(ApiError::new(WurzburgResultCode::ProviderNotActive))
            }
            EnrollProviderUserPersistenceOutcome::OperationalProfileChanged => Err(ApiError::new(
                WurzburgResultCode::ProviderOperationalProfileInvalidState,
            )),
            EnrollProviderUserPersistenceOutcome::RangeNotReady => {
                Err(ApiError::new(WurzburgResultCode::ProviderUserRangeNotReady))
            }
            EnrollProviderUserPersistenceOutcome::ActiveCardSelectionRequired => Err(
                ApiError::new(WurzburgResultCode::ActiveCardSelectionRequired),
            ),
            EnrollProviderUserPersistenceOutcome::ExistingCardNotFound => {
                Err(ApiError::new(WurzburgResultCode::CardNotFound))
            }
            EnrollProviderUserPersistenceOutcome::ExistingCardOwnerMismatch => {
                Err(ApiError::new(WurzburgResultCode::CardOwnerMismatch))
            }
            EnrollProviderUserPersistenceOutcome::ExistingCardRangeMismatch => {
                Err(ApiError::new(WurzburgResultCode::CardRangeMismatch))
            }
            EnrollProviderUserPersistenceOutcome::ExistingCardNotActive => {
                Err(ApiError::new(WurzburgResultCode::CardNotActive))
            }
            EnrollProviderUserPersistenceOutcome::ExistingRelationship => {
                Err(ApiError::new(WurzburgResultCode::ProviderUserAlreadyExists))
            }
            EnrollProviderUserPersistenceOutcome::MultiProviderAttachmentDisabled => Err(
                ApiError::new(WurzburgResultCode::ProviderCardOperationDisabled),
            ),
            EnrollProviderUserPersistenceOutcome::UserLimitReached => {
                Err(ApiError::new(WurzburgResultCode::ProviderUserLimitReached))
            }
            EnrollProviderUserPersistenceOutcome::IdempotencyConflict => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyKeyConflict))
            }
            EnrollProviderUserPersistenceOutcome::IdempotencyInProgress => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyInProgress))
            }
            EnrollProviderUserPersistenceOutcome::IdempotencyInvalidState => {
                Err(ApiError::new(WurzburgResultCode::IdempotencyError))
            }
        }
    }

    async fn provision_accounts(
        &self,
        intent: &ExistingCardProvisioningIntent,
    ) -> Result<(), TigerBeetleError> {
        let provider_user = AppAccount {
            id: intent.provider_user_account_id.as_u128(),
            debits_pending: 0,
            debits_posted: 0,
            credits_pending: 0,
            credits_posted: 0,
            user_data_128: intent.provider_user_id.as_u128(),
            user_data_64: 0,
            user_data_32: 0,
            reserved: 0,
            ledger: self.tb_config.ledger_id,
            code: self.tb_config.user_account_code,
            flags: (AccountFlags::History | AccountFlags::DebitsMustNotExceedCredits).bits(),
            timestamp: 0,
        };
        self.create_and_verify(provider_user).await?;

        for (category, account_id) in PolicyUsageAccountCategory::ALL
            .into_iter()
            .zip(intent.usage_account_ids.ordered())
        {
            self.create_and_verify(AppAccount {
                id: account_id.as_u128(),
                debits_pending: 0,
                debits_posted: 0,
                credits_pending: 0,
                credits_posted: 0,
                user_data_128: intent.card_id.as_u128(),
                user_data_64: 0,
                user_data_32: category.code(),
                reserved: 0,
                ledger: self.tb_config.ledger_id,
                code: self.tb_config.system_account_code,
                flags: AccountFlags::History.bits(),
                timestamp: 0,
            })
            .await?;
        }
        Ok(())
    }

    pub async fn provision_issued_card(
        &self,
        intent: &crate::db::oracle::IssuedCardProvisioningIntent,
    ) -> Result<(), TigerBeetleError> {
        for provider in &intent.providers {
            self.create_and_verify(AppAccount {
                id: provider.account_id.as_u128(),
                debits_pending: 0,
                debits_posted: 0,
                credits_pending: 0,
                credits_posted: 0,
                user_data_128: provider.provider_user_id.as_u128(),
                user_data_64: 0,
                user_data_32: 0,
                reserved: 0,
                ledger: self.tb_config.ledger_id,
                code: self.tb_config.user_account_code,
                flags: (AccountFlags::History | AccountFlags::DebitsMustNotExceedCredits).bits(),
                timestamp: 0,
            })
            .await?;
        }
        for (category, account_id) in PolicyUsageAccountCategory::ALL
            .into_iter()
            .zip(intent.base.usage_account_ids.ordered())
        {
            self.create_and_verify(AppAccount {
                id: account_id.as_u128(),
                debits_pending: 0,
                debits_posted: 0,
                credits_pending: 0,
                credits_posted: 0,
                user_data_128: intent.base.card_id.as_u128(),
                user_data_64: 0,
                user_data_32: category.code(),
                reserved: 0,
                ledger: self.tb_config.ledger_id,
                code: self.tb_config.system_account_code,
                flags: AccountFlags::History.bits(),
                timestamp: 0,
            })
            .await?;
        }
        Ok(())
    }

    #[tracing::instrument(skip(self, expected), fields(tigerbeetle.operation="create_and_verify", tigerbeetle.account_code=expected.code))]
    async fn create_and_verify(&self, expected: AppAccount) -> Result<(), TigerBeetleError> {
        if let Err(error) = self.tb_client.create_account(expected.clone()).await {
            tracing::warn!(
                error.kind = error.diagnostic_kind(),
                "TigerBeetle create outcome requires lookup verification"
            );
        }
        let accounts = self.tb_client.lookup_account(expected.id).await?;
        let actual = accounts.first().ok_or(TigerBeetleError::ClientFailure {
            operation: "verify_provider_user_account",
        })?;
        if actual.id != expected.id
            || actual.user_data_128 != expected.user_data_128
            || actual.user_data_32 != expected.user_data_32
            || actual.ledger != expected.ledger
            || actual.code != expected.code
            || actual.flags != expected.flags
        {
            return Err(TigerBeetleError::ClientFailure {
                operation: "verify_provider_user_account_contract",
            });
        }
        Ok(())
    }
}

fn user_relationship_read_filter(
    actor: &crate::api::auth::TrustedActor,
    user_id: Uuid,
    provider_scope: &'static str,
) -> Result<Option<Uuid>, ApiError> {
    if actor.user_id == Some(user_id) && actor.has_scope("card.funding-order:read") {
        return Ok(None);
    }
    if let Some(provider_id) = actor.provider_id {
        require_scope(actor, provider_scope)?;
        return Ok(Some(provider_id));
    }
    if actor.has_scope("platform.providers:read") || actor.has_scope("support.cards:read") {
        return Ok(None);
    }
    Err(ApiError::with_details(
        WurzburgResultCode::MissingRequiredScope,
        serde_json::json!({
            "required_scopes": ["card.funding-order:read", provider_scope, "platform.providers:read", "support.cards:read"]
        }),
    ))
}

fn enforce_provider_scope(
    actor: &crate::api::auth::TrustedActor,
    provider_id: Uuid,
) -> Result<(), ApiError> {
    if actor.provider_id.is_some_and(|value| value != provider_id) {
        Err(ApiError::new(WurzburgResultCode::ProviderScopeMismatch))
    } else {
        Ok(())
    }
}

fn enforce_onboarding_controls(profile: &ProviderOperationalProfileRecord) -> Result<(), ApiError> {
    if !profile.controls.user_onboarding_enabled || !profile.controls.new_assignment_enabled {
        return Err(ApiError::new(
            WurzburgResultCode::ProviderCardOperationDisabled,
        ));
    }
    if profile.controls.active_windows.is_empty() {
        return Ok(());
    }
    let timezone = profile
        .controls
        .timezone
        .parse::<chrono_tz::Tz>()
        .map_err(|_| ApiError::new(WurzburgResultCode::ProviderOperationalProfileInvalidState))?;
    let local = Utc::now().with_timezone(&timezone);
    let weekday = match local.weekday() {
        chrono::Weekday::Sat => ProviderWeekday::Saturday,
        chrono::Weekday::Sun => ProviderWeekday::Sunday,
        chrono::Weekday::Mon => ProviderWeekday::Monday,
        chrono::Weekday::Tue => ProviderWeekday::Tuesday,
        chrono::Weekday::Wed => ProviderWeekday::Wednesday,
        chrono::Weekday::Thu => ProviderWeekday::Thursday,
        chrono::Weekday::Fri => ProviderWeekday::Friday,
    };
    let time = chrono::NaiveTime::from_hms_opt(local.hour(), local.minute(), local.second())
        .expect("local clock produces a valid time");
    if profile.controls.active_windows.iter().any(|window| {
        window.days.contains(&weekday)
            && window.start_local_time <= time
            && time < window.end_local_time
    }) {
        Ok(())
    } else {
        Err(ApiError::new(
            WurzburgResultCode::ProviderOnboardingWindowClosed,
        ))
    }
}
