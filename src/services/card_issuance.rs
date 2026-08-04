use std::{collections::HashSet, sync::Arc};

use uuid::Uuid;

use crate::{
    api::{
        auth::{TrustedActor, require_scope},
        command::MutationCommandContext,
        error::ApiError,
        result_codes::WurzburgResultCode,
    },
    db::oracle::{
        BeginIssuanceResultOutcome, OracleRepository, PrepareCardIssuanceBatchOutcome,
        PrepareIssuedCardOutcome,
    },
    domain::{
        card_issuance::{
            CardIssuanceBatch, CardIssuanceExportRow, CardIssuanceResultRow,
            CardIssuanceResultStatus,
        },
        user_card::{is_valid_card_number, normalize_digits},
    },
    object_storage::{ObjectStorage, sha256},
};

#[derive(Debug, Clone)]
pub enum CreateCardIssuanceBatchOutcome {
    Created(CardIssuanceBatch),
    Replayed(serde_json::Value),
}

#[derive(Clone)]
pub struct CardIssuanceService {
    repository: Arc<OracleRepository>,
    object_storage: Arc<ObjectStorage>,
    retention_days: i64,
}

impl CardIssuanceService {
    pub fn new(
        repository: Arc<OracleRepository>,
        object_storage: Arc<ObjectStorage>,
        retention_days: i64,
    ) -> Self {
        Self {
            repository,
            object_storage,
            retention_days,
        }
    }

    #[tracing::instrument(skip(self, context), fields(card_issuance.batch_size=batch_size))]
    pub async fn create_batch(
        &self,
        context: &MutationCommandContext,
        batch_size: u16,
    ) -> Result<CreateCardIssuanceBatchOutcome, ApiError> {
        require_scope(&context.actor, "platform.card_issuance:write")?;
        if batch_size == 0 || batch_size > 10_000 || self.retention_days <= 0 {
            return Err(ApiError::new(
                WurzburgResultCode::CardIssuanceBatchContractInvalid,
            ));
        }
        let prepared = match self
            .repository
            .prepare_card_issuance_batch_atomic(context.clone(), batch_size, self.retention_days)
            .await
            .map_err(ApiError::from_database)?
        {
            PrepareCardIssuanceBatchOutcome::Prepared(value) => value,
            PrepareCardIssuanceBatchOutcome::Replayed(value) => {
                return Ok(CreateCardIssuanceBatchOutcome::Replayed(value));
            }
            PrepareCardIssuanceBatchOutcome::NoPendingRequests => {
                return Err(ApiError::new(WurzburgResultCode::CardIssuanceQueueEmpty));
            }
            PrepareCardIssuanceBatchOutcome::IdempotencyConflict => {
                return Err(ApiError::new(WurzburgResultCode::IdempotencyKeyConflict));
            }
            PrepareCardIssuanceBatchOutcome::IdempotencyInProgress => {
                return Err(ApiError::new(WurzburgResultCode::IdempotencyInProgress));
            }
            PrepareCardIssuanceBatchOutcome::IdempotencyInvalidState => {
                return Err(ApiError::new(WurzburgResultCode::IdempotencyError));
            }
        };
        let csv = render_request_csv(&prepared.rows)
            .map_err(|_| ApiError::new(WurzburgResultCode::SystemError))?;
        let stored = match self
            .object_storage
            .put_csv(&prepared.object_key, &csv)
            .await
        {
            Ok(value) => value,
            Err(_error) => {
                tracing::error!(batch_id=%prepared.batch.batch_id, error.kind="object_storage_put", "issuance request-file upload failed");
                // Keep the staged batch and row claims intact. Replaying the
                // same idempotency key reconstructs this exact CSV and retries
                // the immutable object upload without selecting new requests.
                return Err(ApiError::new(WurzburgResultCode::ObjectStorageUnavailable));
            }
        };
        debug_assert_eq!(stored.checksum_sha256, sha256(&csv));
        let batch = self
            .repository
            .complete_card_issuance_batch_request_file_atomic(
                context.clone(),
                prepared.batch.batch_id,
                stored.checksum_sha256,
            )
            .await
            .map_err(ApiError::from_database)?;
        Ok(CreateCardIssuanceBatchOutcome::Created(batch))
    }

    #[tracing::instrument(skip(self, actor), fields(batch_id=%batch_id))]
    pub async fn get_batch(
        &self,
        actor: &TrustedActor,
        batch_id: Uuid,
    ) -> Result<CardIssuanceBatch, ApiError> {
        require_scope(actor, "platform.card_issuance:read")?;
        self.repository
            .get_card_issuance_batch(batch_id)
            .await
            .map_err(ApiError::from_database)?
            .ok_or_else(|| ApiError::new(WurzburgResultCode::CardIssuanceBatchNotFound))
    }

    pub async fn list_batches(
        &self,
        actor: &TrustedActor,
        cursor: Option<crate::domain::card_issuance::CardIssuanceBatchCursor>,
        limit: u16,
    ) -> Result<crate::domain::card_issuance::CardIssuanceBatchPage, ApiError> {
        require_scope(actor, "platform.card_issuance:read")?;
        if limit == 0 || limit > 200 {
            return Err(ApiError::new(
                WurzburgResultCode::CardIssuanceBatchContractInvalid,
            ));
        }
        self.repository
            .list_card_issuance_batches(cursor, limit)
            .await
            .map_err(ApiError::from_database)
    }

    pub async fn get_result(
        &self,
        actor: &TrustedActor,
        batch_id: Uuid,
    ) -> Result<Vec<crate::domain::card_issuance::CardIssuanceBatchResultRow>, ApiError> {
        require_scope(actor, "platform.card_issuance:read")?;
        self.repository
            .get_card_issuance_batch_result(batch_id)
            .await
            .map_err(ApiError::from_database)?
            .ok_or_else(|| ApiError::new(WurzburgResultCode::CardIssuanceBatchNotFound))
    }

    #[tracing::instrument(skip(self, actor), fields(batch_id=%batch_id))]
    pub async fn download_request_file(
        &self,
        actor: &TrustedActor,
        batch_id: Uuid,
    ) -> Result<Vec<u8>, ApiError> {
        require_scope(actor, "platform.card_issuance:read")?;
        let (key, expected_checksum) = self
            .repository
            .get_card_issuance_request_object(batch_id)
            .await
            .map_err(ApiError::from_database)?
            .ok_or_else(|| ApiError::new(WurzburgResultCode::CardIssuanceBatchNotReady))?;
        let bytes = self
            .object_storage
            .get_csv(&key)
            .await
            .map_err(|_| ApiError::new(WurzburgResultCode::ObjectStorageUnavailable))?;
        if sha256(&bytes) != expected_checksum {
            tracing::error!(batch_id=%batch_id, "issuance request-file checksum mismatch");
            return Err(ApiError::new(
                WurzburgResultCode::CardIssuanceFileIntegrityError,
            ));
        }
        Ok(bytes)
    }

    #[tracing::instrument(skip(self, context, bytes, provider_user_service), fields(batch_id=%batch_id, storage.size_bytes=bytes.len()))]
    pub async fn process_result_file(
        &self,
        context: &MutationCommandContext,
        batch_id: Uuid,
        bytes: &[u8],
        provider_user_service: &crate::services::provider_user::ProviderUserService,
    ) -> Result<CreateCardIssuanceBatchOutcome, ApiError> {
        require_scope(&context.actor, "platform.card_issuance:write")?;
        let batch = self.get_batch(&context.actor, batch_id).await?;
        if bytes.is_empty() {
            return Err(ApiError::new(
                WurzburgResultCode::CardIssuanceResultContractInvalid,
            ));
        }
        let rows = parse_result_csv(bytes)?;
        // Content-addressed result objects prevent a rejected retry from
        // overwriting the immutable bank evidence accepted by an earlier call.
        let result_checksum = sha256(bytes);
        let object_key = format!("card-issuance/{batch_id}/results/{result_checksum}.csv");
        let stored = self
            .object_storage
            .put_csv(&object_key, bytes)
            .await
            .map_err(|_| ApiError::new(WurzburgResultCode::ObjectStorageUnavailable))?;
        match self
            .repository
            .begin_card_issuance_result_atomic(
                context.clone(),
                batch.batch_id,
                object_key,
                stored.checksum_sha256,
                rows.clone(),
            )
            .await
            .map_err(ApiError::from_database)?
        {
            BeginIssuanceResultOutcome::Started | BeginIssuanceResultOutcome::Resumed => {}
            BeginIssuanceResultOutcome::Replayed(value) => {
                return Ok(CreateCardIssuanceBatchOutcome::Replayed(value));
            }
            BeginIssuanceResultOutcome::BatchNotFound => {
                return Err(ApiError::new(WurzburgResultCode::CardIssuanceBatchNotFound));
            }
            BeginIssuanceResultOutcome::BatchNotReady => {
                return Err(ApiError::new(WurzburgResultCode::CardIssuanceBatchNotReady));
            }
            BeginIssuanceResultOutcome::RowSetMismatch => {
                return Err(ApiError::new(
                    WurzburgResultCode::CardIssuanceResultRowSetMismatch,
                ));
            }
            BeginIssuanceResultOutcome::IdempotencyConflict => {
                return Err(ApiError::new(WurzburgResultCode::IdempotencyKeyConflict));
            }
            BeginIssuanceResultOutcome::IdempotencyInvalidState => {
                return Err(ApiError::new(WurzburgResultCode::IdempotencyError));
            }
        }

        for row in rows {
            match row.status {
                CardIssuanceResultStatus::Rejected => {
                    if !self
                        .repository
                        .reject_card_issuance_request_atomic(context, batch_id, &row)
                        .await
                        .map_err(ApiError::from_database)?
                    {
                        self.repository
                            .fail_card_issuance_result_row_atomic(
                                batch_id,
                                row.issuance_request_id,
                                "RESULT_ROW_STATE_CONFLICT",
                            )
                            .await
                            .map_err(ApiError::from_database)?;
                    }
                }
                CardIssuanceResultStatus::Issued => {
                    let intent = match self
                        .repository
                        .prepare_issued_card_atomic(context, batch_id, &row)
                        .await
                        .map_err(ApiError::from_database)?
                    {
                        PrepareIssuedCardOutcome::Prepared(value) => value,
                        PrepareIssuedCardOutcome::AlreadyProcessed => continue,
                        PrepareIssuedCardOutcome::RequestNotFound => {
                            self.repository
                                .fail_card_issuance_result_row_atomic(
                                    batch_id,
                                    row.issuance_request_id,
                                    "RESULT_ROW_STATE_CONFLICT",
                                )
                                .await
                                .map_err(ApiError::from_database)?;
                            continue;
                        }
                        PrepareIssuedCardOutcome::InvalidPanRange => {
                            self.repository
                                .fail_card_issuance_result_row_atomic(
                                    batch_id,
                                    row.issuance_request_id,
                                    "ISSUED_PAN_OUTSIDE_RANGE",
                                )
                                .await
                                .map_err(ApiError::from_database)?;
                            continue;
                        }
                        PrepareIssuedCardOutcome::CardNumberConflict => {
                            self.repository
                                .fail_card_issuance_result_row_atomic(
                                    batch_id,
                                    row.issuance_request_id,
                                    "ISSUED_PAN_CONFLICT",
                                )
                                .await
                                .map_err(ApiError::from_database)?;
                            continue;
                        }
                        PrepareIssuedCardOutcome::ActiveCardConflict => {
                            self.repository
                                .fail_card_issuance_result_row_atomic(
                                    batch_id,
                                    row.issuance_request_id,
                                    "ACTIVE_CARD_CONFLICT",
                                )
                                .await
                                .map_err(ApiError::from_database)?;
                            continue;
                        }
                    };
                    if let Err(error) = provider_user_service.provision_issued_card(&intent).await {
                        tracing::error!(batch_id=%batch_id, issuance_request_id=%row.issuance_request_id, error.kind=error.diagnostic_kind(), outcome.uncertain=error.outcome_is_uncertain(), "issued-card TigerBeetle provisioning requires recovery");
                        self.repository
                            .fail_card_issuance_result_row_atomic(
                                batch_id,
                                row.issuance_request_id,
                                "TIGERBEETLE_PROVISIONING_UNCERTAIN",
                            )
                            .await
                            .map_err(ApiError::from_database)?;
                        return Err(ApiError::new(
                            WurzburgResultCode::ProviderUserRecoveryRequired,
                        ));
                    }
                    self.repository
                        .finalize_issued_card_atomic(&context.durable(), &intent, &row)
                        .await
                        .map_err(ApiError::from_database)?;
                }
            }
        }
        let completed = self
            .repository
            .complete_card_issuance_result_atomic(&context.durable(), batch_id)
            .await
            .map_err(ApiError::from_database)?;
        Ok(CreateCardIssuanceBatchOutcome::Created(completed))
    }
}

fn render_request_csv(rows: &[CardIssuanceExportRow]) -> Result<Vec<u8>, csv::Error> {
    let mut writer = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(Vec::new());
    writer.write_record([
        "issuance_request_id",
        "requested_at",
        "card_range_id",
        "funding_mode",
        "national_id",
        "first_name",
        "last_name",
        "birth_date",
        "mobile",
        "delivery_province",
        "delivery_city",
        "delivery_address",
        "postal_code",
        "provider_request_count",
    ])?;
    for row in rows {
        writer.write_record([
            row.issuance_request_id.to_string(),
            row.requested_at.to_rfc3339(),
            row.card_range_id.to_string(),
            row.funding_mode.clone(),
            row.national_id.clone(),
            row.first_name.clone(),
            row.last_name.clone(),
            row.birth_date.clone().unwrap_or_default(),
            row.mobile.clone(),
            row.delivery_province.clone(),
            row.delivery_city.clone(),
            row.delivery_address.clone(),
            row.postal_code.clone(),
            row.provider_request_count.to_string(),
        ])?;
    }
    Ok(writer.into_inner().map_err(|error| error.into_error())?)
}

const RESULT_HEADERS: [&str; 9] = [
    "issuance_request_id",
    "status",
    "card_number",
    "issuer_reference",
    "failure_code",
    "failure_message",
    "produced_at",
    "dispatched_at",
    "tracking_reference",
];

fn parse_result_csv(bytes: &[u8]) -> Result<Vec<CardIssuanceResultRow>, ApiError> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(bytes);
    let headers = reader
        .headers()
        .map_err(|_| ApiError::new(WurzburgResultCode::CardIssuanceResultContractInvalid))?;
    if headers.len() != RESULT_HEADERS.len() || headers.iter().ne(RESULT_HEADERS) {
        return Err(ApiError::new(
            WurzburgResultCode::CardIssuanceResultContractInvalid,
        ));
    }
    let mut rows = Vec::new();
    let mut request_ids = HashSet::new();
    for record in reader.records() {
        let record = record
            .map_err(|_| ApiError::new(WurzburgResultCode::CardIssuanceResultContractInvalid))?;
        let get = |index: usize| record.get(index).unwrap_or_default().trim();
        let issuance_request_id = get(0)
            .parse::<Uuid>()
            .map_err(|_| ApiError::new(WurzburgResultCode::CardIssuanceResultContractInvalid))?;
        if !request_ids.insert(issuance_request_id) {
            return Err(ApiError::new(
                WurzburgResultCode::CardIssuanceResultContractInvalid,
            ));
        }
        let status = match get(1) {
            "ISSUED" => CardIssuanceResultStatus::Issued,
            "REJECTED" => CardIssuanceResultStatus::Rejected,
            _ => {
                return Err(ApiError::new(
                    WurzburgResultCode::CardIssuanceResultContractInvalid,
                ));
            }
        };
        let card_number = optional(get(2)).map(|value| normalize_digits(&value));
        if card_number
            .as_ref()
            .is_some_and(|value| !is_valid_card_number(value))
        {
            return Err(ApiError::new(
                WurzburgResultCode::CardIssuanceResultContractInvalid,
            ));
        }
        let row = CardIssuanceResultRow {
            issuance_request_id,
            status,
            card_number,
            issuer_reference: safe_optional(get(3), 255)?,
            failure_code: safe_optional(get(4), 128)?,
            failure_message: safe_optional(get(5), 1000)?,
            produced_at: parse_optional_time(get(6))?,
            dispatched_at: parse_optional_time(get(7))?,
            tracking_reference: safe_optional(get(8), 255)?,
        };
        row.validate()
            .map_err(|_| ApiError::new(WurzburgResultCode::CardIssuanceResultContractInvalid))?;
        rows.push(row);
    }
    if rows.is_empty() {
        return Err(ApiError::new(
            WurzburgResultCode::CardIssuanceResultContractInvalid,
        ));
    }
    Ok(rows)
}

fn optional(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_string())
}
fn safe_optional(value: &str, max: usize) -> Result<Option<String>, ApiError> {
    if value.chars().count() > max || value.chars().any(char::is_control) {
        return Err(ApiError::new(
            WurzburgResultCode::CardIssuanceResultContractInvalid,
        ));
    }
    Ok(optional(value))
}
fn parse_optional_time(value: &str) -> Result<Option<chrono::DateTime<chrono::Utc>>, ApiError> {
    if value.is_empty() {
        return Ok(None);
    }
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|value| Some(value.with_timezone(&chrono::Utc)))
        .map_err(|_| ApiError::new(WurzburgResultCode::CardIssuanceResultContractInvalid))
}

#[cfg(test)]
mod tests {
    use super::{parse_result_csv, render_request_csv};
    use crate::domain::card_issuance::CardIssuanceExportRow;
    use chrono::Utc;
    use uuid::Uuid;

    #[test]
    fn request_csv_preserves_bank_contract_column_order() {
        let bytes = render_request_csv(&[CardIssuanceExportRow {
            issuance_request_id: Uuid::nil(),
            requested_at: Utc::now(),
            card_range_id: Uuid::nil(),
            funding_mode: "MULTI_PROVIDER".to_string(),
            national_id: "0013547859".to_string(),
            first_name: "First".to_string(),
            last_name: "Last".to_string(),
            birth_date: None,
            mobile: "09120000000".to_string(),
            delivery_province: "Province".to_string(),
            delivery_city: "City".to_string(),
            delivery_address: "Address".to_string(),
            postal_code: "1234567890".to_string(),
            provider_request_count: 2,
        }])
        .unwrap();
        let text = String::from_utf8(bytes).unwrap();
        assert!(text.starts_with(
            "issuance_request_id,requested_at,card_range_id,funding_mode,national_id"
        ));
    }

    #[test]
    fn result_csv_rejects_missing_contract_columns() {
        assert!(parse_result_csv(b"issuance_request_id,status\n").is_err());
    }

    #[test]
    fn result_csv_rejects_duplicate_issuance_request_ids() {
        let request_id = Uuid::new_v4();
        let csv = format!(
            "issuance_request_id,status,card_number,issuer_reference,failure_code,failure_message,produced_at,dispatched_at,tracking_reference\n\
             {request_id},REJECTED,,,BANK_REJECTED,,,,\n\
             {request_id},REJECTED,,,BANK_REJECTED,,,,\n"
        );
        assert!(parse_result_csv(csv.as_bytes()).is_err());
    }

    #[test]
    fn result_csv_accepts_complete_issued_and_rejected_rows() {
        let issued_id = Uuid::new_v4();
        let rejected_id = Uuid::new_v4();
        let csv = format!(
            "issuance_request_id,status,card_number,issuer_reference,failure_code,failure_message,produced_at,dispatched_at,tracking_reference\n\
             {issued_id},ISSUED,6219861000000001,issuer-1,,,,,tracking-1\n\
             {rejected_id},REJECTED,,,BANK_REJECTED,Safe bank rejection,,,\n"
        );
        let rows = parse_result_csv(csv.as_bytes()).expect("valid result rows should parse");
        assert_eq!(rows.len(), 2);
    }
}
