use chrono::{DateTime, Duration, Utc};
use uuid::Uuid;

use crate::{
    api::command::MutationCommandContext,
    db::{
        error::{DbError, DbResult},
        oracle::{
            OracleRepository,
            audit::insert_audit_log,
            idempotency::{
                complete_idempotency_record, fetch_idempotency_record, insert_idempotency_record,
            },
            types::{raw16_to_uuid, uuid_to_raw16},
        },
    },
    domain::{
        audit::{AuditAction, NewAuditLog},
        card_issuance::{
            CardIssuanceBatch, CardIssuanceBatchCursor, CardIssuanceBatchPage,
            CardIssuanceBatchResultRow, CardIssuanceBatchStatus, CardIssuanceExportRow,
            PreparedCardIssuanceBatch,
        },
        idempotency::IdempotencyStatus,
    },
};

#[derive(Debug, Clone)]
pub enum PrepareCardIssuanceBatchOutcome {
    Prepared(Box<PreparedCardIssuanceBatch>),
    Replayed(serde_json::Value),
    NoPendingRequests,
    IdempotencyConflict,
    IdempotencyInProgress,
    IdempotencyInvalidState,
}

impl OracleRepository {
    #[tracing::instrument(skip(self, context), fields(db.system="oracle", db.operation.name="card_issuance_batches.prepare", batch_size))]
    pub async fn prepare_card_issuance_batch_atomic(
        &self,
        context: MutationCommandContext,
        batch_size: u16,
        retention_days: i64,
    ) -> DbResult<PrepareCardIssuanceBatchOutcome> {
        let operation_type = context.operation_type.clone();
        let key = context.idempotency_key.as_str().to_string();
        let request_hash = context.request_hash.clone();
        self.pool.with_transaction("prepare card issuance batch", move |connection| {
            if let Some(existing) = fetch_idempotency_record(connection, &operation_type, &key)? {
                return classify_idempotency(connection, &existing, &request_hash);
            }
            let rows = connection.query(
                "SELECT card_issuance_request_id FROM card_issuance_requests WHERE status='PENDING_EXPORT' AND ROWNUM<=:1 FOR UPDATE SKIP LOCKED",
                &[&i64::from(batch_size)],
            ).map_err(|error| query("failed to claim pending issuance requests", error))?;
            let mut request_ids = Vec::new();
            for row in rows {
                let value: Vec<u8> = row.map_err(|error| query("failed to read claimed issuance request", error))?.get(0).map_err(read)?;
                request_ids.push(raw16_to_uuid(&value)?);
            }
            if request_ids.is_empty() {
                return Ok(PrepareCardIssuanceBatchOutcome::NoPendingRequests);
            }
            if let Some(existing) = fetch_idempotency_record(connection, &operation_type, &key)? {
                return classify_idempotency(connection, &existing, &request_hash);
            }
            insert_idempotency_record(connection, context.new_idempotency_record())?;
            let batch_id = Uuid::new_v4();
            let object_key = format!("card-issuance/{batch_id}/request.csv");
            let expires_at = Utc::now() + Duration::days(retention_days);
            let expires = expires_at.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
            connection.execute(
                "INSERT INTO card_issuance_batches (card_issuance_batch_id,status,request_object_key,request_count,expires_at,created_by_subject,updated_by_subject) VALUES (:1,'CREATING',:2,:3,TO_TIMESTAMP_TZ(:4,'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'),:5,:5)",
                &[&raw(batch_id), &object_key, &(request_ids.len() as i64), &expires, &context.actor.subject],
            ).map_err(|error| query("failed to create issuance batch", error))?;
            // The in-progress idempotency record points to the staged batch so
            // a retry can regenerate the exact CSV after a MinIO/Oracle split.
            connection.execute(
                "UPDATE idempotency_records SET resource_type='card_issuance_batch',resource_id=:1,updated_at=SYSTIMESTAMP WHERE operation_type=:2 AND idempotency_key=:3 AND status='IN_PROGRESS'",
                &[&raw(batch_id), &operation_type, &key],
            ).map_err(|error| query("failed to bind issuance batch recovery resource", error))?;
            let mut export_rows = Vec::with_capacity(request_ids.len());
            for (index, request_id) in request_ids.iter().enumerate() {
                connection.execute(
                    "UPDATE card_issuance_requests SET status='EXPORTED',batch_id=:1,updated_by_subject=:2,updated_at=SYSTIMESTAMP WHERE card_issuance_request_id=:3 AND status='PENDING_EXPORT'",
                    &[&raw(batch_id), &context.actor.subject, &raw(*request_id)],
                ).map_err(|error| query("failed to assign issuance request to batch", error))?;
                connection.execute(
                    "INSERT INTO card_issuance_batch_rows (card_issuance_batch_id,card_issuance_request_id,row_number) VALUES (:1,:2,:3)",
                    &[&raw(batch_id), &raw(*request_id), &((index + 1) as i64)],
                ).map_err(|error| query("failed to create issuance batch row", error))?;
                export_rows.push(fetch_export_row(connection, *request_id)?);
            }
            let now = Utc::now();
            let batch = CardIssuanceBatch {
                batch_id,
                status: CardIssuanceBatchStatus::Creating,
                request_checksum_sha256: None,
                result_checksum_sha256: None,
                request_count: request_ids.len() as u32,
                issued_count: 0,
                rejected_count: 0,
                failed_count: 0,
                expires_at,
                created_at: now,
                updated_at: now,
                completed_at: None,
            };
            Ok(PrepareCardIssuanceBatchOutcome::Prepared(Box::new(PreparedCardIssuanceBatch {
                batch,
                object_key,
                rows: export_rows,
            })))
        }).await
    }

    #[tracing::instrument(skip(self, context), fields(db.system="oracle", db.operation.name="card_issuance_batches.complete_request_file", batch_id=%batch_id))]
    pub async fn complete_card_issuance_batch_request_file_atomic(
        &self,
        context: MutationCommandContext,
        batch_id: Uuid,
        checksum: String,
    ) -> DbResult<CardIssuanceBatch> {
        self.pool.with_transaction("complete issuance request file", move |connection| {
            let statement = connection.execute(
                "UPDATE card_issuance_batches SET status='READY',request_checksum_sha256=:1,updated_by_subject=:2,updated_at=SYSTIMESTAMP WHERE card_issuance_batch_id=:3 AND status='CREATING'",
                &[&checksum, &context.actor.subject, &raw(batch_id)],
            ).map_err(|error| query("failed to finalize issuance request file", error))?;
            if statement.row_count().map_err(|error| query("failed to inspect issuance batch update", error))? != 1 {
                return Err(DbError::Conflict("issuance batch is not awaiting request-file upload".to_string()));
            }
            let batch = fetch_batch(connection, batch_id)?.ok_or_else(|| DbError::Query("issuance batch disappeared".to_string()))?;
            insert_audit_log(connection, NewAuditLog {
                audit_log_id: Uuid::new_v4(), entity_type: "CARD_ISSUANCE_BATCH".to_string(), entity_id: batch_id,
                action_type: AuditAction::Insert, reason: Some("Created bank card-issuance request batch".to_string()),
                old_values: None, new_values: Some(batch_audit_snapshot(&batch)), context: context.audit_context(),
            })?;
            complete_idempotency_record(connection, &context.operation_type, context.idempotency_key.as_str(), "card_issuance_batch", batch_id, serde_json::to_value(&batch).map_err(|error| DbError::Query(format!("failed to serialize issuance batch: {error}")))?)?;
            Ok(batch)
        }).await
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="card_issuance_batches.get", batch_id=%batch_id))]
    pub async fn get_card_issuance_batch(
        &self,
        batch_id: Uuid,
    ) -> DbResult<Option<CardIssuanceBatch>> {
        self.pool
            .with_connection(move |connection| fetch_batch(connection, batch_id))
            .await
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="card_issuance_batches.list", limit))]
    pub async fn list_card_issuance_batches(
        &self,
        cursor: Option<CardIssuanceBatchCursor>,
        limit: u16,
    ) -> DbResult<CardIssuanceBatchPage> {
        self.pool.with_connection(move |connection| {
            let cursor_time=cursor.as_ref().map(|value|value.created_at.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()); let cursor_id=cursor.as_ref().map(|value|raw(value.batch_id)); let fetch=i64::from(limit)+1;
            let rows=connection.query("SELECT card_issuance_batch_id FROM card_issuance_batches WHERE (:1 IS NULL OR created_at<TO_TIMESTAMP_TZ(:2,'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') OR (created_at=TO_TIMESTAMP_TZ(:3,'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') AND card_issuance_batch_id<:4)) ORDER BY created_at DESC,card_issuance_batch_id DESC FETCH FIRST :5 ROWS ONLY", &[&cursor_time,&cursor_time,&cursor_time,&cursor_id,&fetch]).map_err(|error|query("failed to list issuance batches",error))?;
            let mut items=Vec::new();for row in rows{let id:Vec<u8>=row.map_err(|error|query("failed to read issuance batch list row",error))?.get(0).map_err(read)?;items.push(fetch_batch(connection,raw16_to_uuid(&id)?)?.ok_or_else(||DbError::Query("listed issuance batch disappeared".to_string()))?);}let has_next=items.len()>usize::from(limit);if has_next{items.truncate(usize::from(limit));}let next_cursor=has_next.then(||{let item=items.last().expect("non-empty issuance batch page");CardIssuanceBatchCursor{created_at:item.created_at,batch_id:item.batch_id}});Ok(CardIssuanceBatchPage{items,next_cursor})
        }).await
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="card_issuance_batches.result", batch_id=%batch_id))]
    pub async fn get_card_issuance_batch_result(
        &self,
        batch_id: Uuid,
    ) -> DbResult<Option<Vec<CardIssuanceBatchResultRow>>> {
        self.pool.with_connection(move|connection|{if fetch_batch(connection,batch_id)?.is_none(){return Ok(None);}let rows=connection.query("SELECT card_issuance_request_id,row_number,NVL(result_status,'PENDING'),safe_result_code,safe_result_message FROM card_issuance_batch_rows WHERE card_issuance_batch_id=:1 ORDER BY row_number", &[&raw(batch_id)]).map_err(|error|query("failed to read issuance batch results",error))?;let mut values=Vec::new();for row in rows{let row=row.map_err(|error|query("failed to read issuance batch result row",error))?;let id:Vec<u8>=row.get(0).map_err(read)?;let number:i64=row.get(1).map_err(read)?;values.push(CardIssuanceBatchResultRow{issuance_request_id:raw16_to_uuid(&id)?,row_number:u32::try_from(number).map_err(|_|DbError::Query("invalid issuance result row number".to_string()))?,status:row.get(2).map_err(read)?,result_code:row.get(3).map_err(read)?,result_message:row.get(4).map_err(read)?});}Ok(Some(values))}).await
    }

    #[tracing::instrument(skip(self), fields(db.system="oracle", db.operation.name="card_issuance_batches.request_object", batch_id=%batch_id))]
    pub async fn get_card_issuance_request_object(
        &self,
        batch_id: Uuid,
    ) -> DbResult<Option<(String, String)>> {
        self.pool.with_connection(move |connection| {
            match connection.query_row("SELECT request_object_key,request_checksum_sha256 FROM card_issuance_batches WHERE card_issuance_batch_id=:1 AND status IN ('READY','PROCESSING_RESULT','COMPLETED','PARTIALLY_COMPLETED')", &[&raw(batch_id)]) {
                Ok(row) => Ok(Some((row.get(0).map_err(read)?, row.get(1).map_err(read)?))),
                Err(error) if error.kind() == oracle::ErrorKind::NoDataFound => Ok(None),
                Err(error) => Err(query("failed to resolve issuance request object", error)),
            }
        }).await
    }
}

fn fetch_export_row(
    connection: &oracle::Connection,
    request_id: Uuid,
) -> DbResult<CardIssuanceExportRow> {
    let row = connection.query_row(
        "SELECT cir.card_issuance_request_id,TO_CHAR(SYS_EXTRACT_UTC(cir.created_at),'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'),cir.card_range_id,cr.funding_mode,JSON_VALUE(cir.identity_snapshot_json,'$.national_id'),JSON_VALUE(cir.identity_snapshot_json,'$.first_name'),JSON_VALUE(cir.identity_snapshot_json,'$.last_name'),JSON_VALUE(cir.identity_snapshot_json,'$.birth_date'),JSON_VALUE(cir.delivery_snapshot_json,'$.mobile'),JSON_VALUE(cir.delivery_snapshot_json,'$.delivery_province'),JSON_VALUE(cir.delivery_snapshot_json,'$.delivery_city'),JSON_VALUE(cir.delivery_snapshot_json,'$.delivery_address'),JSON_VALUE(cir.delivery_snapshot_json,'$.postal_code'),(SELECT COUNT(*) FROM card_issuance_request_providers p WHERE p.card_issuance_request_id=cir.card_issuance_request_id) FROM card_issuance_requests cir JOIN card_ranges cr ON cr.card_range_id=cir.card_range_id WHERE cir.card_issuance_request_id=:1",
        &[&raw(request_id)],
    ).map_err(|error| query("failed to read issuance export row", error))?;
    let id: Vec<u8> = row.get(0).map_err(read)?;
    let requested: String = row.get(1).map_err(read)?;
    let range: Vec<u8> = row.get(2).map_err(read)?;
    let count: i64 = row.get(13).map_err(read)?;
    Ok(CardIssuanceExportRow {
        issuance_request_id: raw16_to_uuid(&id)?,
        requested_at: parse_time(&requested)?,
        card_range_id: raw16_to_uuid(&range)?,
        funding_mode: row.get(3).map_err(read)?,
        national_id: row.get(4).map_err(read)?,
        first_name: row.get(5).map_err(read)?,
        last_name: row.get(6).map_err(read)?,
        birth_date: row.get(7).map_err(read)?,
        mobile: row.get(8).map_err(read)?,
        delivery_province: row.get(9).map_err(read)?,
        delivery_city: row.get(10).map_err(read)?,
        delivery_address: row.get(11).map_err(read)?,
        postal_code: row.get(12).map_err(read)?,
        provider_request_count: u32::try_from(count)
            .map_err(|_| DbError::Query("invalid provider request count".to_string()))?,
    })
}

pub(crate) fn fetch_batch(
    connection: &oracle::Connection,
    batch_id: Uuid,
) -> DbResult<Option<CardIssuanceBatch>> {
    match connection.query_row(
        "SELECT status,request_checksum_sha256,result_checksum_sha256,request_count,issued_count,rejected_count,failed_count,TO_CHAR(SYS_EXTRACT_UTC(expires_at),'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'),TO_CHAR(SYS_EXTRACT_UTC(created_at),'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'),TO_CHAR(SYS_EXTRACT_UTC(updated_at),'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'),TO_CHAR(SYS_EXTRACT_UTC(completed_at),'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"') FROM card_issuance_batches WHERE card_issuance_batch_id=:1",
        &[&raw(batch_id)],
    ) {
        Ok(row) => {
            let status: String = row.get(0).map_err(read)?;
            let expires: String = row.get(7).map_err(read)?;
            let created: String = row.get(8).map_err(read)?;
            let updated: String = row.get(9).map_err(read)?;
            let completed: Option<String> = row.get(10).map_err(read)?;
            Ok(Some(CardIssuanceBatch {
                batch_id, status: CardIssuanceBatchStatus::from_db_value(&status).ok_or_else(|| DbError::Query("unknown issuance batch status".to_string()))?,
                request_checksum_sha256: row.get(1).map_err(read)?, result_checksum_sha256: row.get(2).map_err(read)?, request_count: number(&row, 3)?, issued_count: number(&row, 4)?, rejected_count: number(&row, 5)?, failed_count: number(&row, 6)?,
                expires_at: parse_time(&expires)?, created_at: parse_time(&created)?, updated_at: parse_time(&updated)?, completed_at: completed.as_deref().map(parse_time).transpose()?,
            }))
        }
        Err(error) if error.kind() == oracle::ErrorKind::NoDataFound => Ok(None),
        Err(error) => Err(query("failed to read issuance batch", error)),
    }
}

fn number(row: &oracle::Row, index: usize) -> DbResult<u32> {
    let value: i64 = row.get(index).map_err(read)?;
    u32::try_from(value).map_err(|_| DbError::Query("invalid issuance batch count".to_string()))
}

fn batch_audit_snapshot(batch: &CardIssuanceBatch) -> serde_json::Value {
    serde_json::json!({"batch_id": batch.batch_id, "status": batch.status, "request_count": batch.request_count, "request_checksum_sha256": batch.request_checksum_sha256, "expires_at": batch.expires_at})
}

fn classify_idempotency(
    connection: &oracle::Connection,
    record: &crate::domain::idempotency::IdempotencyRecord,
    request_hash: &str,
) -> DbResult<PrepareCardIssuanceBatchOutcome> {
    if record.request_hash != request_hash {
        return Ok(PrepareCardIssuanceBatchOutcome::IdempotencyConflict);
    }
    Ok(match record.status {
        IdempotencyStatus::Completed => record
            .response_snapshot
            .clone()
            .map(PrepareCardIssuanceBatchOutcome::Replayed)
            .unwrap_or(PrepareCardIssuanceBatchOutcome::IdempotencyInvalidState),
        IdempotencyStatus::InProgress => {
            let Some(batch_id) = record
                .resource_id
                .filter(|_| record.resource_type.as_deref() == Some("card_issuance_batch"))
            else {
                return Ok(PrepareCardIssuanceBatchOutcome::IdempotencyInvalidState);
            };
            return resume_prepared_batch(connection, batch_id);
        }
        _ => PrepareCardIssuanceBatchOutcome::IdempotencyInvalidState,
    })
}

fn resume_prepared_batch(
    connection: &oracle::Connection,
    batch_id: Uuid,
) -> DbResult<PrepareCardIssuanceBatchOutcome> {
    let Some(batch) = fetch_batch(connection, batch_id)? else {
        return Ok(PrepareCardIssuanceBatchOutcome::IdempotencyInvalidState);
    };
    if batch.status != CardIssuanceBatchStatus::Creating {
        return Ok(PrepareCardIssuanceBatchOutcome::IdempotencyInvalidState);
    }
    let object_key: String = connection
        .query_row_as(
            "SELECT request_object_key FROM card_issuance_batches WHERE card_issuance_batch_id=:1",
            &[&raw(batch_id)],
        )
        .map_err(|error| query("failed to recover issuance request object key", error))?;
    let rows = connection
        .query(
            "SELECT card_issuance_request_id FROM card_issuance_batch_rows WHERE card_issuance_batch_id=:1 ORDER BY row_number",
            &[&raw(batch_id)],
        )
        .map_err(|error| query("failed to recover issuance batch rows", error))?;
    let mut export_rows = Vec::with_capacity(batch.request_count as usize);
    for row in rows {
        let request_id: Vec<u8> = row
            .map_err(|error| query("failed to read recovered issuance row", error))?
            .get(0)
            .map_err(read)?;
        export_rows.push(fetch_export_row(connection, raw16_to_uuid(&request_id)?)?);
    }
    if export_rows.len() != batch.request_count as usize {
        return Ok(PrepareCardIssuanceBatchOutcome::IdempotencyInvalidState);
    }
    Ok(PrepareCardIssuanceBatchOutcome::Prepared(Box::new(
        PreparedCardIssuanceBatch {
            batch,
            object_key,
            rows: export_rows,
        },
    )))
}

fn raw(value: Uuid) -> Vec<u8> {
    uuid_to_raw16(value).to_vec()
}
fn query(context: &str, error: oracle::Error) -> DbError {
    DbError::Query(format!("{context}: {error}"))
}
fn read(error: oracle::Error) -> DbError {
    DbError::Query(format!("failed to read Oracle issuance row: {error}"))
}
fn parse_time(value: &str) -> DbResult<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)
        .map_err(|error| DbError::Query(format!("invalid issuance timestamp: {error}")))?
        .with_timezone(&Utc))
}
