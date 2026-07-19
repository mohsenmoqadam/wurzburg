use uuid::Uuid;

use crate::db::{
    error::{DbError, DbResult},
    oracle::{OracleRepository, types::uuid_to_raw16},
};

impl OracleRepository {
    /// Persists transport-level poison evidence without retaining the message body.
    #[tracing::instrument(
        skip(self, payload_sha256),
        fields(
            db.system = "oracle",
            db.operation.name = "kafka_poison_messages.insert",
            messaging.destination.name = topic,
            messaging.kafka.partition = partition,
            messaging.kafka.offset = offset,
            error.code = error_code
        )
    )]
    pub async fn record_kafka_poison_message(
        &self,
        topic: String,
        partition: i32,
        offset: i64,
        payload_sha256: Option<String>,
        error_code: &'static str,
    ) -> DbResult<()> {
        self.pool
            .with_transaction("record Kafka poison message", move |connection| {
                let id = uuid_to_raw16(Uuid::new_v4()).to_vec();
                match connection.execute(
                    "INSERT INTO kafka_poison_messages (kafka_poison_message_id, topic_name, partition_id, message_offset, payload_sha256, error_code) VALUES (:1, :2, :3, :4, :5, :6)",
                    &[&id, &topic, &partition, &offset, &payload_sha256, &error_code],
                ) {
                    Ok(_) => Ok(()),
                    Err(error) if error.to_string().contains("ORA-00001") => Ok(()),
                    Err(error) => Err(DbError::Query(format!(
                        "failed to persist Kafka poison evidence: {error}"
                    ))),
                }
            })
            .await
    }
}
