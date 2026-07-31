use std::{env, time::Duration};

use rdkafka::{
    ClientConfig, Message,
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::Headers,
};
use uuid::Uuid;
use wurzburg::{
    config::Settings,
    messaging::{
        MessageBrokerAdmin, MessageProducer,
        contract::{InternalEventEnvelope, InternalEventHeaders},
    },
};

/// Scenario goal:
/// Prove the production Kafka boundary against a real broker: native topic
/// administration, typed serialization, partition key delivery, mandatory
/// identity headers, and W3C trace-header transport.
///
/// Oracle facts: none. This scenario tests the broker adapter independently;
/// Oracle outbox behavior is covered by the Card integration scenarios.
/// Final proof: the consumed envelope and headers preserve the exact immutable
/// event/operation identities published by Wurzburg.
#[tokio::test]
async fn publishes_typed_internal_event_through_real_kafka() {
    if env::var("RUN_KAFKA_INTEGRATION_TESTS").ok().as_deref() != Some("1") {
        return;
    }
    let settings = Settings::new().expect("Kafka integration settings should load");
    let round_trip = env::var("RUN_KAFKA_ROUNDTRIP_INTEGRATION_TESTS")
        .ok()
        .as_deref()
        == Some("1");
    let topic = if round_trip {
        format!("wurzburg.contract-test.{}", Uuid::new_v4().simple())
    } else {
        settings.kafka.outbox_relay.topic.clone()
    };
    let admin = round_trip
        .then(|| MessageBrokerAdmin::new(&settings.kafka).expect("Kafka admin should initialize"));
    if let Some(admin) = &admin {
        admin
            .create_topic(&topic)
            .await
            .expect("test topic should exist");
    }

    let event_id = Uuid::new_v4();
    let operation_id = Uuid::new_v4();
    let aggregate_id = Uuid::new_v4();
    let envelope = InternalEventEnvelope::new(
        event_id,
        "CARD_RANGE_CONTROL_PUBLISH_REQUESTED",
        "CARD_RANGE",
        aggregate_id,
        operation_id,
        serde_json::json!({"operational_version":7}),
    );
    let headers = InternalEventHeaders {
        correlation_id: "kafka-contract-scenario".to_string(),
        request_id: Uuid::new_v4().to_string(),
        causation_id: None,
        traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string()),
        tracestate: Some("vendor=value".to_string()),
    };
    MessageProducer::new(&settings.kafka)
        .expect("producer should initialize")
        .send_internal(&topic, &aggregate_id.to_string(), &envelope, &headers)
        .await
        .expect("broker should acknowledge event");

    // Producer acknowledgement is the required deployment smoke test. A full
    // consume round trip is separately gated because it needs admin and read
    // privileges that the production Wurzburg producer must not possess.
    if !round_trip {
        return;
    }

    let consumer: StreamConsumer = consumer_config(&settings)
        .create()
        .expect("Kafka test consumer should initialize");
    consumer
        .subscribe(&[&topic])
        .expect("consumer should subscribe");

    let message = tokio::time::timeout(Duration::from_secs(30), consumer.recv())
        .await
        .expect("event should arrive before timeout")
        .expect("consumer should receive event");
    let consumed: InternalEventEnvelope<serde_json::Value> =
        serde_json::from_slice(message.payload().expect("payload should exist"))
            .expect("envelope should deserialize");
    assert_eq!(consumed.event_id, event_id);
    assert_eq!(consumed.operation_id, operation_id);
    assert_eq!(message.key(), Some(aggregate_id.to_string().as_bytes()));
    let kafka_headers = message.headers().expect("identity headers should exist");
    assert_eq!(
        header(kafka_headers, "event_id"),
        Some(event_id.to_string())
    );
    assert_eq!(
        header(kafka_headers, "operation_id"),
        Some(operation_id.to_string())
    );
    assert_eq!(header(kafka_headers, "traceparent"), headers.traceparent);
    assert_eq!(header(kafka_headers, "tracestate"), headers.tracestate);
    consumer
        .commit_message(&message, CommitMode::Sync)
        .expect("offset should commit");
    drop(message);
    drop(consumer);
    admin
        .expect("round-trip mode has an admin client")
        .delete_topic(&topic)
        .await
        .expect("test topic should be removed");
}

fn consumer_config(settings: &Settings) -> ClientConfig {
    let kafka = &settings.kafka;
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", &kafka.bootstrap_servers)
        .set(
            "group.id",
            format!("wurzburg-contract-test-{}", Uuid::new_v4()),
        )
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("security.protocol", &kafka.security_protocol);
    if let Some(cert) = kafka
        .security_cert
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        config.set("ssl.ca.location", cert);
    }
    if kafka.security_protocol.contains("SASL") {
        config
            .set("sasl.mechanism", kafka.sasl_mechanism.as_deref().unwrap())
            .set("sasl.username", kafka.sasl_username.as_deref().unwrap())
            .set("sasl.password", kafka.sasl_password.as_deref().unwrap());
    }
    config
}

fn header(headers: &impl Headers, key: &str) -> Option<String> {
    headers
        .iter()
        .find(|header| header.key == key)
        .and_then(|header| {
            header
                .value
                .and_then(|value| std::str::from_utf8(value).ok())
                .map(ToOwned::to_owned)
        })
}
