use chrono::{DateTime, Utc};
use opentelemetry::trace::TraceContextExt;
use serde::{Deserialize, Serialize};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

pub const INTERNAL_EVENT_SCHEMA_VERSION: u16 = 1;

/// Stable internal event metadata shared by Wurzburg and Wolfsburg.
///
/// Trace context deliberately lives in Kafka headers rather than this payload.
/// That keeps replayed business data independent from telemetry infrastructure.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InternalEventEnvelope<T> {
    pub event_id: Uuid,
    pub event_type: String,
    pub schema_version: u16,
    pub aggregate_type: String,
    pub aggregate_id: Uuid,
    pub operation_id: Uuid,
    pub occurred_at: DateTime<Utc>,
    pub producer: String,
    pub payload: T,
}

impl<T> InternalEventEnvelope<T> {
    pub fn new(
        event_id: Uuid,
        event_type: impl Into<String>,
        aggregate_type: impl Into<String>,
        aggregate_id: Uuid,
        operation_id: Uuid,
        payload: T,
    ) -> Self {
        Self {
            event_id,
            event_type: event_type.into(),
            schema_version: INTERNAL_EVENT_SCHEMA_VERSION,
            aggregate_type: aggregate_type.into(),
            aggregate_id,
            operation_id,
            occurred_at: Utc::now(),
            producer: "wurzburg".to_string(),
            payload,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InternalEventHeaders {
    pub correlation_id: String,
    pub request_id: String,
    pub causation_id: Option<Uuid>,
    pub traceparent: Option<String>,
    pub tracestate: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeMaterializationReceipt {
    pub receipt_event_id: Uuid,
    pub operation_id: Uuid,
    pub profile_type: String,
    pub aggregate_id: Uuid,
    pub profile_id: Option<Uuid>,
    pub materialized_version: i64,
    pub runtime_key: String,
    pub materialized_at: DateTime<Utc>,
}

impl InternalEventHeaders {
    pub fn from_current_span(correlation_id: String, request_id: String) -> Self {
        let context = tracing::Span::current().context();
        let span_context = context.span().span_context().clone();
        let traceparent = span_context.is_valid().then(|| {
            format!(
                "00-{}-{}-{:02x}",
                span_context.trace_id(),
                span_context.span_id(),
                span_context.trace_flags().to_u8()
            )
        });
        let tracestate = span_context
            .is_valid()
            .then(|| span_context.trace_state().header())
            .filter(|value| !value.is_empty());
        Self {
            correlation_id,
            request_id,
            causation_id: None,
            traceparent,
            tracestate,
        }
    }

    pub fn link_to_span(&self, span: &tracing::Span) {
        let Some(traceparent) = self.traceparent.as_deref() else {
            return;
        };
        let mut carrier =
            std::collections::HashMap::from([("traceparent".to_string(), traceparent.to_string())]);
        if let Some(tracestate) = self.tracestate.as_deref() {
            carrier.insert("tracestate".to_string(), tracestate.to_string());
        }
        let context = opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.extract(&carrier)
        });
        let linked = context.span().span_context().clone();
        if linked.is_valid() {
            span.add_link(linked);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{INTERNAL_EVENT_SCHEMA_VERSION, InternalEventEnvelope};
    use serde_json::json;
    use uuid::Uuid;

    #[test]
    fn internal_envelope_has_stable_version_and_no_embedded_trace_context() {
        let envelope = InternalEventEnvelope::new(
            Uuid::new_v4(),
            "CARD_RANGE_CONTROL_PUBLISH_REQUESTED",
            "CARD_RANGE",
            Uuid::new_v4(),
            Uuid::new_v4(),
            json!({"operational_version": 2}),
        );
        let value = serde_json::to_value(envelope).expect("envelope should serialize");

        assert_eq!(value["schema_version"], INTERNAL_EVENT_SCHEMA_VERSION);
        assert!(value.get("traceparent").is_none());
        assert!(value.get("tracestate").is_none());
        assert!(value.get("partition_key").is_none());
    }
}
