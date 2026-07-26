# Wolfsburg Service Handoff

This document is the architectural starting point for Wolfsburg. It records the
runtime-materialization responsibilities that Wurzburg, Nuremberg, and the Card
and Provider handoffs depend on. Wolfsburg has not been implemented yet, so this
document defines the production contract without preserving PoC behavior.

## 1. Service Purpose

Wolfsburg is the asynchronous event-processing, reconciliation, and Dragonfly
runtime-projection service for HyperCard.

Wolfsburg:

- consumes durable internal Kafka events;
- materializes CP, CPOL, CRCTL, and FEE runtime values in Dragonfly through its
  Redis-compatible protocol;
- emits durable materialization receipts back to Wurzburg;
- persists Nuremberg CMS event facts in Oracle;
- performs rollback orchestration and reconciliation from immutable FundingPlan
  facts;
- preserves same-card ordering for card-affecting work;
- refreshes card runtime state after confirmed financial effects; and
- releases card mutation locks only after the required replacement profile is
  durable.

Wolfsburg has no public business REST API. Health, readiness, metrics, and
strictly internal operator endpoints may exist on a private management listener.

## 2. Ownership Boundary

### Wurzburg

Wurzburg owns:

- canonical Oracle provider, user, card, range, policy, and fee facts;
- TigerBeetle account creation and provider/user funding commands;
- durable outbox events requesting runtime materialization;
- durable inbox processing for Wolfsburg receipts; and
- control-plane status finalized from materialization receipts.

Wurzburg never writes replacement CP, CPOL, CRCTL, or FEE values directly.

### Nuremberg

Nuremberg owns CMS Balance, Confirm, and Rollback request handling, reads runtime
profiles from Dragonfly, performs Confirm ledger planning/mutation, and publishes
immutable CMS/FundingPlan events.

Nuremberg never queries Wurzburg Oracle tables.

### Wolfsburg

Wolfsburg is the sole writer of replacement runtime projections:

```text
CP:{card_number}
CPOL:SingleProvider:{card_range_id}
CPOL:MultiProvider:{card_range_id}
CRCTL:{card_range_id}
FEE:{provider_id}
```

Dragonfly is runtime state, not financial proof. Oracle facts and TigerBeetle
lookups remain the recovery authorities.

## 3. Internal Event Envelope

All Wurzburg-to-Wolfsburg events use one versioned envelope:

```json
{
  "event_id": "uuid",
  "event_type": "CARD_POLICY_PROFILE_PUBLISH_REQUESTED",
  "schema_version": 1,
  "aggregate_type": "CARD_RANGE",
  "aggregate_id": "uuid",
  "operation_id": "uuid",
  "occurred_at": "2026-07-18T00:00:00Z",
  "producer": "wurzburg",
  "payload": {}
}
```

Kafka headers carry:

```text
event_id
event_type
schema_version
operation_id
correlation_id
request_id
causation_id optional
traceparent
tracestate allowlisted and optional
```

Internal events may propagate W3C trace context. Provider-facing events must
never include trace context, baggage, internal correlation identifiers, or
other telemetry internals.

Event payloads must not contain raw PAN, national ID, contacts, credentials,
JWTs, unrestricted metadata, SQL, or live financial balances. A normalized card
number may be used only as the Kafka partition key where same-card ordering is
required; logs and spans must use a masked or hashed representation.

The Kafka record key is transport metadata and is not duplicated inside the
envelope. Wurzburg publishes materialization commands to:

```text
wurzburg.runtime-projection.commands.v1
```

Wolfsburg publishes receipts to:

```text
wolfsburg.runtime-projection.receipts.v1
```

## 4. Card Policy Materialization

Wurzburg emits `CARD_POLICY_PROFILE_PUBLISH_REQUESTED` when:

- the first ACTIVE provider is attached to a range with a complete DRAFT policy;
  or
- a replacement policy is configured while the range already has an ACTIVE
  provider.

Payload:

```json
{
  "card_range_id": "uuid",
  "card_policy_profile_id": "uuid",
  "policy_version": 3,
  "funding_mode": "SINGLE_PROVIDER",
  "withdrawal_limit_authority": "PLATFORM",
  "withdrawal_limits": {
    "per_transaction_min_amount": null,
    "per_transaction_max_amount": 5000000,
    "daily": { "max_amount": 10000000, "max_count": null },
    "weekly": null,
    "monthly": null,
    "yearly": null
  },
  "calendar": {
    "timezone": "Asia/Tehran",
    "week_starts_on": "SATURDAY",
    "window_mode": "CALENDAR"
  }
}
```

For CMS authority, `withdrawal_limits` and `calendar` are null. Null individual
metrics mean that metric is not enforced. Wolfsburg preserves null semantics
exactly and does not invent defaults.

Wolfsburg derives the key from immutable range funding mode:

```text
SINGLE_PROVIDER -> CPOL:SingleProvider:{card_range_id}
MULTI_PROVIDER  -> CPOL:MultiProvider:{card_range_id}
```

Wolfsburg writes the complete value atomically. It must never merge a new policy
with stale Dragonfly fields.

## 5. Range Control Materialization

Wurzburg emits `CARD_RANGE_CONTROL_PUBLISH_REQUESTED` after range status,
operational controls, or provider eligibility changes.

Target:

```text
CRCTL:{card_range_id}
```

Value:

```json
{
  "card_range_id": "uuid",
  "range_status": "ACTIVE",
  "issuance_enabled": true,
  "cms_operation_mode": "FULL",
  "operational_version": 7,
  "eligible_provider_ids": ["uuid"]
}
```

Wolfsburg rejects stale events whose operational version is lower than the
currently materialized version. Equal versions are idempotent replays.

## 6. Card Profile Materialization

Wurzburg and Nuremberg events may request `CP:{card_number}` refresh. Wolfsburg
rebuilds CP from canonical Oracle relationships and live TigerBeetle balances;
it does not trust a stale CP payload embedded in the event.

Wolfsburg must:

- preserve same-card ordering using normalized card number as partition key;
- read balances only from TigerBeetle;
- never persist debit/credit balances in Oracle;
- compute funding-source capacity from current ledger facts and configured
  cardholder caps;
- write a complete replacement CP; and
- release the matching card mutation lock only after the replacement is durable.

## 7. Fee Profile Materialization

Wolfsburg materializes the active provider fee profile at:

```text
FEE:{provider_id}
```

Wolfsburg consumes `PROVIDER_FEE_PROFILE_PUBLISH_REQUESTED` and writes this
complete replacement value:

```json
{
  "provider_fee_profile_id": "uuid",
  "provider_id": "uuid",
  "version": 3,
  "fee_policy": {
    "rate_bps": 125,
    "fixed_amount_rials": 5000,
    "fee_payer": "PROVIDER_USER"
  }
}
```

Fee version replacement follows the same outbox, idempotent write, and receipt
rules as policy materialization. The receipt uses `profile_type = FEE`,
`aggregate_id = provider_id`, `profile_id = provider_fee_profile_id`, and
`runtime_key = FEE:{provider_id}`.

## 8. Materialization Receipt

After a successful Dragonfly write, Wolfsburg emits
`RUNTIME_PROFILE_MATERIALIZED`:

```json
{
  "receipt_event_id": "uuid",
  "operation_id": "uuid",
  "profile_type": "CPOL",
  "aggregate_id": "uuid",
  "profile_id": "uuid",
  "materialized_version": 3,
  "runtime_key": "CPOL:SingleProvider:uuid",
  "materialized_at": "2026-07-18T00:00:00Z"
}
```

Wurzburg consumes this event through its durable inbox. It validates operation,
aggregate, profile, and version before changing control-plane state. Duplicate
receipts are successful replays. A mismatched or out-of-order receipt is stored
as failed evidence and must never activate a policy.

For CPOL, a valid receipt causes one Wurzburg Oracle transaction to:

- persist the receipt;
- mark the materialized DRAFT policy ACTIVE;

For FEE, the same transaction shape persists the receipt, supersedes the prior
ACTIVE provider fee profile, activates the matching frozen DRAFT, records audit
evidence, and completes inbox processing. A mismatch leaves the prior ACTIVE
profile unchanged.
- mark the previous ACTIVE policy SUPERSEDED;
- complete the associated operation; and
- write immutable audit evidence.

## 9. Delivery And Recovery

- Kafka delivery is at least once.
- Every consumer first claims `event_id` in a durable inbox.
- Dragonfly writes are idempotent by aggregate/profile version.
- Consumer offset commit occurs only after durable processing.
- Automatic retry uses bounded exponential backoff with jitter.
- Poison events enter a durable dead-letter state with safe diagnostics.
- A receipt that cannot be parsed is recorded by topic, partition, offset,
  payload SHA-256, and safe error code. Its payload is never stored in the
  transport dead-letter table.
- Generic operator recovery may requeue dead letters; policy-specific retry and
  cancellation APIs do not exist.
- A newer event never permits an older event to overwrite its runtime value.
- Missing Dragonfly data is repaired from Oracle and TigerBeetle facts, never
  from logs or telemetry.

## 10. Observability

OpenTelemetry is mandatory for Kafka consume, inbox claim, Oracle read,
TigerBeetle lookup, Dragonfly materialization, receipt publication, retries,
dead-letter transitions, and reconciliation.

Consumer processing creates a new span linked to the producer trace. Retries and
replays preserve event identity and link to the original trace context rather
than pretending to be the original attempt.

Allowed span fields include event ID, event type, aggregate type, aggregate ID,
profile ID, version, retry count, and safe result code. Never record runtime
payloads, raw card numbers, national IDs, balances, credentials, or unrestricted
metadata.

Tempo/OTel failure is never a correctness dependency. Telemetry export remains
best effort and cannot block materialization or recovery.

## 11. Security

- Wolfsburg uses dedicated least-privilege Kafka, Oracle, Dragonfly, and
  TigerBeetle credentials.
- It accepts no public identity headers.
- Management endpoints are private and authenticated through infrastructure
  identity/mTLS.
- Internal event schemas are allowlisted and version validated.
- Unknown event types or schema versions fail closed into durable recovery.
- Provider-facing event production uses a separate DTO/envelope and strips all
  internal trace and recovery fields.

## 12. Required Scenario Tests

1. First provider attachment materializes DRAFT CPOL and returns a matching
   receipt.
2. Policy replacement leaves the previous CPOL active until the new write is
   durable.
3. Null amount/count metrics remain disabled without generated defaults.
4. Duplicate policy events and receipts are idempotent.
5. An older policy/control version cannot overwrite a newer version.
6. Dragonfly failure produces retry without a false receipt.
7. Kafka replay after process restart resumes from durable inbox state.
8. Receipt mismatch never activates the Oracle policy.
9. CP refresh reads live balances from TigerBeetle and never Oracle.
10. Same-card events remain ordered with multiple Wolfsburg replicas.
11. Card lock release occurs only after durable CP replacement.
12. OTel context links producer, consumer, dependency, retry, and receipt spans
    without leaking protected data.

## 13. Non-Negotiable Rules

- Wolfsburg is the sole replacement writer for CP, CPOL, CRCTL, and FEE.
- Wurzburg remains the canonical Oracle owner.
- TigerBeetle is the only source of financial balances.
- Dragonfly is not proof of money movement.
- Runtime writes and receipts are versioned and idempotent.
- CPOL remains mandatory for both PLATFORM and CMS authority.
- A policy is not ACTIVE in Oracle until its materialization receipt is valid.
- Provider-facing events never carry internal tracing or recovery context.
- Every asynchronous transition is observable and recoverable.
