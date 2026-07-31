# Wurzburg Service Architecture Handoff

This document is the architectural entry point for Wurzburg. It explains why
the service exists, how it collaborates with the surrounding platform, where
each kind of state belongs, and which production rules apply across every
domain.

It intentionally does not duplicate detailed API payloads, Oracle tables, card
policy fields, provider-event schemas, or WSO2 route matrices. Those contracts
live in the focused handoffs referenced below.

## 1. Document Map And Authority

Read this document first, then use the focused handoffs for implementation:

- `WURZBURG_CARD_RANGE_POLICY_HANDOFF.md`
  - card ranges, provider eligibility, range lifecycle and emergency controls
  - card policy authority, immutable policy versions, usage accounts
  - CPOL/CRCTL semantics and required Nuremberg changes
- `WURZBURG_PROVIDER_HANDOFF.md`
  - provider identity/lifecycle, users, cards, TigerBeetle accounts
  - synchronous onboarding, credit grant/full return, operation WAL
  - Kafka provisioning and controlled provider-facing events
- `WURZBURG_WSO2_HANDOFF.md`
  - mTLS/JWT trust boundary, canonical headers, roles/scopes
  - gateway retries, timeouts, rate limits, errors, tracing, ESB acceptance
- `WOLFSBURG_SERVICE_HANDOFF.md`
  - asynchronous event envelopes, CP/CPOL/CRCTL/FEE materialization
  - Dragonfly ownership, receipts, ordering, reconciliation, and tracing
- `docs/adr/0001-oracle-persistence-strategy.md`
  - Oracle persistence rationale and operational strategy
- `docs/oracle/README.md`
  - local/environment Oracle setup and migration operation

Focused handoffs are authoritative for their domains. This service handoff is
authoritative for cross-service ownership and system-wide engineering rules.
When a broad historical statement conflicts with a focused final contract, the
focused contract wins and this overview must be corrected.

## 2. Service Purpose

Wurzburg is the synchronous business, administration, and control-plane service
for the HyperCard funding platform.

It provides APIs and durable commands for:

- provider identity, lifecycle, contacts, operational controls, and Kafka access
- global users and provider-user relationships
- card ranges, provider eligibility, card assignment, and cardholder preferences
- provider-to-user credit grant and full remaining-credit return
- card policy, range control, and provider fee configuration
- account/transaction queries and operational reporting
- batch imports/exports and generated report jobs
- durable events that request runtime-profile materialization

Wurzburg owns canonical business configuration and relationships. It does not
serve the external CMS transaction contract and it is not the runtime profile
reader used for Confirm decisions.

## 3. Platform Topology

```text
Admin UI / Provider systems / Cardholder app
                    |
                    v
                  WSO2
                    |
                    v
               Wurzburg APIs
        /        /      |       \        \
       v        v       v        v        v
    Oracle  TigerBeetle Dragonfly Kafka   MinIO
                         ^         |
                         |         v
CMS <-> Nuremberg -------+------> Wolfsburg
             |                       |
             +---- TigerBeetle ------+

WSO2 / Wurzburg / Nuremberg / Wolfsburg / workers
                    |
                    v
             OpenTelemetry Collector
                    |
                    v
                  Tempo
```

The arrows describe logical dependencies, not permission to bypass service
contracts. In particular, Nuremberg never queries Oracle and public callers
never bypass WSO2.

## 4. Service Boundaries

### Wurzburg

Wurzburg is REST-driven for external business commands and worker-driven for
its outbox, provisioning, batch, receipt, and recovery processing.

Wurzburg owns:

- canonical provider/user/card/range/policy/fee/business-configuration facts
- Oracle transactions, idempotency records, audit, operation WAL, and outbox
- provider-originated TigerBeetle account creation and money movements
- card mutation lock acquisition and stale CP invalidation
- Kafka provider topic/credential administration
- the public provider-event contract and subscription control plane
- import/export/report job metadata and MinIO object references

Wurzburg does not write replacement runtime profiles to Dragonfly. After a
durable CP-affecting command, it leaves the card lock for Wolfsburg.

### Nuremberg

Nuremberg is the synchronous CMS-facing transaction service. It owns:

- CMS authentication/signature validation
- `Balance`, `Confirm`, and `Rollback` HTTP contracts
- Dragonfly reads for card, policy, range-control, and fee runtime facts
- Confirm funding-plan construction and TigerBeetle mutation
- immutable FundingPlan snapshots used by rollback and reconciliation
- durable Kafka publication of CMS transaction outcomes

Nuremberg never queries Wurzburg Oracle and never reconstructs missing runtime
facts from another database. A missing/invalid required profile fails closed.

### Wolfsburg

Wolfsburg is the asynchronous event-processing and runtime-materialization
service. It owns:

- consuming Nuremberg and Wurzburg internal events
- persisting verified CMS event facts and reconciliation state in Oracle
- rollback orchestration from immutable FundingPlan facts
- rebuilding runtime profiles from current Oracle mappings and live
  TigerBeetle state
- writing CP, CPOL, CRCTL, and FEE values to Dragonfly
- emitting materialization receipts and releasing matching card locks
- recovery of stuck CMS/event/materialization workflows
- producing provider-facing events derived from Confirm/Rollback facts using
  the shared public event contract

Wolfsburg has no public business API surface.

### WSO2

WSO2 is the only public gateway to Wurzburg. It owns edge authentication,
coarse scope enforcement, canonical request context, quotas, request limits,
and the private mTLS hop. Wurzburg independently verifies the trusted assertion
and enforces resource ownership and business authorization.

The complete contract is `WURZBURG_WSO2_HANDOFF.md`.

## 5. State Ownership Matrix

```text
State/fact                                  Authoritative owner/store
-----------------------------------------------------------------------------
Provider/user/card/range relationships      Wurzburg / Oracle
Policy, fee, operational config versions    Wurzburg / Oracle
Idempotency, WAL, audit, outbox              Wurzburg / Oracle
CMS event and rollback/reconciliation facts Wolfsburg / Oracle
Money balances and debit/credit counters    TigerBeetle only
Policy usage counters                       TigerBeetle only
Runtime CP/CPOL/CRCTL/FEE projections       Wolfsburg / Dragonfly
CMS FundingPlan                             Nuremberg event facts / Oracle copy
Provider integration event stream           Kafka provider topic
Internal integration/event stream           Kafka internal topics
Import/export/report files                  MinIO
Trace telemetry                             OTel Collector -> Tempo
```

No component may create a second authority for convenience. Derived values may
be cached only when the focused contract explicitly allows it and must never be
treated as proof of a financial effect.

## 6. Oracle: Durable Business And Recovery State

Oracle is Wurzburg's only relational database. PostgreSQL and a dual-database
repository abstraction are not part of the final architecture.

Oracle stores:

- business entities, relationships, statuses, immutable versions, and metadata
- TigerBeetle account/transfer UUID mappings, never balances
- idempotency keys/request hashes and replayable API results
- operation WAL state and deterministic external-effect identities
- integration outbox/inbox and runtime-materialization receipts
- immutable before/after audit snapshots with trusted WSO2 actor context
- batch/report job state and MinIO object references
- business configuration with version/effective time/audit

Oracle conventions:

- UUID: `RAW(16)`
- money: non-negative integer Iranian rials in `NUMBER(38,0)` where persisted
- JSON: Oracle native `JSON`, with schema/application validation
- time: `TIMESTAMP WITH TIME ZONE`, stored/returned in UTC unless a business
  calendar explicitly requires a named timezone
- enums/statuses: constrained strings with check constraints
- concurrency: explicit transactions, row locks, function-based uniqueness,
  and `FOR UPDATE SKIP LOCKED` for leased workers

SQL and Oracle row types remain inside `src/db/oracle`. Handlers call
application services; services own transaction boundaries and business rules.
Because Oracle is the sole implementation, database-neutral repository traits
must not be added merely for abstraction aesthetics. Traits remain appropriate
for genuinely substitutable external dependencies such as clocks, Kafka,
Dragonfly, TigerBeetle, and object storage gateways.

Infrastructure integration and composition modules follow these ownership
boundaries:

```text
src/config/           configuration schema, loading, validation, runtime identity
src/state/            application dependency container and process bootstrap
src/object_storage/   MinIO client, bootstrap, models, key/size/checksum validation
src/messaging/        event contracts, Kafka producer/admin, relay, and receipts
src/ledger/           ledger models/commands and the TigerBeetle client/worker
src/db/oracle/        Oracle persistence, transactions, migrations, and row mapping
```

Business services depend on the narrow public facade of each integration
module. MinIO SDK types, credential construction, bucket provisioning, and
object-key validation must not leak into handlers or application services.
`AppState` is a dependency container rather than an infrastructure provisioning
mechanism: normal server replicas construct clients and verify readiness, while
explicit deployment init jobs create buckets, Kafka resources, and schema.

Module names describe Wurzburg capabilities, while implementation-specific
names remain explicit at infrastructure and diagnostic boundaries. Therefore
application code uses `LedgerClient`, `MessageProducer`, and
`MessageBrokerAdmin`; configuration retains `TigerBeetleConfig` and
`KafkaConfig`; provider SCRAM/ACL APIs retain Kafka terminology; and telemetry
continues to report the concrete `tigerbeetle` and `kafka` systems.

### Migration Policy

- Before the first shared production/staging baseline, disposable draft
  migrations may be rewritten into the clean final Oracle schema described by
  the focused handoffs.
- After that baseline, migration files are immutable and all changes are
  forward-only migrations.
- Startup migration execution uses one Oracle migration lock and validates
  checksums before serving traffic.
- Force rebuild is allowed only in explicitly configured disposable local/test
  environments. Production must reject the force-rebuild configuration.
- Liveness must not depend on migration success; readiness remains false until
  the required schema version is verified.

## 7. TigerBeetle: Sole Financial Source Of Truth

TigerBeetle is the only authority for:

- posted/pending debits and credits
- signed/effective account balances derived from those counters
- provider distributed credit, user credit, CMS settlement, and platform fees
- per-card policy usage counters
- proof that a transfer/account creation exists

Oracle stores account and deterministic transfer identities, not financial
counters. Dragonfly projections, Kafka events, API responses, and reports that
include balance data must obtain it from TigerBeetle at generation time.

All Wurzburg/Nuremberg money and usage accounts use one configured TigerBeetle
ledger so transfers remain valid. Account codes distinguish provider-owned,
provider-fee, CMS-settlement, platform-fee, provider-user, and usage accounts.

Domain identities are UUIDs. Convert UUID binary values to TigerBeetle `u128`
only inside the ledger boundary. Deterministic account/transfer IDs are required
for retry and recovery.

Provider-user accounts prevent negative available credit. Provider-level
accounts follow the signed-balance/flags defined in the Provider handoff.

Redis-compatible runtime data and Oracle rows are never accepted as proof of a
TigerBeetle money movement.

## 8. Dragonfly: Runtime Decision Store

Dragonfly is the deployed Redis-compatible runtime store. Rust clients may use
the Redis protocol/library, but production architecture and operations target
Dragonfly rather than a separate Redis deployment.

Dragonfly contains fast, replaceable projections consumed by Nuremberg:

```text
CP:{card_number}
CPOL:SingleProvider:{card_range_id}
CPOL:MultiProvider:{card_range_id}
CRCTL:{card_range_id}
FEE:{provider_id}
Lock-CP:{card_number}
```

Semantic ownership:

- Wurzburg owns the canonical Oracle inputs and emits durable materialization
  requests.
- Wolfsburg is the sole writer of replacement CP/CPOL/CRCTL/FEE projections.
- Nuremberg is a runtime reader and does not query Oracle on a cache miss.
- Wurzburg and Nuremberg may participate in the shared card lock/invalidation
  protocol, but only Wolfsburg writes the fresh CP and releases a post-commit
  lock after durable processing.

Dragonfly is not a ledger, audit log, event bus, or durable business database.
Every key can be reconstructed from Oracle mappings plus TigerBeetle state.

The canonical funding accounts published for each provider source are:

```rust
pub struct FundingSourceLedgerAccounts {
    pub user_provider_account: Uuid,
    pub provider_fee_account: Uuid,
    pub cms_settlement_account: Uuid,
    pub platform_fee_account: Uuid,
}
```

`PROVIDER_OWNED` is deliberately absent from this runtime contract. Wurzburg
uses it for provider credit grant/return workflows; Nuremberg uses
`provider_fee_account` only when the active provider fee profile selects the
provider as fee payer.

Production operating rules:

- use TLS/authentication and a highly available Dragonfly deployment because
  Nuremberg's synchronous availability depends on runtime reads
- configure a non-evicting policy for profile and lock namespaces; memory
  pressure must alert/fail visibly rather than silently remove correctness keys
- current profile/control keys do not expire merely to refresh themselves;
  replacement is event/version driven
- lock keys always have bounded TTL and ownership tokens
- persistence/replication improve availability and restart behavior but do not
  change Oracle/TigerBeetle authority
- expose pool saturation, command latency/errors, memory, eviction attempts,
  replication health, key materialization age, and lock age as telemetry

Domain/application code depends on a runtime-profile/lock gateway. Dragonfly/
Redis protocol details remain in the infrastructure adapter and configuration.

Nuremberg reads range controls and policy references according to the Card
handoff's no-stale-cache rules. Missing, malformed, or contract-incomplete
runtime values fail closed and trigger the defined warmup/recovery path; they do
not authorize skipped limits or guessed funding accounts.

## 9. Kafka: Durable Integration Boundary

Kafka carries immutable facts and materialization commands between services.
It is not used as an alternative query database.

Internal event classes:

- card-profile refresh requests keyed by normalized card number
- policy/range-control/fee publication requests keyed by range/provider
- Nuremberg Balance/Confirm/Rollback facts keyed to preserve same-card ordering
- Wolfsburg materialization receipts
- configuration/recovery events where a focused contract requires them

Delivery rules:

- delivery is at least once
- every event has an immutable UUID and versioned schema
- consumers claim event IDs through a durable inbox before side effects
- same-card events use a stable card partition key
- producers persist events through an Oracle outbox in the same transaction as
  the state that makes the event true
- broker acknowledgement is publication proof, not consumer/materialization
  proof
- a missing acknowledgement is uncertain; republishing the same event ID is
  safe and expected

Internal runtime-projection topics:

```text
wurzburg.runtime-projection.commands.v1
wolfsburg.runtime-projection.receipts.v1
```

The Wurzburg producer uses idempotent broker delivery, `acks=all`, bounded
in-flight requests, explicit request/delivery deadlines, and an Oracle outbox
lease longer than the delivery deadline. Producer retries preserve the same
event ID. Kafka headers carry event, operation, correlation/request, and W3C
trace context; the business envelope does not embed tracing fields.

Receipt consumers disable auto-commit and commit an offset only after Oracle
inbox/business processing succeeds. Contract-invalid records are durably
recorded by topic/partition/offset, payload hash, and safe error code before the
offset advances. Oracle failures leave the offset uncommitted for retry.

Production deployment must pre-create both internal topics and grant Wurzburg
write access to the command topic and read/group access to the receipt topic.
The ordinary producer identity does not receive topic-admin privileges.

Provider-facing events use separate provider topics and the public envelope in
`WURZBURG_PROVIDER_HANDOFF.md`. Wurzburg is the controlled publisher. Wurzburg
and Wolfsburg may produce public event facts into the shared provider outbox.
Platform-admin subscription gates decide which public event types are sent.

Internal payloads, audit facts, WAL state, database snapshots, secrets, and OTel
trace headers must never leak to provider topics. Kafka consumer offsets and
lag remain Kafka responsibilities and are not duplicated as business tables.

## 10. Idempotency, WAL, Outbox, And Recovery

Every mutating external API requires `Idempotency-Key`.

- same key + same canonical request hash: replay/continue the same command
- same key + different hash: return conflict
- an in-progress or uncertain command is not submitted as a new operation
- final HTTP responses are stored for deterministic replay

Commands that cross Oracle and TigerBeetle use the operation WAL defined in the
Provider handoff. Initial WAL operations include synchronous provider-user
onboarding, credit grant, and full remaining-credit return.

Core recovery principles:

- persist intent and deterministic external IDs before calling TigerBeetle
- treat dependency timeout/disconnect as uncertain, not failed
- resolve uncertainty by exact TigerBeetle account/transfer lookup
- after a verified ledger effect, finalize forward; never silently compensate
- create mandatory Kafka events through the Oracle outbox
- republish immutable event IDs until broker acknowledgement or dead-letter
- use bounded worker leases, backoff, alerts, and operator replay/recovery tools
- never rely on a client retry as the only recovery mechanism

Oracle-only commands do not need a financial WAL. They still commit state,
audit, version changes, and outbox events atomically.

The detailed state machine, full-balance optimistic guard, failure matrix, and
HTTP `200/201/202` semantics are in `WURZBURG_PROVIDER_HANDOFF.md`.

## 11. Card Mutation And Runtime Consistency

Any command that can change an active card's runtime capacity/order/state uses:

```text
Lock-CP:{card_number}
```

System-wide rules:

- acquire atomically with an ownership token and bounded lease
- if already locked, return `409 CARD_PROFILE_LOCKED` before business effects
- if Dragonfly is unavailable before lock/invalidation, fail without mutation
- invalidate stale CP before the external/business effect
- release locally only when failure occurred before a durable effect and token
  ownership is verified
- after a durable effect, retain/renew the lock until Wolfsburg materializes the
  corresponding version and releases it
- never use unconditional lock deletion
- an expired lease does not authorize an old profile to overwrite a newer
  `card_state_version`

Provider-user balance changes do not alter cardholder funding order. A provider
with zero available balance remains in the ordered source list with zero
capacity until the cardholder changes the relationship/order.

## 12. MinIO And File Workflows

MinIO stores binary/large artifacts rather than Oracle:

- bank card-issuance request and result CSV files
- original batch import files
- row-level batch result/error files
- exports and generated transaction reports

Oracle stores job status, requester/audit context, filters, object keys,
checksums, row counts, retention/expiration, and safe error summaries.

Existing-card single-user onboarding is synchronous. A new-card instruction is
asynchronous only because physical bank issuance must complete first; the
provider receives a durable issuance request ID. Bulk onboarding is
asynchronous at the file-job boundary, and every row executes the same
idempotent/recoverable onboarding contract. Report generation is asynchronous.

Card-issuance objects use UUID-only path components. Accepted bank result files
are checksum-addressed and immutable. Oracle stores object key, SHA-256,
retention, row-set membership, and processing state; download/upload APIs
recheck platform scope. PAN, national ID, names, delivery data, CSV bodies, and
object keys are prohibited from logs, spans, metrics, and audit snapshots.

Downloads require authorization re-check and use either short-lived pre-signed
URLs or controlled streaming. Object keys, bucket names, credentials, and signed
URLs must not be exposed in logs/traces/audit snapshots.

File format, size, retention, and parser/report options are validated business
configuration. Infrastructure endpoints and MinIO credentials remain system
configuration/secrets.

## 13. API And WSO2 Principles

All public Wurzburg APIs are reached through WSO2 over the trusted mTLS path.
Wurzburg does not reuse Nuremberg's CMS signature middleware.

Cross-API rules:

- thin Axum handlers and explicit request/response DTOs
- application-service ownership of business rules and transactions
- generated OpenAPI kept in sync with actual routes and headers
- standard HTTP status codes
- centralized `WurzburgResultCode` tuples:
  `(rs_code, symbolic code, default message, HTTP status)`
- structured error body with `rs_code`, `code`, `message`, and `details`
- no handler/service invents ad hoc codes or error strings
- every mutation is idempotent and audited
- provider/cardholder path/body identities cannot override trusted JWT claims
- provider/admin/cardholder routes and scopes remain explicit
- secrets are returned only by dedicated authorized endpoints
- collection APIs use bounded cursor pagination and allowlisted filters/sorts

WSO2 does not automatically retry mutations. Gateway timeout does not prove a
command failed; the caller resolves it with the same idempotency key or the
operation-status API.

The exact JWT/header contract, gateway-owned result codes, role/scope matrix,
timeouts, rate limits, and ESB acceptance tests are in
`WURZBURG_WSO2_HANDOFF.md`.

## 14. OpenTelemetry And Distributed Tracing

OpenTelemetry is required across every synchronous and asynchronous path.

Trace path:

```text
client -> WSO2 -> Wurzburg -> dependencies/outbox
                              |
                              v
                       internal Kafka headers
                              |
                              v
                    workers / Wolfsburg / Nuremberg
                              |
                              v
                    OTel Collector -> Tempo
```

Instrumentation requirements:

- WSO2 creates/continues W3C Trace Context and forwards canonical
  `traceparent`/allowlisted `tracestate` to Wurzburg.
- Axum middleware creates the Wurzburg server span and records route template,
  method, status, centralized result code, service version, and environment.
- Child spans cover validation, authorization, idempotency, Oracle,
  TigerBeetle, Dragonfly, Kafka/outbox, MinIO, provisioning, batch rows, WAL,
  recovery, and materialization receipts.
- Internal Kafka producers inject W3C context into headers. Consumers extract
  it and create consumer/processing spans; retries/replays link to the original
  event context without changing event identity.
- Background work started after the request persists trace context or a span
  link in the durable job/outbox record.
- Provider-facing Kafka messages explicitly exclude `traceparent`,
  `tracestate`, `baggage`, internal correlation IDs, and all OTel details.

The preferred production topology exports OTLP from services to an
OpenTelemetry Collector. The Collector performs batching, retry, resource
normalization, redaction, and sampling policy before exporting traces to Tempo.
Services should not depend directly on Tempo availability.

Tempo stores distributed traces. It is not the metrics or business-audit store.
OTel metrics are exported through the configured metrics pipeline; immutable
business audit remains in Oracle.

### Trace And Metric Safety

Never emit:

- raw/full PAN or national ID
- names, contacts, request/response bodies, unrestricted JSON metadata
- JWT/API tokens, Kafka passwords/certificates, database/MinIO secrets
- Dragonfly profile payloads, signed URLs, SQL bind values

Trace attributes may include safe UUIDs, masked card numbers, operation/event/
profile IDs, account categories, result codes, dependency outcome, and retry
count. High-cardinality identities belong only in sampled traces/logs, never as
metric labels.

Metrics include HTTP/dependency latency and results, pool saturation, WAL age,
outbox lag/dead letters, lock conflicts/age, Kafka consumer lag, provisioning
age, batch outcomes, Dragonfly materialization delay, and recovery backlog.

Parent-based sampling is used at service entry; production collector policy
must retain errors, unusually slow traces, and recovery paths at a higher rate
without recording prohibited data.

## 15. Configuration And Secrets

System configuration is file/environment/secret-store based:

- Oracle DSN/pool/timeouts and migration mode
- TigerBeetle cluster/replica/ledger/account-code configuration
- Dragonfly endpoints, TLS/auth, pool/timeouts, lock leases
- Kafka bootstrap/security/admin/topic/consumer settings
- MinIO endpoints, buckets, credentials, and TLS
- WSO2/JWKS/mTLS trust settings
- OTLP Collector endpoint, resource attributes, sampling, and Tempo pipeline
- worker concurrency, leases, retries, backoff, and alert thresholds

Business configuration is versioned/audited in Oracle:

- provider operational profiles and event-delivery gates
- automatic, Oracle-locked activation of scheduled provider operational
  profiles on every replica, with command/read-path promotion as a safety net
- card policies, fee profiles, and range controls
- import/report formats, limits, and retention
- operational business thresholds that may change without redeployment

Secrets and infrastructure endpoints never belong in business configuration or
API metadata.

## 16. Health, Readiness, And Shutdown

Required operational endpoints:

- liveness: process/event-loop is alive; no dependency calls
- readiness: configuration and Oracle schema are valid and mandatory workers
  can start; returns false during migration/startup drain
- dependency health: authorized/internal details for Oracle, TigerBeetle,
  Dragonfly, Kafka, MinIO, and OTel exporter

A transient optional dependency outage should not make all read-only APIs
unavailable. Each route fails with the centralized dependency result when its
required dependency cannot satisfy the command. Worker/dependency health still
drives alerts and deployment policy.

Graceful shutdown:

- stop accepting new requests/jobs
- handle both `SIGINT` and Kubernetes `SIGTERM` through one process-wide
  shutdown signal shared by the main API listener and the Swagger listener
- drain in-flight Oracle/TigerBeetle commands to configured deadlines
- stop claiming new WAL/outbox/batch rows and release ordinary worker leases
- do not release a post-effect card lock that must remain for Wolfsburg
- flush telemetry best-effort without blocking financial recovery
- leave every uncertain command represented durably for the next instance

`server.graceful_shutdown_timeout_ms` bounds HTTP and worker drain.
`server.telemetry_shutdown_timeout_ms` independently bounds the final OTel
flush. Reaching either deadline is logged as a structured operational failure;
the process then terminates so Kubernetes replacement cannot be blocked by a
stuck connection, dependency, or exporter.

## 17. Security And Data Protection

- Public access is WSO2-only; backend trust uses mTLS plus verified JWT.
- Authorization is enforced at WSO2 and repeated against Oracle ownership in
  Wurzburg.
- PAN and national ID are normalized only at validated domain boundaries and
  masked/redacted everywhere else.
- Kafka provider credentials are envelope-encrypted in Oracle and exposed only
  through dedicated authorized endpoints.
- Database, Dragonfly, Kafka, TigerBeetle, MinIO, TLS, and OTel secrets are never
  logged or returned in generic errors.
- Audit rows contain trusted actor/network context and immutable before/after
  business snapshots, not secrets or live balances.
- Platform admins query immutable audit evidence through
  `GET /api/v1/admin/audit-logs` using allowlisted exact filters and opaque,
  filter-bound keyset pagination. Audit reads require `platform.audit:read`.
- Before/after snapshots preserve safe business-state evidence but centrally
  replace PAN, national ID, names, contacts, and unrestricted metadata with
  `[REDACTED]` on write and again on read for legacy-row protection.
- Provider-facing events are a strict public allowlist with versioned schemas;
  internal events cannot be subscribed accidentally.
- Dependency errors are mapped to stable safe results; stack traces and raw
  driver/broker errors remain internal.

## 18. Production Engineering Guidelines

- No SQL in handlers or domain models.
- No financial balance columns in Oracle.
- Do not execute infrastructure shell commands from the service. Use typed,
  authenticated dependency adapters; isolate any unavoidable blocking driver
  work from async executor threads.
- External calls have explicit deadlines; retries occur only where idempotency
  and deterministic identities make them safe.
- Oracle transactions are short and never held open while waiting for network
  dependencies unless a focused protocol explicitly proves it safe.
- Outbox/inbox/WAL workers use leases, bounded concurrency, backoff, and
  `SKIP LOCKED`.
- Every state transition is explicit, constrained, audited, and tested.
- Immutable profiles/events/facts are replaced by new versions, never edited
  after operational use.
- API DTOs reject unknown/invalid enum values and malformed money/identity data
  according to the focused contract.
- All timestamps use an injected clock in business logic and UTC persistence;
  named timezones are explicit policy inputs.
- No direct Dragonfly write is permitted outside the Wolfsburg materializer,
  except lock acquisition/invalidation operations assigned by the protocol.
- No service infers a ledger effect from an API timeout, Oracle state,
  Dragonfly state, or Kafka event alone.

## 19. Testing Strategy

Testing scales by boundary:

- pure domain tests for validation, lifecycle, policy, limit, and authorization
- Oracle integration tests for transactions, constraints, locking, migrations,
  audit, idempotency, WAL, outbox, inbox, and worker leasing
- TigerBeetle integration tests for account flags, deterministic creation/
  transfer lookup, balances, usage counters, and failure recovery
- Dragonfly integration tests for lock ownership, invalidation, versioned
  profile materialization, fail-closed reads, and stale-write rejection
- Kafka integration tests for partition keys, at-least-once duplicates, inbox
  deduplication, outbox retry, schemas, provider ACLs, and suppression/replay
- MinIO tests for bounded upload/download, checksums, authorization, immutable
  result files, interrupted requests, and retention. Streaming becomes required
  before any configured file limit can exceed the process memory budget.
- WSO2 contract tests for mTLS/JWT/header spoofing/scopes/idempotency/errors
- OTel tests that prove trace continuation across HTTP/internal Kafka/workers
  and prove prohibited fields never reach traces/provider events
- compatibility fixtures shared with Nuremberg and future Wolfsburg

Scenario tests document initial Oracle facts, TigerBeetle accounts/transfers,
Dragonfly keys/locks, Kafka records, expected traces, and final business proof.
Failure tests assert both sides: no duplicate/unintended ledger movement and no
stale runtime profile becoming readable by Nuremberg.

Critical failure-injection points include process loss before/after TigerBeetle,
Oracle commit loss, Kafka acknowledgement uncertainty, Dragonfly outage, stale
lock lease, duplicate/out-of-order event delivery, missing materialization
receipt, MinIO interruption, and OTel/Tempo unavailability.

## 20. Non-Negotiable Architectural Decisions

- Oracle is the sole relational business database.
- TigerBeetle is the sole source of financial balances/counters and transfer
  proof.
- Dragonfly is a replaceable Redis-compatible runtime projection store, never
  financial or audit proof.
- Nuremberg never queries Oracle.
- Wolfsburg is the sole writer of replacement CP/CPOL/CRCTL/FEE profiles.
- Wurzburg owns canonical business facts and durable materialization requests.
- Every mutating public API is idempotent and audited.
- Cross-Oracle/TigerBeetle commands use deterministic identities and WAL
  recovery.
- State plus outbox event is committed atomically in Oracle.
- Kafka is at least once; consumers deduplicate immutable event IDs.
- Same-card ordering and `card_state_version` protect runtime consistency.
- Post-effect card locks are released only after Wolfsburg materialization.
- Provider-user credit return is full remaining balance with a live
  TigerBeetle optimistic guard; arbitrary partial return is unsupported.
- Existing-card onboarding is synchronous. New physical issuance returns `202`
  and completes through the bank MinIO batch workflow; bulk processing is
  asynchronous at the file-job boundary.
- Provider-facing events use the shared public contract and never contain
  internal/secret/OTel data.
- WSO2 is the public trust boundary, but Wurzburg repeats resource/business
  authorization.
- OTel tracing covers every dependency and asynchronous transition; Tempo is
  trace storage, not business truth.
- Sensitive identifiers, secrets, payloads, and high-cardinality values are
  excluded from unsafe telemetry and metrics.

These decisions are the baseline for production implementation. Detailed
domain behavior must be implemented from the focused handoffs without
reintroducing superseded PoC models or duplicated sources of truth.
