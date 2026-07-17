# Wurzburg Card Range And Policy Handoff

This document records the updated design decision reached after reviewing the
original Wurzburg service handoff and refining the card/card-range model.

It is intentionally narrower than `WURZBURG_SERVICE_HANDOFF.md`. Its purpose is
to preserve the current agreement for the next implementation step and to make
the required Nuremberg contract change explicit.

Production runtime storage is Dragonfly. References to Redis keys, commands, or
Redis-compatible clients in this document describe the Dragonfly protocol
contract; they do not introduce a separate Redis deployment.

## 1. Scope Of This Step

The next implementation step focuses on the card-range and policy foundation.

This step must answer:

- Which card ranges exist?
- Is each range `SingleProvider` or `MultiProvider`?
- Which providers are allowed behind each range?
- Which policy profile is active for each range?
- Does the platform enforce withdrawal amount/count limits, or is the external
  CMS authoritative for those limits?
- Which Redis card-policy key should Nuremberg read?

This step does not create or publish `CP:{card_number}` yet. Card profiles are
created later, after actual cards, users, provider funding sources, and
provider-user funding/policy-usage accounts exist.

## 2. Core Design Decisions

### Card Ranges Are First-Class

Card ranges are the root of the card model.

Each range defines:

- its start and end card number
- its funding mode
- lifecycle status
- operational metadata

The range does not directly store provider ownership with a nullable
`owner_provider_id`. Provider participation is represented uniformly in a join
table.

### Provider Participation Is Range-Level Eligibility

The range-provider table only answers:

```text
Which providers are allowed behind this card range?
```

It does not contain cardholder funding priority.

Cardholder funding priority is card-specific and belongs later in the card
profile/funding-source model that produces `CP:{card_number}`.

### No Range-Level Provider Priority

Do not add `priority` to `card_range_providers`.

A range can allow multiple providers. A specific cardholder later decides the
order in which Nuremberg consumes the providers attached to that exact card.

### No Nullable Owner Provider On Card Ranges

Do not add `owner_provider_id` to `card_ranges`.

For `SingleProvider` ranges, the single active row in `card_range_providers`
represents the only allowed provider for that range.

For `MultiProvider` ranges, the active rows in `card_range_providers` represent
the allowed provider set.

### No Relationship Type In Card Range Providers

Do not add `relationship_type: Owner | AllowedFundingSource`.

The funding mode on `card_ranges` already determines how provider participation
must be interpreted:

- `SingleProvider`: exactly one active provider is allowed.
- `MultiProvider`: one or more active providers are allowed.

### Withdrawal Limit Authority Is Immutable Range Structure

Each card range selects exactly one withdrawal-limit authority when the range is
created:

```text
PLATFORM
CMS
```

- `PLATFORM`: Nuremberg enforces per-transaction limits and enabled
  daily/weekly/monthly/yearly amount/count windows.
- `CMS`: the external CMS is authoritative for amount/count limits. Nuremberg
  does not reject Confirm using Wurzburg withdrawal limits and does not read or
  reserve TigerBeetle policy-usage accounts for that card.

Authority applies to the complete withdrawal-limit set: per-transaction
minimum/maximum plus all amount/count windows. Mixed ownership is intentionally
unsupported because it makes rejection and rollback responsibility ambiguous.

The authority is stored once on `card_ranges` and is immutable after creation.
It is not duplicated inside policy JSON. Supporting an authority change would
require a new card range; there is no `CMS <-> PLATFORM` migration workflow.

For `PLATFORM`, the limit calendar basis is also selected at range creation and
is immutable. Numeric and count thresholds remain versioned policy terms and
may change without replacing card usage accounts.

Initial policy creation remains a separate idempotent command. This keeps range
identity/structure separate from immutable policy-version lifecycle. A DRAFT
range may exist without a policy, but activation requires a materialized active
policy.

`CMS` authority affects only withdrawal amount/count enforcement. Card/range
status, funding availability, provider eligibility, fee policy, idempotency,
accounting, authorization, and fraud/security controls remain platform-owned.

## 3. Database Model

### `card_ranges`

```text
card_range_id RAW(16) primary key
start_card_number string, normalized
end_card_number string, normalized
funding_mode string: SINGLE_PROVIDER | MULTI_PROVIDER
withdrawal_limit_authority string: PLATFORM | CMS
limit_calendar_json JSON nullable
status string: DRAFT | ACTIVE | SUSPENDED
issuance_enabled NUMBER(1) default 1
cms_operation_mode string: FULL | BALANCE_ONLY | BLOCKED
operational_version number
metadata_json JSON
created_by_subject string
updated_by_subject string
created_at timestamp with time zone
updated_at timestamp with time zone
```

Rules:

- A card number and each range boundary is exactly 16 ASCII digits: four groups
  of four digits in display form, persisted without separators.
- The first digit must be `1` through `9`. Zeros are valid in every later
  position, including the suffix of either boundary.
- Range boundaries are inclusive: both `start_card_number` and
  `end_card_number` belong to the range.
- No Luhn/check-digit validation is applied to range boundaries. Individual PAN
  validation may be added later at the bank/card-assignment boundary.
- Card ranges must not overlap in any lifecycle state. A suspended range still
  reserves its complete PAN interval.
- `funding_mode` is the source of truth for single-provider vs multi-provider
  behavior.
- `withdrawal_limit_authority` is the source of truth for `PLATFORM` vs `CMS`
  amount/count enforcement.
- `PLATFORM` requires a valid `limit_calendar_json`; `CMS` requires it to be
  null.
- While `DRAFT`, range structure may be corrected. After first activation,
  start/end boundaries, funding mode, authority, and calendar are immutable.
- Activation and reactivation revalidate normalized range details, a
  materialized active policy, and provider eligibility: exactly one ACTIVE
  provider for `SINGLE_PROVIDER`, at least one for `MULTI_PROVIDER`.
- `ACTIVE` ranges can be used for card assignment.
- `issuance_enabled` controls assignment of new cards independently from CMS
  runtime access.
- `cms_operation_mode = FULL` permits Balance, Confirm, and Rollback.
- `cms_operation_mode = BALANCE_ONLY` permits Balance and rejects Confirm and
  Rollback.
- `cms_operation_mode = BLOCKED` rejects Balance, Confirm, and Rollback.
- `SUSPENDED` is the umbrella emergency override: it blocks issuance and every
  CMS operation regardless of the two stored operational controls.
- Every operational-control change increments `operational_version`, requires a
  reason, and writes an immutable audit snapshot.

Recommended lookup indexes:

```text
card_ranges(status, start_card_number, end_card_number)
card_ranges(funding_mode, status)
card_ranges(withdrawal_limit_authority, status)
```

Overlap validation query shape:

```sql
WHERE start_card_number <= :new_end
  AND end_card_number >= :new_start
```

The operational controls support all required combinations. For example,
issuance can be disabled while existing cards remain fully operational, or
issuance can remain enabled while CMS operations are temporarily blocked.

### `card_range_providers`

```text
card_range_id RAW(16)
provider_id RAW(16)
status string: ACTIVE | SUSPENDED
metadata_json JSON
created_by_subject string
updated_by_subject string
created_at timestamp with time zone
updated_at timestamp with time zone

primary key (card_range_id, provider_id)
```

Rules:

- This table is provider eligibility only.
- It does not contain cardholder priority.
- It does not contain provider relationship type.
- Each provider may have at most one `ACTIVE` card-range attachment across all
  ranges. Suspended historical relationships do not consume this slot.
- Only a provider in lifecycle state `ACTIVE` may receive an ACTIVE range
  attachment. `READY` means provisioning is complete but business operation has
  not been authorized yet.
- For `SINGLE_PROVIDER`, exactly one active provider is required before range
  activation.
- For `MULTI_PROVIDER`, one or more active providers may be attached.

Recommended indexes:

```text
primary key (card_range_id, provider_id)
card_range_providers(card_range_id, status)
card_range_providers(provider_id, status)
unique(CASE WHEN status = 'ACTIVE' THEN provider_id END)
```

The final line is implemented as an Oracle function-based unique index and must
be present in the clean baseline DDL.

Every eligibility transition caused by provider creation, provider lifecycle
changes, provider deletion, or provider operational controls must write an
immutable before/after snapshot to the common audit log in the same Oracle
transaction as the current-state change. A second relationship-history table is
not required unless audit query volume later proves that the common audit log is
insufficient.

There is no standalone API whose business purpose is to remove a provider from
behind a card range. Provider range eligibility is selected during provider
creation or provider lifecycle workflows. A provider may be deleted only while
it has no financial activity or transaction history. That deletion is a provider
soft delete and removes the provider from active range eligibility without
physically deleting eligibility history. Once a provider has any transaction
history, it cannot be deleted; only lifecycle or operational suspension flows
may stop new use. Any eligibility removal/suspension preserves ledger accounts
and financial history, requests affected CP refresh, and publishes the required
range-control update. If a single-provider range, or a multi-provider range with
no remaining ACTIVE provider, loses its final active provider eligibility,
Wurzburg also suspends the range.

### Trusted Audit Actor Context

WSO2 owns human/service authentication. Wurzburg does not need a local table of
WSO2 users, but it must receive cryptographically trusted actor context and
persist it with every audit record.

`WURZBURG_WSO2_HANDOFF.md` is the authoritative gateway/ESB contract. This
section defines only the card-range audit facts consumed from that contract.

Preferred identity transport:

- `Authorization: Bearer <jwt>`, or `X-JWT-Assertion` when WSO2 replaces the
  original authorization header
- JWT `sub`: immutable WSO2 user/service subject
- JWT `azp` or `client_id`: calling application
- JWT roles/scopes and optional `provider_id`
- `X-Correlation-Id` and optional `X-Request-Id`
- `X-WSO2-Client-IP`: canonical client IP resolved and overwritten by WSO2

WSO2 must strip caller-supplied `X-WSO2-Client-IP` and identity headers and write
canonical values. Wurzburg trusts them only from the configured WSO2
network/mTLS boundary. `X-Forwarded-For` from an arbitrary client is never
trusted or persisted as the audit source IP.

Each audit row stores at least `actor_subject`, `actor_client_id`, optional
`actor_provider_id`, source IP, JWT issuer, correlation ID, request ID, reason,
and before/after JSON snapshots. IP is supporting evidence, not actor identity.

### `card_policy_profiles`

Each policy profile is one immutable version for exactly one card range and is
not reusable across ranges. Its lifecycle is stored on the same row.

```text
card_policy_profile_id RAW(16) primary key
card_range_id RAW(16) not null
profile_json JSON not null
status string: SCHEDULED | PUBLISHING | ACTIVE | SUPERSEDED | CANCELLED | PUBLICATION_FAILED
version number not null
effective_at timestamp with time zone
superseded_by_profile_id RAW(16) nullable
publication_operation_id RAW(16) not null
created_by_subject string
change_reason string not null
activated_at timestamp with time zone nullable
superseded_at timestamp with time zone nullable
cancelled_by_subject string nullable
cancelled_at timestamp with time zone nullable
cancel_reason string nullable
created_at timestamp with time zone
status_updated_at timestamp with time zone
```

Do not duplicate `funding_mode` as a policy scope field. The funding mode belongs
to `card_ranges`. Do not duplicate authority or calendar in `profile_json`;
those immutable structural facts also belong to `card_ranges`.

For a `PLATFORM` range, the profile body contains only versioned thresholds:

```json
{
  "withdrawal_limits": {
    "per_transaction_min_amount": null,
    "per_transaction_max_amount": null,
    "daily": {
      "max_amount": null,
      "max_count": null
    },
    "weekly": {
      "max_amount": null,
      "max_count": null
    },
    "monthly": {
      "max_amount": null,
      "max_count": null
    },
    "yearly": {
      "max_amount": null,
      "max_count": null
    }
  }
}
```

For a `CMS` range, the profile body is explicit and contains no platform limit
terms:

```json
{
  "withdrawal_limits": null
}
```

Validation rules:

- `PLATFORM` requires a non-null `withdrawal_limits` object. Individual limits
  may be null/disabled.
- `CMS` requires `withdrawal_limits = null`.
- Every submitted profile is immutable from creation. Corrections create a new
  profile ID; no status permits in-place policy-term edits.
- `effective_at <= now` means publish as soon as the durable worker can process
  it. A future value remains `SCHEDULED` until its due time.
- At most one non-terminal candidate (`SCHEDULED`, `PUBLISHING`, or
  `PUBLICATION_FAILED`) may exist per range. The API does not queue a calendar
  of future profiles.
- A second create request conflicts until the existing candidate is activated
  or explicitly cancelled. Replacement is therefore a deliberate cancel-then-
  create workflow.
- Cancellation is allowed only before activation, requires a reason, and never
  changes the current active policy. If no active policy exists, the range
  remains non-operational.
- The old ACTIVE profile remains active while its replacement is scheduled,
  publishing, or publication-failed.
- After Wolfsburg confirms materialization, one Oracle transaction marks the
  candidate ACTIVE and the previous profile SUPERSEDED.
- Old and cancelled rows remain permanently available for audit and FundingPlan
  traceability.

Required constraints/indexes:

```text
unique(card_range_id, version)
unique(CASE WHEN status = 'ACTIVE' THEN card_range_id END)
unique(CASE WHEN status IN ('SCHEDULED', 'PUBLISHING', 'PUBLICATION_FAILED') THEN card_range_id END)
index(status, effective_at)
```

### Policy Usage Account Provisioning

- Wurzburg creates all eight card-scoped TigerBeetle usage accounts whenever a
  provider assigns a card to a user, regardless of range authority or currently
  enabled windows.
- CP always contains the complete usage-account set. Nuremberg uses it only for
  `PLATFORM` authority and ignores it for `CMS` authority.
- Usage accounts measure current consumption; they do not store configured
  limit values. Their `debits_pending` naturally decreases as TigerBeetle
  pending reservations expire.
- Changing amount/count thresholds never resets, zeroes, or replaces usage
  accounts. The new policy compares its thresholds against the existing live
  usage in the same immutable calendar windows.
- Raising a limit makes additional capacity available immediately after policy
  activation. Lowering a limit may block new Confirms until existing usage
  expires below the new threshold; it never erases consumed usage.
- Authority and calendar cannot change, so no high-fanout account migration is
  required.

### `runtime_materialization_receipts`

Wurzburg durably consumes Wolfsburg acknowledgements through an idempotent inbox
table shared by CPOL, CRCTL, and later runtime profiles:

```text
runtime_materialization_receipt_id RAW(16) primary key
receipt_event_id RAW(16) not null unique
operation_id RAW(16) not null
profile_type string: CPOL | CRCTL | CP | FEE
aggregate_id RAW(16) not null
profile_id RAW(16) nullable
materialized_version number not null
redis_key string not null
materialized_at timestamp with time zone not null
received_at timestamp with time zone not null
```

`unique(operation_id, profile_type, materialized_version)` makes duplicate Kafka
delivery a successful replay. Wurzburg validates aggregate/profile/version
against the pending operation before changing Oracle lifecycle state.

## 4. Runtime Policy Contract For Wolfsburg/Nuremberg

Wurzburg owns the Oracle policy profile and emits a durable policy-publication
request. Wolfsburg is the sole Redis materializer. The exact Redis serialization
belongs in the future Wolfsburg handoff; the semantic contract below is the
required input for that work.

Card policies are range-scoped because the system supports multiple
single-provider and multi-provider ranges.

### Final Redis Keys

```text
CPOL:SingleProvider:{card_range_id}
CPOL:MultiProvider:{card_range_id}
```

The key is derived from:

- `funding_mode`
- `card_range_id`

### Range Runtime Control

Emergency range controls must not require rewriting every card profile. Wolfsburg
therefore also materializes:

```text
CRCTL:{card_range_id}
```

Value:

```json
{
  "card_range_id": "uuid",
  "range_status": "ACTIVE",
  "issuance_enabled": false,
  "cms_operation_mode": "FULL",
  "operational_version": 7,
  "eligible_provider_ids": ["provider-uuid"]
}
```

Nuremberg reads `CRCTL` without an in-memory cache on every Balance, Confirm,
and Rollback. It fails closed when the key is missing or invalid. Balance and
Confirm also ignore/reject CP funding sources whose provider is absent from the
current eligible-provider list. Rollback of an already committed FundingPlan
uses its immutable ledger facts, but the range CMS mode may still reject the
external Rollback request when the platform has intentionally blocked it.

Wurzburg card issuance checks the Oracle controls directly. A range-control or
provider-eligibility mutation emits a durable range-control publication event.
The mutating API returns `202` until Wolfsburg confirms the new `CRCTL` version.

### Redis Value

Example:

```json
{
  "id": "card_policy_profile_id",
  "card_range_id": "uuid",
  "funding_mode": "SingleProvider",
  "version": 3,
  "effective_at": "2026-07-16T00:00:00Z",
  "withdrawal_limit_authority": "PLATFORM",
  "withdrawal_limits": {
    "per_transaction_min_amount": null,
    "per_transaction_max_amount": null,
    "daily": {
      "max_amount": null,
      "max_count": null
    },
    "weekly": {
      "max_amount": null,
      "max_count": null
    },
    "monthly": {
      "max_amount": null,
      "max_count": null
    },
    "yearly": {
      "max_amount": null,
      "max_count": null
    }
  },
  "calendar": {
    "timezone": "Asia/Tehran",
    "week_starts_on": "Saturday",
    "window_mode": "Calendar"
  }
}
```

CMS-authoritative Redis value:

```json
{
  "id": "card_policy_profile_id",
  "card_range_id": "uuid",
  "funding_mode": "MultiProvider",
  "version": 1,
  "effective_at": "2026-07-16T00:00:00Z",
  "withdrawal_limit_authority": "CMS",
  "withdrawal_limits": null,
  "calendar": null
}
```

Rules:

- The Redis value contains the immutable `card_policy_profile_id`.
- Wolfsburg composes funding mode, authority, and immutable calendar from the
  card range with thresholds from the selected policy profile.
- Nuremberg stores that exact policy ID in FundingPlan.
- Activating policy terms causes Wurzburg to emit
  `CARD_POLICY_PROFILE_PUBLISH_REQUESTED`; Wolfsburg writes the new policy ID to
  the same range-scoped Redis key.
- Existing `CP:{card_number}` objects are not rewritten when policy thresholds
  change.
- Nuremberg does not use an in-memory CPOL cache. Each Confirm resolves the
  range-scoped key from Redis, so a successful key replacement is visible to the
  next Confirm without TTL uncertainty.

### Policy Activation And Publication

1. `POST .../policy` stores the immutable candidate and durable outbox operation,
   then returns `202 Accepted` with `operation_id`, profile ID, status, and
   `effective_at`.
2. For future-dated profiles, the worker waits until `effective_at`. For
   immediate profiles, publication starts as soon as the Oracle transaction
   commits.
3. The previous policy remains ACTIVE while the candidate is SCHEDULED,
   PUBLISHING, or PUBLICATION_FAILED. If no previous policy exists, the range is
   not operational.
4. Wolfsburg writes the complete candidate CPOL value and emits a durable
   materialization receipt containing operation ID, profile ID, Redis key,
   version, and timestamp.
5. Only after Wurzburg consumes that receipt does it mark the candidate ACTIVE
   and the previous profile SUPERSEDED in one Oracle transaction. The Redis write
   is the runtime switch; the receipt finalizes the control-plane state.
6. `DEAD_LETTER` never changes the active policy. The candidate becomes
   PUBLICATION_FAILED. A platform admin may retry publication or cancel the
   candidate with a mandatory reason; automatic rollback is unnecessary because
   the candidate never became active.
7. The operation-status API and admin panel expose scheduled, publication,
   materialization, and activation states. No fixed client-facing propagation
   SLA is required, but operational metrics and alerts must measure lateness from
   `effective_at`.

## 5. `CP:{card_number}` Relationship

`CP:{card_number}` is not part of this implementation step.

Later, when card profiles exist, `CP:{card_number}` must include:

```json
{
  "card_range_id": "uuid",
  "funding_mode": "SingleProvider"
}
```

It does not need a `policy_key` field.

`policy_usage_accounts` is always present with all eight account IDs. Nuremberg
uses the accounts only when the range authority is `PLATFORM` and ignores them
when authority is `CMS`.

Nuremberg derives the card policy key deterministically:

```text
SingleProvider -> CPOL:SingleProvider:{card_range_id}
MultiProvider  -> CPOL:MultiProvider:{card_range_id}
```

This avoids duplicating derived data in CP and keeps policy lookup stable.

## 6. API Slice For This Step

### Card Ranges

```text
POST   /api/v1/card-ranges
GET    /api/v1/card-ranges/{card_range_id}
GET    /api/v1/card-ranges
PATCH  /api/v1/card-ranges/{card_range_id}
POST   /api/v1/card-ranges/{card_range_id}/activate
POST   /api/v1/card-ranges/{card_range_id}/suspend
PUT    /api/v1/card-ranges/{card_range_id}/operational-controls
```

Create request contains the immutable range structure and initial operational
controls:

```json
{
  "start_card_number": "6219861000000000",
  "end_card_number": "6219861000000999",
  "funding_mode": "SingleProvider",
  "withdrawal_limit_authority": "PLATFORM",
  "limit_calendar": {
    "timezone": "Asia/Tehran",
    "week_starts_on": "Saturday",
    "window_mode": "Calendar"
  },
  "issuance_enabled": true,
  "cms_operation_mode": "FULL",
  "metadata": {}
}
```

For `CMS`, `limit_calendar` must be null.

Operational-control request:

```json
{
  "issuance_enabled": false,
  "cms_operation_mode": "BALANCE_ONLY",
  "reason": "temporary risk control"
}
```

### Range Providers

```text
GET    /api/v1/card-ranges/{card_range_id}/providers
GET    /api/v1/providers/{provider_id}/card-ranges
```

These endpoints are read-only CARD views over provider range eligibility.
Eligibility writes are owned by provider creation and provider lifecycle
workflows. Wurzburg must not expose a standalone CARD API that detaches a
provider from a range.

### Range Policies

```text
POST   /api/v1/card-ranges/{card_range_id}/policy
GET    /api/v1/card-ranges/{card_range_id}/policy
GET    /api/v1/card-ranges/{card_range_id}/policies
GET    /api/v1/card-ranges/{card_range_id}/policies/{card_policy_profile_id}
POST   /api/v1/card-ranges/{card_range_id}/policies/{card_policy_profile_id}/cancel
POST   /api/v1/card-ranges/{card_range_id}/policies/{card_policy_profile_id}/retry-publication
```

`GET .../policy` returns the complete ACTIVE profile composed with immutable
range authority/calendar. `GET .../policies` returns complete historical and
candidate profiles with lifecycle/publication status; it is paginated newest
version first.

Platform-authoritative request:

```json
{
  "effective_at": "2026-07-16T00:00:00Z",
  "reason": "new withdrawal thresholds",
  "withdrawal_limits": {
    "per_transaction_min_amount": null,
    "per_transaction_max_amount": null,
    "daily": null,
    "weekly": null,
    "monthly": null,
    "yearly": null
  }
}
```

CMS-authoritative request:

```json
{
  "effective_at": "2026-07-16T00:00:00Z",
  "reason": "initial CMS-authoritative policy reference",
  "withdrawal_limits": null
}
```

Create response is always `202 Accepted`:

```json
{
  "operation_id": "uuid",
  "card_policy_profile_id": "uuid",
  "card_range_id": "uuid",
  "version": 3,
  "status": "SCHEDULED",
  "publication_status": "PENDING",
  "effective_at": "2026-07-16T00:00:00Z"
}
```

Range activation remains a separate command. It succeeds only when the first
policy is already ACTIVE/materialized and all range/provider prerequisites pass.
`PATCH` never changes start/end boundaries, funding mode, authority, or calendar
after first activation. No migration API for those fields exists.

## 7. API Behavior Rules

- Every mutating API requires `Idempotency-Key`.
- Same idempotency key and same request hash returns the previous result.
- Same idempotency key and different request hash returns conflict.
- Structured API errors must be used.
- Handlers must stay thin.
- Business rules live in application services.
- Oracle is the only persistence target.
- Domain and service code must not depend on Oracle row types or SQL details.

Required centralized result codes include:

```text
INVALID_WITHDRAWAL_LIMIT_AUTHORITY
INVALID_CARD_RANGE_BOUNDARY
CARD_RANGE_IMMUTABLE_FIELD
CARD_RANGE_POLICY_REQUIRED
POLICY_USAGE_ACCOUNTS_NOT_READY
POLICY_CANDIDATE_ALREADY_EXISTS
POLICY_NOT_CANCELLABLE
POLICY_PUBLICATION_PENDING
POLICY_PUBLICATION_FAILED
RANGE_CONTROL_PUBLICATION_PENDING
RANGE_CONTROL_NOT_FOUND
RANGE_CONTROL_INVALID
RANGE_CMS_OPERATION_BLOCKED
RANGE_CMS_BALANCE_ONLY
CARD_POLICY_CONTRACT_INVALID
TRUSTED_ACTOR_CONTEXT_REQUIRED
```

Final structured error shape, matching the centralized source contract:

```json
{
  "error": {
    "rs_code": 6202,
    "code": "INVALID_CARD_RANGE",
    "message": "Card range is invalid",
    "details": {}
  }
}
```

Every API error must be created from `WurzburgResultCode`. Each enum variant
centrally owns one immutable tuple:

```text
(numeric rs_code, symbolic code, default message, HTTP status)
```

New card-range/policy result variants must receive stable, unique numeric
`rs_code` values before their handlers are implemented. Handlers and services
must not invent numeric codes, symbolic strings, messages, or HTTP mappings.
`details` carries structured field/resource context; it must not replace the
central result identity. A safe contextual message override is allowed only
through the centralized `ApiError` constructors and does not change `rs_code`,
symbolic `code`, or HTTP status.

## 8. Implementation Shape

Final module layout:

```text
src/domain/card_range.rs
src/domain/card_policy.rs

src/services/card_range_service.rs
src/services/card_policy_service.rs

src/api/dto/card_range.rs
src/api/dto/card_policy.rs
src/api/handlers/card_range.rs
src/api/handlers/card_policy.rs

src/db/oracle/card_range.rs
src/db/oracle/card_policy.rs
src/db/oracle/repository.rs
src/db/oracle/transaction.rs
```

Wurzburg is Oracle-only. Do not retain `CardRangeRepository`,
`CardPolicyRepository`, or the growing `AppRepository` super-trait merely as a
memory of the abandoned multi-database design.

Final dependency rules:

- Domain models and pure validation remain independent from Oracle and Axum.
- API handlers call application services and never execute SQL.
- Application services use the concrete `OracleRepository`/aggregate repository
  modules.
- Cross-table commands are exposed as explicit atomic Oracle operations; do not
  hide transaction boundaries behind generic CRUD traits.
- Oracle row mapping, SQL, locking, `SKIP LOCKED`, and transaction handling stay
  under `src/db/oracle`.
- Pure domain rules receive direct unit tests. Persistence/service workflows use
  Oracle integration tests rather than mock repository implementations.
- Traits remain appropriate for dependencies that genuinely need substitutable
  test/production implementations, such as Kafka publication, Redis runtime
  materialization clients, clocks, and TigerBeetle gateways. They are not a
  blanket requirement for every module.

`AppState` therefore owns `Arc<OracleRepository>` instead of
`Arc<dyn AppRepository>`. This is an intentional Oracle-only production design,
not permission for SQL to leak into handlers or domain code.

### Final Oracle Schema Baseline

The card-range/policy baseline contains only the final model:

```text
card_ranges
card_range_allocation_locks
card_range_providers
card_policy_profiles
runtime_materialization_receipts
integration_outbox shared with Provider slice
idempotency_records if not already present
```

The existing migrations are development drafts, not a compatibility boundary.
Rewrite `V002__card_ranges_and_policies.sql` as one clean final DDL migration
that creates the final columns, allocation-lock row/table, constraints,
function-based indexes, foreign keys, JSON checks, and audit/outbox relationships
directly. Remove the draft additive `V003__oracle_hardening.sql`; fold every
still-valid hardening rule into final `V002`. Do not add ALTER/backfill/
compatibility branches for the draft schema.

Rebuild the disposable Oracle schema through the configured force-rebuild path
after replacing the baseline. The migration checksum recorded by the rebuilt
schema must match the final file. Once the first production/staging baseline is
released, that migration becomes immutable and every later change requires a
new forward migration.

## 9. Nuremberg Update Required

Nuremberg must implement the final range-scoped contract:

```text
CPOL:SingleProvider:{card_range_id}
CPOL:MultiProvider:{card_range_id}
```

Required Nuremberg model direction:

```rust
pub struct CardPolicyProfile {
    pub id: Uuid,
    pub card_range_id: Uuid,
    pub funding_mode: FundingMode,
    pub withdrawal_limit_authority: WithdrawalLimitAuthority,
    pub version: u64,
    pub effective_at: DateTime<Utc>,
    pub withdrawal_limits: Option<WithdrawalLimits>,
    pub calendar: Option<LimitCalendarPolicy>,
}

pub enum WithdrawalLimitAuthority {
    Platform,
    Cms,
}

pub enum FundingMode {
    SingleProvider,
    MultiProvider,
}
```

Nuremberg behavior:

- `Platform`: validate per-transaction limits, read only enabled usage accounts,
  and reserve enabled amount/count windows in the linked TigerBeetle batch.
- `Cms`: skip all Wurzburg withdrawal-limit checks, usage-account reads, and
  usage reservations. CMS remains responsible for enforcing its limits before
  calling Confirm.
- CPOL is mandatory for both authorities. A missing or invalid policy fails
  closed; CMS authority is never inferred from absence.
- CP always carries all eight usage accounts. A PLATFORM policy with a missing
  required account fails closed with a contract/configuration result code.
- Nuremberg stores the resolved authority and policy snapshot in FundingPlan so
  Rollback/reconciliation never reinterpret the transaction using a newer
  policy.

When Nuremberg reads `CP:{card_number}`, it uses:

```text
card_range_id
funding_mode
```

to derive the policy key.

Nuremberg reads `CRCTL:{card_range_id}` for every CMS operation and enforces
range status, `cms_operation_mode`, operational version, and current provider
eligibility before using CP. Missing/invalid CRCTL fails closed.

Nuremberg must not keep CPOL or CRCTL in its current process-local business
cache. Confirm reads CPOL from Redis each time; Balance, Confirm, and Rollback
read CRCTL each time. Redis reads should be pipelined with other independent
profile reads where possible. This makes the next request observe a Wolfsburg
materialization immediately without TTL or invalidation races.

### FundingPlan Policy Snapshot

The FundingPlan must contain enough immutable data to explain Confirm and drive
Rollback without consulting the latest policy:

```rust
pub struct FundingPlanPolicySnapshot {
    pub card_policy_id: Uuid,
    pub card_range_id: Uuid,
    pub funding_mode: FundingMode,
    pub policy_version: u64,
    pub policy_effective_at: DateTime<Utc>,
    pub withdrawal_limit_authority: WithdrawalLimitAuthority,
    pub withdrawal_limits: Option<WithdrawalLimits>,
    pub calendar: Option<LimitCalendarPolicy>,
}
```

FundingPlan also retains its existing exact `limit_reservations`. Each
reservation stores policy ID, window, metric, usage/counterparty account IDs,
amount, expiration instant, timeout, and deterministic transfer ID. The Redis
key need not be persisted because it is deterministically derived from
`card_range_id` and funding mode; the immutable policy/range identities are the
audit facts.

For `CMS`, limits/calendar are null and `limit_reservations` is empty. For
`PLATFORM`, the complete resolved limits/calendar snapshot is stored even when
some windows are disabled.

## 10. Later Steps

After this foundation, continue with:

1. Minimal provider core.
2. Provider ledger accounts.
3. Cards and card ownership.
4. Card-provider funding sources.
5. Cardholder funding order.
6. Wurzburg policy/card-state outbox events.
7. Wolfsburg `CP`/`CPOL` materialization and consumer inbox.
8. Provider/user funding with WAL and recovery.

## 11. Required Integration Scenarios

1. Range boundaries accept exactly 16 digits, reject a leading zero, include
   both endpoints, and reject overlap with DRAFT, ACTIVE, or SUSPENDED ranges.
2. After first activation, range boundaries, funding mode, authority, and
   calendar cannot be patched.
3. Range activation/reactivation rejects missing policy, unmaterialized policy,
   and invalid provider cardinality.
4. `issuance_enabled = false` blocks new assignment while existing cards remain
   usable under `cms_operation_mode = FULL`.
5. `BALANCE_ONLY` permits Balance and rejects Confirm/Rollback; `BLOCKED` rejects
   all three; range SUSPENDED overrides every operational control.
6. Range-control changes publish a new `CRCTL` version, return `202` while
   pending, and become visible to Nuremberg on the next request without a local
   cache.
7. Only ACTIVE providers may be attached, and the active-only unique constraint
   prevents one provider from joining two ranges.
8. Provider deletion is allowed only before any financial activity or
   transaction history exists; it soft-deletes the provider, removes active
   range eligibility without deleting history, refreshes affected CPs, and
   suspends a range that loses its final active provider.
9. Every mutation records trusted WSO2 subject/client/provider context, canonical
   source IP, correlation/request IDs, reason, and before/after snapshots; spoofed
   public identity/forwarding headers are rejected or ignored.
10. `PLATFORM` range creation requires immutable calendar configuration; `CMS`
    requires null calendar. Authority cannot change later.
11. Card assignment creates all eight usage accounts for both PLATFORM and CMS
    ranges, and CP always carries the complete account set.
12. PLATFORM policy accepts disabled individual windows; CMS policy requires
    null limits.
13. Immediate and future policy creation return `202`, keep the prior policy
    active, and activate only after Wolfsburg materialization receipt.
14. Only one candidate policy may exist; cancellation requires a reason and
    retains the immutable row; a second candidate requires cancel-then-create.
15. Publication failure leaves the old policy active, marks the candidate
    PUBLICATION_FAILED, and supports idempotent admin retry or cancellation.
16. Changing limits creates a new immutable profile without replacing/zeroing
    usage accounts. Lower limits evaluate against existing live pending usage.
17. CPOL and CRCTL missing/invalid cases fail closed in Nuremberg. PLATFORM also
    fails closed when required usage account data is missing.
18. CMS CPOL remains mandatory, but Confirm creates no usage reads or limit
    reservations.
19. FundingPlan stores the complete policy snapshot and exact reservations, and
    Rollback uses those stored facts after later policy replacement.
20. Policy replacement publishes one idempotent event and does not rewrite
    unrelated CP objects.
