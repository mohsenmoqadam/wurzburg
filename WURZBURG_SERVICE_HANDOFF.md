# Wurzburg Service Handoff

This document is the working handoff for the final Wurzburg implementation.
It is intended to be given to Codex or engineers working inside the Wurzburg
repository.

Wurzburg currently has a useful PoC implementation, but the final service must
be designed from the current Nuremberg contracts, the Oracle production target,
and the newly separated Wolfsburg event-processing responsibility.

## 1. Service Purpose

Wurzburg is the synchronous business and administration service for the HyperCard
funding domain.

Its responsibilities are:

- Expose REST APIs for provider, user, card, card range, funding, policy, fee,
  transaction, and reporting workflows.
- Support batch user import/export and batch transaction reporting, including
  Excel export.
- Own durable business state through an RDBMS abstraction.
- Target Oracle as the production RDBMS while allowing PostgreSQL as a local/dev
  adapter.
- Create and manage provider-owned ledger accounts.
- Create and manage user-provider ledger accounts.
- Create and manage card-scoped ledger/control account relationships required by
  Nuremberg.
- Create and manage card ranges and card assignments.
- Generate the Redis profiles consumed by Nuremberg:
  - `CP:{card_number}`
  - `CPOL:SingleProvider:{provider_id}`
  - `CPOL:MultiProvider:DEFAULT`
  - `FEE:{provider_id}`
- Provide stable administrative/query APIs through WSO2.
- Keep Nuremberg independent from the Wurzburg database.

Wurzburg is not the CMS transaction processor. Nuremberg owns the external CMS
`Balance`, `Confirm`, and `Rollback` API contracts. Wurzburg supplies the facts
that Nuremberg needs to make fast deterministic ledger decisions.

## 2. Wurzburg, Wolfsburg, And Nuremberg Boundary

The system is intentionally split into three services.

### Wurzburg

Wurzburg is synchronous and REST-driven.

It owns:

- provider creation and management
- user creation/linking
- provider-user funding operations
- card range management
- card assignment and provider attachment
- policy and fee profile management
- transaction/reporting queries
- Redis profile publication for Nuremberg

Wurzburg APIs are expected to be exposed through WSO2. They do not use the
CMS-style `ApiKey`/`Signature` middleware used by Nuremberg CMS endpoints.

Wurzburg must trust only explicitly configured WSO2 identity headers/claims.
Header names, issuer/audience validation, and required roles/scopes must be
configuration-driven so a deployment cannot accidentally trust arbitrary caller
headers.

### Wolfsburg

Wolfsburg is asynchronous and Kafka-driven.

It owns:

- consuming Nuremberg CMS events
- persisting Balance/Confirm/Rollback event facts into the RDBMS
- processing/orchestrating rollback requests from persisted FundingPlan facts
- refreshing CP after successful ledger mutations
- clearing `Lock-CP:{card_number}` after the fresh CP is durable
- reconciliation of stuck or incomplete operations
- critical-state cleanup
- reconciliation for CMS-fired operations that are stuck, missing final events,
  or left in a long-pending state

Wolfsburg should not expose public business REST APIs. It should scale through
Kafka consumer groups and preserve same-card ordering internally.

Wolfsburg handles operations fired by CMS/Nuremberg events. Provider-to-user
credit/debit APIs remain Wurzburg-owned synchronous workflows.

### Nuremberg

Nuremberg is the CMS-facing transaction service.

It owns:

- CMS signature verification
- `POST /HyperCard/Balance`
- `POST /HyperCard/Confirm`
- `POST /HyperCard/Rollback`
- Redis reads for CP/policy/fee profiles
- Confirm planning
- TigerBeetle ledger mutation for Confirm
- Kafka event publication for Wolfsburg

Nuremberg must not query the Wurzburg RDBMS.

## 3. Current PoC Inventory

The current Wurzburg repo already contains useful foundations:

- Rust/Axum API server.
- Swagger/OpenAPI generation.
- Repository traits in `src/db/traits.rs`.
- PostgreSQL adapter under `src/db/postgres`.
- Initial PostgreSQL migration.
- Redis pool.
- Kafka producer/admin helpers.
- TigerBeetle client/worker.
- OTel tracing foundation.

Current PoC API groups:

- Providers
- Users
- Transactions
- Priorities

Current PoC routes include:

```text
POST   /api/v1/providers
GET    /api/v1/providers/{id}
GET    /api/v1/providers/certificate

POST   /api/v1/users
GET    /api/v1/users/{id}
GET    /api/v1/users/{id}/balance

POST   /api/v1/transactions/{provider_id}/users/{user_id}/credit
POST   /api/v1/transactions/{provider_id}/users/{user_id}/debit
GET    /api/v1/transactions/providers/{provider_id}
GET    /api/v1/transactions/providers/{provider_id}/users/{user_id}

POST   /api/v1/users/{user_id}/priorities
GET    /api/v1/users/{user_id}/priorities
GET    /api/v1/users/{user_id}/priorities/active
DELETE /api/v1/users/{user_id}/priorities/active
```

Important PoC limitations:

- REST handlers directly perform TigerBeetle operations.
- Some flows mutate TigerBeetle before durable DB recovery state is mature.
- Provider fee terms are embedded in provider rows.
- Priority models overlap conceptually with Nuremberg funding source ordering
  but do not match the final CP contract.
- Kafka admin credential/topic setup is provider-centric PoC behavior and needs
  product/security review.
- PostgreSQL enum/migration details are not Oracle-ready.
- No production Kafka consumer exists in Wurzburg; event processing belongs to
  Wolfsburg.

## 4. Persistence And Oracle Strategy

The production RDBMS target is Oracle.

Wurzburg must keep a clean persistence abstraction:

- Domain/service logic depends on repository traits.
- PostgreSQL remains a development adapter only.
- Oracle is the production adapter.
- SQL must not leak into API handlers.
- API handlers should call domain/application services, not raw repository
  methods directly once the final implementation starts.

### Oracle Modeling Rules

Recommended Oracle representation:

- UUID values: `RAW(16)` for compact binary identity.
- Money amounts: integer minor units, stored as `NUMBER(38,0)` or a constrained
  integer-compatible numeric type.
- Status/enums: constrained strings or lookup tables, not PostgreSQL enum types.
- JSON metadata: Oracle native JSON type where available, otherwise `CLOB`
  with JSON validation constraints.
- Timestamps: timezone-aware timestamp columns.
- Idempotency keys: unique indexed strings.
- Audit log values: JSON/CLOB snapshots plus actor metadata.

The handoff assumes Oracle support will be implemented as a repository adapter,
not by rewriting business logic.

### Required Database Guarantees

Wurzburg must have durable tables and unique constraints for:

- providers
- users
- user-provider links
- provider-owned ledger accounts
- provider CMS settlement accounts
- provider platform fee accounts
- user-provider ledger accounts
- card ranges
- cards
- card-provider funding sources
- cardholder funding order changes
- card policy profiles
- provider fee profiles
- policy usage account IDs
- provider/user funding movements
- batch import/export jobs
- report export jobs
- Nuremberg event facts persisted by Wolfsburg
- rollback facts and statuses
- audit logs
- idempotency records

Every externally triggered mutating REST operation must have an idempotency
strategy.

Production persistence decisions:

- Store UUIDs in Oracle as `RAW(16)`.
- Use Oracle 26 native JSON capabilities for JSON documents, request snapshots,
  profile snapshots, import/export metadata, and event payload snapshots.
- Keep repository traits database-neutral, but make Oracle semantics the design
  target. PostgreSQL can remain a local/development adapter if it does not
  weaken production behavior.

## 5. Identity Model

Use UUIDs in the Wurzburg domain.

Important identities:

- `provider_id`
- `user_id`
- `card_id`
- `card_number`
- `card_range_id`
- `user_provider_link_id`
- `provider_ledger_account_id`
- `provider_cms_account_id`
- `provider_platform_account_id`
- `user_provider_ledger_account_id`
- `card_policy_profile_id`
- `provider_fee_profile_id`
- policy usage account IDs
- business transaction IDs
- idempotency keys

TigerBeetle account IDs are the `u128` representation of the UUID binary value.
At the Wurzburg/Nuremberg domain boundary, work with UUIDs. Convert to `u128`
only at the TigerBeetle boundary.

## 6. Provider Domain

A provider is an organization that can fund users/cards.

Provider facts:

- legal name
- trade name
- tax ID
- contact information
- status
- provider-owned TigerBeetle ledger account
- fee profile relationship
- policy/profile publishing permissions
- operational metadata

Provider-owned ledger account:

- Created when the provider is created or activated.
- Stored durably in Wurzburg.
- Published to Nuremberg only when Nuremberg needs it through CP funding source
  ledger accounts.
- Used when provider funds fees or when Wurzburg moves credit from provider to
  user-provider account.

Provider settlement/revenue accounts:

- Each provider also needs provider-scoped CMS settlement and platform fee
  accounts.
- Nuremberg credits the provider-CMS account when a Confirm moves principal from
  a user-provider account to CMS settlement for that provider.
- Nuremberg credits the provider-platform account when fees are owed to the
  platform for that provider.
- These accounts make provider-level receivables visible at any time:
  - how much credit remains in the provider-owned account
  - how much user-provider credit is outstanding
  - how much the provider owes/has settled with CMS
  - how much platform fee has accrued for that provider

Provider Kafka access:

- Providers may need Kafka credentials so Wurzburg/Wolfsburg can emit provider
  user/card/transaction events to provider-specific integrations.
- Credential creation and rotation should be managed by a dedicated API, not
  implicitly tied to provider creation.
- A provider may request new credentials, but only one credential set should be
  active at a time unless product explicitly introduces overlapping rotation
  windows.
- Credential details must be configurable and auditable.
- Do not expose Kafka credentials in normal provider read APIs.

Provider lifecycle:

```text
Draft -> Active -> Suspended/Inactive
```

Provider APIs should support at least:

```text
POST   /api/v1/providers
GET    /api/v1/providers/{provider_id}
PATCH  /api/v1/providers/{provider_id}
POST   /api/v1/providers/{provider_id}/activate
POST   /api/v1/providers/{provider_id}/suspend
GET    /api/v1/providers
```

The final API shape can evolve, but provider state transitions must be explicit.

## 7. User And Provider Linking

A user is globally identified by national ID.

Rules:

- One provider can create/link a user.
- Other providers can later link to the same global user.
- The global `User` record stores shared identity facts.
- The `UserProviderLink` stores provider-specific facts and metadata.
- Each `(user_id, provider_id)` pair may have a provider-level relationship
  account for provider-to-user funding operations.
- Provider A must not overwrite Provider B's user-provider metadata.

Required durable model:

```text
User:
- user_id
- national_id
- shared identity metadata
- status

UserProviderLink:
- user_id
- provider_id
- provider-specific external metadata
- provider_level_user_account_id
- status
```

Provider-level user account:

- Represents the user's funded balance at one provider.
- Wurzburg credits it by moving value from the provider-owned account.
- It is not published directly in CP for Confirm. Wurzburg allocates or maps
  credit into card-specific funding accounts that are published to Nuremberg.

Card-scoped ledger rule:

- A user may have multiple single-provider cards and multiple multi-provider
  cards.
- Ledger accounts that Nuremberg uses for card funding and policy usage must be
  tied to the card context, not only to the global user/provider link.
- The durable model must be able to answer:
  - which cards a user owns
  - which providers are behind each card
  - which card-specific funding account is used for a specific card/provider
  - which policy usage accounts belong to that card
- This prevents two cards owned by the same user from accidentally sharing
  funding state or limit consumption.

User APIs should support at least:

```text
POST /api/v1/providers/{provider_id}/users
GET  /api/v1/users/{user_id}
GET  /api/v1/users/by-national-id/{national_id}
GET  /api/v1/providers/{provider_id}/users/{user_id}
GET  /api/v1/users/{user_id}/providers
```

## 8. Card Range Domain

Card ranges are first-class Wurzburg domain objects.

Reason:

- Nuremberg CP depends on whether a card is single-provider or multi-provider.
- A provider creating/linking a user must not attach a card in an invalid range.
- The final funding mode must be deterministic and auditable.

Card range fields:

- `card_range_id`
- start PAN/card number
- end PAN/card number
- range status
- `funding_mode`: `SingleProvider` or `MultiProvider`
- optional owner provider for single-provider ranges
- issuer/platform metadata
- created/updated audit fields

Rules:

### SingleProvider Range

- Owned by exactly one provider.
- Cards in this range can only have that provider as funding source.
- Wurzburg resolves card policy with:

```text
CPOL:SingleProvider:{provider_id}
```

### MultiProvider Range

- Owned by platform/core, not by one provider.
- Cards in this range can have multiple provider funding sources.
- Wurzburg resolves card policy with:

```text
CPOL:MultiProvider:DEFAULT
```

`platform/core owned` means no single provider controls exclusivity for the
range. It does not prevent a provider from requesting/issuing a card in that
range for a user. It only means Wurzburg must allow other active providers to be
attached to the same card later if the cardholder chooses that.

Card range APIs should support:

```text
POST   /api/v1/card-ranges
GET    /api/v1/card-ranges/{card_range_id}
GET    /api/v1/card-ranges
PATCH  /api/v1/card-ranges/{card_range_id}
POST   /api/v1/card-ranges/{card_range_id}/activate
POST   /api/v1/card-ranges/{card_range_id}/suspend
```

Potential request shape:

```json
{
  "start_card_number": "6219861000000000",
  "end_card_number": "6219861099999999",
  "funding_mode": "SingleProvider",
  "owner_provider_id": "uuid-or-null",
  "description": "Provider-owned range"
}
```

Validation:

- ranges must not overlap
- `SingleProvider` requires `owner_provider_id`
- `MultiProvider` must not be owned by a normal provider unless product later
  explicitly allows it
- card numbers must be normalized and validated before persistence

## 9. Card Domain

A card links a card number to a global user and a funding mode derived from its
range.

Card facts:

- `card_id`
- `card_number`
- `card_range_id`
- `user_id`
- national ID snapshot or relation
- status
- funding mode
- card metadata

Card ownership rules:

- One user may own multiple cards.
- A user may simultaneously own:
  - multiple single-provider cards
  - multiple multi-provider cards
- Each card has its own policy usage account set.
- Each card-provider funding source must point to the exact ledger accounts
  Wurzburg wants Nuremberg to use for that card/provider combination.
- Funding accounts published in CP are always card-specific in the final design.
  Do not publish a global `(user_id, provider_id)` account directly as the
  Confirm debit account.

Card creation/linking rules:

- Provider sends card number and national ID/user facts.
- Wurzburg finds the card range for the card number.
- Wurzburg enforces the card range funding mode.
- Wurzburg creates or links the global user.
- Wurzburg creates or links the provider-user account.
- Wurzburg attaches the provider as a funding source when allowed.
- Wurzburg creates or links the card-specific funding account for every
  `(card_number, provider_id)` funding source.
- Wurzburg creates policy usage account UUIDs for the card if not already
  created.
- Wurzburg publishes or refreshes `CP:{card_number}`.

Card APIs should support:

```text
POST /api/v1/cards
GET  /api/v1/cards/{card_number}
GET  /api/v1/users/{user_id}/cards
POST /api/v1/cards/{card_number}/providers/{provider_id}
DELETE /api/v1/cards/{card_number}/providers/{provider_id}
PATCH /api/v1/cards/{card_number}/status
PUT  /api/v1/cards/{card_number}/funding-order
POST /api/v1/cards/{card_number}/publish-profile
```

Provider attachment rules:

- Single-provider card:
  - provider must match the range owner
  - only one funding source allowed
- Multi-provider card:
  - multiple providers allowed
  - each provider must have an active user-provider link/account
  - funding priority must be deterministic

Provider-order update rule:

- The cardholder controls the funding priority order for providers behind their
  card.
- When the cardholder changes provider order, Wurzburg must persist the new
  order and publish the refreshed `CP:{card_number}` to Redis.
- If `Lock-CP:{card_number}` exists because Nuremberg/Wolfsburg is processing a
  critical operation for that card, Wurzburg must reject the API request with a
  clear conflict response instead of overwriting Redis.
- Recommended HTTP response for a locked CP:
  - status: `409 Conflict`
  - error code: `CARD_PROFILE_LOCKED`
  - message: `Card profile is currently locked by an in-flight transaction. Try again later.`

Funding-order update request should be explicit and idempotent:

```json
{
  "ordered_provider_ids": ["uuid-1", "uuid-2", "uuid-3"],
  "reason": "cardholder preference update"
}
```

Validation:

- every provider must already be attached to the card
- the list must include every active funding provider exactly once
- single-provider cards cannot be reordered into multiple providers
- CP lock must be checked before Redis publish
- DB update and Redis publish must be recoverable if Redis is temporarily down

## 10. Funding Source Ordering

Nuremberg consumes prioritized funding sources from CP.

Wurzburg must own the durable source of this ordering.

Final CP shape expected by Nuremberg:

```rust
pub struct FundingSource {
    pub provider_id: Uuid,
    pub priority: u16,
    pub max_amount: u64,
    pub ledger_accounts: FundingSourceLedgerAccounts,
}

pub struct FundingSourceLedgerAccounts {
    pub user_provider_account: Uuid,
    pub provider_account: Uuid,
    pub cms_account: Uuid,
    pub platform_account: Uuid,
}
```

Rules:

- Funding sources are ordered by `priority`.
- Nuremberg consumes provider 1 first, then provider 2, and so on.
- `max_amount` is Wurzburg's published available capacity for that funding
  source.
- For single-provider cards, exactly one funding source is valid.
- For multi-provider cards, at least one funding source is required.
- Priority values must be unique per card.
- Funding order is controlled by the cardholder/user.
- Providers cannot unilaterally reorder themselves ahead of other providers for
  a card.
- Platform/admin tooling may expose support operations, but the business owner
  of the preference is the cardholder.
- Every order change must be idempotent, audited, persisted, and followed by CP
  refresh if the card is not locked.

## 11. Card Policy Profiles

Wurzburg publishes card withdrawal policy profiles to Redis.

Redis keys:

```text
CPOL:SingleProvider:{provider_id}
CPOL:MultiProvider:DEFAULT
```

Model expected by Nuremberg:

```rust
pub struct CardPolicyProfile {
    pub id: Uuid,
    pub scope: CardPolicyScope,
    pub withdrawal_limits: WithdrawalLimits,
    pub calendar: LimitCalendarPolicy,
}

pub enum CardPolicyScope {
    SingleProvider { provider_id: Uuid },
    MultiProviderDefault,
}

pub struct WithdrawalLimits {
    pub per_transaction_min_amount: Option<u64>,
    pub per_transaction_max_amount: Option<u64>,
    pub daily: Option<WithdrawalWindowLimit>,
    pub weekly: Option<WithdrawalWindowLimit>,
    pub monthly: Option<WithdrawalWindowLimit>,
    pub yearly: Option<WithdrawalWindowLimit>,
}

pub struct WithdrawalWindowLimit {
    pub max_amount: Option<u64>,
    pub max_count: Option<u32>,
}
```

Rules:

- Policy IDs are immutable.
- If policy terms change and old terms must remain auditable, publish a new
  policy `id`.
- Do not rewrite all CP objects when policy terms change.
- Policy changes apply immediately to new Nuremberg Confirm decisions after the
  Redis object is refreshed.
- Nuremberg stores the exact policy ID used in FundingPlan.
- Per-transaction min/max are stateless checks.
- Daily/weekly/monthly/yearly amount/count windows use TigerBeetle usage
  accounts from CP.

Policy APIs should support:

```text
POST /api/v1/card-policies/single-provider/{provider_id}
GET  /api/v1/card-policies/single-provider/{provider_id}
POST /api/v1/card-policies/multi-provider-default
GET  /api/v1/card-policies/multi-provider-default
```

## 12. Provider Fee Profiles

Fee profiles are provider-scoped and published to Redis.

Redis key:

```text
FEE:{provider_id}
```

This key means there is one active fee profile per provider. If the business
later needs different fee deals by card segment, campaign, merchant category, or
customer group, the key pattern must be extended. Do not add that complexity
until the product explicitly requires segmented fee deals.

Model expected by Nuremberg:

```rust
pub struct ProviderFeeProfile {
    pub id: Uuid,
    pub provider_id: Uuid,
    pub fee_policy: FeePolicy,
}

pub struct FeePolicy {
    pub rate_bps: u32,
    pub fixed_amount: u64,
    pub user_fee_source: FeeFundingSource,
    pub provider_fee_source: FeeFundingSource,
}

pub enum FeeFundingSource {
    User,
    Provider,
}
```

Rules:

- Fee profile IDs are immutable.
- Fee changes should not require CP rewrites.
- If old terms must remain auditable, publish a new fee profile `id`.
- Fee changes apply immediately to new Nuremberg Confirm decisions after the
  Redis object is refreshed.
- Nuremberg stores the exact fee profile ID and fee policy snapshot in
  FundingPlan.

Fee profile APIs should support:

```text
POST /api/v1/providers/{provider_id}/fee-profile
GET  /api/v1/providers/{provider_id}/fee-profile
```

## 13. Redis Publishing Contracts

Wurzburg must publish Redis facts in the exact shape Nuremberg expects.

### CardProfile

Key:

```text
CP:{card_number}
```

Shape:

```json
{
  "user_id": "uuid",
  "card_number": "string",
  "funding_mode": "SingleProvider | MultiProvider",
  "policy_usage_accounts": {
    "amount_daily_limit_account": "uuid",
    "amount_weekly_limit_account": "uuid",
    "amount_monthly_limit_account": "uuid",
    "amount_yearly_limit_account": "uuid",
    "count_daily_limit_account": "uuid",
    "count_weekly_limit_account": "uuid",
    "count_monthly_limit_account": "uuid",
    "count_yearly_limit_account": "uuid"
  },
  "funding_sources": [
    {
      "provider_id": "uuid",
      "priority": 1,
      "max_amount": 1000000,
      "ledger_accounts": {
        "user_provider_account": "uuid",
        "provider_account": "uuid",
        "cms_account": "uuid",
        "platform_account": "uuid"
      }
    }
  ]
}
```

Publishing rules:

- CP is Wurzburg-owned data.
- Nuremberg only reads CP from Redis.
- After successful Confirm, Nuremberg deletes stale CP.
- Wolfsburg later writes a fresh CP after event processing/reconciliation.
- CP must not include mutable provider-wide policy values.
- CP may include future additive fields; Nuremberg ignores unknown fields.
- `user_provider_account` is the exact account Nuremberg debits for this
  card/provider funding source.
- `provider_account` is the provider-owned account Nuremberg debits only when a
  provider-funded fee must be collected from that provider.
- `cms_account` is the provider-scoped CMS settlement account credited by
  Confirm principal movement.
- `platform_account` is the provider-scoped platform fee account credited by
  Confirm fee movement.
- The funding debit account published as `user_provider_account` must be
  card-specific. It must not be guessed by Nuremberg or replaced with a global
  provider-user account.

### CardPolicyProfile

Keys:

```text
CPOL:SingleProvider:{provider_id}
CPOL:MultiProvider:DEFAULT
```

### ProviderFeeProfile

Key:

```text
FEE:{provider_id}
```

## 14. TigerBeetle Responsibilities

Wurzburg creates and funds accounts that Nuremberg later uses.

Account categories:

- provider-owned ledger account
- provider-level user account used by Wurzburg provider-to-user funding
  workflows
- card-specific funding account used by Nuremberg Confirm for one
  `(card_number, provider_id)` funding source
- provider-scoped CMS settlement account
- provider-scoped platform fee account
- policy usage accounts for each card/window/metric
- technical/control accounts needed by accounting or reconciliation

Rules:

- Create provider-owned account when provider becomes active.
- Create provider-level user account when `(user, provider)` link becomes
  active.
- Create provider CMS settlement and platform fee accounts when provider becomes
  active, unless explicitly provisioned earlier.
- Create card-specific funding/control accounts for every active
  `(card_number, provider_id)` funding source published in CP.
- Create policy usage accounts when the card is created or when CP inputs are
  first generated.
- Store UUIDs durably.
- Convert UUID to `u128` only at TigerBeetle boundary.
- Use TigerBeetle account flags so user-provider account cannot go negative.

Provider-to-user credit movement:

- Debit provider-owned account.
- Credit the relevant card-specific funding account when the credit is intended
  to be spendable by a card.
- If Wurzburg accepts provider-level user credit without a card, that balance is
  not spendable by Nuremberg until Wurzburg assigns it to one or more
  card-specific funding accounts and refreshes CP.
- Persist durable transaction facts.
- Refresh CP for impacted cards.

Confirm-time movement performed by Nuremberg:

- Debit the published `user_provider_account` for principal.
- Credit the published `cms_account` for provider CMS settlement.
- Debit the published fee source account when user/provider fee policy requires
  a fee debit.
- Credit the published `platform_account` for platform fee accrual.

These account relationships let Wurzburg and Wolfsburg report, for every
provider, provider-owned remaining credit, user-provider outstanding credit, CMS
settlement amounts, and platform fee accrual.

Important recovery rule:

- Do not rely on Redis as proof of ledger movement.
- Durable DB state plus TigerBeetle lookup is the proof.

## 15. REST API Design Principles

Wurzburg APIs are not CMS APIs.

Rules:

- No CMS `ApiKey`/`Signature` middleware.
- WSO2 handles external auth, authentication, rate limiting, and consumer
  policy.
- Wurzburg should still validate caller identity/claims from trusted WSO2
  headers or tokens.
- Every mutating API must support idempotency.
- Use standard HTTP status codes.
- Return structured error bodies, not plain strings.
- Do not return secrets such as Kafka passwords except in the immediate
  create/rotate credential response.

Recommended common response error shape:

```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Human-readable message",
    "details": {}
  }
}
```

Recommended idempotency header:

```text
Idempotency-Key
```

### WSO2 Trust Boundary

Wurzburg must never trust arbitrary identity headers from the public internet.
Only the WSO2 gateway or an approved internal network path may inject trusted
caller identity.

Final trust inputs:

- WSO2 must call Wurzburg through a trusted internal path, preferably mTLS.
- Wurzburg should validate a JWT forwarded by WSO2. Preferred header:
  `Authorization: Bearer <jwt>`.
- If WSO2 strips/replaces the original `Authorization` header, the accepted
  fallback header is `X-JWT-Assertion`.
- Required correlation header: `X-Correlation-Id`.
- Optional request identifier header: `X-Request-Id`.

Final required JWT claims:

```text
iss              expected WSO2 issuer
aud              must include wurzburg-api
sub              authenticated human/service subject
azp/client_id    calling WSO2 application/client identity
scope/scp        OAuth scopes granted to the caller
roles            coarse Wurzburg roles
provider_id      required for provider-scoped callers
tenant_id        optional future multi-tenant boundary
```

Recommended service behavior:

- reject requests missing required trusted claims
- reject requests coming from an untrusted upstream
- map WSO2 roles/scopes to Wurzburg permissions
- log only claim identifiers needed for audit
- do not let clients spoof provider identity through request body fields when a
  trusted provider claim is present

Provider-scoped callers must be restricted to the `provider_id` claim. If a
provider-scoped token tries to operate on another provider in the path or body,
Wurzburg must return `403 Forbidden`.

Final role/scope matrix:

```text
Role: wurzburg_provider_operator
Scopes:
- provider.users:write
- provider.users:read
- provider.cards:read
- provider.funding:write
- provider.transactions:read

Role: wurzburg_provider_admin
Scopes:
- provider.users:write
- provider.users:read
- provider.cards:write
- provider.cards:read
- provider.funding:write
- provider.transactions:read
- provider.kafka_credentials:rotate

Role: wurzburg_platform_admin
Scopes:
- platform.providers:write
- platform.providers:read
- platform.card_ranges:write
- platform.card_ranges:read
- platform.policies:write
- platform.policies:read
- platform.fee_profiles:write
- platform.fee_profiles:read
- platform.recovery:write
- platform.config:write
- platform.config:read

Role: wurzburg_support
Scopes:
- support.users:read
- support.cards:read
- support.transactions:read
- support.recovery:read

Role: wurzburg_reporting
Scopes:
- reports.transactions:read
- reports.transactions:export
```

Provider roles are always provider-scoped. Platform roles may operate across
providers. Support is read-only by default. Reporting can create export jobs but
must not mutate business state.

## 16. Suggested Final API Groups

### Providers

```text
POST   /api/v1/providers
GET    /api/v1/providers/{provider_id}
GET    /api/v1/providers
PATCH  /api/v1/providers/{provider_id}
POST   /api/v1/providers/{provider_id}/activate
POST   /api/v1/providers/{provider_id}/suspend
GET    /api/v1/providers/{provider_id}/ledger
```

### Provider Kafka Credentials

```text
POST   /api/v1/providers/{provider_id}/kafka-credentials
GET    /api/v1/providers/{provider_id}/kafka-credentials
POST   /api/v1/providers/{provider_id}/kafka-credentials/rotate
POST   /api/v1/providers/{provider_id}/kafka-credentials/revoke
```

Rules:

- credential creation is independent from provider creation
- only one credential set is active at a time unless an explicit rotation window
  is introduced
- returned secrets must be limited to the creation/rotation response
- normal read APIs return metadata only, never the password/secret
- ACL/topic details must be configurable and auditable

### Users And Provider Links

```text
POST   /api/v1/providers/{provider_id}/users
GET    /api/v1/users/{user_id}
GET    /api/v1/users/by-national-id/{national_id}
GET    /api/v1/providers/{provider_id}/users/{user_id}
GET    /api/v1/users/{user_id}/providers
GET    /api/v1/users/{user_id}/balances
```

### User Import/Export

```text
POST   /api/v1/user-imports
POST   /api/v1/user-imports/{job_id}/file
GET    /api/v1/user-imports/{job_id}
GET    /api/v1/user-imports/{job_id}/errors
GET    /api/v1/user-imports/{job_id}/original-file
GET    /api/v1/user-imports/{job_id}/result-file
POST   /api/v1/user-exports
GET    /api/v1/user-exports/{job_id}
GET    /api/v1/user-exports/{job_id}/file
```

Rules:

- imports must be batch jobs, not long-running HTTP requests
- each row must have row-level validation status
- successful rows must be idempotent
- failed rows must be exportable for correction
- exports must support provider/user/card filters according to caller
  permissions

Final import file decision:

- Input format: CSV.
- Encoding: UTF-8.
- Header row: required.
- Delimiter, quote character, escape character, date format, and maximum row
  count are business configuration values stored in the Wurzburg database.
- Default delimiter: comma.
- Default maximum batch size: 50,000 rows.
- Original uploaded file must be stored in MinIO.
- Parsed result file must be stored in MinIO.
- Job metadata, validation summary, requester identity, original object key,
  result object key, and row-level error counts are stored in Oracle.

Recommended row-level error schema:

```json
{
  "row_number": 17,
  "external_reference": "optional-provider-row-id",
  "status": "FAILED",
  "error_code": "INVALID_NATIONAL_ID",
  "message": "National ID is invalid",
  "field": "national_id",
  "raw_value": "masked-or-redacted-value"
}
```

Result file format:

- CSV with all original columns plus:
  - `import_status`
  - `wurzburg_user_id`
  - `wurzburg_card_id`
  - `error_code`
  - `error_message`
- Failed rows must be downloadable as either the full result file or an errors
  only result file.

### Provider/User Funding

```text
POST   /api/v1/providers/{provider_id}/users/{user_id}/credit
POST   /api/v1/providers/{provider_id}/users/{user_id}/debit
GET    /api/v1/providers/{provider_id}/transactions
GET    /api/v1/providers/{provider_id}/users/{user_id}/transactions
```

### Card Ranges

```text
POST   /api/v1/card-ranges
GET    /api/v1/card-ranges/{card_range_id}
GET    /api/v1/card-ranges
PATCH  /api/v1/card-ranges/{card_range_id}
POST   /api/v1/card-ranges/{card_range_id}/activate
POST   /api/v1/card-ranges/{card_range_id}/suspend
```

### Cards

```text
POST   /api/v1/cards
GET    /api/v1/cards/{card_number}
GET    /api/v1/users/{user_id}/cards
POST   /api/v1/cards/{card_number}/providers/{provider_id}
DELETE /api/v1/cards/{card_number}/providers/{provider_id}
PUT    /api/v1/cards/{card_number}/funding-order
PATCH  /api/v1/cards/{card_number}/status
POST   /api/v1/cards/{card_number}/publish-profile
```

### Policies

```text
POST   /api/v1/card-policies/single-provider/{provider_id}
GET    /api/v1/card-policies/single-provider/{provider_id}
POST   /api/v1/card-policies/multi-provider-default
GET    /api/v1/card-policies/multi-provider-default
```

### Fee Profiles

```text
POST   /api/v1/providers/{provider_id}/fee-profile
GET    /api/v1/providers/{provider_id}/fee-profile
```

### Business Configuration

System configuration and business configuration must be separated.

System configuration remains file/env based:

- database URLs
- Kafka bootstrap servers
- Redis URL
- MinIO endpoint and credentials
- TigerBeetle cluster settings
- OTel endpoints
- TLS/mTLS settings

Business configuration lives in Oracle and can change without redeploying:

- import CSV delimiter/quote/escape/date format
- import maximum batch size
- report retention periods
- report allowed output formats
- provider Kafka credential defaults
- CP lock conflict error policy/message
- default card policy calendar values
- operational thresholds for stale jobs/reconciliation

Recommended APIs:

```text
GET   /api/v1/config/business
GET   /api/v1/config/business/{key}
PUT   /api/v1/config/business/{key}
GET   /api/v1/config/business/audit
```

Recommended database model:

```text
business_config:
- key
- value_json
- value_type
- version
- status
- effective_at
- updated_by
- updated_at
- description

business_config_audit:
- key
- old_value_json
- new_value_json
- old_version
- new_version
- changed_by
- changed_at
- reason
```

Runtime behavior:

- Wurzburg instances cache business config in memory with a short TTL.
- Every config read must know the config `version`.
- Config update writes an audit row and increments the key version.
- Wurzburg instances must observe config changes by polling a lightweight
  version table or consuming an internal config-changed event/outbox.
- Config changes must be validated before activation.
- Business config must not be used for secrets.
- Operations that need deterministic audit should store the config version used
  in their job/transaction record.

### Reporting

```text
GET    /api/v1/transactions
GET    /api/v1/transactions/{transaction_id}
GET    /api/v1/providers/{provider_id}/transactions
GET    /api/v1/users/{user_id}/transactions
GET    /api/v1/cards/{card_number}/transactions
POST   /api/v1/reports/transactions
GET    /api/v1/reports/transactions/{job_id}
GET    /api/v1/reports/transactions/{job_id}/file
```

Transaction reports must support batch generation and Excel output.

Final report file decision:

- Default output format: `.xlsx`.
- Optional output format: CSV when explicitly requested and allowed by business
  configuration.
- Generated report files must be stored in MinIO.
- Report job metadata must be stored in Oracle:
  - requester identity
  - trusted WSO2 subject/client/provider claims
  - filter snapshot
  - output format
  - MinIO object key
  - file checksum
  - generated row count
  - job status
  - expiration timestamp
  - audit timestamps
- Default retention:
  - report files: 14 days
  - import original files: 30 days
  - import result files: 90 days
- Retention values are business configuration values stored in Oracle.

Recommended report APIs:

```text
POST   /api/v1/reports/transactions
GET    /api/v1/reports/transactions/{job_id}
GET    /api/v1/reports/transactions/{job_id}/file
DELETE /api/v1/reports/transactions/{job_id}/file
```

The download endpoint should return a short-lived pre-signed MinIO URL or stream
the file through Wurzburg after rechecking caller permissions.

## 17. Idempotency And Recovery

Every mutating REST command must be idempotent.

Examples:

- create provider
- create/link user
- create card range
- assign card
- attach provider to card
- credit user-provider account
- debit user-provider account
- create/update policy profile
- create/update fee profile
- publish CP/profile to Redis

Rules:

- Store idempotency key durably with request hash and final result.
- If same idempotency key arrives with same request hash, return prior result.
- If same idempotency key arrives with different request hash, return conflict.
- Ledger-affecting operations must use deterministic TigerBeetle IDs where
  practical.
- If DB succeeds and Redis publish fails, the operation must be recoverable.
- If TigerBeetle succeeds and DB write fails, the operation must be detected and
  repaired through reconciliation.

For provider-to-user credit movement, prefer a clear WAL/state machine:

```text
INIT
VALIDATED
DB_INTENT_SAVED
CALLING_LEDGER
LEDGER_SUCCESS
LEDGER_FAILED
DB_FINALIZED
REDIS_REFRESHED
COMPLETED
RECOVERY_REQUIRED
```

## 18. Wolfsburg Event Dependencies

Although Wolfsburg is a separate service, Wurzburg must define the durable
tables and domain facts Wolfsburg needs.

Wolfsburg consumes Nuremberg events:

- `BALANCE_WARMUP_REQUESTED`
- `BALANCE_PROCESSED`
- `CONFIRM_PLANNED`
- `CONFIRM_PROCESSED`
- `ROLLBACK_REQUESTED`

Wolfsburg requires durable storage for:

- event ID
- event type
- card number
- transaction ID
- request snapshot
- CMS result
- FundingPlan
- processing status
- reconciliation status
- timestamps
- error details

Wolfsburg must preserve same-card event order after Kafka consumption.

Rollback ownership decision:

- Wolfsburg executes or orchestrates rollback using persisted FundingPlan facts
  from Nuremberg events.
- Rollback must not recalculate fees, funding allocations, or limit
  reservations from the latest CP/profile.
- Wurzburg must provide the durable tables, repository APIs, and TigerBeetle
  lookup/account facts Wolfsburg needs to process rollback and reconciliation.
- Nuremberg remains the CMS-facing API surface, but rollback recovery and CP
  refresh after rollback are Wolfsburg responsibilities.

## 19. Observability

Wurzburg must emit OTel traces for:

- HTTP request entry
- validation
- repository operations
- TigerBeetle account/transfer operations
- Redis profile publishing
- policy/fee profile publishing
- CP generation
- idempotency lookup
- recovery/WAL state transitions

Never emit:

- raw card number
- national ID
- raw request body
- Redis CP payload
- Kafka credentials
- database secrets

Allowed identifiers:

- masked card number
- provider ID
- user ID
- card ID
- policy/profile IDs
- transaction IDs
- idempotency key hash

## 20. Testing Requirements

Wurzburg needs scenario-driven tests similar to Nuremberg.

Required test groups:

- provider creation
- provider idempotency
- provider Kafka credential creation, rotation, revocation, and secret masking
- user creation/linking by national ID
- linking one user to multiple providers
- batch user import with mixed valid/invalid rows
- batch user export with provider/user/card filters
- card range creation and overlap rejection
- single-provider card assignment
- multi-provider card assignment
- invalid provider attachment to single-provider card
- one user owning multiple single-provider cards
- one user owning multiple multi-provider cards
- cardholder funding-order update success
- cardholder funding-order update rejection while `Lock-CP:{card_number}` exists
- provider-to-user credit movement
- user-provider debit movement
- provider ledger account, user-provider/card account, provider-CMS account, and
  provider-platform account creation
- CP generation for single-provider card
- CP generation for multi-provider card
- card policy profile Redis publishing
- provider fee profile Redis publishing
- immediate policy/fee update visibility after Redis refresh
- transaction report query filters
- transaction report Excel export job lifecycle
- Redis unavailable recovery behavior
- TigerBeetle unavailable recovery behavior
- WSO2 trusted-claim authorization and spoofed-header rejection
- Oracle repository adapter tests
- PostgreSQL/dev adapter tests if retained

Integration tests should seed database/TigerBeetle/Redis facts explicitly and
assert final Redis profiles match the Nuremberg contract.

Test style requirements:

- Prefer one scenario file per important business path.
- At the beginning of each test, document:
  - the scenario goal
  - facts inserted into the database
  - TigerBeetle accounts/transfers created
  - Redis keys expected before and after the operation
  - the final business proof the scenario is asserting
- Do not rely on hidden global fixtures for money movement; tests must make
  account setup visible.
- Failure tests must assert no unintended ledger movement and no unsafe Redis CP
  overwrite.

## 21. Migration From Current PoC

Recommended implementation order:

1. Keep current repo as the starting point.
2. Introduce domain/application service modules between handlers and repository.
3. Define final DTOs and error response model.
4. Define Oracle-ready repository traits and domain models.
5. Add card range and card domain models.
6. Add policy and fee profile models matching Nuremberg.
7. Add Redis profile publisher module.
8. Refactor provider/user APIs to use service layer.
9. Refactor credit/debit APIs with idempotency and WAL/recovery states.
10. Implement CP generation and Redis publishing.
11. Add integration tests for CP/profile compatibility with Nuremberg.
12. Implement Oracle adapter.
13. Decide what remains PostgreSQL-only for local development.

Do not copy the PoC API shape blindly. Use it as a learning artifact and rebuild
the final Wurzburg surface around the Nuremberg Redis contracts and the
Wurzburg/Wolfsburg split.

## 22. Finalized Decisions And Remaining Decisions

Finalized decisions:

- Oracle stores UUIDs as `RAW(16)`.
- Oracle 26 native JSON should be used for JSON payloads/snapshots.
- Provider-to-user credit/debit is Wurzburg-owned.
- Wolfsburg handles CMS-fired event processing, rollback, reconciliation, and
  critical-state cleanup.
- Card funding order is controlled by the cardholder/user.
- Policy changes apply immediately after Redis refresh.
- Provider fee changes apply immediately after Redis refresh.
- `FEE:{provider_id}` means one active provider fee profile. Add deal/segment
  dimensions only if product later requires different fee profiles per segment.
- Provider Kafka credentials are not one-time-only. Providers may rotate/get new
  credentials, but only one credential set should be active at a time unless an
  explicit rotation window is introduced.
- Rollback execution/orchestration belongs to Wolfsburg using persisted
  FundingPlan facts.
- Multi-provider ranges are platform/core controlled in the sense that no single
  provider owns exclusivity for the range.
- WSO2 authentication data is accepted through a trusted JWT in `Authorization:
  Bearer <jwt>` or fallback `X-JWT-Assertion`, plus `X-Correlation-Id`.
- WSO2 authorization uses the documented Wurzburg role/scope matrix.
- Report files are stored in MinIO. Default report output is `.xlsx`; CSV is
  optional when business config allows it.
- Import original files and import result files are stored in MinIO.
- User import format is CSV with UTF-8 encoding and a required header row.
- Import parser parameters, batch size, report formats, retention periods, and
  similar operational business values are stored in Oracle business config.
- Card funding accounts are always card-specific for the final design.

Remaining decisions:

1. Exact MinIO bucket names and object key naming convention.
2. Exact CSV columns for each import/export operation.
3. Exact report column set for every report type.
4. Exact internal mechanism for business config change notification:
   lightweight DB polling, DB outbox, or internal Kafka event.

## 23. Non-Negotiable Rules

- Nuremberg must never query the Wurzburg RDBMS.
- Wurzburg must publish Redis profiles in the exact Nuremberg contract shape.
- CP must not contain high-fanout mutable policy values.
- If `Lock-CP:{card_number}` exists, Wurzburg must not overwrite CP and must
  return a conflict response for card/profile mutation APIs.
- Policy and fee profile IDs must be immutable for audit.
- Provider/user/card ledger account IDs must be UUIDs in Wurzburg domain.
- Final funding accounts published in CP are card-specific.
- TigerBeetle `u128` conversion belongs at the ledger boundary.
- Every ledger-affecting REST operation must be idempotent.
- Redis is not proof of money movement.
- Same-card Kafka ordering must be preserved by Wolfsburg.
- After successful Nuremberg Confirm, Wolfsburg must refresh CP and clear lock
  only after durable processing/reconciliation.
- Cardholder-controlled provider order is the source of CP funding priority.
- Provider Kafka credentials must be explicitly managed and never leaked through
  ordinary read APIs.
- Business configuration belongs in Oracle with versioning and audit; system
  secrets and infrastructure endpoints do not.
