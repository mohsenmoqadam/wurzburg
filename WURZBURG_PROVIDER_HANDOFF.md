# Wurzburg Provider Handoff

This document captures the Provider design understanding before implementing
the Provider API set.

Production runtime storage is Dragonfly. References to Redis keys, locks,
commands, or Redis-compatible clients in this document describe the Dragonfly
protocol contract; they do not introduce a separate Redis deployment.

It extends the card-range foundation documented in
`WURZBURG_CARD_RANGE_POLICY_HANDOFF.md`. The trusted gateway, JWT/header,
role/scope, idempotency, timeout, tracing, and ESB delivery contract is finalized
in `WURZBURG_WSO2_HANDOFF.md`.

## 0. Relationship To Main Service Handoff

`WURZBURG_SERVICE_HANDOFF.md` is the broad system handoff and should be used for
service boundaries, recovery, Dragonfly/Nuremberg responsibilities, WSO2 trust,
observability, reporting/import/export direction, and integration-test style.

This Provider handoff is narrower and records the refined decisions reached
after the main handoff was written. When they differ, this document and
`WURZBURG_CARD_RANGE_POLICY_HANDOFF.md` represent the current Provider/Card
design.

For Redis publication ownership specifically, Section 13 of this document is
newer and supersedes statements in both older handoffs that assign profile
materialization directly to Wurzburg. The range-scoped CPOL keys and policy
data model remain valid; only the writer changes to Wolfsburg.

Current refinements over the main service handoff:

- Oracle is the only persistence target for this implementation path.
- Card policy Redis keys are range-scoped:
  - `CPOL:SingleProvider:{card_range_id}`
  - `CPOL:MultiProvider:{card_range_id}`
- Range emergency controls are materialized as `CRCTL:{card_range_id}`.
- `card_ranges.owner_provider_id` is not used. Provider eligibility is
  represented by `card_range_providers`.
- Each provider can be attached to exactly one card range.
- Card-range attachment is admin-only.
- Provider-user TigerBeetle accounts are the real provider-funded balance
  buckets for users.
- `card_provider_funding_sources` maps a card to one or more provider-user
  accounts and stores card-specific priority/max rules.
- Each provider may assign only one active card to each linked user. Therefore
  a provider-user account can be behind at most one active card at a time.
- There is no separate card-specific funding balance account in the current
  Provider design.
- Every assigned card receives all eight card-specific policy usage accounts.
  Nuremberg uses them for daily/weekly/monthly/yearly amount/count windows only
  when the range authority is `PLATFORM`.
- Debit, credit, balance, and usage values are never stored in Oracle. They are
  read from TigerBeetle when responses/events/Redis payloads/reports are built.
- Wurzburg never materializes `CP`, `CPOL`, `CRCTL`, or `FEE` values in Redis.
  It owns the canonical Oracle facts, TigerBeetle commands, and durable events.
- Wolfsburg is the sole Redis profile materializer. It rebuilds runtime values
  from current Oracle mappings and live TigerBeetle state.
- Wurzburg may acquire `Lock-CP:{card_number}` and delete stale CP as part of the
  shared card-mutation protocol, but only Wolfsburg writes a replacement CP and
  releases a post-commit lock.

## 1. Final Domain Scope

- Providers are legal/business entities.
- Providers can be funding sources behind card ranges.
- Each provider can be attached to exactly one card range.
- Card ranges already define the allowed funding model:
  - `SINGLE_PROVIDER`
  - `MULTI_PROVIDER`
- `card_range_providers` is already the eligibility table that says which
  providers are allowed behind a range.
- A card belongs to exactly one final user/cardholder.
- A provider can register/link users into Wurzburg, but Wurzburg owns the global
  user registry.
- If a user already exists by national identity, Wurzburg links that user to the
  requesting provider instead of creating a duplicate global user.
- Global users are uniquely identified by national ID. Each global user also
  receives an internal UUID used by Wurzburg APIs and persistence.
- Provider-assigned cards must belong to a card range for which that provider is
  eligible.
- Providers do not own card ranges. The bank/system defines card ranges and
  assigns eligible providers to those ranges.
- Providers are expected to know which card number belongs to which national ID
  before calling Wurzburg user/card APIs. The card number may come from a future
  bank-owned card-number issuing service.
- Providers have several operational states and emergency controls.
- Provider events are delivered through Kafka only when the effective platform
  gates and provider subscription permit delivery; onboarding still provisions
  Kafka access independently.
- Provider balance/credit movement uses TigerBeetle accounts.

## 2. Provider Responsibilities

A Provider can:

- be attached to exactly one card range as an eligible funding provider
- add/link users
- assign cards to users from eligible ranges
- grant credit to linked users/cards
- reclaim the full remaining credit from linked users/cards
- receive operational and financial events through Kafka
- query its own accounts and transactions
- query card-level transactions limited to its own funding participation

System admins can:

- create and configure providers
- configure provider card-range eligibility during provider lifecycle workflows
- inspect all provider/user/card/transaction data
- override or suspend provider capabilities
- query complete multi-provider card transaction history

## 3. Provider Identity Model

Provider identity must represent the legal entity and its operational contact
channels.

Provider identity fields:

- `provider_id`
- legal registered name
- trade/commercial name
- optional tax/economic identifier
- optional registration number
- email addresses
- website URL
- mailing/registered address
- lifecycle status
- operational metadata
- created/updated actor and timestamps

Users created or linked by providers are not identified externally by Wurzburg's
UUID. Provider user onboarding uses national ID as the unique external identity.
Wurzburg creates and returns an internal UUID for later API use.

Contact information is modeled separately from the provider identity because:

- finance, technical, and notification contacts have different lifecycles
- emergency operations should be able to target the correct contact group
- this avoids packing contact data into unqueryable JSON too early

Identity rules:

- `tax_id` and `registration_number` are optional informational attributes in
  Wurzburg. Neither has a database uniqueness constraint and neither is an
  activation prerequisite.
- Normalize surrounding whitespace and Persian/Arabic digits to ASCII before
  storage. Reject control characters and values longer than their schema limit;
  do not invent a Wurzburg-specific legal checksum for informational fields.
- Iranian legal persons receive official national/legal identifiers under the
  national legal-person registry regulations. Wurzburg may later verify these
  fields against an official service, but provider creation does not currently
  depend on that external verification.
- No contact type is mandatory for activation. Contacts are informational and
  operational routing data.
- An active NOTIFICATION contact with `sms_enabled = true` and a valid mobile
  number receives configured critical system SMS notifications. Absence of such
  a contact disables SMS only; it never blocks provider operation.
- Legal name, trade name, tax/economic identifier, registration number, address,
  and contacts are editable. Every change requires trusted WSO2 actor context
  and an immutable before/after audit record; it never creates a new provider.

## 4. Provider Lifecycle

Provider status:

```text
PENDING_PROVISIONING
READY
ACTIVE
SUSPENDED
INACTIVE
FAILED
```

Meaning:

- `PENDING_PROVISIONING`: required TigerBeetle account verification is not yet
  conclusive and durable recovery is still running.
- `READY`: all required TigerBeetle accounts are verified and the provider is
  waiting for explicit admin activation.
- `ACTIVE`: provider can operate within its configured limits.
- `SUSPENDED`: provider-level emergency umbrella is active; every provider-
  scoped business capability and provider-facing event is blocked.
- `INACTIVE`: provider is administratively inactive but may be activated again.
- `FAILED`: provisioning failed and needs operator intervention.

Provisioning never activates a provider automatically. The create request first
attempts bounded synchronous creation and exact lookup verification of all four
deterministic TigerBeetle accounts. It returns `READY` when verification
succeeds. If the dependency result is unavailable or uncertain, it returns
`202 PENDING_PROVISIONING` and the worker performs the same verification before
transitioning to `READY`. Kafka provisioning has its own retryable job and does
not block this transition. A platform admin then uses the explicit activation
API to transition `READY -> ACTIVE`.

Activation requires all four verified provider TigerBeetle accounts and an
operational profile effective at activation time. Kafka availability is not an
activation prerequisite; failed/suspended Kafka access disables outbound
provider events while financial/API capabilities continue according to the
operational profile. Card-range attachment is also not an activation
prerequisite; without one the provider cannot onboard cards or move card-linked
credit.

Provisioning jobs use configurable exponential backoff. Exhausting the
configured attempt limit moves the provider to `FAILED`. An idempotent admin
retry command creates a new provisioning attempt and transitions the provider
back to `PENDING_PROVISIONING`; it does not create a second provider.

Lifecycle enforcement:

- `SUSPENDED` is the provider-level equivalent of the card-range emergency
  umbrella. Provider-scoped onboarding, card commands, grant/return,
  transaction/report access, credential retrieval/rotation, and provider-facing
  Kafka publication are rejected/suppressed.
- Platform-admin inspection, audit, reconciliation, recovery, and lifecycle
  commands remain available while suspended.
- Suspending one provider behind a multi-provider range removes only that
  provider from new Balance/Confirm funding decisions; other eligible providers
  remain usable. Rollback continues from immutable FundingPlan facts unless a
  range-level CMS control blocks the external operation.
- Independent operational-profile switches control capabilities while status is
  ACTIVE. Disabling one capability does not imply provider suspension.
- `SUSPENDED` and `INACTIVE` may transition back to ACTIVE through the normal
  idempotent activation command. Activation validates only current mandatory
  prerequisites: provider TigerBeetle accounts and an effective operational
  profile. It does not require contacts, Kafka, or a card-range attachment.
- `FAILED` is reserved for failed core provisioning and returns to
  `PENDING_PROVISIONING` through the retry command.

## 5. Card Range Relationship

The already-agreed model remains correct:

```text
card_range_providers(card_range_id, provider_id, status, metadata_json)
```

Rules:

- This table means eligibility only.
- Each provider can have only one active card-range attachment.
- It does not store cardholder priority.
- It does not store relationship type.
- For `SINGLE_PROVIDER`, exactly one active provider is allowed before range
  activation.
- For `MULTI_PROVIDER`, one or more active providers may exist.
- Provider APIs that create cards/users must verify that the provider is active
  behind the selected card range.
- Only lifecycle `ACTIVE` providers may hold an ACTIVE range attachment;
  `READY` is not operational eligibility.
- Card-range eligibility is selected during provider creation and provider
  lifecycle workflows; Wurzburg does not expose a standalone CARD API that
  detaches a provider from a range.
- The relationship row stores current lifecycle state. Every attach, suspend,
  reactivate, move, provider deactivation, or operational-control transition
  writes an immutable audit snapshot.
- Provider and card-range relationship rows are never physically deleted.
  `SUSPENDED` and `INACTIVE` lifecycle controls stop new use while preserving
  every relationship, financial fact, audit record, TigerBeetle account, and
  TigerBeetle transfer.
- Suspending eligibility blocks new onboarding, card assignment, credit grant,
  and use of that provider in new Balance/Confirm decisions. Full credit return
  remains allowed so the provider can reclaim existing user credit.
- Affected card funding sources are suspended and CP refresh is requested. If
  the final active provider eligibility is removed, the range is suspended as
  defined by the card-range handoff.

## 6. Global User Registry And Provider Links

Wurzburg has one global user registry.

Rules:

- User uniqueness is based on national identity.
- `user_id` is an internal UUID generated by Wurzburg.
- If the national ID does not exist, create a global user then link.
- If the national ID exists, link the existing user to the provider.
- `provider_customer_reference` is required, belongs to `provider_users`, is
  unique within one provider, and is immutable after creation.
- Mandatory onboarding input is `national_id`, `first_name`, `last_name`,
  `card_number`, and `provider_customer_reference`; `provider_id` comes from the
  trusted path/JWT scope. Server timestamps are generated by Wurzburg. Mobile,
  birth date, and metadata are optional.
- Normalize Persian/Arabic digits to ASCII while preserving leading zeroes.
  `national_id` must be exactly 10 digits and pass the Iranian national-code
  checksum; `card_number` must be exactly 16 digits and pass the range/card
  validation finalized in the card handoff. Names and provider customer
  references are trimmed non-empty strings of at most 255 characters.
- If an existing national ID has different submitted first/last names, Wurzburg
  retains the canonical global values, creates/uses the provider link, stores
  the provider-submitted identity snapshot on that link, and sets
  `identity_mismatch = true` with mismatched field names. The command succeeds
  and remains searchable/auditable for review.
- A later provider does not need an additional Wurzburg verification workflow.
  Trusted provider identity, matching national ID/PAN ownership, range
  eligibility, and card/provider cardinality checks are sufficient.

## 7. Provider User And Card Ledger Accounts

Provider user onboarding is not only an identity/link operation. It also creates
the ledger/control accounts that later make provider credit, card funding, and
Nuremberg limits work.

When a provider adds a user:

1. Wurzburg finds or creates the global `users` row by `national_id`.
2. Wurzburg creates or reuses the `provider_users` link.
3. Wurzburg creates or reuses the provider-user TigerBeetle account for
   `(provider_id, user_id)`. This account is the real balance bucket funded by
   that provider for that user.
4. Wurzburg stores only the TigerBeetle account UUID and account mapping in
   Oracle. Debit, credit, and balance still come only from TigerBeetle.

Meaning:

- A provider may assign only one active card to each linked user.
- A provider-user account can be behind at most one active card at a time.
- If the physical card is lost and the bank reprints the same card number, no
  new Wurzburg card/funding-source account mapping is needed.
- If the old card number must be retired/released and a new card number is
  registered for the same user/provider, Wurzburg must suspend/close the old
  card funding-source row before attaching the same provider-user account to
  the new card.
- A retired PAN is never reassigned to another user. Card replacement with a
  different PAN creates a new card row and preserves the old row for audit.

When a provider assigns a card or becomes a funding source behind a card,
Wurzburg links that card to the existing provider-user account through
`card_provider_funding_sources`.

Important rule:

- `provider_user_accounts` are the actual funded accounts. If four providers
  are behind a user's card, there are four provider-user accounts, each with its
  own TigerBeetle balance.
- `card_provider_funding_sources` does not create another balance account. It
  says which provider-user accounts are allowed behind this card and stores
  card-specific selection rules such as priority and maximum consumable amount.
- The account published to Nuremberg inside `CP:{card_number}` as
  `user_provider_account` is the provider-user account selected for that
  card/provider funding source. Wolfsburg materializes this mapping from
  Wurzburg-owned facts.
- On Confirm, Nuremberg consumes providers according to the card-specific
  priority/max rules and debits the corresponding provider-user TigerBeetle
  account.
- The same provider-user account must not be active behind multiple cards at
  the same time.

Each assigned card needs its own policy usage account set regardless of range
authority. These accounts are technical TigerBeetle accounts used by Nuremberg
to track limit consumption when authority is `PLATFORM`; Nuremberg ignores them
when authority is `CMS`.

These eight accounts correspond to:

```text
amount: daily, weekly, monthly, yearly
count:  daily, weekly, monthly, yearly
```

Rules:

- Create and verify all eight policy usage accounts when the card is assigned,
  before its CP first becomes available.
- Store the account UUIDs durably in Oracle.
- Never store usage amount/count balances in Oracle.
- Nuremberg reads these account UUIDs from `CP:{card_number}` and uses
  TigerBeetle to apply the policy windows.

Onboarding and movement rules:

- Single-user onboarding is one atomic business command: find/create user,
  create/reuse provider link, assign/attach the card, synchronously provision
  the provider-user account and all eight policy usage accounts, persist all
  mappings, and enqueue the CP refresh event.
- The API does not return success until every Oracle/TigerBeetle account and
  mapping required by that row is verified. CP materialization remains
  asynchronous; Nuremberg fails closed until Wolfsburg publishes CP.
- A provider may grant credit immediately after the atomic onboarding command,
  even while initial CP materialization is pending.
- Credit return is a full-balance operation, not an arbitrary reduction. The
  caller supplies the remaining balance it last observed. While holding the
  card mutation lock, Wurzburg reads the live spendable provider-user balance
  from TigerBeetle. The operation succeeds only when both amounts match, then
  transfers that entire live remainder back to `PROVIDER_OWNED`. A mismatch
  returns `409 PROVIDER_USER_BALANCE_CHANGED` with the current authorized
  balance and performs no ledger movement.
- A provider and the authenticated cardholder may request the same full-balance
  return. Both entry points call one application command, WAL, TigerBeetle
  transfer, CP refresh, audit, and provider-event path. The initiator is stored
  as `PROVIDER` or `CARDHOLDER`.
- File batches are asynchronous MinIO jobs, not one atomic HTTP transaction.
  The original file is stored in MinIO; each row executes the same atomic single-
  item command independently; the result/error for every row is written to a
  result file in MinIO. APIs expose job status and authorized downloads for both
  original and result files.

### Final Sync/Async Onboarding Decision

Provider creation uses a synchronous TigerBeetle fast path because it is a
low-volume administration command and a definitive response is operationally
valuable. The same deterministic account IDs and durable provisioning job make
timeouts, process loss, and uncertain dependency outcomes recoverable. Kafka
infrastructure remains asynchronous and independent from Provider readiness.

Single-user onboarding remains synchronous. The caller receives one definitive
response only after the global user/link, card mapping, provider-user account,
all eight usage accounts, audit facts, and outbox intent are durably verified.
Making this API asynchronous would add polling and intermediate product states
without improving correctness; deterministic account IDs and a durable
onboarding command record handle crash recovery inside the synchronous flow.

Bulk onboarding is asynchronous only at the file-job boundary. The worker
executes the exact synchronous single-user command independently for each row.
Therefore the platform has one onboarding business contract, not separate
single and batch semantics.

## 8. Cards And Provider Funding Sources

Each card belongs to exactly one user. The card stores its `card_range_id`, and
its funding mode is always derived from that range. `funding_mode` must not be
duplicated on the `cards` table.

Rules:

- Card ranges are defined by the bank/system and assigned to providers through
  `card_range_providers`.
- Provider onboarding/user-card APIs receive the card number already known to
  the provider.
- Wurzburg validates that the submitted card number belongs to a card range
  where the provider is active and eligible.
- For a single-provider card, one active funding source is allowed.
- For a multi-provider card, multiple active funding sources are allowed.
- Cardholder funding priority and provider cap are card-specific, not
  range-specific.
- This model is the canonical input used by Wolfsburg when materializing
  `CP:{card_number}`.

## 9. Provider Ledger Accounts

Each provider has four provider-level TigerBeetle accounts:

```text
PROVIDER_OWNED
PROVIDER_FEE
CMS_SETTLEMENT
PLATFORM_FEE
```

Definitions:

- `PROVIDER_OWNED`: provider distributed-credit control account. When the
  provider grants 100 units of credit to a user, value moves from this account
  to that provider-user/card account. When credit is reduced/reclaimed from the
  user, value moves back to this account. Therefore this account reflects the
  provider's currently distributed credit position across its users. This
  account may go negative.
- `PROVIDER_FEE`: account used when fees are charged to the provider instead of
  the user/card balance. This account has no required initial balance and may go
  negative.
- `CMS_SETTLEMENT`: account that receives money when Nuremberg confirms a cash
  withdrawal from provider-user/card balance.
- `PLATFORM_FEE`: account that receives calculated platform fee, regardless of
  whether the fee source was user/card balance or provider fee balance.

Rules:

- Store provider ledger accounts as rows by account category.
- Create/provision all four account categories for every provider.
- Oracle stores only account identity, mapping, lifecycle, and configuration
  data for ledger accounts.
- Debit, credit, and balance values must never be persisted in Oracle/RDBMS.
  TigerBeetle is the only source of truth for posted/pending debit, posted/
  pending credit, and computed balance.
- Any API response, event payload, Redis object, or report that includes debit,
  credit, or balance must read those values from TigerBeetle at generation time.
- API `account_name` values are stable display labels derived from
  `account_category`; they are not a second persisted account identity.
- Provider-user/card ledger accounts should be a separate table, not stored in
  `provider_ledger_accounts`.
- Fee calculation and payer selection are controlled by the provider's active,
  versioned fee profile. If `fee_payer` is `PROVIDER`, Nuremberg debits
  `PROVIDER_FEE` and credits `PLATFORM_FEE`. If it is `PROVIDER_USER`,
  Nuremberg debits the participating provider-user account and credits
  `PLATFORM_FEE`.

All Wurzburg money and usage accounts belong to one configured TigerBeetle
ledger. TigerBeetle cannot transfer between ledgers, so provider accounts,
provider-user accounts, settlement accounts, fee accounts, and policy usage
accounts must never be split across ledgers. Each category has a distinct,
stable TigerBeetle account code. The ledger ID and code map are deployment
configuration validated at startup; they are not provider-specific and cannot
be changed while accounts exist.

All four provider-level accounts may have a negative signed balance. The
provider-user account must reject a debit that would make it negative. Policy
usage accounts follow the debit/credit convention defined by Nuremberg's limit
account contract.

### Provider Fee Profiles

Each provider has at most one `DRAFT` and one `ACTIVE` fee profile. Historical
profiles are immutable and remain `SUPERSEDED` after replacement.

```text
ProviderFeeProfile:
- provider_fee_profile_id
- provider_id
- version
- rate_bps
- fixed_amount_rials
- fee_payer: PROVIDER_USER | PROVIDER
- status: DRAFT | ACTIVE | SUPERSEDED
- publication_operation_id
- immutable lifecycle/audit fields
```

The fee for each provider allocation in a Nuremberg funding plan is:

```text
fee = ceil(provider_principal * rate_bps / 10000) + fixed_amount_rials
```

Rules:

- `rate_bps` is between `0` and `10000`, inclusive.
- `fixed_amount_rials` is between `0` and `9007199254740991`, inclusive.
- A zero-rate, zero-fixed profile is valid and explicitly means no fee.
- Provider creation does not require a fee profile.
- Card-range attachment requires either an editable DRAFT or an ACTIVE fee
  profile. Attaching a provider freezes and publishes its initial DRAFT.
- A card range cannot become ACTIVE until every attached active provider has an
  ACTIVE fee profile confirmed by a Wolfsburg materialization receipt.
- Updating an attached provider creates or updates a replacement DRAFT and
  publishes it immediately. The old ACTIVE profile remains authoritative until
  the matching receipt atomically activates the replacement.
- There is no cancel or manual publication-retry API. Durable outbox retry and
  operational recovery own delivery; ambiguous runtime state is never exposed.
- Provider suspension preserves `FEE:{provider_id}` and all fee history. Runtime
  eligibility is controlled separately through provider/range/card controls.

Platform-admin APIs:

```text
PUT /api/v1/providers/{provider_id}/fee-profile
GET /api/v1/providers/{provider_id}/fee-profile
GET /api/v1/providers/{provider_id}/fee-profiles
GET /api/v1/providers/{provider_id}/fee-profiles/{fee_profile_id}
```

The current-profile API returns only ACTIVE state. DRAFT and SUPERSEDED records
are available through history and by-ID APIs.

Accordingly, TigerBeetle provider-level accounts do not set
`debits_must_not_exceed_credits` or `credits_must_not_exceed_debits`.
Provider-user accounts set `debits_must_not_exceed_credits`. Account flags,
ledger ID, and account code are verified after creation and during
reconciliation.

TigerBeetle exposes unsigned debit and credit counters. Wurzburg presents the
following signed decimal-string values without persisting them in Oracle:

```text
posted_balance = credits_posted - debits_posted
effective_balance =
  credits_posted + credits_pending - debits_posted - debits_pending
```

Ledger APIs return all four source counters plus `posted_balance` and
`effective_balance`. Positive means net credit and negative means net debit for
every category. Business controls use the exact category-specific measure
defined in section 10 rather than interpreting a generic `balance` field.

## 10. Provider Operational Controls

Provider controls should be explicit and versioned/auditable.

- Provider operational profiles are scheduled.
- A future-dated profile is created with `SCHEDULED` status.
- A profile becomes `ACTIVE` only after `effective_at`.
- Activating a profile and superseding the previous active profile occur in one
  Oracle transaction.
- APIs and workers resolve the active provider profile using `status = ACTIVE`
  and `effective_at <= SYSTIMESTAMP`, ordered by version descending. This query
  rule is retained as a safety net even when the scheduler is delayed.
- Creating a future-dated profile must not affect current provider behavior
  until its effective time.
- A lightweight in-process scheduler promotes due profiles. Every command/read
  that resolves an operational profile also performs the same due-profile
  promotion under an Oracle provider lock before evaluation. This makes
  `effective_at` authoritative even if the scheduler was delayed or restarted;
  no separate activation API is required.

Operational profile body:

```json
{
  "timezone": "Asia/Tehran",
  "user_onboarding": {
    "enabled": true,
    "active_windows": [],
    "max_total_users": null
  },
  "credit_grant": {
    "enabled": true,
    "mode": "FixedLimit",
    "limit_amount_rials": 1000000000
  },
  "credit_return": {
    "enabled": true
  },
  "card_operations": {
    "new_assignment_enabled": true,
    "same_pan_reprint_enabled": true,
    "new_pan_replacement_enabled": true,
    "attach_existing_multi_provider_card_enabled": true
  },
  "event_delivery": {
    "enabled": true,
    "disabled_reason": null
  }
}
```

`credit_grant` controls whether and how much a provider may grant credit to its
linked users. `credit_return` controls whether the provider or cardholder may
return the full remaining provider-user balance.

`event_delivery` is the platform-controlled, scheduled provider-wide gate for
outbound events. Per-event decisions belong only to
`provider_event_subscriptions`, avoiding two competing allowlists. Disabling
the gate requires an operator reason and never disables internal
Wurzburg-to-Wolfsburg materialization events.

Effective provider event delivery is layered:

```text
global platform kill switch
AND active provider operational-profile event_delivery gate
AND provider subscription for the event type
AND provider status = ACTIVE
AND Kafka credential status = ACTIVE
```

The global kill switch is versioned Oracle business configuration intended for
platform-wide emergencies. Provider subscriptions are platform-admin delivery
decisions configured through the admin UI/API; providers cannot enable an event
that the platform has disabled.

Canonical global config key:

```text
provider_event_delivery.global_enabled = true | false
```

Changing it requires platform-admin scope, reason, version increment, and audit.

At provisioning, Wurzburg seeds every currently supported provider event type
as disabled. A platform admin explicitly enables the desired set after the
provider confirms contract readiness. Event types introduced in a later schema
release also default to disabled for every existing provider, preventing an
unexpected payload from reaching an older consumer.

Credit grant limit modes:

```text
FixedLimit
CmsDebtLimit
OutstandingCreditLimit
```

All monetary limits are integer Iranian rials. Each mode applies one precise
test against `limit_amount_rials`:

- `FixedLimit`: projected total distributed credit, measured as net debits of
  `PROVIDER_OWNED`, must remain within the configured limit.
- `CmsDebtLimit`: current provider debt to CMS, measured as net credits of
  `CMS_SETTLEMENT`, must be below the configured limit before the grant. The
  grant is rejected when the debt has reached or exceeded the limit.
- `OutstandingCreditLimit`: projected outstanding user credit must remain
  within the limit. Outstanding credit is
  `max(0, PROVIDER_OWNED net debits - CMS_SETTLEMENT net credits)`.

The service reads the required counters from TigerBeetle in the grant command;
Oracle never stores cached exposure balances. A profile selects exactly one
mode and supplies exactly one non-negative `limit_amount_rials`.

`max_total_users` counts every distinct user ever linked to the provider,
including suspended and closed links. It is a lifetime onboarding ceiling, not
a concurrent-active-user limit.

`active_windows` is either empty, meaning unrestricted time, or a list of
weekly recurring windows:

```json
{
  "days": ["SATURDAY", "SUNDAY"],
  "start_local_time": "08:00:00",
  "end_local_time": "18:00:00"
}
```

The profile's IANA `timezone` applies to every window. Start is inclusive and
end is exclusive. A window cannot cross midnight; split it into two windows.
Overlapping windows are rejected. Evaluation uses the timezone database in the
running release, so daylight-saving behavior follows that zone automatically.

The four `card_operations` switches are independent. Provider `SUSPENDED`
remains the umbrella control above every capability switch; a separate
`read_only` flag would duplicate that rule.

An active profile is immutable. At most one future `SCHEDULED` profile exists
per provider. Creating a replacement atomically marks the previous candidate
`CANCELLED`, creates a new version, and writes both snapshots to audit. A
scheduled candidate may also be cancelled explicitly. At `effective_at`, the
new profile becomes active and the old active profile becomes `SUPERSEDED` in
one Oracle transaction. There is no multi-profile schedule queue.

`effective_at` is stored only in its dedicated Oracle timestamp column. The
`profile_json` document contains operational controls only; duplicating the
schedule timestamp inside JSON would create two competing sources of truth.

The scheduler configuration is deployment configuration, not business data:

```toml
[provider_operational_profile_scheduler]
enabled = true
batch_size = 50
poll_interval_ms = 1000
```

Every replica may run the scheduler. Oracle provider-row locks and
`SKIP LOCKED` make due-profile promotion safe across replicas. The worker is
owned by the process lifecycle, propagates OTel context into Oracle work, and
must drain during graceful shutdown.

## 11. Kafka Provisioning

Provider onboarding provisions Kafka resources asynchronously. The provisioning
worker requires these broker-level capabilities:

```text
create_provider_topic(topic_name)
delete_provider_topic(topic_name)
create_scram_user(username, password)
delete_scram_user(username)
grant_consumer_acls(topic_name, consumer_group, username)
revoke_consumer_acls(topic_name, consumer_group, username)
```

Adapter behavior:

- Topic creation/deletion uses the native Kafka Admin API. Existing-topic and
  missing-topic broker results are idempotent success respectively.
- Wurzburg uses librdkafka's native Admin API through a memory-safe Rust adapter
  for SCRAM-user mutation and exact ACL administration. The runtime never
  executes Kafka shell scripts.
- SCRAM and ACL provisioning remains part of the durable asynchronous provider
  job. A successful topic creation alone does not complete Kafka provisioning.
- `grant_consumer_acls` grants `Read` and `Describe` on the provider topic and
  `Read` only on that provider's consumer-group prefix. It must not grant access
  to all consumer groups.
- `revoke_consumer_acls` removes the corresponding topic and consumer-group
  permissions during suspension or credential replacement.

Every administration call has a deadline, safe diagnostic mapping, tracing, and
idempotent replay behavior. A Kafka admin failure must be persisted on the
provisioning job and must never be converted into successful Kafka provisioning.

### Provisioning Design

- Provider provisioning has a synchronous TigerBeetle fast path backed by the
  same durable recovery job used after timeout or process loss.
- `POST /providers` creates the provider in `PENDING_PROVISIONING`, then creates
  and verifies all four deterministic TigerBeetle accounts within the bounded
  HTTP deadline. It returns `READY` when that verification completes, or `202`
  with `PENDING_PROVISIONING` when recovery must continue after the response.
- Wurzburg stores durable provisioning job rows.
- A worker resumes uncertain or incomplete TigerBeetle provisioning by exact
  account lookup. Kafka provisioning always runs independently and
  asynchronously because broker administration is not part of HTTP success.
- Provider becomes `READY` after required TigerBeetle provisioning succeeds;
  activation remains an explicit platform-admin command. Kafka provisioning is
  independent and may continue or be retried after the provider is ready.
- A retryable core TigerBeetle failure keeps the provider in
  `PENDING_PROVISIONING` with the next attempt time. Exhausting its configured
  attempt limit moves the provider to `FAILED`. A Kafka job may itself become
  `FAILED`, but it leaves the provider lifecycle unchanged and may be retried
  independently.
- Kafka is outbound from Wurzburg to providers for now. Provider operations are
  requested through HTTP APIs, not inbound Kafka commands.
- Kafka credentials are retrieved through a dedicated API after provisioning.
- Kafka passwords are envelope-encrypted before Oracle persistence and are
  never logged, traced, or returned by ordinary provider APIs.
- The topic and username templates are resolved before storage. Do not persist
  literal placeholders such as `{provider_id_simple}`.

Provisioning steps:

1. Generate `provider_id`.
2. Resolve Kafka names:
   - `topic_name = provider.events.{provider_id.simple()}`
   - `username = provider_user_{provider_id.simple()}`
   - `consumer_group = provider_group_{provider_id.simple()}`
   - `password = random UUID without hyphens`
3. Insert provider row, Kafka access row, initial event-subscription rows, four
   ledger account rows, and provisioning job rows in Oracle.
4. Worker creates the Kafka topic.
5. Worker creates the SCRAM-SHA-512 user.
6. Worker grants topic and provider-scoped consumer-group ACLs for provider
   events.
7. Worker provisions TigerBeetle accounts.
8. Worker marks provisioning jobs `SUCCEEDED`.
9. Worker marks provider `READY` after all required TigerBeetle resources are
   verified. Kafka failure is visible and retryable but does not block readiness
   or later provider activation.

Kafka access table:

```text
provider_kafka_access
- provider_kafka_access_id RAW(16) primary key
- provider_id RAW(16) not null unique
- topic_name VARCHAR2(255) not null unique
- username VARCHAR2(255) not null unique
- consumer_group VARCHAR2(255) not null unique
- security_protocol VARCHAR2(64) not null
- sasl_mechanism VARCHAR2(64) not null
- bootstrap_servers_json JSON not null
- security_cert CLOB nullable
- credential_status PROVISIONING | ACTIVE | ROTATING | SUSPENDING | SUSPENDED |
  RESUMING | REVOKED | FAILED
- last_delivered_at TIMESTAMP WITH TIME ZONE nullable
- rotated_at TIMESTAMP WITH TIME ZONE nullable
- created_at
- updated_at
```

Credential history is separate from stable connection metadata:

```text
provider_kafka_credentials
- provider_kafka_credential_id RAW(16) primary key
- provider_kafka_access_id RAW(16) not null
- provider_id RAW(16) not null
- credential_version NUMBER(19,0) not null
- password_ciphertext VARCHAR2(4000) not null
- encryption_key_version VARCHAR2(128) not null
- status CANDIDATE | ACTIVE | SUPERSEDED | REVOKED | FAILED
- activated_at nullable
- superseded_at nullable
- revoked_at nullable
- created_at
- updated_at
```

Oracle enforces at most one ACTIVE and one CANDIDATE credential per provider.
Rotation keeps the ACTIVE credential readable until broker mutation and
verification succeed, then promotes the CANDIDATE and supersedes the old row in
one Oracle transaction.

Credential creation, reveal, rotation, suspension, and revocation write an
immutable credential audit row containing actor, action, credential version,
timestamp, and reason. Audit rows never contain plaintext passwords.

Provisioning job table:

```text
provider_provisioning_jobs
- provider_provisioning_job_id RAW(16) primary key
- provider_id RAW(16) not null
- job_type TIGERBEETLE_PROVISION | KAFKA_PROVISION | KAFKA_ROTATE |
  KAFKA_SUSPEND | KAFKA_RESUME
- status PENDING | RUNNING | SUCCEEDED | FAILED | CANCELLED
- attempt_count NUMBER not null
- next_attempt_at TIMESTAMP WITH TIME ZONE nullable
- locked_by VARCHAR2(255) nullable
- locked_until TIMESTAMP WITH TIME ZONE nullable
- error_code VARCHAR2(128) nullable
- error_message VARCHAR2(2000) nullable
- request_json JSON default '{}' not null
- result_json JSON default '{}' not null
- created_at
- updated_at
```

Kafka credential retrieval API:

```text
GET /api/v1/providers/{provider_id}/kafka/credentials
GET /api/v1/providers/{provider_id}/kafka/status
```

Response:

```json
{
  "provider_id": "uuid",
  "topic": "provider.events.4f3c2e1a0b9d4c7e8f6a123456789abc",
  "brokers": ["host:port"],
  "security_protocol": "SASL_SSL",
  "sasl_mechanism": "SCRAM-SHA-512",
  "username": "provider_user_4f3c2e1a0b9d4c7e8f6a123456789abc",
  "consumer_group": "provider_group_4f3c2e1a0b9d4c7e8f6a123456789abc",
  "password": "stored-provider-password",
  "security_cert": "certificate content",
  "credential_version": 1,
  "credential_status": "ACTIVE"
}
```

The status endpoint is the polling resource for asynchronous administration. It
never returns a password, ciphertext, certificate, or broker error text:

```json
{
  "provider_id": "uuid",
  "access_status": "PROVISIONING",
  "active_credential_version": null,
  "candidate_credential_version": 1,
  "latest_operation": {
    "operation_id": "uuid",
    "operation_type": "KAFKA_PROVISION",
    "status": "PENDING",
    "attempt_count": 0,
    "error_code": null
  }
}
```

The same certificate content is also available from the dedicated endpoint:

```text
GET /api/v1/providers/kafka/certificate
```

Topic partition count, replication factor, retention, maximum message size,
security protocol, and SASL mechanism are platform deployment configuration and
are identical for all providers. The platform generates one exact consumer
group per provider and grants ACLs only to that group.

The authorized credential endpoint returns the currently usable password on
every call. Passwords remain envelope-encrypted in Oracle and are never exposed
by provider detail/list APIs, logs, audit snapshots, metrics, or traces. Both
the credential response and the separate certificate endpoint return the
security certificate so provider integration is self-contained.

## 12. Provider Database Contract

All Provider tables should be Oracle-native and should follow the current
project conventions:

- UUIDs stored as `RAW(16)`
- JSON stored as Oracle `JSON`
- monetary values stored as integer Iranian rials using `NUMBER(38,0)`
- status values constrained with check constraints
- immutable/versioned rows for operational profiles
- `created_at` and `updated_at` populated consistently
- indexes for every high-frequency lookup path

### Final Provider Schema Baseline

The existing Provider DDL is development draft material, not a compatibility
boundary. Before the first release, rewrite/squash it into clean Oracle baseline
migrations containing only the final schema in this handoff. Do not preserve
legacy provider columns, lifecycle states, account-category constraints, or
foreign-key names through ALTER/backfill compatibility logic.

Rebuild the disposable Oracle schema through the configured force-rebuild path.
After the first production/staging baseline is released, migrations become
immutable and all subsequent schema changes use new forward migrations.

### providers

```text
providers
- provider_id RAW(16) primary key
- legal_name VARCHAR2(255) not null
- trade_name VARCHAR2(255) not null
- tax_id VARCHAR2(64) nullable
- registration_number VARCHAR2(128) nullable
- email_address VARCHAR2(255) nullable
- website_url VARCHAR2(512) nullable
- mailing_address VARCHAR2(2000) nullable
- status PENDING_PROVISIONING | READY | ACTIVE | SUSPENDED | INACTIVE | FAILED
- metadata_json JSON default '{}' not null
- created_by VARCHAR2(255) not null
- updated_by VARCHAR2(255) nullable
- created_at TIMESTAMP WITH TIME ZONE default SYSTIMESTAMP not null
- updated_at TIMESTAMP WITH TIME ZONE default SYSTIMESTAMP not null
```

Indexes/constraints:

```text
index(status)
index(tax_id)
index(registration_number)
```

### card_range_providers

This table already exists for card-range eligibility. Provider implementation
must also enforce the final Provider rule while preserving attachment history:

```text
card_range_providers
- card_range_id RAW(16) not null
- provider_id RAW(16) not null
- status ACTIVE | SUSPENDED
- metadata_json JSON default '{}' not null
- created_at
- updated_at
- primary key(card_range_id, provider_id)
```

Active-row constraint:

```text
at most one ACTIVE row per provider_id
```

In Oracle this requires a function-based unique index (or an equivalent current
assignment model); a normal `unique(provider_id)` would also block historical
suspended rows. In a multi-provider card range, many providers can point to the
same `card_range_id`, but each individual provider still has only one active
range.

### provider_contacts

```text
provider_contacts
- provider_contact_id RAW(16) primary key
- provider_id RAW(16) not null
- contact_type FINANCE | TECHNICAL | OPERATIONS | SECURITY | NOTIFICATION | LEGAL
- name VARCHAR2(255) nullable
- email VARCHAR2(255) nullable
- phone VARCHAR2(64) nullable
- mobile VARCHAR2(64) nullable
- sms_enabled NUMBER(1) default 0 not null
- metadata_json JSON default '{}' not null
- status ACTIVE | SUSPENDED
- created_at
- updated_at
```

Indexes:

```text
index(provider_id, contact_type, status)
check(sms_enabled in (0, 1))
```

### provider_user_accounts

```text
provider_user_accounts
- provider_user_account_id RAW(16) primary key
- provider_id RAW(16) not null
- user_id RAW(16) not null
- tigerbeetle_account_id RAW(16) not null unique
- status PROVISIONING | ACTIVE | SUSPENDED | CLOSED | FAILED_PROVISIONING
- metadata_json JSON default '{}' not null
- created_at
- updated_at
```

Do not add debit, credit, or balance columns to this table.

Constraints:

```text
unique(provider_id, user_id)
```

### card_policy_usage_accounts

```text
card_policy_usage_accounts
- card_id RAW(16) primary key
- amount_daily_limit_account RAW(16) not null unique
- amount_weekly_limit_account RAW(16) not null unique
- amount_monthly_limit_account RAW(16) not null unique
- amount_yearly_limit_account RAW(16) not null unique
- count_daily_limit_account RAW(16) not null unique
- count_weekly_limit_account RAW(16) not null unique
- count_monthly_limit_account RAW(16) not null unique
- count_yearly_limit_account RAW(16) not null unique
- status PROVISIONING | ACTIVE | SUSPENDED | FAILED_PROVISIONING
- created_at
- updated_at
```

Do not add consumed amount/count columns to this table.

Every assigned card has exactly one row containing all eight accounts. Missing
usage-account data is incomplete provisioning for both authorities, even though
Nuremberg does not read the accounts in `CMS` mode.

### provider_ledger_accounts

```text
provider_ledger_accounts
- provider_ledger_account_id RAW(16) primary key
- provider_id RAW(16) not null
- account_category PROVIDER_OWNED | PROVIDER_FEE | CMS_SETTLEMENT | PLATFORM_FEE
- tigerbeetle_account_id RAW(16) not null unique
- status ACTIVE | SUSPENDED | CLOSED
- metadata_json JSON default '{}' not null
- created_at
- updated_at
```

Do not add debit, credit, or balance columns to this table.

Constraints:

```text
unique(provider_id, account_category)
```

Provider creation enqueues provisioning for all four account categories.

### provider_operational_profiles

```text
provider_operational_profiles
- provider_operational_profile_id RAW(16) primary key
- provider_id RAW(16) not null
- status SCHEDULED | ACTIVE | SUPERSEDED | CANCELLED
- version NUMBER(19,0) not null
- effective_at TIMESTAMP WITH TIME ZONE not null
- profile_json JSON not null
- superseded_by_profile_id RAW(16) nullable
- created_by VARCHAR2(255) not null
- updated_by VARCHAR2(255) not null
- created_at TIMESTAMP WITH TIME ZONE default SYSTIMESTAMP not null
- activated_at TIMESTAMP WITH TIME ZONE nullable
- superseded_at TIMESTAMP WITH TIME ZONE nullable
- cancelled_at TIMESTAMP WITH TIME ZONE nullable
- updated_at TIMESTAMP WITH TIME ZONE default SYSTIMESTAMP not null
```

Indexes/constraints:

```text
unique(provider_id, version)
index(provider_id, status, effective_at)
at most one SCHEDULED row per provider_id through a function-based unique index
```

### provider_bank_accounts

Provider bank accounts are useful and should stay as a future table. Provider
exposure/credit limits belong to the versioned operational profile above, not a
second standalone table in this handoff.

```text
provider_bank_accounts
- provider_bank_account_id RAW(16) primary key
- provider_id RAW(16) not null
- bank_name
- account_holder_name
- account_number
- sheba_number
- card_number nullable
- currency IRR | USD | EUR
- is_default NUMBER(1)
- status ACTIVE | SUSPENDED | CLOSED
- created_at
- updated_at
```

### provider_event_subscriptions

Provider preferences are current-state rows with immutable audit snapshots for
every change:

```text
provider_event_subscriptions
- provider_event_subscription_id RAW(16) primary key
- provider_id RAW(16) not null
- event_type VARCHAR2(128) not null
- enabled NUMBER(1) not null
- version NUMBER(19,0) not null
- updated_by VARCHAR2(255) not null
- reason VARCHAR2(1000) nullable
- created_at TIMESTAMP WITH TIME ZONE default SYSTIMESTAMP not null
- updated_at TIMESTAMP WITH TIME ZONE default SYSTIMESTAMP not null
```

Constraints:

```text
unique(provider_id, event_type)
check(enabled in (0, 1))
```

A complete subscription replacement locks the provider's set, compares
`expected_version` with the current common row version, increments once, and
assigns that new version to every supported event-type row in one Oracle
transaction. The common audit log stores the complete before/after sets.

### users, provider_users, cards

These tables are part of later provider-user/card slices. They are included here
only to clarify the relationship model:

- `users` is the global Wurzburg user registry.
- Users are unique by `national_id`.
- `provider_users` links a global user to a provider.
- `provider_user_accounts` is the funded TigerBeetle balance account for one
  provider and one user.
- `cards` links one card number to exactly one global user and one card range.
- `card_provider_funding_sources` links a card to one or more funding
  providers. For a single-provider range it has one active row. For a
  multi-provider range it can have multiple active rows.
- Each `card_provider_funding_sources` row points to the provider-user account
  used as the funding balance for that card/provider.
- Cardholder funding priority is stored on the card funding source, not on
  `card_range_providers`.

```text
users
- user_id RAW(16) primary key
- national_id VARCHAR2(32) unique not null
- first_name VARCHAR2(255) not null
- last_name VARCHAR2(255) not null
- birth_date DATE nullable
- mobile VARCHAR2(64) nullable
- metadata_json JSON default '{}' not null
- status ACTIVE | SUSPENDED
- created_at
- updated_at

provider_users
- provider_id RAW(16) not null
- user_id RAW(16) not null
- provider_customer_reference VARCHAR2(255) not null
- supplied_first_name VARCHAR2(255) not null
- supplied_last_name VARCHAR2(255) not null
- identity_mismatch NUMBER(1) default 0 not null
- status PROVISIONING | ACTIVE | SUSPENDED | FAILED_PROVISIONING
- metadata_json JSON default '{}' not null
- created_at
- updated_at
- primary key(provider_id, user_id)
- unique(provider_id, provider_customer_reference)
- check(identity_mismatch in (0, 1))

cards
- card_id RAW(16) primary key
- card_number VARCHAR2(32) unique not null
- user_id RAW(16) not null
- card_range_id RAW(16) not null
- state_version NUMBER(19,0) default 0 not null
- status PROVISIONING | ACTIVE | SUSPENDED | EXPIRED | FAILED_PROVISIONING
- metadata_json JSON default '{}' not null
- created_at
- updated_at

card_provider_funding_sources
- card_id RAW(16) not null
- provider_id RAW(16) not null
- provider_user_account_id RAW(16) not null
- priority NUMBER nullable while not ACTIVE
- max_amount NUMBER(38,0) nullable
- status PROVISIONING | ACTIVE | SUSPENDED | CLOSED | FAILED_PROVISIONING
- metadata_json JSON default '{}' not null
- primary key(card_id, provider_id)
```

Active rows require a non-null priority that is unique within the card. A
provider-user account may appear in at most one ACTIVE funding-source row; use
an Oracle function-based unique index so suspended/closed history is retained.
`max_amount = null` means the funding source has no cardholder-defined cap.

`PROVISIONING` rows are internal recovery facts and are never returned as an
active link/card or published to CP. Synchronous onboarding creates the staged
rows and WAL intent in one Oracle transaction, provisions deterministic
TigerBeetle account IDs, then activates every staged row and inserts the outbox
event in one final Oracle transaction. A definitive pre-ledger failure marks
the staged rows `FAILED_PROVISIONING`; an uncertain outcome remains hidden and
is resolved by deterministic TigerBeetle lookup before retry or finalization.

### operation_wal

Every synchronous command that crosses Oracle and TigerBeetle uses one durable
write-ahead record. The first implementation covers user onboarding, credit
grant, and full-balance credit return:

```text
operation_wal
- operation_id RAW(16) primary key
- operation_type USER_ONBOARDING | CREDIT_GRANT | CREDIT_RETURN
- idempotency_record_id RAW(16) not null unique
- provider_id RAW(16) not null
- user_id RAW(16) nullable
- card_id RAW(16) nullable
- state INTENT_RECORDED | EXTERNAL_EFFECT_SUBMITTING | EVENT_PENDING |
        COMPLETED | FAILED_BEFORE_EXTERNAL_EFFECT | RECOVERY_REQUIRED
- deterministic_effects_json JSON not null
- result_json JSON default '{}' not null
- safe_error_code VARCHAR2(128) nullable
- safe_error_message VARCHAR2(2000) nullable
- recovery_attempt_count NUMBER(19,0) default 0 not null
- next_recovery_at TIMESTAMP WITH TIME ZONE nullable
- locked_by VARCHAR2(255) nullable
- locked_until TIMESTAMP WITH TIME ZONE nullable
- created_at TIMESTAMP WITH TIME ZONE default SYSTIMESTAMP not null
- updated_at TIMESTAMP WITH TIME ZONE default SYSTIMESTAMP not null
- completed_at TIMESTAMP WITH TIME ZONE nullable
```

`deterministic_effects_json` contains only allocated UUIDs, TigerBeetle command
IDs, account/transfer IDs, and expected effect types needed for recovery. It
must not contain raw PAN, national ID, contact data, Kafka credentials, access
tokens, or unrestricted request metadata.

Indexes:

```text
index(state, next_recovery_at)
index(provider_id, created_at)
```

### provider_credit_movements

This immutable financial-command fact is linked to the WAL operation:

```text
provider_credit_movements
- movement_id RAW(16) primary key
- operation_id RAW(16) not null unique
- movement_type GRANT | RETURN_FULL_BALANCE
- initiated_by PROVIDER | CARDHOLDER
- provider_id RAW(16) not null
- user_id RAW(16) not null
- card_id RAW(16) not null
- provider_user_account_id RAW(16) not null
- provider_owned_account_id RAW(16) not null
- amount_rials NUMBER(38,0) not null
- expected_remaining_amount_rials NUMBER(38,0) nullable
- provider_reference VARCHAR2(255) nullable
- deterministic_transfer_id RAW(16) not null unique
- status PENDING | APPLIED | FAILED
- card_state_version NUMBER(19,0) nullable
- reason VARCHAR2(1000) nullable
- metadata_json JSON default '{}' not null
- error_code VARCHAR2(128) nullable
- error_message VARCHAR2(2000) nullable
- created_at TIMESTAMP WITH TIME ZONE default SYSTIMESTAMP not null
- updated_at TIMESTAMP WITH TIME ZONE default SYSTIMESTAMP not null
```

Constraints/indexes:

```text
unique(provider_id, provider_reference) when provider_reference is not null
check(amount_rials > 0)
index(status, updated_at)
index(card_id, created_at)
```

For `GRANT`, `amount_rials` is caller-supplied and
`expected_remaining_amount_rials` is null. For `RETURN_FULL_BALANCE`,
`expected_remaining_amount_rials` is required and `amount_rials` is the exact
matching live balance read from TigerBeetle under the card lock. Cardholder
returns do not require a provider reference; their uniqueness comes from the
idempotency record and operation ID.

### integration_outbox

```text
integration_outbox
- outbox_event_id RAW(16) primary key
- operation_id RAW(16) nullable
- producer_service WURZBURG | WOLFSBURG
- delivery_channel INTERNAL | PROVIDER
- provider_id RAW(16) nullable
- original_event_id RAW(16) nullable
- delivery_generation NUMBER(10,0) default 1 not null
- event_sequence NUMBER(10,0) not null
- aggregate_type VARCHAR2(64) not null
- aggregate_id RAW(16) not null
- event_type VARCHAR2(128) not null
- partition_key VARCHAR2(255) not null
- schema_version NUMBER(10,0) not null
- payload_json JSON not null
- delivery_gate_snapshot_json JSON default '{}' not null
- trace_context_json JSON default '{}' not null
- status PENDING | PUBLISHING | PUBLISHED | SUPPRESSED | DEAD_LETTER
- attempt_count NUMBER(19,0) default 0 not null
- next_attempt_at TIMESTAMP WITH TIME ZONE nullable
- locked_by VARCHAR2(255) nullable
- locked_until TIMESTAMP WITH TIME ZONE nullable
- broker_topic VARCHAR2(255) nullable
- broker_partition NUMBER(10,0) nullable
- broker_offset NUMBER(19,0) nullable
- published_at TIMESTAMP WITH TIME ZONE nullable
- last_error_code VARCHAR2(128) nullable
- last_error_message VARCHAR2(2000) nullable
- created_at TIMESTAMP WITH TIME ZONE default SYSTIMESTAMP not null
- updated_at TIMESTAMP WITH TIME ZONE default SYSTIMESTAMP not null
```

Indexes/constraints:

```text
unique(operation_id, event_sequence)
index(status, next_attempt_at)
index(aggregate_type, aggregate_id, created_at)
unique(original_event_id, provider_id, delivery_generation)
```

The operation uniqueness constraint applies when `operation_id` is not null;
the provider replay uniqueness constraint applies when `original_event_id` is
not null. Internal events require `delivery_channel = INTERNAL` and no provider
gate snapshot. Provider events require `delivery_channel = PROVIDER`, a
provider ID, and the exact gate versions/reason captured in
`delivery_gate_snapshot_json`.

For `delivery_channel = PROVIDER`, `payload_json` must validate against the
published provider-event schema. `trace_context_json` is internal outbox
metadata that may link producer/publisher spans, but the publisher must never
copy it into the provider payload or Kafka headers.

The outbox publisher claims rows with bounded leases and Oracle
`FOR UPDATE SKIP LOCKED`, publishes idempotently by `outbox_event_id`, and stores
broker acknowledgement metadata. `DEAD_LETTER` requires an operational alert
and replay tooling; it never authorizes dropping the business event.

## 13. Runtime Profiles, Locks, Outbox, And Events

### Ownership Boundary

Runtime profiles consumed by Nuremberg are materialized by Wolfsburg:

```text
CP:{card_number}
CPOL:SingleProvider:{card_range_id}
CPOL:MultiProvider:{card_range_id}
CRCTL:{card_range_id}
FEE:{provider_id}
```

The canonical FEE projection is:

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

Wurzburg emits `PROVIDER_FEE_PROFILE_PUBLISH_REQUESTED` in the same Oracle
transaction that freezes the DRAFT. The event aggregate and partition key are
the provider ID. Wolfsburg writes the complete replacement and returns a `FEE`
materialization receipt; only that receipt changes Oracle ACTIVE/SUPERSEDED
state.

The exact Redis JSON contracts belong in the Wolfsburg handoff because
Wolfsburg is the only service that writes these values. This document defines
the Wurzburg-owned source facts, refresh triggers, lock protocol, and event
contract.

Responsibilities:

- Wurzburg validates synchronous business commands.
- Wurzburg persists canonical business state in Oracle.
- Wurzburg executes provider-originated TigerBeetle movements using
  deterministic transfer IDs.
- Wurzburg stores the business operation and outbox event durably.
- Wurzburg attempts request-path Kafka publication after commit.
- Wolfsburg consumes the event, reads current Oracle mappings and live
  TigerBeetle balances, and materializes the latest Redis profile.
- Nuremberg reads Redis and never queries the Wurzburg database.

Kafka acknowledgement means the refresh command is durably present on the
broker. It does not mean Wolfsburg has already updated Redis.

### CP Capacity And Funding Order

`CP.funding_sources[].max_amount` is runtime available capacity, not a copy of
the configured cardholder cap. For each active card funding source, Wolfsburg
calculates:

```text
available_balance = live spendable provider-user balance from TigerBeetle
configured_cap    = card_provider_funding_sources.max_amount

CP.max_amount = available_balance                         when cap is null
CP.max_amount = min(available_balance, configured_cap)    when cap is set
```

The result is always a non-negative `u64`. Pending debits that reduce spendable
funds must be included in the TigerBeetle available-balance calculation.

Rules:

- Granting credit does not change cardholder priority or cap.
- Returning credit does not remove the provider or change its priority.
- An active provider with zero available balance remains in the ordered source
  list with `max_amount = 0` so the cardholder preference is preserved.
- Any balance change on an account behind an active card requires CP refresh,
  including both credit grant and full-balance credit return.
- A provider-user balance change with no active card funding-source attachment
  does not require CP refresh.
- Cardholder order/cap changes require CP refresh but no TigerBeetle movement.

### Refresh Trigger Matrix

```text
Business change                                 Required materialization
--------------------------------------------------------------------------
Active-card provider-user credit grant          CP
Active-card provider-user credit return         CP
Funding order or cardholder cap change           CP
Funding source attach/detach/status change       CP
Card activation/suspension/replacement           CP
Range operational-control change                 CRCTL for that card range
Range provider eligibility change                CRCTL and affected cards' CP
Successful Confirm or Rollback                   CP
Balance warmup after CP miss                     CP
Card-range policy activation                     CPOL for that card range
Provider fee-profile activation                  FEE for that provider
Provider-user credit without active card         none
```

Policy and fee changes do not rewrite CP. Their immutable profile IDs are
materialized under the stable range/provider Redis key. Nuremberg reads CPOL and
CRCTL without a process-local cache, as finalized by the card-range handoff.

### Shared Card Mutation Lock

Every Wurzburg command that can change an active card's CP acquires the shared
Redis lock before changing Oracle or TigerBeetle:

```text
Lock-CP:{card_number}
```

The lock is acquired atomically with `SET key value NX PX ttl`. Its structured
value contains:

```json
{
  "operation": "WurzburgCardMutation",
  "owner_service": "wurzburg",
  "operation_id": "uuid",
  "idempotency_key_hash": "sha256",
  "card_state_version": 42,
  "locked_at": "timestamp",
  "expires_at": "timestamp"
}
```

Lock rules:

- If the lock already exists, Wurzburg returns `409 CARD_PROFILE_LOCKED` before
  any domain-state, TigerBeetle, outbox, or CP mutation. The idempotency lookup/
  attempt record is not a business-domain mutation.
- If Redis is unavailable before lock acquisition, Wurzburg returns `503` and
  performs no business mutation.
- Wurzburg releases its own lock with token-safe compare-and-delete only when
  the command fails before any durable business effect.
- After final validation and while holding the lock, Wurzburg deletes CP before
  applying the CP-affecting Oracle/TigerBeetle mutation. This guarantees that a
  stale CP cannot survive a successful mutation.
- If CP invalidation fails, Wurzburg performs no business mutation and returns
  `503`.
- If CP was deleted but a later Oracle/TigerBeetle step fails without applying
  the intended business change, Wurzburg records the failure, enqueues a
  `CARD_PROFILE_REFRESH_REQUESTED` event with reason `MUTATION_ABORTED`, and
  leaves the lock for Wolfsburg to restore current CP safely.
- After a durable CP-affecting effect, Wurzburg leaves the lock for Wolfsburg.
- Wolfsburg writes a fresh versioned CP and then releases the matching lock.
- Lock deletion must verify the operation/token; unconditional `DEL` is not
  allowed.
- The outbox/recovery worker renews the lock lease while publication is pending.
- If a lease nevertheless expires, CP remains absent. Wolfsburg uses
  `card_state_version` to prevent an older event from overwriting newer state.

Batch jobs process each row as an independent atomic command. Each row acquires
only its own card lock, records its own idempotency result, and writes its own
result-file entry. One locked or failed row does not roll back successful rows.

Lock TTL, renewal interval, maximum ownership duration, stale-alert threshold,
and reconciliation thresholds are file/environment configuration because they
are deployment mechanics, not mutable business policy. CP may remain absent
until Wolfsburg is available and processes the durable Kafka event. Crossing
the configured SLA raises an alert and recovery work item; it never permits
Wurzburg to restore a possibly stale CP or bypass version checks.

### Durable Command And Outbox Flow

#### Final WAL Boundary

Oracle, TigerBeetle, Kafka, Redis, and the Wurzburg process cannot share one
transaction. Synchronous HTTP and idempotency do not remove uncertain outcomes:
TigerBeetle can apply a transfer before Wurzburg crashes, Oracle can commit
before Kafka acknowledges, and Kafka can acknowledge before Oracle records the
acknowledgement. The WAL is therefore mandatory for user onboarding, credit
grant, and full-balance credit return.

The recovery responsibilities remain separated:

- `idempotency_records` owns request-hash conflict detection and HTTP replay.
- `operation_wal` owns cross-system progress, deterministic effect IDs, leases,
  and recovery state.
- staged onboarding rows or `provider_credit_movements` own domain facts.
- `integration_outbox` owns at-least-once internal/provider event publication.
- `runtime_materialization_receipts` proves Wolfsburg published the required
  Redis state.

WAL state progression:

```text
INTENT_RECORDED
  -> EXTERNAL_EFFECT_SUBMITTING
  -> EVENT_PENDING
  -> COMPLETED

INTENT_RECORDED/EXTERNAL_EFFECT_SUBMITTING
  -> FAILED_BEFORE_EXTERNAL_EFFECT

INTENT_RECORDED/EXTERNAL_EFFECT_SUBMITTING
  -> RECOVERY_REQUIRED
```

`RECOVERY_REQUIRED` is used only when Wurzburg cannot yet prove whether the
deterministic external effect exists or cannot safely finalize it. It is not a
generic business failure.

#### Credit Grant And Return Protocol

1. Claim `Idempotency-Key` and compare the canonical request hash. Allocate one
   immutable `operation_id`, `movement_id`, TigerBeetle `transfer_id`, internal
   refresh `event_id`, and provider event ID deterministically from the command.
2. Validate the trusted actor, provider/card/user relationship, lifecycle,
   operational profile, and command shape without mutating business state.
3. If the provider-user account is behind an active card funding source,
   acquire `Lock-CP:{card_number}`. If unavailable, return the documented
   conflict before creating a WAL intent. A provider may return stranded credit
   after eligibility/card detachment; when no active CP can reference the
   account, no card lock or CP refresh is required.
4. Read the provider-user and provider-owned TigerBeetle accounts while holding
   the lock. Grant validates the selected exposure mode. Full-balance return
   validates that the live spendable balance is positive and exactly equals
   `expected_remaining_amount_rials`; arbitrary/partial amounts are rejected.
5. In Oracle transaction A, insert `operation_wal(INTENT_RECORDED)`, insert the
   pending immutable movement, and bind the idempotency record to the operation.
6. For an active card funding source, delete stale `CP:{card_number}`. A
   definitive invalidation failure before deletion marks the WAL/movement failed
   and returns `503`. After successful deletion, any definitive pre-ledger
   failure atomically marks the command failed and inserts
   `CARD_PROFILE_REFRESH_REQUESTED(MUTATION_ABORTED)`; the lock remains until
   Wolfsburg restores current CP. A crash in this window is resolved from the
   WAL in the same way. Skip this step when no active CP references the account.
7. Commit `EXTERNAL_EFFECT_SUBMITTING` before calling TigerBeetle, then submit
   the deterministic transfer:
   - grant: debit `PROVIDER_OWNED`, credit the provider-user account
   - return: debit the provider-user account, credit `PROVIDER_OWNED`
8. Treat TigerBeetle success and `exists` as success only after looking up and
   verifying the exact debit account, credit account, amount, ledger, code, and
   flags. A timeout/disconnect is uncertain, never a definitive failure.
9. In Oracle transaction B, after verified ledger success, mark the movement
   `APPLIED`, increment `card_state_version`, insert the mandatory CP-refresh
   outbox event and controlled provider-facing event, set WAL state
   `EVENT_PENDING`, write audit facts, and store the replayable command result.
10. Attempt request-path Kafka publication. Broker-acknowledged or deliberately
    `SUPPRESSED` provider events are terminal delivery outcomes. Once every
    mandatory outbox event is broker-acknowledged, mark the WAL `COMPLETED` and
    persist the final idempotent response.
11. Wolfsburg consumes the internal refresh event, rebuilds CP from Oracle and
    live TigerBeetle state, emits the materialization receipt, and releases the
    matching card lock.

The return transfer amount is never trusted from an event alone. The event/API
value is an optimistic expectation; TigerBeetle under the lock is authoritative.
This closes the race where a Confirm spends more credit after the caller
observed a balance but before the return request arrived.

#### Failure And Recovery Matrix

```text
Failure point                           Recovery decision
-----------------------------------------------------------------------------
Before WAL intent                       No durable business effect; return error
After INTENT_RECORDED, before TB         Retry same deterministic effect
During/after TB with uncertain result    Lookup exact transfer; never create new ID
TB verified, before Oracle transaction B Finalize movement/card/outbox from WAL
Oracle finalized, before Kafka ack       Publish existing outbox rows
Kafka acked, ack status not persisted    Republish same event_id; consumer dedupes
Kafka published, CP receipt missing      Wolfsburg reconciles/materializes CP
Definitive TB rejection                  Mark failed; never pretend transfer applied
```

The recovery worker claims WAL/outbox rows using bounded leases and Oracle
`FOR UPDATE SKIP LOCKED`. It may repeat only deterministic TigerBeetle commands
and immutable Kafka events. It must not generate replacement IDs, recalculate a
historical return amount, or infer ledger success from Oracle/Redis.

Automatic financial compensation is prohibited. If the ledger effect exists,
the system finalizes forward from WAL facts. Any exceptional reverse movement
requires a separate explicit, idempotent, audited business command.

For Oracle-only commands such as funding-order changes, no external-effect WAL
is needed. Wurzburg still invalidates CP after final validation and commits the
domain update, card-state-version increment, audit, and outbox insert in one
Oracle transaction.

### Idempotency And HTTP Completion

The same idempotency key and request hash always refers to one command:

- `COMPLETED`: replay the stored final response.
- `EVENT_PENDING`: observe or retry publication without repeating DB/TigerBeetle
  effects.
- `RECOVERY_REQUIRED`: return the operation state and let reconciliation resume
  from durable facts.
- `FAILED_BEFORE_EXTERNAL_EFFECT`: replay the stored terminal error without
  submitting TigerBeetle again.
- Same key with a different request hash: return
  `409 IDEMPOTENCY_KEY_CONFLICT`.

HTTP semantics:

- `200/201`: business mutation committed and Kafka acknowledged the outbox
  event. Redis materialization may still be in progress.
- `202`: either the ledger result is still uncertain and WAL recovery is
  required, or the verified business mutation is committed but mandatory Kafka
  acknowledgement is pending. Response includes `operation_id`,
  `command_status`, and publication/materialization statuses.
- `409 CARD_PROFILE_LOCKED`: no mutation occurred.
- `503`: dependency failure occurred before any durable business effect.
- A dependency failure after commit must never be represented as if the
  business mutation did not occur.

Operation status API:

```text
GET /api/v1/operations/{operation_id}
```

The response distinguishes `command_status`, `event_publication_status`, and
`profile_materialization_status`. Ledger-affecting commands also expose the
movement ID and movement status. It never exposes internal stack traces or raw
dependency errors.

After writing Redis successfully, Wolfsburg records a materialization receipt
against `operation_id` with the written state version and timestamp before it
releases the lock. This receipt is the source for
`profile_materialization_status`; Kafka acknowledgement alone cannot mark a
profile as materialized.

Publisher retry/backoff limits, lease duration, dead-letter threshold, replay
limits, and maximum materialization delay are file/environment configuration.
Wolfsburg sends `RUNTIME_PROFILE_MATERIALIZED` receipts through Kafka. Wurzburg
deduplicates each receipt in a durable inbox and records the aggregate ID,
operation ID, state/profile version, Redis key kind, and materialized timestamp.
If a receipt is missing, Wolfsburg owns proof against Redis and republishes the
receipt; Wurzburg does not infer success by reading Redis independently.

Required deployment configuration shape:

```toml
[card_profile_lock]
ttl_ms = 30000
renew_interval_ms = 10000
max_ownership_ms = 300000
stale_alert_ms = 60000

[outbox]
poll_interval_ms = 250
lease_ms = 30000
initial_backoff_ms = 500
max_backoff_ms = 60000
dead_letter_attempts = 20

[runtime_materialization]
pending_alert_ms = 60000
reconciliation_interval_ms = 5000

[provider_events]
payload_retention_days = 30
max_replay_age_days = 30
```

These are initial defaults, not business promises. Production values are tuned
from measured Kafka/Wolfsburg latency and alerting objectives without changing
the API or database contract.

### Internal Materialization Events

CP-affecting events from Wurzburg and Nuremberg use one ordered internal card
state stream. The physical topic name is system configuration, but every event
on this stream is keyed by normalized `card_number` so Kafka and Wolfsburg
preserve same-card ordering.

CPOL, CRCTL, and FEE publication requests use a separate internal profile-
control stream because they are keyed by `card_range_id` and `provider_id`, not
by card number. They do not participate in the card mutation lock.

Wurzburg events carry the Oracle `card_state_version` allocated by the command
transaction. Nuremberg cannot allocate that Oracle version; its Confirm and
Rollback events carry their deterministic transaction identity instead.
Wolfsburg allocates the next card state version atomically when it persists a
verified Nuremberg ledger outcome, then materializes CP with that version.

Wurzburg emits:

```text
CARD_PROFILE_REFRESH_REQUESTED
CARD_POLICY_PROFILE_PUBLISH_REQUESTED
CARD_RANGE_CONTROL_PUBLISH_REQUESTED
PROVIDER_FEE_PROFILE_PUBLISH_REQUESTED
```

`CARD_PROFILE_REFRESH_REQUESTED` carries a reason:

```text
CARD_CREATED
CREDIT_GRANTED
CREDIT_RETURNED
FUNDING_ORDER_CHANGED
FUNDING_SOURCE_CHANGED
CARD_STATUS_CHANGED
CARD_REPLACED
MUTATION_ABORTED
```

Internal event envelope:

```json
{
  "event_id": "uuid",
  "event_type": "CARD_PROFILE_REFRESH_REQUESTED",
  "schema_version": 1,
  "aggregate_type": "CARD",
  "aggregate_id": "card-id",
  "partition_key": "normalized-card-number",
  "card_state_version": 42,
  "operation_id": "uuid",
  "idempotency_key_hash": "sha256",
  "correlation_id": "uuid-or-trusted-correlation-value",
  "causation_id": "uuid-or-null",
  "occurred_at": "timestamp",
  "payload": {
    "reason": "CREDIT_RETURNED",
    "provider_id": "uuid",
    "user_id": "uuid",
    "movement_id": "uuid-or-null"
  }
}
```

Events contain identities and versions, not Redis payload snapshots or cached
balances. Wolfsburg always rebuilds from current Oracle mappings and current
TigerBeetle state. W3C `traceparent`, `tracestate`, and optional `baggage` are
injected into Kafka headers and continued by Wolfsburg.

Kafka delivery is at least once. Wolfsburg must claim each `event_id` through a
durable consumer-inbox uniqueness constraint before applying side effects, and
must treat duplicate delivery as successful replay. Same-card serialization
and `card_state_version` protect ordering; inbox deduplication protects repeated
delivery.

### Provider-Facing Events

#### Final Granular Delivery Contract

Provider integration events are separate from internal materialization events
and are delivered to the provider-specific Kafka topic. Wurzburg creates events
for provider/user/card/credit facts. Wolfsburg creates events derived from
verified Nuremberg Confirm/Rollback facts. Both services validate and insert the
same public envelope into the shared Oracle provider outbox; the Wurzburg
provider-event publisher is the single component that evaluates controls and
publishes to provider topics.

Delivery rules:

- Internal card/profile materialization events are mandatory and never pass
  through provider event-delivery controls.
- Only a trusted platform admin may change a provider subscription. Provider
  credentials may read their effective subscription state but cannot mutate it.
- Provider-facing event configuration may exist while delivery is disabled;
  Kafka topic and credential provisioning remain independent lifecycle facts.
- The delivery worker evaluates the effective layered gate immediately before
  publishing, not only when the business event is created.
- If any gate is closed, the worker records `SUPPRESSED` with the exact global
  config version, operational-profile ID, subscription version, and reason.
- A missing subscription is treated as disabled; absence never means opt-in.
- Disabling delivery does not delete the source business event or Kafka
  credentials.
- Re-enabling delivery affects new events only. Suppressed events are replayed
  only through an explicit audited platform-admin replay command.
- Replay is allowed only for `SUPPRESSED` or `DEAD_LETTER` deliveries and creates
  the next `delivery_generation`; `PUBLISHED` deliveries cannot be replayed by
  this API.
- Already broker-acknowledged events cannot be recalled.
- Provider-facing delivery is at least once; provider consumers must deduplicate
  by `event_id`.
- Delivery configuration changes are idempotent, versioned, and audited.

Provider-facing payloads must never be used to rebuild Redis, calculate current
balances, or prove ledger movement.

The initial version-1 provider event catalog is:

```text
PROVIDER_STATUS_CHANGED
USER_ONBOARDED
CARD_ASSIGNED
CARD_REPLACED
CREDIT_GRANTED
CREDIT_RETURNED
WITHDRAWAL_CONFIRMED
WITHDRAWAL_ROLLED_BACK
FEE_CHARGED
```

Each event type has a versioned JSON Schema and one common immutable envelope:

```json
{
  "event_id": "uuid",
  "event_type": "CREDIT_RETURNED",
  "schema_version": 1,
  "occurred_at": "2026-07-17T12:00:00.000Z",
  "provider_id": "uuid",
  "subject": {
    "subject_type": "PROVIDER_USER_ACCOUNT",
    "subject_id": "uuid",
    "user_id": "uuid",
    "card_id": "uuid",
    "masked_card_number": "621986******0000",
    "provider_customer_reference": "customer-123"
  },
  "data": {
    "currency": "IRR",
    "amount_rials": "500000",
    "observed_remaining_credit_rials": "0",
    "balance_observed_at": "2026-07-17T12:00:00.000Z",
    "initiated_by": "CARDHOLDER"
  }
}
```

Envelope rules:

- The catalog above is an explicit public allowlist. Admin configuration cannot
  subscribe a provider to an internal event name, arbitrary Kafka topic, audit
  record, or unknown schema.
- `event_id` is immutable and is the provider consumer's deduplication key.
- `schema_version` versions the selected `event_type` payload. A breaking field
  or semantic change requires a new version; replay retains the original one.
- Monetary values are base-10 integer strings in Iranian rials to avoid JSON/
  JavaScript integer precision loss.
- Timestamps are UTC RFC 3339 with millisecond precision.
- Optional fields are omitted rather than sent as ambiguous nulls.
- PAN is masked. National ID, full PAN, contact information, secrets, raw
  metadata, and internal database snapshots are prohibited.
- `operation_id`, idempotency hashes, WAL/recovery state, internal correlation/
  causation IDs, Redis keys/payloads, and TigerBeetle internals are prohibited.
- Provider Kafka records do not carry W3C `traceparent`, `tracestate`, `baggage`,
  or any other OTel/distributed-tracing headers. Allowed Kafka headers are only
  content type, event ID, event type, and schema version.
- The producer must fetch any included balance from TigerBeetle at event
  generation time and label it as an observed current balance, not a historical
  post-transaction proof. Later operations may make it stale; the full-return
  optimistic guard handles that race. Oracle balance caches are forbidden.

Canonical contract artifacts live under:

```text
contracts/provider-events/v1/envelope.schema.json
contracts/provider-events/v1/{event_type}.schema.json
```

The schemas are canonical. Before provider-event publication is implemented, a
shared Rust contract module generated from or verified against these artifacts
must own envelope DTOs, event enums, validation, masking, decimal-string
serialization, and maximum payload size. Wurzburg and Wolfsburg must use the
same version. CI validates example payloads against JSON Schema and rejects
undocumented fields.

Every provider-facing type may be suppressed by the platform/provider gates;
mandatory internal materialization and audit events use the separate internal
channel and cannot be suppressed.

The shared `integration_outbox` is the sole durable record of provider publication,
suppression, replay generation, and dead-letter state.
`PUBLISHED` means Kafka acknowledged the record; it does not mean the provider
consumed it. Kafka consumer groups, offsets, lag, retention, and redelivery are
Kafka responsibilities and are not duplicated in Oracle.

An audited replay creates a new outbox generation containing the original
immutable envelope, payload, event ID, and schema version. It never rebuilds an
old event with today's schema. Provider-event payload retention and maximum
replay age are deployment configuration; a request outside that retained
window returns a centralized replay-expired result code.

Delivery while a provider is suspended follows the single lifecycle decision
in section 4.

## 14. Provider APIs

Initial Provider administration APIs:

```text
POST   /api/v1/providers
GET    /api/v1/providers/{provider_id}
GET    /api/v1/providers
GET    /api/v1/providers/{provider_id}/ledger
PATCH  /api/v1/providers/{provider_id}
POST   /api/v1/providers/{provider_id}/activate
POST   /api/v1/providers/{provider_id}/suspend
POST   /api/v1/providers/{provider_id}/deactivate
POST   /api/v1/providers/{provider_id}/retry-provisioning
```

Provider range eligibility is managed through provider-centric commands and is
never physically deleted:

```text
PUT  /api/v1/providers/{provider_id}/card-range
POST /api/v1/providers/{provider_id}/card-range/suspend
POST /api/v1/providers/{provider_id}/card-range/reactivate
```

Initial assignment creates or reactivates a relationship row. Suspension keeps
that row and preserves every card, funding source, audit fact, Oracle financial
fact, and TigerBeetle account/transfer. A move keeps the prior relationship as
`SUSPENDED` and is rejected while that range still has active cards, active
funding sources, or outstanding provider-user credit. Historical transactions
alone never cause deletion and do not prevent a safe move.

Provider contacts:

```text
POST   /api/v1/providers/{provider_id}/contacts
GET    /api/v1/providers/{provider_id}/contacts
PATCH  /api/v1/providers/{provider_id}/contacts/{contact_id}
POST   /api/v1/providers/{provider_id}/contacts/{contact_id}/suspend
POST   /api/v1/providers/{provider_id}/contacts/{contact_id}/reactivate
```

Contacts are never physically deleted. Suspension and reactivation are explicit,
idempotent state-transition commands so history, notification routing decisions,
and audit evidence remain unambiguous. Updating a suspended contact does not
reactivate it.

Provider operational controls:

```text
POST   /api/v1/providers/{provider_id}/operational-profiles
GET    /api/v1/providers/{provider_id}/operational-profile
GET    /api/v1/providers/{provider_id}/operational-profiles
POST   /api/v1/providers/{provider_id}/operational-profiles/{profile_id}/cancel
```

The singular GET returns the full currently effective profile. The plural GET
returns version-cursor-paginated full historical/scheduled profiles. The cancel
command cancels only a future `SCHEDULED` candidate, requires an idempotency key
and audit reason, and never deletes the row. A command endpoint is used because
cancellation is an auditable state transition rather than resource deletion.

Kafka provisioning:

```text
GET    /api/v1/providers/{provider_id}/kafka/credentials
GET    /api/v1/providers/{provider_id}/kafka/status
GET    /api/v1/providers/kafka/certificate
POST   /api/v1/providers/{provider_id}/kafka/provision
POST   /api/v1/providers/{provider_id}/kafka/rotate-credentials
POST   /api/v1/providers/{provider_id}/kafka/suspend
POST   /api/v1/providers/{provider_id}/kafka/resume
```

Provider event delivery:

```text
GET  /api/v1/admin/provider-event-types
GET  /api/v1/admin/providers/{provider_id}/event-subscriptions
PUT  /api/v1/admin/providers/{provider_id}/event-subscriptions
GET  /api/v1/admin/providers/{provider_id}/events
POST /api/v1/admin/provider-events/{outbox_event_id}/replay
GET  /api/v1/providers/{provider_id}/event-subscriptions
```

Subscription update request:

```json
{
  "expected_version": 4,
  "subscriptions": [
    {
      "event_type": "CREDIT_GRANTED",
      "enabled": true
    },
    {
      "event_type": "CREDIT_RETURNED",
      "enabled": false
    }
  ],
  "reason": "platform delivery policy"
}
```

The PUT body is a complete replacement of the provider's supported event-type
state and uses optimistic version checking. Only platform operators may mutate
subscriptions, the global kill switch, or the scheduled `event_delivery` gate.
The provider-scoped GET is read-only. Mutating subscription/replay APIs require
`Idempotency-Key`, platform-admin scope, reason, and immutable audit snapshots.

Subscription reads return both configured and effective state:

```json
{
  "provider_id": "uuid",
  "version": 5,
  "global_delivery_enabled": true,
  "provider_delivery_enabled": true,
  "credential_status": "ACTIVE",
  "subscriptions": [
    {
      "event_type": "CREDIT_RETURNED",
      "schema_versions": [1],
      "configured_enabled": true,
      "effective_enabled": true,
      "blocked_by": []
    }
  ]
}
```

The event-type catalog returns only public provider events with their supported
schema versions and contract artifact names. Admin delivery queries filter by
provider, event type, status, occurred-at range, and event ID. They return the
masked public envelope and delivery metadata, never internal trace/WAL fields.

Provider user onboarding:

```text
POST   /api/v1/providers/{provider_id}/users
GET    /api/v1/providers/{provider_id}/users
GET    /api/v1/providers/{provider_id}/users/{user_id}
```

The onboarding command creates or resolves the global user, links the provider,
assigns the card, provisions the provider-user and eight policy-usage accounts,
and records the refresh event as one synchronous atomic business command. A
separate provider card-create API is not needed for the initial assignment.

Card queries and replacement:

```text
GET    /api/v1/providers/{provider_id}/cards
POST   /api/v1/providers/{provider_id}/users/{user_id}/cards/reprint
POST   /api/v1/providers/{provider_id}/users/{user_id}/cards/replace
```

Cardholder funding controls:

```text
PUT /api/v1/cards/{card_number}/funding-order
```

Request:

```json
{
  "sources": [
    {
      "provider_id": "uuid-1",
      "max_amount": 1000000
    },
    {
      "provider_id": "uuid-2",
      "max_amount": null
    }
  ],
  "reason": "cardholder preference update"
}
```

Array order defines contiguous priorities starting at `1`. Every active funding
provider must appear exactly once. `max_amount = null` means no cardholder cap;
the CP runtime capacity is still limited by the live TigerBeetle balance. The
API acquires the shared CP lock and returns `409 CARD_PROFILE_LOCKED` without a
DB change when the card is busy.

Credit operations:

```text
POST   /api/v1/providers/{provider_id}/credits/grant
GET    /api/v1/providers/{provider_id}/users/{user_id}/credit
POST   /api/v1/providers/{provider_id}/credits/return
GET    /api/v1/cards/{card_number}/providers/{provider_id}/credit
POST   /api/v1/cards/{card_number}/providers/{provider_id}/credit/return
```

Grant request:

```json
{
  "user_id": "uuid",
  "card_number": "16-digit PAN",
  "amount_rials": 1000000,
  "provider_reference": "immutable-provider-operation-reference",
  "reason": "business reason",
  "metadata": {}
}
```

`user_id`, `card_number`, positive integer `amount_rials`, and
`provider_reference` are required. The card must be the provider's active card
for that user. `(provider_id, provider_reference)` is unique and complements
the HTTP `Idempotency-Key`.

Both authorized live-credit GET routes read TigerBeetle and return:

```json
{
  "provider_id": "uuid",
  "user_id": "uuid",
  "card_id": "uuid",
  "currency": "IRR",
  "observed_remaining_amount_rials": 500000,
  "observed_at": "2026-07-17T12:00:00.000Z"
}
```

The observed amount is suitable for the optimistic return guard but is not a
reservation; a concurrent Confirm may make it stale before the return arrives.

Provider full-balance return request:

```json
{
  "user_id": "uuid",
  "card_number": "16-digit PAN",
  "expected_remaining_amount_rials": 500000,
  "provider_reference": "immutable-provider-operation-reference",
  "reason": "close remaining credit",
  "metadata": {}
}
```

Cardholder full-balance return request:

```json
{
  "expected_remaining_amount_rials": 500000,
  "reason": "return provider credit"
}
```

The trusted WSO2 `user_id` claim must own the card for the cardholder route.
Neither route accepts an arbitrary return amount. The expected amount is an
optimistic guard obtained from the authorized live-credit GET, prior API
response, or provider event. Wurzburg returns the entire matching TigerBeetle
balance or returns `409 PROVIDER_USER_BALANCE_CHANGED` without side effects.

Successful response:

```json
{
  "movement_id": "uuid",
  "movement_type": "GRANT",
  "provider_id": "uuid",
  "user_id": "uuid",
  "card_id": "uuid",
  "amount_rials": 1000000,
  "provider_reference": "provider-reference",
  "status": "COMPLETED",
  "event_publication_status": "PUBLISHED",
  "profile_materialization_status": "PENDING",
  "created_at": "timestamp"
}
```

Batch jobs:

```text
POST /api/v1/providers/{provider_id}/batch-jobs
POST /api/v1/providers/{provider_id}/batch-jobs/{job_id}/file
GET  /api/v1/providers/{provider_id}/batch-jobs/{job_id}
GET  /api/v1/providers/{provider_id}/batch-jobs/{job_id}/original-file
GET  /api/v1/providers/{provider_id}/batch-jobs/{job_id}/result-file
GET  /api/v1/providers/{provider_id}/batch-jobs/{job_id}/errors
```

Supported `job_type` values are `USER_ONBOARDING`, `CREDIT_GRANT`, and
`CREDIT_RETURN`. The original and result files live in MinIO; Oracle stores
job metadata and row outcomes. Every row runs as an independent atomic,
idempotent command and the result file contains the input columns plus status,
created IDs, movement ID, centralized result code, and message.

Transaction queries:

```text
GET /api/v1/providers/{provider_id}/accounts/{account_category}/transactions
GET /api/v1/providers/{provider_id}/cards/{card_number}/transactions
GET /api/v1/admin/cards/{card_number}/transactions
```

Rules:

- Mutating APIs require `Idempotency-Key`.
- Every API must use centralized `WurzburgResultCode`.
- Every API must be covered by OTel server spans through router middleware.
- Oracle, TigerBeetle, Redis-lock, outbox, and Kafka operations create child
  spans and propagate W3C trace context into internal Kafka headers only.
  Provider-facing Kafka records explicitly exclude tracing context.
- WSO2 owns external gateway policy, and Wurzburg validates the trusted WSO2
  JWT/claims and enforces provider scope as defined by the main service handoff.
- Provider and admin route groups remain separate.

All collection APIs use `page_size` and opaque `page_token`. Default page size
is 50 and the accepted range is 1 through 200. Each resource defines an
allowlist of filters and sort fields; unknown filters/sorts are validation
errors. Default order is `(created_at DESC, primary_id DESC)`, and the token
captures the normalized filters, order, and last key so concurrent inserts do
not cause offset drift.

Funding-order mutation requires either a trusted JWT `user_id` claim matching
the card owner plus `card.funding-order:write`, or the platform/bank admin scope
`platform.cards.funding-order:write`. Provider-scoped credentials alone cannot
change cardholder order.

Common `202 Accepted` response:

```json
{
  "operation_id": "uuid",
  "command_status": "APPLIED",
  "event_publication_status": "PENDING",
  "profile_materialization_status": "PENDING",
  "status_url": "/api/v1/operations/{operation_id}",
  "retry_after_seconds": 2
}
```

For an uncertain TigerBeetle result, `command_status` is `RECOVERY_REQUIRED`
and publication/materialization remain `NOT_STARTED`. For a verified ledger
effect awaiting Kafka, `command_status` is `APPLIED` as shown above.

Polling returns `200` for both pending and terminal operation resources. A
terminal command failure is represented by `command_status = FAILED` plus the
centralized error object; transport-level lookup/auth failures use normal HTTP
errors.

### Trusted WSO2 Actor Contract

Provider APIs use exactly the finalized card-range trust contract:

`WURZBURG_WSO2_HANDOFF.md` is authoritative for transport, claim validation,
scope mapping, spoofing protection, retries, limits, and gateway errors. The
facts below are the subset persisted by Provider audit.

- `Authorization: Bearer <jwt>`, or `X-JWT-Assertion` when WSO2 replaces it
- JWT `sub`, `azp` or `client_id`, roles/scopes, optional `provider_id`, and
  optional `user_id` for cardholder-authorized commands
- required `X-Correlation-Id` and optional `X-Request-Id`
- `X-WSO2-Client-IP`, resolved, stripped, and overwritten by WSO2

Wurzburg trusts this context only from the configured WSO2 network/mTLS
boundary. It never trusts public caller identity headers or arbitrary
`X-Forwarded-For`. Every mutation stores actor subject/client/provider, source
IP, issuer, correlation/request IDs, reason, and before/after JSON snapshots in
the common audit log in the same Oracle transaction as the state change.

### Observability And Distributed Tracing

Every route has an OTel server span. Validation, idempotency, Oracle,
TigerBeetle, Redis lock/invalidation, Kafka/outbox, MinIO, provisioning, and
recovery steps create child spans with dependency status and centralized result
code. W3C `traceparent` and `tracestate` are extracted from trusted HTTP input,
injected into Kafka headers, and linked into asynchronous batch/recovery work.

Metrics cover request latency/results, dependency latency/errors, provisioning
age, lock conflicts, movement recovery age, outbox lag/dead letters, provider
event suppression, batch row outcomes, and materialization receipt delay.
Spans, logs, and metrics must never contain raw PAN, national ID, contact data,
Kafka passwords/certificates, request bodies, or metadata JSON. Use UUIDs,
result codes, account categories, event types, and hashed/masked identifiers.

### Detailed Provider API Payloads

Create provider:

```text
POST /api/v1/providers
Headers: Idempotency-Key
```

Request:

```json
{
  "legal_name": "Legal Provider Name",
  "trade_name": "Provider Brand",
  "tax_id": "1234567890",
  "registration_number": "REG-123",
  "email_address": "ops@example.com",
  "website_url": "https://example.com",
  "mailing_address": "Registered address",
  "metadata": {},
  "contacts": [
    {
      "contact_type": "TECHNICAL",
      "name": "Technical Contact",
      "email": "tech@example.com",
      "phone": "+982100000000",
      "mobile": "+989120000000",
      "sms_enabled": false,
      "metadata": {}
    }
  ],
  "operational_profile": {
    "effective_at": "2026-07-14T00:00:00Z",
    "profile": {
      "timezone": "Asia/Tehran",
      "user_onboarding": {
        "enabled": true,
        "active_windows": [],
        "max_total_users": null
      },
      "credit_grant": {
        "enabled": true,
        "mode": "FixedLimit",
        "limit_amount_rials": 1000000000
      },
      "credit_return": {
        "enabled": true
      },
      "card_operations": {
        "new_assignment_enabled": true,
        "same_pan_reprint_enabled": true,
        "new_pan_replacement_enabled": true,
        "attach_existing_multi_provider_card_enabled": true
      },
      "event_delivery": {
        "enabled": true,
        "disabled_reason": null
      }
    }
  }
}
```

Response:

```json
{
  "provider_id": "uuid",
  "status": "PENDING_PROVISIONING",
  "legal_name": "Legal Provider Name",
  "trade_name": "Provider Brand",
  "tax_id": "1234567890",
  "provisioning": {
    "job_id": "uuid",
    "status": "PENDING"
  },
  "created_at": "2026-07-14T00:00:00Z",
  "updated_at": "2026-07-14T00:00:00Z"
}
```

Get provider:

```text
GET /api/v1/providers/{provider_id}
```

Provider detail returns canonical Oracle identity, lifecycle, and provisioning
facts. Live financial values are deliberately separated so this endpoint does
not become unavailable when TigerBeetle is temporarily unavailable:

```json
{
  "provider_id": "uuid",
  "legal_name": "Legal Provider Name",
  "trade_name": "Provider Brand",
  "tax_id": "1234567890",
  "registration_number": "REG-123",
  "email_address": "ops@example.com",
  "website_url": "https://example.com",
  "mailing_address": "Registered address",
  "status": "ACTIVE",
  "metadata": {},
  "core_provisioning_status": "SUCCEEDED",
  "kafka_provisioning_status": "SUCCEEDED",
  "created_at": "2026-07-14T00:00:00Z",
  "updated_at": "2026-07-14T00:00:00Z"
}
```

Live provider ledger:

```text
GET /api/v1/providers/{provider_id}/ledger
```

This endpoint batch-reads exactly `PROVIDER_OWNED`, `PROVIDER_FEE`,
`CMS_SETTLEMENT`, and `PLATFORM_FEE` from TigerBeetle. It returns all four raw
counters plus signed decimal-string `posted_balance` and `effective_balance` in
IRR. Oracle stores only account UUID/category mappings. Missing accounts or an
unavailable TigerBeetle dependency fail the whole request with
`PROVIDER_LEDGER_UNAVAILABLE`; partial financial responses are forbidden.

List providers:

```text
GET /api/v1/providers?status=ACTIVE&tax_id=...&page_size=50&page_token=...
```

Response:

```json
{
  "data": [
    {
      "provider_id": "uuid",
      "legal_name": "Legal Provider Name",
      "trade_name": "Provider Brand",
      "tax_id": "1234567890",
      "status": "ACTIVE",
      "created_at": "2026-07-14T00:00:00Z",
      "updated_at": "2026-07-14T00:00:00Z"
    }
  ],
  "next_page_token": null
}
```

Provider lists never perform live TigerBeetle lookups. Ledger accounts and live
balances are available only from the dedicated ledger endpoint.
Page tokens are opaque, filter-bound keyset cursors ordered by
`(created_at DESC, provider_id DESC)`; changing a filter while reusing a token
is rejected.

Update provider identity:

```text
PATCH /api/v1/providers/{provider_id}
Headers: Idempotency-Key
```

Request supports editable provider identity fields. Omitted fields are
unchanged; explicit JSON `null` clears only nullable fields:

```json
{
  "legal_name": "New Registered Name",
  "trade_name": "New Brand",
  "tax_id": null,
  "registration_number": "REG-456",
  "email_address": "ops-new@example.com",
  "website_url": "https://new.example.com",
  "mailing_address": "New address",
  "metadata": {},
  "reason": "Registered provider details were updated"
}
```

`legal_name`, `trade_name`, and `metadata` cannot be cleared. Every mutation
requires a non-empty reason, trusted WSO2 context, and `Idempotency-Key`.

Lifecycle commands:

```text
POST /api/v1/providers/{provider_id}/activate
POST /api/v1/providers/{provider_id}/suspend
POST /api/v1/providers/{provider_id}/deactivate
Headers: Idempotency-Key
```

Request:

```json
{
  "reason": "operator reason",
  "metadata": {},
  "reason": "Finance escalation contact added"
}
```

Operational profile:

```text
POST /api/v1/providers/{provider_id}/operational-profile
GET  /api/v1/providers/{provider_id}/operational-profile
Headers for POST: Idempotency-Key
```

Create contact:

```text
POST /api/v1/providers/{provider_id}/contacts
Headers: Idempotency-Key
```

Request:

```json
{
  "contact_type": "FINANCE",
  "name": "Finance Contact",
  "email": "finance@example.com",
  "phone": "+982100000000",
  "mobile": "+989120000000",
  "sms_enabled": true,
  "metadata": {}
}
```

`sms_enabled` may be true only when a mobile number is present. Important
system SMS notifications are sent to every active notification contact with
this flag enabled; contacts are otherwise informational and optional.

Contact updates use the same omitted-versus-null semantics as provider identity:
omitted values remain unchanged and explicit `null` clears nullable contact
fields. `contact_type`, `metadata`, and `status` cannot be cleared. Clearing the
mobile number while `sms_enabled` remains true is rejected.

Contact listing supports exact `contact_type` and `status` filters plus
filter-bound opaque keyset pagination through `page_size` and `page_token`.
The maximum page size is 100.

Provider user onboarding:

```text
POST /api/v1/providers/{provider_id}/users
Headers: Idempotency-Key
```

Request:

```json
{
  "national_id": "national-id",
  "first_name": "First",
  "last_name": "Last",
  "card_number": "6219861000000000",
  "provider_customer_reference": "customer-123",
  "metadata": {}
}
```

Response includes the resolved `user_id`, provider link, card/card-range IDs,
provider-user TigerBeetle account ID, all eight policy-usage account IDs,
identity-mismatch flag, provisioning result, event-publication status, and
profile-materialization status. The raw national ID and PAN are never returned
in list/event/audit payloads where their UUID or masked form is sufficient.

Kafka operations:

```text
GET  /api/v1/providers/{provider_id}/kafka/credentials
POST /api/v1/providers/{provider_id}/kafka/provision
POST /api/v1/providers/{provider_id}/kafka/rotate-credentials
POST /api/v1/providers/{provider_id}/kafka/suspend
Headers for POST: Idempotency-Key
```

Provider user onboarding, card assignment, and credit APIs are intentionally
listed in this document but should be implemented after Provider identity,
contacts, operational profiles, ledger account provisioning, and Kafka
provisioning are stable.

## 15. API Response Contract

Provider APIs should reuse the centralized result/error model already introduced
for the card-range slice.

Success responses return the resource body directly, matching the card APIs.
Error responses must always be:

```json
{
  "error": {
    "rs_code": 1234,
    "code": "MACHINE_READABLE_CODE",
    "message": "Human readable message",
    "details": {}
  }
}
```

Each provider result variant centrally owns its stable numeric `rs_code`,
symbolic code, default message, and HTTP status through `WurzburgResultCode`.
Provider-specific result codes must be added centrally, not as ad hoc values in
handlers:

```text
PROVIDER_NOT_FOUND
PROVIDER_ALREADY_EXISTS
PROVIDER_NOT_ACTIVE
PROVIDER_SUSPENDED
PROVIDER_PROVISIONING_PENDING
PROVIDER_PROVISIONING_FAILED
PROVIDER_OPERATION_NOT_ALLOWED
PROVIDER_CONTACT_NOT_FOUND
PROVIDER_OPERATIONAL_PROFILE_NOT_FOUND
PROVIDER_KAFKA_ACCESS_NOT_FOUND
PROVIDER_KAFKA_CREDENTIAL_NOT_AVAILABLE
PROVIDER_LEDGER_ACCOUNT_NOT_FOUND
PROVIDER_EVENT_SUBSCRIPTION_INVALID
PROVIDER_EVENT_NOT_FOUND
PROVIDER_EVENT_REPLAY_NOT_ALLOWED
PROVIDER_EVENT_REPLAY_EXPIRED
PROVIDER_USER_NOT_FOUND
PROVIDER_CUSTOMER_REFERENCE_CONFLICT
PROVIDER_USER_LIMIT_REACHED
PROVIDER_CREDIT_GRANT_DISABLED
PROVIDER_CREDIT_RETURN_DISABLED
PROVIDER_USER_BALANCE_CHANGED
PARTIAL_CREDIT_RETURN_NOT_ALLOWED
PROVIDER_CREDIT_LIMIT_EXCEEDED
PROVIDER_CARD_OPERATION_DISABLED
CARD_PROFILE_LOCKED
OPERATION_NOT_FOUND
OPERATION_RECOVERY_REQUIRED
EVENT_PUBLICATION_PENDING
```

## 16. Implementation Order

Do not start with user credit or card funding transfers.

Wurzburg is Oracle-only. Use direct Oracle persistence modules behind
application services; do not create database-neutral repository traits whose
only implementation is Oracle. API handlers own HTTP extraction/response only,
services own transactions and business rules, and Oracle SQL remains inside
`src/db/oracle`.

Final module shape:

```text
src/domain/provider.rs
src/domain/provider_user.rs
src/domain/provider_event.rs
src/domain/provider_movement.rs
src/domain/operation_wal.rs

src/services/provider_service.rs
src/services/provider_user_service.rs
src/services/provider_credit_service.rs
src/services/provider_event_service.rs
src/services/provider_recovery_service.rs

src/db/oracle/provider.rs
src/db/oracle/provider_user.rs
src/db/oracle/provider_movement.rs
src/db/oracle/provider_event.rs
src/db/oracle/operation_wal.rs
src/db/oracle/outbox.rs

src/api/dto/provider.rs
src/api/dto/provider_user.rs
src/api/dto/provider_credit.rs
src/api/handlers/provider.rs
src/api/handlers/provider_user.rs
src/api/handlers/provider_credit.rs
src/api/handlers/provider_event.rs
```

Baseline migration files contain only this final schema. No compatibility
ALTER/backfill is required for disposable draft tables; force rebuild recreates
the schema from the clean baseline. Once the first shared environment adopts
that baseline, normal forward-only migration discipline begins.

Recommended order:

1. Clean Oracle baseline migrations, Oracle persistence modules, application
   services, and common audit integration.
2. Provider identity, contacts, lifecycle, and scheduled operational profiles.
3. Provider ledger account records, synchronous TigerBeetle verification, and
   durable recovery of uncertain outcomes.
4. Provider Kafka credential provisioning and explicit admin activation.
5. Idempotency records, operation WAL, Oracle outbox,
   publisher, receipt inbox, recovery worker, and operation-status API.
6. Provider event subscriptions, platform delivery gates, durable delivery
   decisions, suppression, and audited replay.
7. Provider-to-card-range eligibility integration.
8. Global users, provider-user links, and provider-user TigerBeetle accounts.
9. Card assignment, state version, funding-source priority/cap mapping, and
   policy usage accounts.
10. Shared CP lock/invalidation protocol and card-state refresh events.
11. Credit grant/full-balance-return APIs with deterministic TigerBeetle
    transfers.
12. Transaction and live-ledger query APIs.
13. Contract tests for Wurzburg events consumed by the future Wolfsburg
    materializer.

## 17. Final Lifecycle And Audit Rules

These decisions are final and must not be reopened during implementation:

- Kafka secrets are returned only by the dedicated credential API, never by
  ordinary provider detail/list APIs. Rotation updates the stable SCRAM user,
  revokes the previous password immediately, and writes an immutable credential
  audit record. Only one password is usable at a time.
- Provider list APIs do not perform TigerBeetle balance fan-out. Live balances
  belong to provider detail/ledger endpoints.
- A PAN identity is immutable and never reassigned to another user. Replacement
  with a new PAN retires the old card and creates a new card row.
- Relationship tables hold current lifecycle state; every transition writes an
  immutable audit snapshot. Function-based unique indexes enforce active-only
  uniqueness.
- Contact `DELETE` is a soft suspension with an audit record, never a physical
  delete.

## 18. Required Integration Scenarios

Provider tests follow the scenario-comment style defined by the main service
handoff and make Oracle, TigerBeetle, Redis-lock, and Kafka facts visible.

Required runtime-profile scenarios:

1. Credit grant on an active card preserves funding order, increases live
   capacity, invalidates CP, emits one refresh event, and does not release the
   post-commit lock in Wurzburg.
2. Full-balance credit return preserves funding order and republishes the
   provider with `max_amount = 0` instead of removing it.
3. Credit change without an active card updates TigerBeetle but emits no CP
   refresh event.
4. Funding-order/cap update changes Oracle only, increments card state version,
   invalidates CP, and emits one refresh event.
5. Existing `Lock-CP` returns `409` with no domain-state, TigerBeetle, outbox,
   or CP mutation; only idempotency-attempt state may exist.
6. Redis unavailable before lock acquisition returns `503` with no durable
   business effect.
7. Kafka unavailable after commit returns `202`, leaves a pending outbox row,
   and an outbox retry publishes without repeating the TigerBeetle transfer.
8. Repeating the same idempotency key while the event is pending resumes the
   same operation; a different request hash returns conflict.
9. Process loss after TigerBeetle success is reconciled through deterministic
   transfer lookup, Oracle finalization, and one outbox event.
10. A stale lower `card_state_version` event cannot overwrite a newer CP.
11. Wolfsburg contract fixtures can calculate non-null CP `max_amount` from the
    live balance and optional configured cap.
12. Policy activation emits CPOL publication only; fee activation emits FEE
    publication only; neither rewrites CP.
13. Range operational/provider-eligibility changes emit CRCTL publication and
    affected-card CP refresh without deleting financial history.
14. W3C trace context continues from Wurzburg HTTP through outbox publication
    and the future Wolfsburg consumer fixture.
15. Global provider-event kill switch records provider delivery as `SUPPRESSED`
    while internal CP/CPOL/CRCTL/FEE events continue normally.
16. A disabled provider subscription suppresses only that event type and records
    the subscription/config versions used for the decision.
17. Disabling the scheduled platform gate before a pending delivery is claimed
    suppresses the pending event; re-enabling does not replay it automatically.
18. Explicit admin replay creates an audited new delivery attempt with the same
    source `event_id`, and duplicate broker delivery remains consumer-safe.

Required provider/control scenarios:

1. Optional tax/registration/contact fields do not block creation or activation,
   and duplicate informational identifiers are accepted.
2. Identity/contact edits persist trusted WSO2 actor, canonical client IP,
   correlation IDs, reason, and before/after snapshots atomically.
3. `SUSPENDED` blocks every provider-scoped business command and provider event
   while platform-admin inspection, recovery, and reactivation remain available.
4. Kafka provisioning failure does not block `READY`/activation after all four
   TigerBeetle accounts and the effective operational profile are verified.
5. All provider-level accounts are created in the configured shared ledger with
   the correct account codes and negative-balance flags; provider-user accounts
   reject negative balance.
6. Ledger/detail APIs derive posted/effective signed balances from live
   TigerBeetle counters and Oracle contains no balance columns.
7. Each grant mode enforces its documented projected IRR exposure boundary at
   exactly below, equal to, and above the limit.
8. Weekly windows honor inclusive start, exclusive end, configured timezone,
   and reject overlapping or cross-midnight definitions.
9. Scheduled profile replacement cancels the prior candidate with audit; due
   activation supersedes the old immutable profile atomically.
10. Existing national ID resolves one global user; name mismatch preserves the
    canonical name, records the provider-supplied snapshot, and sets the audit
    flag without rejecting onboarding.
11. Provider customer reference is immutable and unique per provider, while the
    same global user may be linked to another provider without extra verification.
12. Onboarding either creates/verifies every Oracle mapping, provider-user
    account, and eight usage accounts or returns failure with no partial row.
13. One provider-user account cannot be active behind two cards; same-PAN
    reprint keeps the mapping and new-PAN replacement retires the old card.
14. Full return succeeds only when `expected_remaining_amount_rials` equals the
    positive live TigerBeetle balance. Lower, higher, zero, and stale expected
    values return the centralized conflict/validation result with no movement.
15. Batch original/result files are stored in MinIO and mixed rows produce
    independent atomic outcomes with centralized result codes.
16. Every HTTP, batch, provisioning, movement, outbox, and recovery path emits
    connected OTel spans without PAN, national ID, contacts, secrets, or raw
    payloads in telemetry.
17. Provider and cardholder return routes create the same WAL/movement shape,
    differ only by trusted initiator, and emit one `CREDIT_RETURNED` event.
18. A Confirm racing a return is serialized by `Lock-CP`; after the observed
    balance changes, the return fails rather than reclaiming a partial amount.
19. Crash after WAL intent but before TigerBeetle safely reuses the same transfer
    ID; crash after TigerBeetle success finalizes from exact transfer lookup.
20. Kafka timeout after broker acceptance republishes the same immutable event
    ID and produces no duplicate ledger effect.
21. Synchronous onboarding hides `PROVISIONING` rows, recovers deterministic
    account creation after process loss, and returns only after every required
    row/account/outbox fact is finalized.
22. Bulk onboarding runs the same single-row command and WAL independently; it
    introduces no second onboarding state machine.
23. Admin subscription replacement enforces `expected_version`, audits the full
    before/after set, and provider credentials cannot mutate subscriptions.
24. Wurzburg and Wolfsburg fixtures validate every public provider event against
    the same JSON Schema/DTO crate.
25. Provider Kafka records contain masked identifiers and permitted headers only;
    no internal operation/WAL/Redis/TigerBeetle/OTel fields are present.

Failure scenarios must assert both sides of the proof: no duplicate ledger
movement and no stale Redis profile becoming readable by Nuremberg.
