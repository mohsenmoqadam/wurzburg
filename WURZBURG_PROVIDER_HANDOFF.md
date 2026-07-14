# Wurzburg Provider Handoff

This document captures the Provider design understanding before implementing
the Provider API set.

It extends the card-range foundation documented in
`WURZBURG_CARD_RANGE_POLICY_HANDOFF.md`.

## 0. Relationship To Main Service Handoff

`WURZBURG_SERVICE_HANDOFF.md` is the broad system handoff and should be used for
service boundaries, recovery, Redis/Nuremberg responsibilities, WSO2 trust,
observability, reporting/import/export direction, and integration-test style.

This Provider handoff is narrower and records the refined decisions reached
after the main handoff was written. When they differ, this document and
`WURZBURG_CARD_RANGE_POLICY_HANDOFF.md` represent the current Provider/Card
design.

Current refinements over the main service handoff:

- Oracle is the only persistence target for this implementation path.
- Card policy Redis keys are range-scoped:
  - `CPOL:SingleProvider:{card_range_id}`
  - `CPOL:MultiProvider:{card_range_id}`
- `card_ranges.owner_provider_id` is not used. Provider eligibility is
  represented by `card_range_providers`.
- Each provider can be attached to exactly one card range.
- Card-range attachment is admin-only.
- Provider-user TigerBeetle accounts are the real provider-funded balance
  buckets for users.
- `card_provider_funding_sources` maps a card to one or more provider-user
  accounts and stores card-specific priority/max rules.
- There is no separate card-specific funding balance account in the current
  Provider design.
- Card policy usage accounts remain card-specific and are used for
  daily/weekly/monthly/yearly amount/count windows.
- Debit, credit, balance, and usage values are never stored in Oracle. They are
  read from TigerBeetle when responses/events/Redis payloads/reports are built.

## 1. Current Understanding

My current confidence level is high on the core domain direction, and medium on
the exact operational policy details that still need final numbers/rules.

What is clear:

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
- Provider-created cards must belong to a card range for which that provider is
  eligible.
- Providers do not own card ranges. The bank/system defines card ranges and
  assigns eligible providers to those ranges.
- Providers are expected to know which card number belongs to which national ID
  before calling Wurzburg user/card APIs. The card number may come from a future
  bank-owned card-number issuing service.
- Providers have several operational states and emergency controls.
- Provider events are delivered through Kafka, and provider onboarding must
  provision the Kafka access needed for event delivery.
- Provider balance/credit movement eventually depends on TigerBeetle accounts.

## 2. Provider Responsibilities

A Provider can:

- be attached to exactly one card range as an eligible funding provider
- add/link users
- assign cards to users from eligible ranges
- grant credit to linked users/cards
- reduce/revoke credit from linked users/cards
- receive operational and financial events through Kafka
- query its own accounts and transactions
- query card-level transactions limited to its own funding participation

System admins can:

- create and configure providers
- attach/detach providers to card ranges
- inspect all provider/user/card/transaction data
- override or suspend provider capabilities
- query complete multi-provider card transaction history

## 3. Provider Identity Model

Provider identity should be richer than the current PoC `providers` table.

Required identity fields:

- `provider_id`
- legal registered name
- trade/commercial name
- tax/economic identifier
- registration number, if required
- email addresses
- website URL
- mailing/registered address
- lifecycle status
- operational metadata
- created/updated actor and timestamps

Users created or linked by providers are not identified externally by Wurzburg's
UUID. Provider user onboarding uses national ID as the unique external identity.
Wurzburg creates and returns an internal UUID for later API use.

Contact information should not stay as a single phone/email field forever.

Recommended structure:

```text
provider_contacts
- provider_contact_id RAW(16) primary key
- provider_id RAW(16) not null
- contact_type string:
  FINANCE | TECHNICAL | OPERATIONS | SECURITY | NOTIFICATION | LEGAL
- name string nullable
- email string nullable
- phone string nullable
- mobile string nullable
- metadata_json JSON default '{}' not null
- status ACTIVE | SUSPENDED
- created_at
- updated_at
```

Reason:

- finance, technical, and notification contacts have different lifecycles
- emergency operations should be able to target the correct contact group
- this avoids packing contact data into unqueryable JSON too early

## 4. Provider Lifecycle

Provider status:

```text
DRAFT
PENDING_PROVISIONING
ACTIVE
SUSPENDED
INACTIVE
FAILED
```

Meaning:

- `DRAFT`: provider identity exists but is not operational.
- `PENDING_PROVISIONING`: Kafka/TigerBeetle/resources are being provisioned.
- `ACTIVE`: provider can operate within its configured limits.
- `SUSPENDED`: provider exists but operational actions are blocked.
- `INACTIVE`: provider is retired.
- `FAILED`: provisioning failed and needs operator intervention.

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
- Card-range attachment is admin-only.

## 6. Global User Registry And Provider Links

Wurzburg should have one global user registry.

User model:

```text
users
- user_id RAW(16) primary key
- national_id string unique not null
- first_name string
- last_name string
- birth_date date nullable
- mobile string nullable
- metadata_json JSON default '{}' not null
- status ACTIVE | SUSPENDED
- created_at
- updated_at
```

Provider-to-user link:

```text
provider_users
- provider_id RAW(16) not null
- user_id RAW(16) not null
- provider_customer_reference string nullable
- status ACTIVE | SUSPENDED
- metadata_json JSON default '{}' not null
- created_at
- updated_at
- primary key (provider_id, user_id)
```

Rules:

- User uniqueness is based on national identity.
- `user_id` is an internal UUID generated by Wurzburg.
- If the national ID does not exist, create a global user then link.
- If the national ID exists, link the existing user to the provider.
- Provider-specific customer references belong to `provider_users`, not global
  `users`.

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

Provider-user account table:

```text
provider_user_accounts
- provider_user_account_id RAW(16) primary key
- provider_id RAW(16) not null
- user_id RAW(16) not null
- tigerbeetle_account_id RAW(16) not null unique
- status ACTIVE | SUSPENDED | CLOSED
- metadata_json JSON default '{}' not null
- created_at
- updated_at
```

Constraints:

```text
unique(provider_id, user_id)
```

When a provider assigns a card or becomes a funding source behind a card,
Wurzburg links that card to the existing provider-user account:

```text
card_provider_funding_sources
- card_id RAW(16) not null
- provider_id RAW(16) not null
- provider_user_account_id RAW(16) not null
- priority NUMBER nullable
- max_amount NUMBER(19,0) nullable
- status ACTIVE | SUSPENDED | CLOSED
- metadata_json JSON default '{}' not null
- created_at
- updated_at
- primary key(card_id, provider_id)
```

Constraints:

```text
unique(card_id, provider_id)
```

Important rule:

- `provider_user_accounts` are the actual funded accounts. If four providers
  are behind a user's card, there are four provider-user accounts, each with its
  own TigerBeetle balance.
- `card_provider_funding_sources` does not create another balance account. It
  says which provider-user accounts are allowed behind this card and stores
  card-specific selection rules such as priority and maximum consumable amount.
- The account published to Nuremberg inside `CP:{card_number}` as
  `user_provider_account` is the provider-user account selected for that
  card/provider funding source.
- On Confirm, Nuremberg consumes providers according to the card-specific
  priority/max rules and debits the corresponding provider-user TigerBeetle
  account.
- If the same provider-user account is allowed behind multiple cards for the
  same user, those cards share that provider-user balance. TigerBeetle remains
  the source of truth for concurrent debits.

Each card also needs its own policy usage account set for withdrawal window
limits. These accounts are technical TigerBeetle accounts used by Nuremberg to
track limit consumption.

Card policy usage account table:

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
- status ACTIVE | SUSPENDED
- created_at
- updated_at
```

These eight accounts correspond to:

```text
amount: daily, weekly, monthly, yearly
count:  daily, weekly, monthly, yearly
```

Rules:

- Create policy usage accounts when a card is created or when CP inputs are
  first generated.
- Store the account UUIDs durably in Oracle.
- Never store usage amount/count balances in Oracle.
- Nuremberg reads these account UUIDs from `CP:{card_number}` and uses
  TigerBeetle to apply the policy windows.

## 8. Cards And Provider Funding Sources

Each card belongs to exactly one user.

Card model:

```text
cards
- card_id RAW(16) primary key
- card_number string unique not null
- user_id RAW(16) not null
- card_range_id RAW(16) not null
- funding_mode copied/derived from card range
- status ACTIVE | SUSPENDED | EXPIRED
- metadata_json JSON default '{}' not null
- created_at
- updated_at
```

Provider funding relationship for each card:

```text
card_provider_funding_sources
- card_id RAW(16) not null
- provider_id RAW(16) not null
- provider_user_account_id RAW(16) not null
- priority number nullable
- max_amount number nullable
- status ACTIVE | SUSPENDED
- metadata_json JSON default '{}' not null
- primary key (card_id, provider_id)
```

Rules:

- Card ranges are defined by the bank/system and assigned to providers through
  `card_range_providers`.
- Provider onboarding/user-card APIs receive the card number already known to
  the provider.
- Wurzburg validates that the submitted card number belongs to a card range
  where the provider is active and eligible.
- For a single-provider card, one active funding source is allowed.
- For a multi-provider card, multiple active funding sources are allowed.
- Cardholder/provider funding priority is card-specific, not range-specific.
- This model is the source for future `CP:{card_number}` generation.

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
  provider's currently distributed credit position across its users.
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
- Provider-user/card ledger accounts should be a separate table, not stored in
  `provider_ledger_accounts`.
- Fee source is controlled by range/card policy. If the configured fee source
  is provider-funded, Nuremberg/Wurzburg movement should debit `PROVIDER_FEE`
  and credit `PLATFORM_FEE`.

## 10. Provider Operational Controls

Provider controls should be explicit and versioned/auditable.

- Provider operational profiles are scheduled.
- A profile becomes active only after `effective_at`.
- APIs and workers must resolve the active provider profile using both status
  and `effective_at <= now`.
- Creating a future-dated profile must not affect current provider behavior
  until its effective time.

Operational profile table:

```text
provider_operational_profiles
- provider_operational_profile_id RAW(16) primary key
- provider_id RAW(16) not null
- status ACTIVE | SUPERSEDED | SUSPENDED
- profile_json JSON not null
- version number not null
- effective_at timestamp with time zone
- created_by
- created_at
```

Operational profile body:

```json
{
  "user_onboarding": {
    "enabled": true,
    "active_windows": [],
    "max_total_users": null,
    "max_users_per_day": null
  },
  "credit_grant": {
    "enabled": true,
    "mode": "FixedLimit",
    "fixed_limit_amount": null,
    "cms_debt_limit_amount": null,
    "provider_vs_cms_delta_limit_amount": null
  },
  "credit_reduction": {
    "enabled": true
  },
  "card_issuance": {
    "enabled": true
  },
  "event_delivery": {
    "enabled": true
  }
}
```

Credit grant limit modes:

```text
FixedLimit
CmsDebtLimit
ProviderOwnedVsCmsSettlementDelta
```

Additional controls supported by the operational profile:

- max cards per provider
- max active cards per user per provider
- max batch size per API call
- daily API operation limits
- emergency read-only mode
- event delivery pause
- Kafka credential suspension
- per-card-range onboarding enablement

## 11. Kafka Provisioning

Provider onboarding must provision Kafka resources.

### Legacy PoC Reference

The previous PoC branch created Kafka access directly inside
`POST /api/v1/providers`.

Legacy create-provider request:

```json
{
  "legal_name": "Legal Provider Name",
  "trade_name": "Provider Brand",
  "tax_id": "1234567890",
  "email_address": "ops@example.com",
  "office_phone": "+982100000000",
  "website_url": "https://example.com",
  "mailing_address": "Registered address",
  "alert_phone_numbers": ["+989120000000"],
  "fee_rate_bps": 0,
  "fixed_fee_amount": 0
}
```

Legacy response:

```json
{
  "provider_id": "uuid",
  "ledger_account_id": "uuid",
  "kafka": {
    "topic": "provider.events.{provider_id_simple}",
    "brokers": ["host:port"],
    "security_protocol": "SASL_SSL",
    "sasl_mechanism": "SCRAM-SHA-512",
    "username": "provider_user_{provider_id_simple}",
    "password": "{random_uuid_simple}",
    "security_cert": "{certificate_file_content}"
  }
}
```

Legacy generated Kafka data:

```text
topic_name      = provider.events.{provider_id.simple()}
kafka_username  = provider_user_{provider_id.simple()}
kafka_password  = random UUID without hyphens
brokers         = kafka.consumer_defaults.bootstrap_servers split by comma
security_protocol = kafka.consumer_defaults.security_protocol
sasl_mechanism  = SCRAM-SHA-512
security_cert   = contents of Kafka CA certificate file
```

Legacy persistence:

```text
providers.kafka_config JSONB contained the full Kafka settings object,
including username, password, broker list, protocol, mechanism, topic, and
certificate content.
```

Legacy handler order:

1. Validate provider request.
2. Generate `provider_id` and one `ledger_account_id`.
3. Create one TigerBeetle provider ledger account using:
   - `id = ledger_account_id.as_u128()`
   - `user_data_128 = provider_id.as_u128()`
   - `ledger = config.tigerbeetle.ledger_id`
   - `code = config.tigerbeetle.provider_account_code`
4. Generate Kafka topic/user/password/settings.
5. Insert provider row in PostgreSQL.
6. Run Kafka admin shell commands inside `tokio::task::spawn_blocking`.
7. Return provider ID, ledger account ID, and Kafka settings.

Legacy Kafka admin calls:

```text
create_provider_topic(topic_name)
create_scram_user(username, password)
grant_consumer_acls(topic_name, username)
```

The old code logged Kafka provisioning errors but still returned success if the
database insert had already succeeded. This is not acceptable for the production
Provider implementation because the provider can appear active while Kafka
access is incomplete.

### Current Kafka Admin Module

The current `AppKafkaAdmin` still provides the useful low-level operations:

```text
create_provider_topic(topic_name)
delete_provider_topic(topic_name)
create_scram_user(username, password)
delete_scram_user(username)
grant_consumer_acls(topic_name, username)
create_topic(topic_name)
grant_producer_acls(topic_name, username)
```

Command behavior:

- `create_provider_topic` executes `kafka-topics.sh --create` with configured
  partitions and replication factor. `TopicExistsException` is treated as
  idempotent success.
- `delete_provider_topic` executes `kafka-topics.sh --delete`.
  `UnknownTopicOrPartitionException` is treated as idempotent success.
- `create_scram_user` executes `kafka-configs.sh --alter --add-config
  SCRAM-SHA-512=[password=...] --entity-type users --entity-name ...`.
- `delete_scram_user` executes `kafka-configs.sh --alter --delete-config
  SCRAM-SHA-512 --entity-type users --entity-name ...`.
- `grant_consumer_acls` grants `Read` and `Describe` on the provider topic, then
  grants `Read` on all consumer groups.
- `grant_producer_acls` grants `Write` and `Describe` on the topic.

Because these shell commands block, they must stay inside `spawn_blocking` when
called from async code.

### Final Kafka Provisioning Design

- Provider provisioning is asynchronous.
- `POST /providers` creates the provider in `PENDING_PROVISIONING`.
- Wurzburg stores a provisioning job/outbox row.
- A worker completes Kafka/TigerBeetle provisioning.
- Provider becomes `ACTIVE` only after provisioning succeeds.
- If provisioning fails, provider moves to `FAILED` or remains
  `PENDING_PROVISIONING` with retry metadata.
- Kafka is outbound from Wurzburg to providers for now. Provider operations are
  requested through HTTP APIs, not inbound Kafka commands.
- Kafka credentials are retrieved through a dedicated API after provisioning.
- Plaintext Kafka password storage in Oracle is acceptable for this phase.
- The topic and username templates are resolved before storage. Do not persist
  literal placeholders such as `{provider_id_simple}`.

Provisioning steps:

1. Generate `provider_id`.
2. Resolve Kafka names:
   - `topic_name = provider.events.{provider_id.simple()}`
   - `username = provider_user_{provider_id.simple()}`
   - `password = random UUID without hyphens`
3. Insert provider row, Kafka access row, four ledger account rows, and
   provisioning job rows in Oracle.
4. Worker creates the Kafka topic.
5. Worker creates the SCRAM-SHA-512 user.
6. Worker grants consumer ACLs for provider events.
7. Worker provisions TigerBeetle accounts.
8. Worker marks provisioning jobs `SUCCEEDED`.
9. Worker marks provider `ACTIVE` only after all required resources are ready.

Production Kafka access table:

```text
provider_kafka_access
- provider_kafka_access_id RAW(16) primary key
- provider_id RAW(16) not null unique
- topic_name VARCHAR2(255) not null unique
- username VARCHAR2(255) not null unique
- password VARCHAR2(512) not null
- security_protocol VARCHAR2(64) not null
- sasl_mechanism VARCHAR2(64) not null
- bootstrap_servers_json JSON not null
- security_cert CLOB nullable
- credential_status ACTIVE | ROTATING | SUSPENDED | REVOKED
- last_delivered_at TIMESTAMP WITH TIME ZONE nullable
- rotated_at TIMESTAMP WITH TIME ZONE nullable
- created_at
- updated_at
```

Production provisioning job table:

```text
provider_provisioning_jobs
- provider_provisioning_job_id RAW(16) primary key
- provider_id RAW(16) not null
- job_type PROVIDER_CREATE | KAFKA_PROVISION | KAFKA_ROTATE | TB_PROVISION
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

Production Kafka credential retrieval API:

```text
GET /api/v1/providers/{provider_id}/kafka/credentials
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
  "password": "stored-provider-password",
  "security_cert": "certificate content",
  "credential_status": "ACTIVE"
}
```

Certificate delivery can stay as a separate endpoint if we do not want to
include certificate content in the credential response:

```text
GET /api/v1/providers/kafka/certificate
```

## 12. Provider Database Contract

All Provider tables should be Oracle-native and should follow the current
project conventions:

- UUIDs stored as `RAW(16)`
- JSON stored as Oracle `JSON`
- status values constrained with check constraints
- immutable/versioned rows for operational profiles
- `created_at` and `updated_at` populated consistently
- indexes for every high-frequency lookup path

### providers

```text
providers
- provider_id RAW(16) primary key
- legal_name VARCHAR2(255) not null
- trade_name VARCHAR2(255) not null
- tax_id VARCHAR2(64) not null
- registration_number VARCHAR2(128) nullable
- email_address VARCHAR2(255) not null
- website_url VARCHAR2(512) nullable
- mailing_address VARCHAR2(2000) not null
- status DRAFT | PENDING_PROVISIONING | ACTIVE | SUSPENDED | INACTIVE | FAILED
- metadata_json JSON default '{}' not null
- created_by VARCHAR2(255) not null
- updated_by VARCHAR2(255) nullable
- created_at TIMESTAMP WITH TIME ZONE default SYSTIMESTAMP not null
- updated_at TIMESTAMP WITH TIME ZONE default SYSTIMESTAMP not null
```

Indexes/constraints:

```text
unique(tax_id)
index(status)
```

### card_range_providers

This table already exists for card-range eligibility. For Provider
implementation it must also enforce the final Provider rule:

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

Constraints:

```text
unique(provider_id)
```

The unique provider constraint means a provider can be attached to only one card
range. In a multi-provider card range, many providers can point to the same
`card_range_id`, but each individual provider still has only one range.

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
- metadata_json JSON default '{}' not null
- status ACTIVE | SUSPENDED
- created_at
- updated_at
```

Indexes:

```text
index(provider_id, contact_type, status)
```

### provider_user_accounts

```text
provider_user_accounts
- provider_user_account_id RAW(16) primary key
- provider_id RAW(16) not null
- user_id RAW(16) not null
- tigerbeetle_account_id RAW(16) not null unique
- status ACTIVE | SUSPENDED | CLOSED
- metadata_json JSON default '{}' not null
- created_at
- updated_at
```

Do not add debit, credit, or balance columns to this table.

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
- status ACTIVE | SUSPENDED
- created_at
- updated_at
```

Do not add consumed amount/count columns to this table.

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

Provider creation should enqueue provisioning for all four account categories,
not just one legacy provider ledger account.

### provider_operational_profiles

```text
provider_operational_profiles
- provider_operational_profile_id RAW(16) primary key
- provider_id RAW(16) not null
- status ACTIVE | SUPERSEDED | SUSPENDED
- version NUMBER(19,0) not null
- effective_at TIMESTAMP WITH TIME ZONE not null
- profile_json JSON not null
- superseded_by_profile_id RAW(16) nullable
- created_by VARCHAR2(255) not null
- created_at TIMESTAMP WITH TIME ZONE default SYSTIMESTAMP not null
```

Indexes/constraints:

```text
unique(provider_id, version)
index(provider_id, status, effective_at)
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
- provider_customer_reference VARCHAR2(255) nullable
- status ACTIVE | SUSPENDED
- metadata_json JSON default '{}' not null
- created_at
- updated_at
- primary key(provider_id, user_id)

cards
- card_id RAW(16) primary key
- card_number VARCHAR2(32) unique not null
- user_id RAW(16) not null
- card_range_id RAW(16) not null
- funding_mode SINGLE_PROVIDER | MULTI_PROVIDER
- status ACTIVE | SUSPENDED | EXPIRED
- metadata_json JSON default '{}' not null
- created_at
- updated_at

card_provider_funding_sources
- card_id RAW(16) not null
- provider_id RAW(16) not null
- provider_user_account_id RAW(16) not null
- priority NUMBER nullable
- max_amount NUMBER(19,0) nullable
- status ACTIVE | SUSPENDED
- metadata_json JSON default '{}' not null
- primary key(card_id, provider_id)
```

## 13. Event Model

Provider-facing events should include:

- provider created/provisioned/suspended
- user linked
- card issued
- credit granted
- credit reduced/revoked
- withdrawal confirmed by Nuremberg
- withdrawal rollback/reversal
- fee charged
- balance snapshot or balance response, if required

Every event should include:

- event ID
- event type
- provider ID
- card number or card ID when relevant
- user ID when relevant
- idempotency/correlation IDs
- occurred_at
- payload version

## 14. Provider APIs

Initial Provider administration APIs:

```text
POST   /api/v1/providers
GET    /api/v1/providers/{provider_id}
GET    /api/v1/providers
PATCH  /api/v1/providers/{provider_id}
POST   /api/v1/providers/{provider_id}/activate
POST   /api/v1/providers/{provider_id}/suspend
```

Provider contacts:

```text
POST   /api/v1/providers/{provider_id}/contacts
GET    /api/v1/providers/{provider_id}/contacts
PATCH  /api/v1/providers/{provider_id}/contacts/{contact_id}
DELETE /api/v1/providers/{provider_id}/contacts/{contact_id}
```

Provider operational controls:

```text
POST   /api/v1/providers/{provider_id}/operational-profile
GET    /api/v1/providers/{provider_id}/operational-profile
```

Kafka provisioning:

```text
POST   /api/v1/providers/{provider_id}/kafka/provision
POST   /api/v1/providers/{provider_id}/kafka/rotate-credentials
POST   /api/v1/providers/{provider_id}/kafka/suspend
```

Provider user onboarding:

```text
POST   /api/v1/providers/{provider_id}/users
POST   /api/v1/providers/{provider_id}/users/batch
GET    /api/v1/providers/{provider_id}/users
GET    /api/v1/providers/{provider_id}/users/{user_id}
```

Card assignment:

```text
POST   /api/v1/providers/{provider_id}/cards
POST   /api/v1/providers/{provider_id}/cards/batch
GET    /api/v1/providers/{provider_id}/cards
```

Credit operations:

```text
POST   /api/v1/providers/{provider_id}/credits/grant
POST   /api/v1/providers/{provider_id}/credits/grant/batch
POST   /api/v1/providers/{provider_id}/credits/reduce
POST   /api/v1/providers/{provider_id}/credits/reduce/batch
```

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
- WSO2 owns external access control. Wurzburg should still keep provider/admin
  route boundaries clear so WSO2 policy mapping is simple and mistakes are less
  likely.

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
      "metadata": {}
    }
  ],
  "operational_profile": {
    "effective_at": "2026-07-14T00:00:00Z",
    "profile": {
      "user_onboarding": {
        "enabled": true,
        "active_windows": [],
        "max_total_users": null,
        "max_users_per_day": null
      },
      "credit_grant": {
        "enabled": true,
        "mode": "FixedLimit",
        "fixed_limit_amount": null,
        "cms_debt_limit_amount": null,
        "provider_vs_cms_delta_limit_amount": null
      },
      "credit_reduction": {
        "enabled": true
      },
      "card_issuance": {
        "enabled": true
      },
      "event_delivery": {
        "enabled": true
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

Response includes:

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
  "contacts": [],
  "ledger_accounts": [
    {
      "account_category": "PROVIDER_OWNED",
      "account_name": "Provider Owned",
      "tigerbeetle_account_id": "uuid",
      "debits_posted": 0,
      "credits_posted": 0,
      "balance": 0,
      "status": "ACTIVE"
    },
    {
      "account_category": "PROVIDER_FEE",
      "account_name": "Provider Fee",
      "tigerbeetle_account_id": "uuid",
      "debits_posted": 0,
      "credits_posted": 0,
      "balance": 0,
      "status": "ACTIVE"
    },
    {
      "account_category": "CMS_SETTLEMENT",
      "account_name": "CMS Settlement",
      "tigerbeetle_account_id": "uuid",
      "debits_posted": 0,
      "credits_posted": 0,
      "balance": 0,
      "status": "ACTIVE"
    },
    {
      "account_category": "PLATFORM_FEE",
      "account_name": "Platform Fee",
      "tigerbeetle_account_id": "uuid",
      "debits_posted": 0,
      "credits_posted": 0,
      "balance": 0,
      "status": "ACTIVE"
    }
  ],
  "kafka": {
    "topic": "provider.events.4f3c2e1a0b9d4c7e8f6a123456789abc",
    "username": "provider_user_4f3c2e1a0b9d4c7e8f6a123456789abc",
    "credential_status": "ACTIVE"
  },
  "created_at": "2026-07-14T00:00:00Z",
  "updated_at": "2026-07-14T00:00:00Z"
}
```

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
      "ledger_accounts": [
        {
          "account_category": "PROVIDER_OWNED",
          "account_name": "Provider Owned",
          "tigerbeetle_account_id": "uuid",
          "debits_posted": 0,
          "credits_posted": 0,
          "balance": 0,
          "status": "ACTIVE"
        },
        {
          "account_category": "PROVIDER_FEE",
          "account_name": "Provider Fee",
          "tigerbeetle_account_id": "uuid",
          "debits_posted": 0,
          "credits_posted": 0,
          "balance": 0,
          "status": "ACTIVE"
        },
        {
          "account_category": "CMS_SETTLEMENT",
          "account_name": "CMS Settlement",
          "tigerbeetle_account_id": "uuid",
          "debits_posted": 0,
          "credits_posted": 0,
          "balance": 0,
          "status": "ACTIVE"
        },
        {
          "account_category": "PLATFORM_FEE",
          "account_name": "Platform Fee",
          "tigerbeetle_account_id": "uuid",
          "debits_posted": 0,
          "credits_posted": 0,
          "balance": 0,
          "status": "ACTIVE"
        }
      ],
      "created_at": "2026-07-14T00:00:00Z",
      "updated_at": "2026-07-14T00:00:00Z"
    }
  ],
  "next_page_token": null
}
```

Update provider identity:

```text
PATCH /api/v1/providers/{provider_id}
Headers: Idempotency-Key
```

Request supports identity/contact-safe fields only:

```json
{
  "trade_name": "New Brand",
  "email_address": "ops-new@example.com",
  "website_url": "https://new.example.com",
  "mailing_address": "New address",
  "metadata": {}
}
```

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
  "metadata": {}
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
  "metadata": {}
}
```

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

Success responses may return the resource body directly for now, matching the
current card APIs. Error responses must always be:

```json
{
  "error": {
    "code": "MACHINE_READABLE_CODE",
    "message": "Human readable message",
    "rs_code": 1234,
    "details": {}
  }
}
```

Provider-specific result codes should be added centrally, not as ad hoc strings
in handlers:

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
```

## 16. Implementation Order

Do not start with user credit or card funding transfers.

Recommended order:

1. Provider identity and contacts.
2. Provider lifecycle and operational profile.
3. Provider ledger account records and TigerBeetle account provisioning
   workflow.
4. Kafka provisioning model and admin flow.
5. Provider-to-card-range eligibility checks integration.
6. Global users, provider-user links, and provider-user TigerBeetle accounts.
7. Card assignment and card funding-source priority/max mapping.
8. Card policy usage accounts for amount/count limit windows.
9. Credit grant/reduce APIs.
10. CP generation/refresh for Nuremberg.
11. Transaction query APIs.
12. Provider event publishing.

## 17. Final Decisions

1. `PROVIDER_FEE` has no required initial balance and can go negative.

2. Card-range attachment is admin-only.

3. Provider operations are HTTP APIs for now. Kafka is used for outbound
   provider event delivery from Wurzburg to providers, not for inbound provider
   operation requests.

4. Admin transaction views must be completely separate `/admin` APIs, not the
   provider transaction APIs with a role flag.

5. Debit, credit, and balance values are never stored in Oracle/RDBMS. They are
   always fetched from TigerBeetle when building responses, events, Redis
   payloads, or reports.
