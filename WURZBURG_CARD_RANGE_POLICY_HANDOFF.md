# Wurzburg Card Range And Policy Handoff

This document records the updated design decision reached after reviewing the
original Wurzburg service handoff and refining the card/card-range model.

It is intentionally narrower than `WURZBURG_SERVICE_HANDOFF.md`. Its purpose is
to preserve the current agreement for the next implementation step and to make
the required Nuremberg contract change explicit.

## 1. Scope Of This Step

The next implementation step focuses on the card-range and policy foundation.

This step must answer:

- Which card ranges exist?
- Is each range `SingleProvider` or `MultiProvider`?
- Which providers are allowed behind each range?
- Which policy profile is active for each range?
- Which Redis card-policy key should Nuremberg read?

This step does not create or publish `CP:{card_number}` yet. Card profiles are
created later, after actual cards, users, provider funding sources, and
card-specific ledger accounts exist.

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

## 3. Database Model

### `card_ranges`

```text
card_range_id RAW(16) primary key
start_card_number string, normalized
end_card_number string, normalized
funding_mode string: SINGLE_PROVIDER | MULTI_PROVIDER
status string: DRAFT | ACTIVE | SUSPENDED
metadata_json JSON
created_by RAW(16)
updated_by RAW(16)
created_at timestamp with time zone
updated_at timestamp with time zone
```

Rules:

- Card ranges must not overlap.
- Card numbers must be normalized before persistence.
- `funding_mode` is the source of truth for single-provider vs multi-provider
  behavior.
- `DRAFT` ranges are editable.
- `ACTIVE` ranges can be used for card assignment.
- `SUSPENDED` ranges cannot issue new cards, but suspension does not
  automatically block existing cards.

Recommended lookup indexes:

```text
card_ranges(status, start_card_number, end_card_number)
card_ranges(funding_mode, status)
```

Overlap validation query shape:

```sql
WHERE start_card_number <= :new_end
  AND end_card_number >= :new_start
  AND status <> 'DELETED'
```

### `card_range_providers`

```text
card_range_id RAW(16)
provider_id RAW(16)
status string: ACTIVE | SUSPENDED
metadata_json JSON
created_by RAW(16)
updated_by RAW(16)
created_at timestamp with time zone
updated_at timestamp with time zone

primary key (card_range_id, provider_id)
```

Rules:

- This table is provider eligibility only.
- It does not contain cardholder priority.
- It does not contain provider relationship type.
- For `SINGLE_PROVIDER`, exactly one active provider is required before range
  activation.
- For `MULTI_PROVIDER`, one or more active providers may be attached.

Recommended indexes:

```text
primary key (card_range_id, provider_id)
card_range_providers(card_range_id, status)
card_range_providers(provider_id, status)
```

If detailed history is needed later, use a separate history table instead of
adding a surrogate row ID to the current-state table:

```text
card_range_provider_history
- history_id RAW(16)
- card_range_id RAW(16)
- provider_id RAW(16)
- old_status
- new_status
- changed_by
- changed_at
- reason
```

### `card_policy_profiles`

Policy profiles are immutable/auditable.

```text
card_policy_profile_id RAW(16) primary key
profile_json JSON
status string: DRAFT | ACTIVE | SUPERSEDED | SUSPENDED
version number
effective_at timestamp with time zone
superseded_by_profile_id RAW(16) nullable
created_by RAW(16)
created_at timestamp with time zone
```

Do not duplicate `funding_mode` as a policy scope field. The funding mode belongs
to `card_ranges`.

The profile body should contain the withdrawal policy and calendar fields that
Nuremberg needs:

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
  },
  "calendar": {}
}
```

### `card_range_policy_assignments`

```text
card_range_id RAW(16)
card_policy_profile_id RAW(16)
status string: ACTIVE | SUPERSEDED
assigned_by RAW(16)
assigned_at timestamp with time zone
superseded_at timestamp with time zone nullable

primary key (card_range_id, card_policy_profile_id)
```

Rules:

- Only one active policy assignment is allowed per card range.
- Changing policy creates a new `card_policy_profiles` row.
- The old assignment is marked `SUPERSEDED`.
- The old profile row is retained for audit and FundingPlan traceability.

## 4. Redis Policy Contract

The original handoff section 11 used provider/default scoped keys:

```text
CPOL:SingleProvider:{provider_id}
CPOL:MultiProvider:DEFAULT
```

That is no longer the target design.

The updated design supports multiple single-provider ranges and multiple
multi-provider ranges. Therefore card policies are range-scoped.

### New Redis Keys

```text
CPOL:SingleProvider:{card_range_id}
CPOL:MultiProvider:{card_range_id}
```

The key is derived from:

- `funding_mode`
- `card_range_id`

### Redis Value

Example:

```json
{
  "id": "card_policy_profile_id",
  "card_range_id": "uuid",
  "funding_mode": "SingleProvider",
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
  "calendar": {}
}
```

Rules:

- The Redis value contains the immutable `card_policy_profile_id`.
- Nuremberg stores that exact policy ID in FundingPlan.
- Updating policy terms publishes a new policy profile ID to the same
  range-scoped Redis key.
- Existing `CP:{card_number}` objects do not need to be rewritten when only the
  policy terms change.

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
```

### Range Providers

```text
POST   /api/v1/card-ranges/{card_range_id}/providers/{provider_id}
DELETE /api/v1/card-ranges/{card_range_id}/providers/{provider_id}
GET    /api/v1/card-ranges/{card_range_id}/providers
GET    /api/v1/providers/{provider_id}/card-ranges
```

### Range Policies

```text
POST   /api/v1/card-ranges/{card_range_id}/policy
GET    /api/v1/card-ranges/{card_range_id}/policy
```

## 7. API Behavior Rules

- Every mutating API requires `Idempotency-Key`.
- Same idempotency key and same request hash returns the previous result.
- Same idempotency key and different request hash returns conflict.
- Structured API errors must be used.
- Handlers must stay thin.
- Business rules live in application services.
- Oracle is the only persistence target.
- Domain and service code must not depend on Oracle row types or SQL details.

Recommended structured error shape:

```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Human-readable message",
    "details": {}
  }
}
```

## 8. Implementation Shape

Recommended module layout:

```text
src/domain/card_range.rs
src/domain/card_policy.rs

src/services/card_range_service.rs
src/services/card_policy_service.rs

src/api/dto/card_range.rs
src/api/dto/card_policy.rs
src/api/handlers/card_range.rs
src/api/handlers/card_policy.rs

src/db/traits/card_range.rs
src/db/traits/card_policy.rs

src/db/oracle/card_range.rs
src/db/oracle/card_policy.rs
```

Recommended Oracle migration contents:

```text
card_ranges
card_range_providers
card_policy_profiles
card_range_policy_assignments
idempotency_records if not already present
```

## 9. Nuremberg Update Required

Nuremberg currently expects section-11 style policy keys and scope:

```text
CPOL:SingleProvider:{provider_id}
CPOL:MultiProvider:DEFAULT
```

```rust
pub enum CardPolicyScope {
    SingleProvider { provider_id: Uuid },
    MultiProviderDefault,
}
```

Nuremberg must be updated to the range-scoped contract:

```text
CPOL:SingleProvider:{card_range_id}
CPOL:MultiProvider:{card_range_id}
```

Suggested Nuremberg model direction:

```rust
pub struct CardPolicyProfile {
    pub id: Uuid,
    pub card_range_id: Uuid,
    pub funding_mode: FundingMode,
    pub withdrawal_limits: WithdrawalLimits,
    pub calendar: LimitCalendarPolicy,
}

pub enum FundingMode {
    SingleProvider,
    MultiProvider,
}
```

When Nuremberg reads `CP:{card_number}`, it uses:

```text
card_range_id
funding_mode
```

to derive the policy key.

## 10. Later Steps

After this foundation, continue with:

1. Minimal provider core.
2. Provider ledger accounts.
3. Cards and card ownership.
4. Card-provider funding sources.
5. Cardholder funding order.
6. `CP:{card_number}` generation.
7. Profile publish worker/outbox.
8. Provider/user funding with WAL and recovery.

