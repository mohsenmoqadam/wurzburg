# ADR 0001: Oracle Persistence Strategy

## Status

Accepted for the Wurzburg final-service branch.

## Context

Wurzburg owns durable business state for providers, users, cards, funding
relationships, profile publication, idempotency, audit, imports, reports, and
the facts Wolfsburg needs for event processing and rollback recovery.

Staging and production target Oracle. The final service must not inherit
database assumptions from the proof of concept such as native enum types,
`JSONB`, `ON CONFLICT`, trigger behavior, or SQLx row mapping inside domain
models.

The architecture goal is to match the discipline used in Nuremberg: external
systems get explicit boundary modules, handlers stay thin, and business
behavior lives behind application services and repository traits.

## Decision

Wurzburg will be Oracle-only for its RDBMS.

The main implementation path uses an Oracle adapter behind repository traits.

Rust domain and application modules must not depend on Oracle row types,
SQLx derive macros, or SQL dialect details.

## Oracle Modeling Rules

- UUID domain identities are stored in Oracle as `RAW(16)`.
- Money amounts are integer minor units stored as `NUMBER(38,0)` or a narrower
  constrained numeric type when safe.
- Status values are constrained strings or lookup-table references, not
  database-specific enum types.
- JSON documents use Oracle native JSON where available. If a deployment does
  not support the native JSON type, use `CLOB` with JSON validation constraints.
- Timestamps are timezone-aware.
- Every externally triggered mutation has a durable idempotency record with the
  key, request hash, operation type, status, and response/resource snapshot.
- Ledger-affecting operations persist a recoverable intent before calling
  TigerBeetle.

## Rust Driver Direction

The initial Oracle boundary uses the `oracle` crate. The crate is ODPI-C based,
provides Oracle connection pooling, and requires Oracle Client libraries at
runtime. Because it is a blocking driver, Wurzburg isolates Oracle calls inside
the `db::oracle` module and uses `tokio::task::spawn_blocking` for async service
integration.

Oracle application configuration uses separate `username`, `password`, and
`connect_string` fields. Wurzburg does not use an Oracle URL containing
credentials. The agreed target service name is `HYPERCARD`, registered by DBA,
with the application connect string:

```text
//87.247.175.207:1521/HYPERCARD
```

The DBA/application-user setup script is documented in:

```text
docs/oracle/create_wurzburg_user.sql
```

Application code should call repository traits and should not know whether a
specific operation is backed by a blocking driver, an async driver, or a test
double.

## Module Boundary

Initial shape:

```text
src/db/
  error.rs
  traits/
  oracle/
    mod.rs
    pool.rs
    types.rs
    health.rs
```

The Oracle module owns:

- connection configuration parsing
- pool creation
- blocking execution isolation
- Oracle error mapping
- Oracle-specific type conversion such as UUID `RAW(16)`
- migration execution once the migration strategy is added

## Consequences

This makes early development a little stricter because Oracle must be available
for full persistence verification. That strictness is intentional: Oracle is the
production contract.

The existing PoC remains useful as inventory, but final Wurzburg features should
be rebuilt through the new service and repository boundaries.
