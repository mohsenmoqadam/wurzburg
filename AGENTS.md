# Wurzburg Engineering Rules

## Language

- All source code, comments, documentation, commit messages, and generated artifacts must be written in English.
- Do not add Persian text to repository files.

## Observability

- OpenTelemetry and distributed tracing are mandatory for every Wurzburg service, worker, dependency boundary, and asynchronous transition.
- Every new service/API must be instrumented when it is introduced, not deferred to a later cleanup phase.
- Structured/distributed logs are mandatory for production diagnostics and must use the same safe-field policy as spans.
- Every HTTP route must run inside a server span and record the route template, method, status, centralized result code, service version, and environment.
- Validation, authorization, idempotency, Oracle, TigerBeetle, Dragonfly, Kafka/outbox, MinIO, WAL, recovery, and materialization receipt work must create child spans or linked spans as appropriate.
- Internal Kafka events must propagate W3C `traceparent` and allowlisted `tracestate`; retries and replays must preserve event identity and link to the original trace context.
- Provider-facing Kafka events must never include `traceparent`, `tracestate`, `baggage`, internal correlation IDs, or other OTel internals.
- Never emit raw PAN, national ID, names, contacts, request/response bodies, unrestricted metadata, JWTs, API tokens, Kafka credentials, database credentials, MinIO secrets, signed URLs, Dragonfly profile payloads, or SQL bind values into logs, spans, metrics, provider events, or audit snapshots.
- Tempo/OTel availability is not a business correctness dependency. Telemetry export must be best-effort and must not block financial recovery.

## Testing

- Scenario test filenames should expose the category, main scenario area, and outcome class.
- Use names like `api_trusted_context_scenarios_success.rs` and `api_trusted_context_scenarios_failed.rs`.
- Prefer scenario tests for contract, security, idempotency, recovery, and cross-boundary behavior.
- Public APIs must be tested through a real running Wurzburg instance as part of the integration path, using the same HTTP headers, idempotency rules, authorization, business validation, result-code contract, and telemetry behavior expected in production.
- JWT and WSO2 trust-boundary tests must use real signed JWTs and configured validation keys/issuer/audience/algorithms. Do not bypass token validation with hand-built claims or test-only shortcuts in API tests.

## Security

- Do not add simplified, fake, or bypass security paths to production code. Test fixtures may generate realistic signed inputs, but runtime code must use the same validation boundary intended for production.
- JWT validation must reject unsigned tokens, symmetric-algorithm confusion, unexpected algorithms, invalid issuer/audience, expired/not-yet-valid tokens, and conflicting `azp`/`client_id` claims.

## Architecture

- Oracle is the only relational persistence target for Wurzburg.
- Do not add PostgreSQL, SQLx, or database-neutral repository abstractions for Wurzburg business persistence.
- Keep handlers thin. Business rules belong in application services, and SQL/Oracle row mapping belongs under `src/db/oracle`.
- Wurzburg owns canonical Oracle facts, TigerBeetle commands, and durable events. Wolfsburg is the sole writer of replacement CP, CPOL, CRCTL, and FEE runtime projections.
