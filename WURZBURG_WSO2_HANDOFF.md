# Wurzburg WSO2 Integration Handoff

This document is the final gateway contract between Wurzburg and the WSO2/ESB
team. It defines what WSO2 validates, strips, creates, and forwards; what
Wurzburg validates again; and how provider, cardholder, admin, support, and
reporting callers are isolated.

It must be read with:

- `WURZBURG_CARD_RANGE_POLICY_HANDOFF.md`
- `WURZBURG_PROVIDER_HANDOFF.md`
- the generated Wurzburg OpenAPI document

Where older handoffs contain shorter WSO2 notes, this document is authoritative.

## 1. Boundary And Ownership

WSO2 owns the public API edge:

- client authentication and token validation
- application/client identification
- coarse role/scope authorization
- provider and cardholder claim injection
- rate limits, quotas, request-size limits, and CORS
- canonical correlation, request, client-IP, and trace headers
- gateway TLS and the private mTLS connection to Wurzburg
- gateway-owned error responses

Wurzburg remains responsible for:

- independently validating the configured internal JWT/assertion
- rejecting calls not received through the trusted mTLS/network boundary
- enforcing provider/cardholder resource ownership against Oracle facts
- business authorization and lifecycle/policy checks
- idempotency, WAL, Oracle/TigerBeetle/Kafka recovery, and audit
- the centralized result-code contract for application responses

WSO2 authorization is necessary but never the only business authorization.

## 2. Network Trust Model

The Wurzburg listener is private and is not internet-routable. Production
traffic reaches it only through approved WSO2 gateway nodes using mTLS.

Required controls:

- WSO2 validates the Wurzburg server certificate and configured service name.
- Wurzburg validates the WSO2 client certificate against a dedicated internal
  CA and an allowlist of accepted SAN identities.
- Certificate expiry and failed handshakes are monitored and alerted.
- Direct requests without the approved mTLS identity are rejected before HTTP
  identity headers are considered.
- WSO2 strips every inbound header reserved by this contract, then writes the
  canonical value. Public callers cannot supply trusted actor or network facts.

Reserved headers:

```text
X-JWT-Assertion
X-Correlation-Id
X-Request-Id
X-WSO2-Client-IP
X-WSO2-Gateway-Id
X-WSO2-Original-Method
X-WSO2-Original-Path
```

## 3. JWT And Claim Contract

Each deployment selects exactly one backend token transport:

```text
Authorization: Bearer <jwt>
```

or, when WSO2 must replace the public Authorization header:

```text
X-JWT-Assertion: <jwt>
```

WSO2 must not forward both. Wurzburg accepts only the transport configured for
that environment and rejects an ambiguous/missing assertion.

WSO2 validates the public credential, then forwards a WSO2-signed JWT with:

```text
iss          exact configured WSO2 issuer
aud          array/string containing wurzburg-api
sub          immutable authenticated human/service subject
azp          calling OAuth application/client ID
client_id    accepted instead of azp only when configured
iat          issued-at time
nbf          not-before time
exp          expiry time
jti          unique token identifier
roles        array of Wurzburg role strings
scope/scp    space-delimited string or array of granted scopes
provider_id  UUID, required for provider roles
user_id      UUID, required for cardholder roles
tenant_id    optional future tenant boundary; ignored unless enabled
```

Validation rules in both WSO2 and Wurzburg:

- Verify signature through configured WSO2 JWKS and support key rotation by
  `kid` with bounded cache refresh.
- Allow only configured asymmetric algorithms, initially `RS256`. Reject
  `none`, symmetric algorithms, and algorithm/key-type confusion.
- Validate `iss`, `aud`, `exp`, `nbf`, and required claims. Initial clock skew
  allowance is 60 seconds.
- Reject tokens with both conflicting `azp` and `client_id` values.
- Normalize roles/scopes into deduplicated exact strings; no prefix matching.
- Validate UUID claims before forwarding.
- Provider roles require exactly one `provider_id` and no caller-controlled
  provider identity from path/body may override it.
- Cardholder roles require exactly one `user_id`; Wurzburg still verifies card
  ownership in Oracle.
- Platform/support/reporting service identities do not require `provider_id` or
  `user_id` unless using a provider/cardholder-scoped route.

Tokens and raw JWT claims must never be written to application, gateway, audit,
or OTel logs.

### Provider And Cardholder Claim Provisioning

`provider_id` is bound to the provider's WSO2 application/service account by
platform administration. It is never accepted from a login form, URL, request
body, or arbitrary client metadata.

`user_id` is the Wurzburg UUID linked server-side to the authenticated bank/IAM
customer subject. The bank identity integration establishes this mapping after
the global Wurzburg user exists. WSO2 must not derive it from PAN, forward a
national ID, or let the application choose it. A cardholder token without a
verified mapping cannot call cardholder-scoped Wurzburg APIs.

Changing either binding is a privileged, audited IAM operation outside normal
business requests. Wurzburg still compares the trusted claim to current Oracle
ownership on every resource operation.

## 4. Canonical Request Context

WSO2 creates and forwards:

```text
X-Correlation-Id  stable across one end-to-end business request/retry chain
X-Request-Id      unique for this individual HTTP attempt
X-WSO2-Client-IP  canonical original IPv4/IPv6 client address
X-WSO2-Gateway-Id configured gateway instance/cluster identity
```

Rules:

- Accept an inbound correlation ID only when it is 1-128 characters and matches
  `[A-Za-z0-9._:-]+`; otherwise generate a UUID. WSO2 always overwrites the
  backend header with the accepted/generated value.
- Always generate a new UUID `X-Request-Id`, even for an idempotent retry.
- Resolve client IP only through the configured trusted-proxy chain. Strip
  caller-supplied `X-Forwarded-For`, `Forwarded`, and `X-Real-IP` before applying
  the trusted proxy policy.
- Serialize IPv4/IPv6 in canonical text form without a port.
- Set original method/path from the gateway route, not from public headers.
- Wurzburg persists subject, client ID, optional provider/user ID, issuer,
  canonical client IP, correlation ID, request ID, reason, and before/after
  snapshots for every audited mutation.

## 5. Roles And Scopes

Canonical roles:

```text
wurzburg_provider_operator
wurzburg_provider_admin
wurzburg_cardholder
wurzburg_platform_admin
wurzburg_support
wurzburg_reporting
```

Canonical scopes:

```text
provider.users:read
provider.users:write
provider.cards:read
provider.cards:write
provider.funding:read
provider.funding:write
provider.transactions:read
provider.kafka_credentials:read
provider.kafka_credentials:rotate
provider.events:read

card.funding-order:read
card.funding-order:write
card.credit:read
card.credit:return
card.transactions:read

platform.providers:read
platform.providers:write
platform.card_ranges:read
platform.card_ranges:write
platform.policies:read
platform.policies:write
platform.fee_profiles:read
platform.fee_profiles:write
platform.provider_events:read
platform.provider_events:write
platform.provider_events:replay
platform.config:read
platform.config:write
platform.recovery:read
platform.recovery:write

support.users:read
support.cards:read
support.transactions:read
support.recovery:read

reports.transactions:read
reports.transactions:export
```

Roles are coarse personas; every route requires explicit scopes. Holding a role
without the route scope is insufficient.

## 6. Route Authorization Matrix

```text
Route group                                  Required scope
-----------------------------------------------------------------------------
Provider list/detail/lifecycle/admin         platform.providers:read/write
Provider contacts/operational profile        platform.providers:read/write
Provider range attachment                    platform.card_ranges:write
Card-range and range-control reads           platform.card_ranges:read
Card-range mutations                         platform.card_ranges:write
Card-policy reads/mutations                  platform.policies:read/write
Provider fee-profile reads/mutations         platform.fee_profiles:read/write
Provider Kafka credential read               provider.kafka_credentials:read
Provider Kafka access/job status              provider.kafka_credentials:read
Provider Kafka provision/rotate/suspend      provider.kafka_credentials:rotate
Provider user/card reads                     provider.users:read/provider.cards:read
Provider user onboarding/card replacement    provider.users:write/provider.cards:write
Provider live credit read                     provider.funding:read
Provider credit grant/full return            provider.funding:write
Provider transaction reads                   provider.transactions:read
Provider subscription read                   provider.events:read
Admin event catalog/subscription/delivery    platform.provider_events:read/write
Admin event replay                           platform.provider_events:replay
Cardholder funding order read/write          card.funding-order:read/write
Cardholder provider-credit read/full return  card.credit:read/card.credit:return
Cardholder transaction reads                 card.transactions:read
Support reads                                matching support.*:read
Report query/export                          reports.transactions:read/export
Recovery inspection/mutation                 platform.recovery:read/write
Business configuration                       platform.config:read/write
```

For a route showing `read/write`, GET requires read and mutations require write.
Provider role calls are constrained to JWT `provider_id`. Cardholder calls are
constrained to JWT `user_id`. Platform-admin routes are under `/api/v1/admin` or
are explicitly identified as platform operations in OpenAPI.

WSO2 rejects an obvious path/claim provider mismatch. Wurzburg repeats this
check and verifies the provider/user/card relationship from Oracle to prevent
gateway-policy mistakes or stale claims.

## 7. Idempotency And Retry Policy

Every POST, PUT, PATCH, and DELETE business mutation requires:

```text
Idempotency-Key: 1-255 visible ASCII characters
```

WSO2 behavior:

- Reject a missing/invalid key before forwarding.
- Preserve the key byte-for-byte; never generate or rewrite it.
- Include it in the allowed-header/CORS policy.
- Do not cache mutation responses by idempotency key; Wurzburg owns durable
  request hashing, replay, conflict detection, WAL, and recovery.
- Never automatically retry a mutating request, including on connect reset,
  timeout, `502`, `503`, or `504`. The caller retries explicitly with the same
  key and body.
- Do not automatically retry reads at the gateway. Client libraries may retry
  idempotent GETs with bounded backoff and the same correlation ID.

File upload endpoints require idempotency on job creation and upload-finalize
commands. Streaming body transport itself is not replayed by WSO2.

## 8. Synchronous And Asynchronous APIs

WSO2 must preserve Wurzburg status codes and bodies:

- Provider creation is asynchronous and normally returns `202` plus an
  operation/provisioning resource.
- Single provider-user onboarding is synchronous and may include deterministic
  TigerBeetle account provisioning before its final response.
- Credit grant and full-balance credit return are synchronous WAL commands.
  They return `200/201` when mandatory Kafka publication is acknowledged or
  `202` when the financial effect is applied but publication/recovery is still
  pending.
- Bulk onboarding/funding begins an asynchronous MinIO-backed job. Individual
  rows execute the same synchronous single-item command.
- Policy/profile activation that awaits Wolfsburg materialization returns the
  documented `202` operation resource.

Initial upstream timeouts:

```text
Connect to Wurzburg                    2 seconds
Normal reads                          15 seconds
Normal Oracle-only mutations          30 seconds
Onboarding/credit WAL commands         45 seconds
Provider creation/job creation        15 seconds
File upload                            120 seconds
Authorized file download/streaming    300 seconds
```

Timeouts are deployment configuration and must exceed Wurzburg's own dependency
deadlines enough to return a structured response. A gateway timeout does not
prove that a mutation failed; callers resolve it using the same idempotency key
or operation-status API.

## 9. Request And File Controls

Required gateway controls:

- Accept JSON only as `application/json` for JSON commands.
- Require UTF-8 and reject invalid encoding.
- Initial JSON body limit: 1 MiB.
- File endpoints accept only the documented CSV MIME types and configured
  maximum object size; WSO2 streams rather than buffers entire files.
- Reject unsupported methods/content types before forwarding.
- Preserve decimal strings and JSON integers without numeric transformation.
- Do not log request/response bodies for provider, user, card, funding,
  credential, import, or report endpoints.
- Never terminate a file upload as successful until Wurzburg returns its durable
  job/object result.

Initial per-client rate-limit baselines:

```text
Provider/admin reads                 600 requests/minute
Provider funding/onboarding writes  120 requests/minute
Cardholder writes                     60 requests/minute per subject
Platform-admin mutations              60 requests/minute
Report creation                       20 requests/minute
File uploads                          10 jobs/hour, max 5 concurrent
```

Limits are independently configurable by route and client application. Rate
limiting must key provider routes by `client_id + provider_id`, cardholder routes
by `client_id + user_id`, and platform routes by `client_id + sub`.

## 10. OTel And Distributed Tracing

WSO2 participates in W3C Trace Context for the internal HTTP hop:

- Validate inbound `traceparent`; generate a new trace when absent/invalid.
- Apply a configured `tracestate` allowlist and strip untrusted `baggage`.
- Create a gateway server span and inject canonical `traceparent`/`tracestate`
  into the WSO2-to-Wurzburg request.
- Record route templates, HTTP status, gateway result code, client application
  ID, and safe role/scope outcomes.
- Never put JWTs, PAN, national ID, contact data, Kafka credentials, request
  bodies, query secrets, or unrestricted metadata in spans/logs.

Wurzburg continues the HTTP trace and propagates it only through internal Kafka
events and asynchronous jobs. Provider-facing Kafka events never include
`traceparent`, `tracestate`, `baggage`, internal correlation IDs, or other OTel
details.

## 11. Response And Error Contract

WSO2 passes Wurzburg status, headers, and JSON body without changing
`rs_code`, symbolic `code`, message, or details.

All gateway-generated errors use the same envelope:

```json
{
  "error": {
    "rs_code": 6001,
    "code": "INVALID_ACCESS_TOKEN",
    "message": "Access token is invalid",
    "details": {}
  }
}
```

Reserved WSO2 result-code range:

```text
6000 AUTHENTICATION_REQUIRED       401
6001 INVALID_ACCESS_TOKEN          401
6002 ACCESS_FORBIDDEN              403
6003 RATE_LIMIT_EXCEEDED           429
6004 REQUEST_TOO_LARGE             413
6005 UNSUPPORTED_MEDIA_TYPE        415
6006 GATEWAY_TIMEOUT               504
6007 UPSTREAM_UNAVAILABLE          503
6008 INVALID_GATEWAY_REQUEST       400
6009 METHOD_NOT_ALLOWED            405
```

Missing/invalid idempotency headers use the shared application catalog so the
result is identical whether detected at WSO2 or Wurzburg:

```text
6100 MISSING_IDEMPOTENCY_KEY        400
6104 INVALID_IDEMPOTENCY_KEY        400
```

These catalogs must also exist in the shared API result-code documentation and
must not overlap other application codes. WSO2 must not expose its policy names,
stack traces, backend hostnames, certificate details, or raw token-validation
errors.

WSO2 forwards `Retry-After` where supplied by Wurzburg and sets it for `429`.
It must not transform a Wurzburg `202` into `200`.

## 12. API Publication And Versioning

- WSO2 imports the generated Wurzburg OpenAPI document for route/method/body
  definitions; handwritten gateway resources must not drift from OpenAPI.
- Public base path remains `/api/v1` for this release.
- Admin, provider, and cardholder products are separate WSO2 API products or
  subscription plans even when they route to the same Wurzburg deployment.
- Swagger/OpenAPI access is internal/admin only and is not part of provider or
  cardholder products.
- `/health` may be exposed only to internal monitoring. Database health details
  and configuration are not public.
- Breaking request/response or scope changes require a new API version and a
  coordinated WSO2/Wurzburg deployment.

## 13. Audit And Security Logging

WSO2 security logs record:

- timestamp, gateway ID, correlation/request IDs
- token issuer, subject, client ID, role/scope decision
- optional provider/user UUID claims
- canonical client IP, route template, method, HTTP/result code, latency
- rate-limit/quota policy outcome

Never log:

- bearer/assertion token or JWT payload
- idempotency key in plaintext; log a one-way hash if required
- PAN, national ID, names, contacts, metadata, request/response bodies
- Kafka password/certificate, database/MinIO credentials, or signed URLs

Gateway logs support access/security investigation. Oracle's immutable common
audit log remains the proof of business-state changes.

## 14. WSO2 Acceptance Scenarios

The ESB and Wurzburg teams must jointly verify:

1. Valid platform-admin JWT reaches an allowed admin route with canonical
   correlation/request/client-IP context.
2. Public spoofing of every reserved header is stripped and cannot alter audit.
3. Missing, expired, wrong-issuer, wrong-audience, wrong-algorithm, and unknown-
   key JWTs fail with the reserved structured gateway result.
4. Provider token for provider A cannot call provider B path/body resources;
   both WSO2 and Wurzburg reject it.
5. Cardholder token cannot read/return another user's provider credit or change
   another card's funding order.
6. Platform-admin provider-event APIs are unavailable to provider credentials;
   provider subscription GET remains read-only.
7. Missing/invalid `Idempotency-Key` is rejected and valid keys are preserved
   exactly through WSO2.
8. WSO2 never retries a timed-out mutation; repeating through the client with
   the same key reaches Wurzburg idempotent replay/WAL recovery.
9. `202`, `409`, `429`, `503`, and `504` responses preserve the documented
   status and structured result body.
10. JSON/file size and media-type policies reject invalid input without body
    logging or partial forwarding.
11. W3C trace context continues WSO2 to Wurzburg and internal Kafka, while a
    provider Kafka fixture proves no trace/baggage/internal headers are present.
12. Credential, PAN, national ID, token, and body scanning finds no prohibited
    values in gateway logs, metrics, or traces.
13. JWKS key rotation succeeds without accepting an unknown/retired key beyond
    the configured cache and token lifetime.
14. mTLS failure or direct backend access is rejected before trusted headers are
    consumed.

## 15. Delivery Checklist For The ESB Team

- Configure private mTLS route and certificate rotation monitoring.
- Configure JWT issuer, audience, JWKS, algorithm, claim, and clock-skew rules.
- Implement reserved-header stripping and canonical request context.
- Publish role/scope mappings and separate API products/subscription plans.
- Enforce idempotency header, request limits, rate limits, timeouts, and no
  mutation retries.
- Implement reserved structured gateway result codes `6000-6009` and shared
  idempotency codes `6100`/`6104`.
- Configure body/secret redaction and safe OTel attributes.
- Import and pin the generated OpenAPI version.
- Run and archive the joint acceptance scenarios before environment promotion.
