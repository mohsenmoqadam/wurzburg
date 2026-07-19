# Wurzburg Non-Production Infrastructure

Development, test, and staging use the same Docker Compose topology and differ
only through Wurzburg configuration/environment overrides. Production will use
the same service contracts under Kubernetes.

The same commands can select another non-production configuration without
changing topology:

```text
make dev-init APP_ENVIRONMENT=test
make dev-init APP_ENVIRONMENT=staging
```

## Fast Start

From the repository root:

```text
make dev-init
make dev-run
```

`dev-init` is non-destructive. It starts local dependencies, idempotently
creates Kafka contract resources, idempotently formats/starts TigerBeetle,
applies pending Oracle migrations, and runs dependency verification.

To rebuild the Wurzburg Oracle schema in a non-production environment:

```text
make dev-reset
```

The explicit reset confirmation is enforced by `wurzburg-admin`; production
always rejects reset.

Useful focused commands:

```text
make dev-infra-up
make dev-infra-reset CONFIRM=reset-local-infrastructure
make dev-kafka-init
make dev-db-migrate
make dev-db-reset
make dev-verify
make dev-infra-down
```

`dev-infra-reset` deletes all Wurzburg Compose volumes, including local Kafka,
Dragonfly, MinIO, TigerBeetle, and telemetry data. Its confirmation value is
mandatory. It does not reset the external Oracle schema; use `make dev-reset`
for that separate operation.

## Kafka Contract

The host listener is `SASL_SSL://kafka.arshamnovin.ir:9092`. Development hosts
map that name to `127.0.0.1`, and Compose serves the matching certificate from
`secrets/kafka.arshamnovin.ir.crt`. Wurzburg therefore exercises hostname
verification, TLS, SCRAM authentication, topic authorization, and ACLs through
the same client contract used by test and staging.

The broker's internal `kafka:29092` listener remains plaintext and is available
only to Compose infrastructure services such as the one-shot bootstrap and
Kafka UI. It is not a Wurzburg application endpoint.

The one-shot `kafka-init` service creates:

```text
wurzburg.runtime-projection.commands.v1
wolfsburg.runtime-projection.receipts.v1
```

It also creates Wurzburg and Wolfsburg SCRAM identities and grants only their
required topic, group, and idempotent-write permissions. Re-running the service
is safe. A contract hash in the Kafka volume makes unchanged reruns immediate;
changing the script or any bootstrapped setting forces reconciliation.
Production credentials and ACLs will be supplied by Ansible or the Kafka
platform operator rather than this local bootstrap service.

## Runtime And Migration Separation

The normal `wurzburg` process never migrates or resets Oracle. It verifies that
all required migrations exist with exact checksums before opening listeners.
Local development uses `wurzburg-admin`; container and staging deployment use a
one-shot admin container; Kubernetes will use one migration Job before rolling
out replicas.

Every Wurzburg process appends its pod/host and process identity to Kafka client
IDs and its Oracle outbox lease owner. The receipt consumer group remains stable
so replicas cooperate correctly.

## Kubernetes Deployment Contract

Production application pods never create infrastructure and never mutate the
Oracle schema. The deployment order is:

1. DevOps provisions Oracle credentials, Kafka identities/topics/ACLs,
   Dragonfly, TigerBeetle, MinIO, and OTel endpoints through the platform's
   secret and infrastructure automation.
2. One Kubernetes Job runs `wurzburg-admin db migrate` using the exact Wurzburg
   image being deployed. The Job must succeed before the Deployment rollout.
3. Every Wurzburg pod runs the normal `wurzburg` binary. Startup verifies the
   exact Oracle migration versions and checksums and fails closed if the Job was
   skipped or a migration differs.
4. Readiness and liveness checks control traffic; they never run migrations.

Running three or more Wurzburg pods therefore does not create a schema race.
Kafka topics, SCRAM identities, and ACLs are not created by application pods or
the Oracle migration Job. Production uses Ansible or a Kafka operator for those
resources. The local `kafka-init` container is the executable development
equivalent of that infrastructure contract.

Database reset and Compose volume reset are non-production operator commands,
never pod startup behavior. TigerBeetle format is likewise an infrastructure
bootstrap operation; the local one-shot container models it, while production
storage provisioning remains a DevOps responsibility.

## Configuration Overrides

Do not commit secrets. Put machine-specific values in `config/local.toml` or
environment variables. `config/local.toml` and `infra/.env` are ignored by Git.
Container deployments should override host endpoints with Compose service DNS,
for example `kafka:29092`, `dragonfly:6379`, and `otel-collector:4317`.
