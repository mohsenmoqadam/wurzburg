COMPOSE := docker compose -f infra/docker-compose.yml
APP_ENVIRONMENT ?= development

.PHONY: dev-infra-up dev-infra-down dev-infra-reset dev-kafka-init dev-object-storage-init dev-db-migrate dev-db-reset dev-dependencies-verify dev-init dev-reset dev-verify dev-run

dev-infra-up:
	$(COMPOSE) up -d --wait dragonfly kafka minio tigerbeetle tempo otel-collector grafana kafka-ui
	$(COMPOSE) run --rm kafka-init

dev-infra-down:
	$(COMPOSE) down

dev-infra-reset:
	@test "$(CONFIRM)" = "reset-local-infrastructure" || (echo "Set CONFIRM=reset-local-infrastructure to delete local Compose volumes" && exit 1)
	$(COMPOSE) down --volumes
	$(MAKE) dev-infra-up

dev-kafka-init:
	$(COMPOSE) run --rm kafka-init

dev-object-storage-init:
	APP_ENVIRONMENT=$(APP_ENVIRONMENT) cargo run --bin wurzburg-admin -- object-storage init

dev-db-migrate:
	APP_ENVIRONMENT=$(APP_ENVIRONMENT) cargo run --bin wurzburg-admin -- db migrate

dev-db-reset:
	APP_ENVIRONMENT=$(APP_ENVIRONMENT) cargo run --bin wurzburg-admin -- db reset --confirm-non-production-reset

dev-dependencies-verify:
	APP_ENVIRONMENT=$(APP_ENVIRONMENT) cargo run --bin wurzburg-admin -- dependencies verify

dev-init: dev-infra-up dev-object-storage-init dev-db-migrate dev-dependencies-verify

dev-reset: dev-infra-up dev-object-storage-init dev-db-reset dev-dependencies-verify

dev-verify:
	APP_ENVIRONMENT=$(APP_ENVIRONMENT) cargo run --bin wurzburg-admin -- doctor

dev-run:
	APP_ENVIRONMENT=$(APP_ENVIRONMENT) cargo run --bin wurzburg
