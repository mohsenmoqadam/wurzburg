#!/usr/bin/env bash
set -euo pipefail

required=(
  KAFKA_BOOTSTRAP_SERVER
  WURZBURG_KAFKA_USERNAME
  WURZBURG_KAFKA_PASSWORD
  WURZBURG_KAFKA_ADMIN_USERNAME
  WURZBURG_KAFKA_ADMIN_PASSWORD
  WOLFSBURG_KAFKA_USERNAME
  WOLFSBURG_KAFKA_PASSWORD
  WURZBURG_COMMAND_TOPIC
  WOLFSBURG_RECEIPT_TOPIC
  WURZBURG_RECEIPT_GROUP
  WOLFSBURG_COMMAND_GROUP
  KAFKA_TOPIC_PARTITIONS
  KAFKA_TOPIC_REPLICATION_FACTOR
)
for variable in "${required[@]}"; do
  if [[ -z "${!variable:-}" ]]; then
    echo "Required environment variable ${variable} is missing" >&2
    exit 1
  fi
done

marker_file="/var/lib/kafka-data/.wurzburg-contract-bootstrap"
script_checksum="$(sha256sum "$0" | cut -d ' ' -f 1)"
contract_checksum="$(printf '%s\n' \
  "${script_checksum}" \
  "${WURZBURG_KAFKA_USERNAME}" \
  "${WURZBURG_KAFKA_PASSWORD}" \
  "${WURZBURG_KAFKA_ADMIN_USERNAME}" \
  "${WURZBURG_KAFKA_ADMIN_PASSWORD}" \
  "${WOLFSBURG_KAFKA_USERNAME}" \
  "${WOLFSBURG_KAFKA_PASSWORD}" \
  "${WURZBURG_COMMAND_TOPIC}" \
  "${WOLFSBURG_RECEIPT_TOPIC}" \
  "${WURZBURG_RECEIPT_GROUP}" \
  "${WOLFSBURG_COMMAND_GROUP}" \
  "${KAFKA_TOPIC_PARTITIONS}" \
  "${KAFKA_TOPIC_REPLICATION_FACTOR}" | sha256sum | cut -d ' ' -f 1)"

if [[ -f "${marker_file}" ]] && [[ "$(cat "${marker_file}")" == "${contract_checksum}" ]]; then
  echo "Local Kafka contract resources are unchanged; skipping bootstrap"
  exit 0
fi

create_scram_identity() {
  local username="$1"
  local password="$2"
  kafka-configs \
    --bootstrap-server "${KAFKA_BOOTSTRAP_SERVER}" \
    --alter \
    --add-config "SCRAM-SHA-512=[password=${password}]" \
    --entity-type users \
    --entity-name "${username}"
}

create_topic() {
  local topic="$1"
  kafka-topics \
    --bootstrap-server "${KAFKA_BOOTSTRAP_SERVER}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${KAFKA_TOPIC_PARTITIONS}" \
    --replication-factor "${KAFKA_TOPIC_REPLICATION_FACTOR}"
}

grant_topic_acl() {
  local principal="$1"
  local topic="$2"
  shift 2
  local arguments=()
  for operation in "$@"; do
    arguments+=(--operation "${operation}")
  done
  kafka-acls \
    --bootstrap-server "${KAFKA_BOOTSTRAP_SERVER}" \
    --add \
    --allow-principal "User:${principal}" \
    "${arguments[@]}" \
    --topic "${topic}"
}

grant_group_acl() {
  local principal="$1"
  local group="$2"
  kafka-acls \
    --bootstrap-server "${KAFKA_BOOTSTRAP_SERVER}" \
    --add \
    --allow-principal "User:${principal}" \
    --operation Read \
    --group "${group}"
}

grant_prefixed_group_acl() {
  local principal="$1"
  local prefix="$2"
  kafka-acls \
    --bootstrap-server "${KAFKA_BOOTSTRAP_SERVER}" \
    --add \
    --allow-principal "User:${principal}" \
    --operation Read \
    --resource-pattern-type prefixed \
    --group "${prefix}"
}

grant_idempotent_write() {
  local principal="$1"
  kafka-acls \
    --bootstrap-server "${KAFKA_BOOTSTRAP_SERVER}" \
    --add \
    --allow-principal "User:${principal}" \
    --operation IdempotentWrite \
    --cluster
}

grant_cluster_acl() {
  local principal="$1"
  shift
  local arguments=()
  for operation in "$@"; do
    arguments+=(--operation "${operation}")
  done
  kafka-acls \
    --bootstrap-server "${KAFKA_BOOTSTRAP_SERVER}" \
    --add \
    --allow-principal "User:${principal}" \
    "${arguments[@]}" \
    --cluster
}

grant_prefixed_topic_acl() {
  local principal="$1"
  local prefix="$2"
  shift 2
  local arguments=()
  for operation in "$@"; do
    arguments+=(--operation "${operation}")
  done
  kafka-acls \
    --bootstrap-server "${KAFKA_BOOTSTRAP_SERVER}" \
    --add \
    --allow-principal "User:${principal}" \
    "${arguments[@]}" \
    --resource-pattern-type prefixed \
    --topic "${prefix}"
}

create_scram_identity "${WURZBURG_KAFKA_USERNAME}" "${WURZBURG_KAFKA_PASSWORD}"
create_scram_identity "${WURZBURG_KAFKA_ADMIN_USERNAME}" "${WURZBURG_KAFKA_ADMIN_PASSWORD}"
create_scram_identity "${WOLFSBURG_KAFKA_USERNAME}" "${WOLFSBURG_KAFKA_PASSWORD}"
create_topic "${WURZBURG_COMMAND_TOPIC}"
create_topic "${WOLFSBURG_RECEIPT_TOPIC}"

grant_topic_acl "${WURZBURG_KAFKA_USERNAME}" "${WURZBURG_COMMAND_TOPIC}" Write Describe
grant_topic_acl "${WURZBURG_KAFKA_USERNAME}" "${WOLFSBURG_RECEIPT_TOPIC}" Read Describe
grant_group_acl "${WURZBURG_KAFKA_USERNAME}" "${WURZBURG_RECEIPT_GROUP}"
grant_idempotent_write "${WURZBURG_KAFKA_USERNAME}"
grant_prefixed_topic_acl "${WURZBURG_KAFKA_USERNAME}" "provider.events." Write Describe
grant_prefixed_topic_acl "${WURZBURG_KAFKA_USERNAME}" "wurzburg.contract-test." Read Write Describe
grant_prefixed_group_acl "${WURZBURG_KAFKA_USERNAME}" "wurzburg-contract-test-"

grant_cluster_acl "${WURZBURG_KAFKA_ADMIN_USERNAME}" Alter Describe Create
grant_prefixed_topic_acl "${WURZBURG_KAFKA_ADMIN_USERNAME}" "provider.events." Create Delete Describe
grant_prefixed_topic_acl "${WURZBURG_KAFKA_ADMIN_USERNAME}" "wurzburg.contract-test." Create Delete Describe
grant_topic_acl "${WURZBURG_KAFKA_ADMIN_USERNAME}" "${WURZBURG_COMMAND_TOPIC}" Describe
grant_topic_acl "${WURZBURG_KAFKA_ADMIN_USERNAME}" "${WOLFSBURG_RECEIPT_TOPIC}" Describe

grant_topic_acl "${WOLFSBURG_KAFKA_USERNAME}" "${WURZBURG_COMMAND_TOPIC}" Read Describe
grant_group_acl "${WOLFSBURG_KAFKA_USERNAME}" "${WOLFSBURG_COMMAND_GROUP}"
grant_topic_acl "${WOLFSBURG_KAFKA_USERNAME}" "${WOLFSBURG_RECEIPT_TOPIC}" Write Describe
grant_idempotent_write "${WOLFSBURG_KAFKA_USERNAME}"
grant_prefixed_topic_acl "${WOLFSBURG_KAFKA_USERNAME}" "provider.events." Write Describe

printf '%s' "${contract_checksum}" >"${marker_file}"
echo "Local Kafka contract resources are ready"
