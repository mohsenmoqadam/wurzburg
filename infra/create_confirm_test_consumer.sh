#!/bin/bash
set -euo pipefail

KAFKA_BIN_DIR="/home/mohsen/Codes/war/wurzburg/infra/kafka_2.12-3.9.2/bin"
BOOTSTRAP_SERVER="localhost:9092"

CONSUMER_USER="nuremberg_confirm_test_consumer"
CONSUMER_PASS="nuremberg_confirm_test_consumer_password"
TOPIC_NAME="nuremberg.events"

# Important:
# This is a prefix, not a wildcard pattern.
# Test code should generate group IDs like:
#   nuremberg-confirm-it-1720612345678
GROUP_PREFIX="nuremberg-confirm-it-"

echo "Creating SCRAM-SHA-512 credentials for test consumer ($CONSUMER_USER)..."
"$KAFKA_BIN_DIR/kafka-configs.sh" \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --alter \
  --add-config "SCRAM-SHA-512=[password=$CONSUMER_PASS]" \
  --entity-type users \
  --entity-name "$CONSUMER_USER"

echo "Ensuring topic exists: $TOPIC_NAME"
"$KAFKA_BIN_DIR/kafka-topics.sh" \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --create \
  --if-not-exists \
  --topic "$TOPIC_NAME" \
  --partitions 3 \
  --replication-factor 1

echo "Granting Read and Describe ACLs on topic $TOPIC_NAME to $CONSUMER_USER..."
"$KAFKA_BIN_DIR/kafka-acls.sh" \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --add \
  --allow-principal "User:$CONSUMER_USER" \
  --operation Read \
  --operation Describe \
  --topic "$TOPIC_NAME"

echo "Granting Read ACL on consumer group prefix $GROUP_PREFIX to $CONSUMER_USER..."
"$KAFKA_BIN_DIR/kafka-acls.sh" \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --add \
  --allow-principal "User:$CONSUMER_USER" \
  --operation Read \
  --group "$GROUP_PREFIX" \
  --resource-pattern-type prefixed

echo "Current ACLs for topic $TOPIC_NAME:"
"$KAFKA_BIN_DIR/kafka-acls.sh" \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --list \
  --topic "$TOPIC_NAME"

echo "Current ACLs for group prefix $GROUP_PREFIX:"
"$KAFKA_BIN_DIR/kafka-acls.sh" \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --list \
  --group "$GROUP_PREFIX" \
  --resource-pattern-type prefixed

echo "Setup completed successfully."
echo
echo "Use these Nuremberg test config values:"
echo "sasl_username = \"$CONSUMER_USER\""
echo "sasl_password = \"$CONSUMER_PASS\""
echo "group_id_prefix = \"$GROUP_PREFIX\""
