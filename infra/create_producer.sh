#!/bin/bash

KAFKA_BIN_DIR="/home/mohsen/Codes/war/wurzburg/infra/kafka_2.12-3.9.2/bin"
BOOTSTRAP_SERVER="localhost:9092"

PRODUCER_USER="producer_user"
PRODUCER_PASS="producer_password"

echo "Creating SCRAM-SHA-512 credentials for global producer ($PRODUCER_USER)..."
$KAFKA_BIN_DIR/kafka-configs.sh \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --alter \
  --add-config "SCRAM-SHA-512=[password=$PRODUCER_PASS]" \
  --entity-type users \
  --entity-name $PRODUCER_USER

echo "Granting Write and Describe ACLs on ALL topics to $PRODUCER_USER..."
$KAFKA_BIN_DIR/kafka-acls.sh \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --add \
  --allow-principal User:$PRODUCER_USER \
  --operation Write \
  --operation Describe \
  --topic '*'

echo "Setup completed successfully."
