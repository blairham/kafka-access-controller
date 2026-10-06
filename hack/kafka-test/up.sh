#!/usr/bin/env bash
# Start the single-node Kafka the integration tests use: KRaft, the standard
# authorizer, a PLAINTEXT listener whose ANONYMOUS user is the super user (the
# controller's admin connection) and a SASL/SCRAM listener for the service
# principals the tests provision. Reused if already running.
set -euo pipefail

name=kafkatest
image='apache/kafka:3.9.1@sha256:4ceccc577f03f51f6af8dbfda55194d0d892f4fa7913ffbded567ce3895622ed'

if ! docker inspect "$name" >/dev/null 2>&1; then
  # The image maps KAFKA_* to properties: _ is ., __ is _, ___ is -.
  docker run --rm -d --name "$name" \
    -p 19092:19092 -p 19093:19093 \
    -e CLUSTER_ID=kafkacontrollertest01 \
    -e KAFKA_NODE_ID=1 \
    -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS=PLAINTEXT://:19092,SASL://:19093,CONTROLLER://:19094 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:19092,SASL://127.0.0.1:19093 \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,SASL:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:19094 \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
    -e KAFKA_SASL_ENABLED_MECHANISMS=SCRAM-SHA-512,SCRAM-SHA-256 \
    -e KAFKA_LISTENER_NAME_SASL_SCRAM___SHA___512_SASL_JAAS_CONFIG='org.apache.kafka.common.security.scram.ScramLoginModule required;' \
    -e KAFKA_LISTENER_NAME_SASL_SCRAM___SHA___256_SASL_JAAS_CONFIG='org.apache.kafka.common.security.scram.ScramLoginModule required;' \
    -e KAFKA_AUTHORIZER_CLASS_NAME=org.apache.kafka.metadata.authorizer.StandardAuthorizer \
    -e KAFKA_SUPER_USERS='User:ANONYMOUS' \
    -e KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND=false \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
    "$image" >/dev/null
fi

for _ in $(seq 1 60); do
  if docker exec "$name" /opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:19092 --list >/dev/null 2>&1; then
    echo "kafka ready on 127.0.0.1:19092 (admin) and 127.0.0.1:19093 (SCRAM)"
    exit 0
  fi
  sleep 1
done
echo "kafka did not become ready; docker logs $name" >&2
exit 1
