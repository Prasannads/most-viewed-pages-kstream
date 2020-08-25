#!/bin/bash

SCHEMA_REGISTRY=schema-registry
SCHEMA_REGISTRY_PORT=8081
echo "Waiting for SR to launch on ${SCHEMA_REGISTRY}:${SCHEMA_REGISTRY_PORT}..."

while ! nc -z ${SCHEMA_REGISTRY} ${SCHEMA_REGISTRY_PORT}; do
    echo "Waiting for SR to launch on ${SCHEMA_REGISTRY}:${SCHEMA_REGISTRY_PORT}..."
  sleep 0.1
done
echo "Schema Registry up and available"

KAFKA_HOST=broker
KAFKA_PORT=29092
echo "Waiting for kafka to launch on ${KAFKA_HOST}:${KAFKA_PORT}..."

while ! nc -z ${KAFKA_HOST} ${KAFKA_PORT}; do
    echo "Waiting for kafka to launch on ${KAFKA_HOST}:${KAFKA_PORT}..."
  sleep 0.1
done
echo "Kafka Broker up and available"

KAFKA_CONNECT=connect
KAFKA_CONNECT_PORT=8083
echo "Waiting for kafka connect to launch on ${KAFKA_CONNECT}:${KAFKA_CONNECT_PORT}..."

while ! nc -z ${KAFKA_CONNECT} ${KAFKA_CONNECT_PORT}; do
    echo "Waiting for kafka connect to launch on ${KAFKA_CONNECT}:${KAFKA_CONNECT_PORT}..."
  sleep 0.1
done
echo "Kafka connect up and available"

chmod 777 /usr/bin/create-schemas-topics-and-connectors.sh
chmod 777 /usr/bin/connector-configuration/*
chmod 777 /usr/bin/schemas/*
sh /usr/bin/create-schemas-topics-and-connectors.sh
java -jar /usr/lib/most-viewed-pages-kstream.jar
