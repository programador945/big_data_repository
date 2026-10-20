#!/usr/bin/env bash

set -e

echo "Creando el topic 'ventas' en Kafka..."

docker exec kafka-lab \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic ventas \
  --partitions 3 \
  --replication-factor 1

echo "Topic creado correctamente."
