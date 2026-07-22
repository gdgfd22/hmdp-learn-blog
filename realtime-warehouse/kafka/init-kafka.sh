#!/usr/bin/env bash
set -euo pipefail

KAFKA_BIN=/opt/kafka/bin
until "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server kafka:29092 --list >/dev/null 2>&1; do
  sleep 2
done
for topic in ods_behavior_event dwd_behavior_event dwd_order_change dirty_behavior_event; do
  "$KAFKA_BIN/kafka-topics.sh" \
    --bootstrap-server kafka:29092 \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions 3 \
    --replication-factor 1
done
