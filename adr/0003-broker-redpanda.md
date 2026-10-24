# ADR 0003: Redpanda as the broker

Status: accepted (2026-10-24)

## Context
A Kafka-compatible broker is needed, and RAM is limited (about 6 GiB free).

## Decision
Use Redpanda in single-broker dev mode (`--smp 1 --memory 768M --overprovisioned --reserve-memory 0M`). Topic `order.created` has 6 partitions and replication factor 1.

## Consequences
Lighter than Apache Kafka (single binary, no JVM, no ZooKeeper). Speaks the Kafka API, so spring-kafka works unchanged. Single broker means no high availability, which is stated as a limitation.
