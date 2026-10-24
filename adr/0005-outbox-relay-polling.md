# ADR 0005: Outbox relay by polling

Status: accepted (2026-10-24)

## Context
The relay can poll with FOR UPDATE SKIP LOCKED, wake on LISTEN/NOTIFY, or read the WAL with CDC (Debezium).

## Decision
Poll by predicate (`published_at IS NULL AND next_attempt_at <= now()`) with `FOR UPDATE SKIP LOCKED`, leases and a partial index. Never poll by a sequence cursor, which can skip rows from late-committing transactions.

## Consequences
Simple, no extra service. Adds small publish latency. LISTEN/NOTIFY is compared experimentally in M5 if time allows; Debezium is out of scope.
