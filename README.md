# Latchkey

Idempotent commands, outbox events, and replay-safe consumers on PostgreSQL + Redpanda.

Stack: Java 21, Spring Boot 3, PostgreSQL, Redpanda (Kafka API), Docker Compose.

## Status

Work in progress. Milestones M-1 (environment) and M0 (design freeze) done.

| Milestone | State |
|---|---|
| M-1 Environment | done |
| M0 Design docs | done |
| M1 Core API | done |
| M2 Outbox + relay | pending |
| M3 Consumer safety | pending |
| M3f Frontend console | pending |
| M4 Telemetry | pending |
| M5 Load + chaos | pending |
| M6 Packaging | pending |
| M7 Evidence | pending |

## Docs

- [Environment](docs/environment.md)
- [Review of the source guide (G1-G29)](docs/pdf-review.md)
- [Guarantees and non-guarantees](docs/guarantees.md)
- [API contract](docs/api.md)
- [Architecture and sequence diagrams](docs/architecture.md)
- [Threat model](docs/threat-model.md)
- [Architecture decision records](adr/)

Full README (what it is, what it is not, quick start, measured results) is written in M7.
