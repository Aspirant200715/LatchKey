# Review of the source guide

Source: `idempotent_event_processing_system_guide.pdf` (prepared 14 Oct 2026) and the execution plan `latchkey-execution-plan.pdf`, which lists 29 gaps (G1–G29) in the guide.
Each gap is marked **adopted** (taken as the plan states), **adapted** (taken with a change for Java/Spring Boot or this machine) or **rejected** (not done, with the reason).

## Correctness and design

| # | Sev | Gap | Decision | Reason |
|---|---|---|---|---|
| G1 | High | Single-transaction claim contradicts "in progress" status | adopted | Two modes. Mode A (local only) has no `IN_PROGRESS` row; Mode B (external side effect) commits `IN_PROGRESS` with a lease. See `guarantees.md`. |
| G2 | High | `/v1/orders` vs `/v1/operations` mismatch | adopted | `POST /v1/orders` is the domain endpoint; `GET /v1/operations/{id}` is the generic recovery endpoint. |
| G3 | High | `operation_id` missing from schema | adopted | `operation_id uuid NOT NULL UNIQUE`, server generated, returned in body and `Location`. |
| G4 | High | Wrong status for key reuse with different payload | adapted | Follows the expired IETF draft: missing key 400, mismatch 422, in-flight 409 + `Retry-After`. It is a design choice, not an RFC rule; the report says so. |
| G5 | High | Outbox ordering ignored | adapted | Kafka key = `aggregate_id`; `aggregate_version` on every event; consumers park out-of-order versions. Guarantee stated as "per aggregate, within a partition". |
| G6 | High | Stable event ID rule | adopted | `event_id` is generated inside the producing transaction and never regenerated. Test republishes one row 5 times and asserts one effect. |
| G7 | High | Polling by auto-increment cursor can skip rows | adopted | Poll by predicate (`published_at IS NULL`) with `FOR UPDATE SKIP LOCKED`, never by cursor. |
| G8 | Med | No lease or retry columns on outbox | adopted | `attempts`, `next_attempt_at`, `locked_by`, `locked_until`, `last_error`. |
| G9 | Med | Payload fingerprint under-specified | adapted | SHA-256 over the RFC 8785 canonical form of the raw body, prefixed with the operation name. Implemented with Jackson sorted keys plus number normalisation, unit-tested. |
| G10 | Med | No rule for what is stored on errors | adopted | 400 is never stored. Deterministic business outcomes are stored as terminal. 5xx, deadlock and timeout roll back and allow retry. |
| G11 | Med | Isolation level not chosen | adopted | `READ COMMITTED` + unique constraint + conditional `UPDATE`. No `SERIALIZABLE`. |
| G12 | Med | No business tables | adopted | `orders`, `inventory`, `inventory_reservations` (the effect ledger). |
| G13 | Med | Retry/backoff/DLQ/replay not designed | adopted | Designed in `architecture.md` section 4 and implemented in M3. |
| G14 | Med | Inbox without retention or payload hash | adopted | `payload_hash` column; mismatch for a known `event_id` goes to the DLQ. Batched retention job. |
| G15 | Med | Expiry stored but no cleanup | adopted | Batched `DELETE` job. No time partitioning, because a partitioned table cannot enforce the `(tenant, operation, key)` unique constraint across partitions. |
| G16 | Low | Inconsistent ID types, no `tenant_id` on outbox | adopted | `uuid` everywhere; `tenant_id` and `partition_key` on outbox. |
| G17 | Low | Redis listed as optional | adopted | Dropped. In-process token bucket for rate limiting. |
| G18 | High | Fault injection has no mechanism | adopted | Named fail points enabled by `FAILPOINTS=name`, in-repo class. Toxiproxy only for network faults. |
| G19 | High | No independent correctness checker | adopted | `verifier` runs after every test; exit code is pass/fail. |
| G20 | Med | k6 sketch covers unique keys only | adopted | Hot-key and mismatch k6 scripts in `loadtest/`. |
| G21 | Med | Trace context not carried through outbox | adopted | `traceparent` stored in `outbox_events.headers`, forwarded as a Kafka header. |
| G22 | Med | Load generator shares the machine | adapted | CPU pinning with `cpuset`; pinning recorded beside every result. Core numbers adjusted to this machine (18 logical cores). |
| G23 | Med | Memory budget unaddressed | adapted | Java-specific limits in `architecture.md` section 5; observability is an opt-in Compose profile. |
| G24 | Med | No timeline, cut line or risks | adopted | Milestones and cut line in the project plan; M6b is first to cut. |
| G25 | Med | No authentication design | adopted | Static per-tenant API keys, SHA-256 hashed in `tenants`. Tenant comes from the credential, never from a header. |
| G26 | Low | No deliverables list | adopted | README, report, demo script, raw results (M7). |
| G27 | Low | Reading list links are labels only | adopted | Each link is opened and checked before it is cited in the report. |
| G28 | Low | External side effect never built | adopted (optional) | M6b mock payment provider, built only if time allows. |
| G29 | Low | No formal evidence | rejected | TLA+/Alloy is optional and out of scope for a course project. |

## Decisions D1–D6

| # | Decision | Chosen | ADR |
|---|---|---|---|
| D1 | Language | Java 21 + Spring Boot 3 (changed from the plan's Go default) | [0001](../adr/0001-language-java-spring-boot.md) |
| D2 | Deadline / team | Single developer; cut line applies | [0002](../adr/0002-scope-and-cut-line.md) |
| D3 | Broker | Redpanda | [0003](../adr/0003-broker-redpanda.md) |
| D4 | Container runtime | Docker + Compose | [0004](../adr/0004-docker-compose.md) |
| D5 | Outbox relay | Polling with `FOR UPDATE SKIP LOCKED` | [0005](../adr/0005-outbox-relay-polling.md) |
| D6 | Kubernetes | Not used | [0006](../adr/0006-no-kubernetes.md) |
