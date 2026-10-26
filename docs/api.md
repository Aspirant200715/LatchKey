# API contract

Base path `/v1`. JSON only. Authentication: `Authorization: Bearer <tenant api key>`. The tenant is resolved from the key; no tenant header is accepted.

## POST /v1/orders

Headers: `Idempotency-Key` (required), `Content-Type: application/json`.
Body: `{ "sku": "string (1-64)", "quantity": integer > 0 and <= 1000 }`.

| Case | Status | Notes |
|---|---|---|
| Missing, empty, over 255 chars or non-printable-ASCII key | 400 `INVALID_IDEMPOTENCY_KEY` | Nothing stored |
| Invalid JSON, wrong types, unknown fields, quantity out of range | 400 `INVALID_REQUEST` | Nothing stored |
| Body over 16 KiB | 413 `PAYLOAD_TOO_LARGE` | Nothing stored |
| Missing or wrong API key | 401 `UNAUTHORIZED` | |
| Rate limit exceeded (per tenant) | 429 `RATE_LIMITED` + `Retry-After` | |
| New key, valid body | 201 + `Location: /v1/operations/{operation_id}` | Body includes `operation_id`, `order_id` |
| Same key, same hash, COMPLETED | stored status and body + `Idempotent-Replayed: true` | |
| Same key, same hash, in flight, or lock timeout | 409 `REQUEST_IN_FLIGHT` + `Retry-After` | |
| Same key, different hash | 422 `IDEMPOTENCY_KEY_REUSED` | State unchanged |
| Database unavailable, deadlock, timeout | 503 `SERVICE_UNAVAILABLE` + `Retry-After` | Never served from cache; retry allowed |
| Key past `expires_at` | treated as a new request | Documented |

Out-of-stock is not decided here. It is decided asynchronously by the consumer and recorded in the ledger. An unknown SKU (not in the `skus` catalog) is a deterministic rejection at the API: 422 `UNKNOWN_SKU`, stored as `FAILED_TERMINAL` and replayed on retry. After a record expires, `GET /v1/operations/{id}` for it returns 404; the order itself is kept.

Error body: `{ "error": { "code": "...", "message": "..." } }`. Codes are stable.

## GET /v1/operations/{operation_id}

Tenant-scoped. Another tenant's id returns 404 (not 403, to avoid confirming existence).
Returns `operation_id`, `status` (`IN_PROGRESS|COMPLETED|FAILED_TERMINAL`), `response_code`, `response_body`, `created_at`.

## GET /v1/operations/{operation_id}/inventory

Tenant-scoped end-to-end demo status. Returns whether the outbox event is waiting to publish or published, and whether inventory is still processing, reserved, rejected for insufficient stock, or unavailable. It reads the operation and outbox in `orders_db` and asks the private consumer service for its reservation ledger result. A not-yet-seen event is a normal pending state; the console polls briefly and can be refreshed later.

## Health and metrics

- `GET /health/live`: process is up.
- `GET /health/ready`: database reachable (and broker for relay/consumer).
- `GET /metrics`: Prometheus format, served on an internal management port and not exposed publicly. The scrape includes standard JVM/HTTP metrics plus `latchkey_consumer_events_total`, `latchkey_consumer_processing_seconds`, `latchkey_relay_records_total`, `latchkey_outbox_unpublished`, and `latchkey_outbox_oldest_unpublished_seconds`.
- With Compose profile `obs`, Prometheus scrapes all three app roles and Grafana provisions a LatchKey dashboard. Set `OTLP_TRACING_EXPORT_ENABLED=true` and `TRACING_SAMPLING_PROBABILITY=1.0` to export sampled traces to Jaeger over OTLP/HTTP.

## Demo console

The local console is served at `/` by the API profile. It can create an order, retry with the same idempotency key, recover the stored operation, and track event publication and inventory reservation. It asks for a tenant API key and keeps it in page memory only. Administrative overview, verification, and fail-point endpoints remain planned; they are not implemented yet.

## Key rules

- Key: 1-255 visible ASCII characters (0x21-0x7E, so no spaces); a UUID is recommended.
- Never log raw keys, payloads or credentials; log `operation_id`.
- Payload hash: SHA-256 over the RFC 8785 canonical JSON of the body, prefixed by the operation name; compared in constant time.
