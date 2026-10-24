# Threat model (STRIDE)

| Threat | Category | Scenario | Mitigation |
|---|---|---|---|
| Key guessing across tenants | Spoofing / Information disclosure | A client guesses another tenant's idempotency key to read its result | Uniqueness and lookups are scoped by `tenant_id`, which comes from the credential only. Cross-tenant lookups return 404. Tested. |
| API key theft | Spoofing | Leaked tenant key | Keys stored only as SHA-256; never logged; rotation by replacing the hash row. |
| Payload tampering on retry | Tampering | Same key sent with a changed body to alter the stored result | `payload_hash` compare in constant time; mismatch returns 422 and state is unchanged. |
| Replay abuse | Repudiation / Tampering | Re-sending a captured request to duplicate an order | Idempotency key returns the stored result; one effect. Keys expire; expiry documented. |
| DLQ replay abuse | Elevation of privilege | A user republishes dead letters | Replay needs an admin credential, supports `--dry-run`, writes an audit log line, and reuses the original `event_id` so the inbox blocks repeat effects. |
| Log leakage | Information disclosure | Raw keys, payloads or credentials in logs or metric labels | Log `operation_id` only; no tenant-controlled metric labels; test greps logs for secrets. |
| Resource exhaustion | Denial of service | Huge body, key flood, hot key | Body limit 16 KiB, key length cap, per-tenant token bucket, `lock_timeout` returns 409 instead of hanging. |
| Metrics and admin exposure | Information disclosure | `/metrics` or admin endpoints reachable publicly | Served on an internal management port, admin key required, port not published beyond localhost in Compose. |
| DB privilege abuse | Elevation of privilege | API role drops tables or the consumer reads `orders_db` | Least-privilege roles: API role cannot `DROP`; consumer role has no access to `orders_db`. |
| Plain HTTP | Information disclosure | Traffic sniffed | Compose is local HTTP only; TLS termination is stated as a production requirement. |
