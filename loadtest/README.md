# Load and failure testing (M5)

These k6 scenarios exercise the API against a running local Compose stack. Install k6 on the host and start the stack with `docker compose up --build -d`. The API is exposed at `http://localhost:8080`.

Run one workload at a time. Keep the output file outside Git; it can be archived with the M7 evidence. `RUN_ID` makes keys unique between runs. For hot-key and mismatch workloads, reuse one key only within that run so the test measures concurrency rather than old records.

```sh
set -a
. ./.env
set +a
export LATCHKEY_API_KEY="$LATCHKEY_SEED_KEY_A"
export RUN_ID="$(date +%Y%m%d-%H%M%S)"
k6 run --summary-export="/tmp/latchkey-unique-$RUN_ID.json" loadtest/unique-keys.js
k6 run --summary-export="/tmp/latchkey-hot-key-$RUN_ID.json" loadtest/hot-key.js
k6 run --summary-export="/tmp/latchkey-mismatch-$RUN_ID.json" loadtest/key-mismatch.js
```

Tune `VUS`, `DURATION`, `BASE_URL`, `SKU`, `QUANTITY`, and `P95_LIMIT_MS` through environment variables. Defaults are 20 VUs for unique keys and 50 VUs for contended keys, each for 30 seconds. The scripts report response outcomes and latency. The built-in p95 threshold is a starting guardrail, not a published performance claim; record machine details and adjust it for the host before comparing runs.

The scenarios create committed demo orders and outbox events in the local database. Use a disposable local volume when you want a clean baseline: `docker compose down -v` deletes all local Postgres and Redpanda data.

## Failure coverage

The Maven integration suite covers the planned four crash windows and broker recovery:

| Failure point | Recovery asserted | Coverage |
|---|---|---|
| Before order transaction commit | Retry creates one order | `OrderApiTest.failureBeforeCommitLeavesNothingBehindAndRetrySucceeds` |
| After commit, before HTTP response | Retry returns stored response | `OrderApiTest.failureAfterCommitBeforeResponseRetryReturnsStoredResultWithOneOrder` |
| After broker publish, before outbox mark | Lease expiry republishes same event ID | `RelayIntegrationTest.crashAfterPublishBeforeMarkRepublishesTheSameEventIdAndEventuallyMarks` |
| Broker unavailable | Outbox retains rows and drains after recovery | `RelayIntegrationTest.brokerDownKeepsEveryRowAndDrainsAfterRecovery` |
| After consumer DB commit, before offset ack | Redelivery is a no-op | `ConsumerIntegrationTest.crashAfterDatabaseCommitBeforeAckRedeliversAsNoOp` |

Run the suite with `mvn test` on a host with Docker available. It uses Testcontainers; k6 workloads are separate and should be run against the Compose stack.
