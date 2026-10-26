# Latchkey

Idempotent commands, outbox events, and replay-safe consumers on PostgreSQL + Redpanda.

Stack: Java 21, Spring Boot 3, PostgreSQL, Redpanda (Kafka API), Docker Compose.

## Status

Work in progress. Milestones M-1 to M2 done (environment, design freeze, idempotent API, outbox relay).

| Milestone | State |
|---|---|
| M-1 Environment | done |
| M0 Design docs | done |
| M1 Core API | done |
| M2 Outbox + relay | done |
| M3 Consumer safety | in progress |
| M3f Frontend console | pending |
| M4 Telemetry | pending |
| M5 Load + chaos | pending |
| M6 Packaging | pending |
| M7 Evidence | pending |

## Run locally with Docker Compose

```sh
cp .env.example .env  # only needed on the first run; then replace every placeholder
# Edit .env and set strong local secrets.
docker compose config
docker compose up --build -d
docker compose ps
docker compose logs -f order-api relay consumer
```

Stop the stack with `docker compose down`; `docker compose down -v` also deletes the local database and broker data.

To preview a dead letter replay, load the local admin key into the shell and pass it through the one-off CLI container without putting the value in the command line:

```sh
set -a
. ./.env
set +a
export LATCHKEY_REPLAY_KEY="$LATCHKEY_ADMIN_KEY"
docker compose run --rm --no-deps \
  -e SPRING_PROFILES_ACTIVE=cli -e LATCHKEY_ADMIN_KEY -e LATCHKEY_REPLAY_KEY \
  consumer replay --dlq-id=1 --dry-run
```

Remove `--dry-run` to republish the event.

## Docs

- [Environment](docs/environment.md)
- [Review of the source guide (G1-G29)](docs/pdf-review.md)
- [Guarantees and non-guarantees](docs/guarantees.md)
- [API contract](docs/api.md)
- [Architecture and sequence diagrams](docs/architecture.md)
- [Threat model](docs/threat-model.md)
- [Architecture decision records](adr/)

Full README (what it is, what it is not, quick start, measured results) is written in M7.
