# Environment

Recorded: 2026-10-24

```
kernel: Linux 7.2.8-arch1-2
cpu: Intel(R) Core(TM) Ultra 5 125H (18 logical cores)
ram: 15Gi total, 6.8Gi available
openjdk version "21.0.12.1" 2026-08-18
Apache Maven 3.9.16 (2bdd9fddda4b155ebf8000e807eb73fd829a51d5)
Docker version 29.8.2, build 7fc2dff9bc
Docker Compose version 5.5.1
k6 v2.3.0 (commit/devel, go1.27.1-X:nodwarf5, linux/amd64)
```

## Keys and passwords

Copy `.env.example` to `.env` and replace every `change-me-...` value before starting Compose. `.env` is ignored by Git and should not be submitted.

| Variable | Used for | Needed for |
|---|---|---|
| `POSTGRES_PASSWORD` | Main PostgreSQL login | Every Compose run |
| `INVENTORY_DB_PASSWORD` | Consumer's separate database login | Every Compose run |
| `LATCHKEY_SEED_KEY_A` | Tenant A bearer API key | Console order/demo requests |
| `LATCHKEY_SEED_KEY_B` | Tenant B bearer API key | Optional second-tenant/isolation demo |
| `LATCHKEY_ADMIN_KEY` | Authorizes dead-letter replay | Only CLI replay |
| `GRAFANA_PASSWORD` | Grafana `admin` password | `obs` profile |

The two tenant API keys are seeded into the database as hashes. Paste either value into the console's **Tenant API key** field. The CLI replay command also sets `LATCHKEY_REPLAY_KEY` to the same value as `LATCHKEY_ADMIN_KEY`; it is a temporary environment variable, not another `.env` secret. For normal order creation, the admin key is not used.

Prometheus and Jaeger do not require API keys in this local-only Compose setup. Ports are bound to `127.0.0.1`.
