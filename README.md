# Notification Platform

Event-driven notification system built with Go and Apache Kafka.
Delivers notifications across iOS (APNs), Android (FCM), SMS (Twilio), and Email (SendGrid).

## Quick start

```bash
# 1. Start all infrastructure and application services
docker compose up -d

# 2. Send a test notification
curl -X POST http://localhost:8080/api/v1/notify \
  -H "Content-Type: application/json" \
  -d '{
    "user_id":     "user-123",
    "channel":     "email",
    "template_id": "order-confirmed",
    "payload":     { "order_id": "ORD-456", "amount": "₹999" }
  }'

# 3. Check delivery status
curl http://localhost:8080/api/v1/notifications?id=<notification_id>

# 4. View metrics
open http://localhost:9090   # Prometheus
open http://localhost:3000   # Grafana (admin/admin)
```

## Running locally without Docker

```bash
# Terminal 1 — infrastructure only
docker compose up kafka postgres redis -d

# Terminal 2 — API server
POSTGRES_DSN="postgres://notif:notif@localhost:5432/notifications" \
KAFKA_BROKER="localhost:9092" \
REDIS_ADDR="localhost:6379" \
go run ./cmd/server

# Terminal 3 — workers
POSTGRES_DSN="postgres://notif:notif@localhost:5432/notifications" \
KAFKA_BROKER="localhost:9092" \
go run ./cmd/worker
```

## Project structure

```
cmd/
  server/main.go      — HTTP API entrypoint (port 8080)
  worker/main.go      — Kafka consumer workers entrypoint

internal/
  config/             — all configuration, including Kafka routing table
  domain/             — core structs (Notification, User, DeliveryLog) and interfaces
  api/                — HTTP handlers, middleware, router
  kafka/              — producer and consumer wrappers
  worker/             — worker loop: idempotency → persist → send → update
  sender/             — one file per channel (APNs, FCM, Twilio, SendGrid)
  store/              — PostgreSQL implementation of all repository interfaces
  ratelimit/          — Redis sliding window rate limiter
  retry/              — exponential backoff used by all workers

pkg/
  logger/             — structured slog wrapper
  metrics/            — Prometheus counters and histograms
```

## Key design decisions

### 1. Kafka topology (Release 1 vs Release 2)

**Current (Release 1):** 1 topic `notifications`, 4 partitions (one per channel).
**Future (Release 2):** 4 topics, 1 partition each.

To migrate: open `internal/config/config.go` and swap the two commented routing blocks.
Zero other files change — the producer and all workers read routing from config.

### 2. Persist before send

Every worker writes a `delivery_log` with `status=pending` BEFORE calling the
third-party API. If the worker crashes between persist and send, the log shows
`pending` — a recovery job can find and retry all pending logs safely.

Reversing the order (send first, persist after) creates "ghost" notifications:
sent to the user but invisible in the DB. Unauditable and impossible to recover.

### 3. Idempotency

Before persisting, each worker checks `delivery_logs` for an existing `sent` record
for that `notification_id`. If found, it skips — this handles Kafka's at-least-once
redelivery without sending duplicates to users.

### 4. Redis cache in front of Postgres

The API handler resolves user data (email, phone, device token) via `UserCache`,
which checks Redis before hitting Postgres. At high throughput, this keeps Postgres
query rate low regardless of notification volume.

## Environment variables

| Variable              | Default               | Description                    |
|-----------------------|-----------------------|--------------------------------|
| SERVER_PORT           | 8080                  | HTTP listen port               |
| POSTGRES_DSN          | (required)            | PostgreSQL connection string   |
| KAFKA_BROKER          | localhost:9092        | Kafka broker address           |
| REDIS_ADDR            | localhost:6379        | Redis address                  |
| REDIS_CACHE_TTL       | 5m                    | User cache TTL                 |

## API

### POST /api/v1/notify
Send a notification. Returns 202 immediately — delivery is async.

```json
Request:  { "user_id", "channel", "template_id", "payload" }
Response: { "notification_id", "status": "queued", "message" }
```

### GET /api/v1/notifications?id={id}
Check the status of a notification.

### GET /health
Health check — returns `{ "status": "ok" }`.

### GET /metrics
Prometheus metrics endpoint.
