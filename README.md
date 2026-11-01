# bet-settlement-go

[![ci](https://github.com/Big-Lapa/bet-settlement-go/actions/workflows/ci.yml/badge.svg)](https://github.com/Big-Lapa/bet-settlement-go/actions/workflows/ci.yml)

A sportsbook-style **bet settlement service** in Go. It accepts bets over HTTP,
consumes final match results from Kafka, settles bets **exactly once** in
PostgreSQL, and publishes `bet-settled` events through a **transactional outbox**.

The project is small on purpose. Its goal is to show idiomatic Go for a
high-load, money-critical backend: clear boundaries, explicit error handling,
safe concurrency and correctness under at-least-once delivery.

```
            POST /bets                         match-results (Kafka)
                │                                      │
                ▼                                      ▼
        ┌──────────────┐                  ┌────────────────────────┐
        │  httpapi     │                  │ kafka.ResultConsumer   │
        │  (net/http)  │                  │ batch → worker pool    │
        └──────┬───────┘                  └───────────┬────────────┘
               │          app.Service (use cases)     │
               └──────────────►  ◄────────────────────┘
                                │
                     one DB transaction:
          processed_results + bets (FOR UPDATE) + outbox
                                │
                                ▼
                        ┌──────────────┐     poll (SKIP LOCKED)    ┌──────────────┐
                        │  PostgreSQL  │ ───────────────────────►  │ outbox.Relay │ ──► bet-settled (Kafka)
                        └──────────────┘                           └──────────────┘
```

## Quick start

```bash
# No infrastructure at all (in-memory storage, results via HTTP):
make run-memory

curl -X POST localhost:8080/bets \
  -d '{"player_id":"p1","match_id":"m1","selection":"HOME","stake":1000,"odds":1850}'
curl -X POST localhost:8080/results -d '{"match_id":"m1","outcome":"HOME"}'
curl localhost:8080/players/p1/stats
```

Full stack with Postgres and Kafka:

```bash
make up                              # postgres + kafka + service
make result MATCH=m1 OUTCOME=HOME    # publish a result to Kafka
make test-integration                # unit + Postgres integration tests
```

Money is stored in cents (`stake: 1000` = 10.00). Odds are decimal odds × 1000
(`1850` = 1.85). No floating point touches money.

## API

| Method | Path | Description |
|---|---|---|
| `POST` | `/bets` | Place a bet |
| `GET` | `/bets/{id}` | Get a bet |
| `GET` | `/players/{id}/stats` | Aggregated player stats (one SQL query with `FILTER`) |
| `POST` | `/results` | Submit a match result manually (normally comes from Kafka) |
| `GET` | `/healthz`, `/readyz` | Liveness / readiness probes |

## Correctness guarantees

- **Idempotent settlement.** Kafka delivers at least once. `processed_results`
  has the match ID as primary key; `INSERT … ON CONFLICT DO NOTHING` in the same
  transaction as the settlement makes a repeated result a no-op, also under
  concurrent delivery (covered by a test with 10 parallel deliveries on real Postgres).
- **No lost or phantom events.** Bet updates and outbox rows commit atomically.
  The relay publishes after commit; delivery to consumers is at-least-once and
  every event carries `bet_id` for deduplication.
- **No lost results.** Offsets are committed only after the whole batch is
  processed. Transient DB errors are retried with exponential backoff; if
  retries run out the process exits and Kubernetes restarts it — the batch is
  re-delivered.
- **Row locking.** `SELECT … FOR UPDATE` on pending bets; `UPDATE … WHERE
  status = 'PENDING'` as a second guard; the relay uses `FOR UPDATE SKIP LOCKED`
  so several instances can run side by side.

## Project layout

```
cmd/settlement/          composition root: wiring, errgroup, graceful shutdown
internal/domain/         entities and rules, no infrastructure imports
internal/app/            use cases + ports (interfaces) they need
internal/adapter/
    postgres/            pgx repositories, tx-in-context, embedded migrations
    kafka/               consumer (batch + worker pool + retry), producer
    httpapi/             net/http handlers, middleware, error mapping
    memory/              in-memory implementation of all ports
internal/outbox/         transactional outbox relay
internal/worker/         generic worker pool and retry with backoff
internal/config/         env-based config, errors.Join validation
```

Dependencies point inwards: `adapter → app → domain`. Interfaces are declared
by the consumer (`app/ports.go`, `httpapi.BetService`) and satisfied implicitly.

## Go techniques shown

| Technique | Where |
|---|---|
| Hexagonal architecture, consumer-side interfaces | `internal/app/ports.go`, `internal/adapter/httpapi/handlers.go` |
| Constructor DI, functional options | `internal/app/service.go` (`NewService`, `WithClock`) |
| Sentinel errors, `%w` wrapping, `errors.Is/As`, custom error type with `Unwrap` | `internal/domain/errors.go` |
| Integer money / fixed-point odds | `internal/domain/money.go` |
| Transaction passed through `context.Context` | `internal/adapter/postgres/db.go` |
| Goroutines, channels, `sync.WaitGroup`, `sync.Once`, cancellation | `internal/worker/pool.go` |
| Generics | `worker.Process[T]`, `httpapi.decode[T]` |
| Retry with exponential backoff, timer without leaks | `internal/worker/retry.go` |
| `errgroup`, `signal.NotifyContext`, graceful HTTP shutdown | `cmd/settlement/main.go` |
| Middleware chain, `slog` structured logs, request ID, recover | `internal/adapter/httpapi/middleware.go` |
| Go 1.22 pattern routing (`POST /bets`, `{id}`) | `internal/adapter/httpapi/handlers.go` |
| `embed` for migrations | `internal/adapter/postgres/db.go` |
| Compile-time interface checks `var _ I = (*T)(nil)` | `memory/store.go`, `postgres/repo.go` |
| Table-driven tests, subtests, `t.Parallel`, fakes, `httptest`, benchmarks | `*_test.go` |
| Race detector in CI, golangci-lint, multi-stage distroless Docker image | `.github/workflows/ci.yml`, `Dockerfile` |

## What a production version would add

Dead-letter topic for poison messages, Prometheus metrics and OpenTelemetry
traces, schema registry for event contracts, proper migration tool (goose),
partition-aware ordering per match, Testcontainers for Kafka tests.
