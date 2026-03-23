# sysd-simulator

`sysd-simulator` is a local Docker testbed for comparing two write paths under load:

- `POST /write-direct`: request blocks on a PostgreSQL `INSERT`
- `POST /write-queue`: request publishes to Kafka and returns immediately while a background worker drains into PostgreSQL

The setup is intentionally constrained so the failure point shows up at laptop-safe load levels instead of requiring unrealistic local throughput.

## Prerequisites

- Docker Desktop
- k6

Install k6 on macOS:

```sh
brew install k6
```

## Start the stack

```sh
docker compose up --build -d
```

Services:

- API: `http://localhost:8080`
- PostgreSQL: `localhost:5432`
- Kafka: `localhost:9092`

Quick health check:

```sh
curl http://localhost:8080/status
```

## Run the direct-write test

```sh
k6 run k6/test-direct.js
```

Override the target URL if needed:

```sh
BASE_URL=http://localhost:8080 k6 run k6/test-direct.js
```

What to look for:

- rising `http_req_failed`
- increasing latency (`p95`, `p99`)
- `500` responses once the direct path hits connection pressure

## Run the queue-backed test

```sh
k6 run k6/test-queue.js
```

Override the target URL if needed:

```sh
BASE_URL=http://localhost:8080 k6 run k6/test-queue.js
```

What to look for:

- mostly `202 Accepted` responses
- lower client-side latency than the direct path
- `queue_depth` growing during the run and draining afterward

## Observe the simulation

Poll the live metrics endpoint while a test is running:

```sh
watch -n 1 'curl -s http://localhost:8080/status'
```

Useful fields in `/status`:

- `db_rows`: current row count in `orders`
- `queue_depth`: approximate backlog still waiting to be written
- `arrival_rate_rps`: average request arrival rate seen by the app
- `avg_write_latency_ms`: average per-row database write latency
- `service_rate_per_conn`: measured writes/sec each DB connection sustains
- `utilization_rho`: `lambda / (c * mu)` based on observed app metrics

## Verify row counts

Check how many rows made it into PostgreSQL:

```sh
docker compose exec -T postgres psql -U postgres -d simulator -c "SELECT COUNT(*) FROM orders;"
```

Inspect the newest rows:

```sh
docker compose exec -T postgres psql -U postgres -d simulator -c "SELECT id, user_id, amount, status, created_at FROM orders ORDER BY id DESC LIMIT 10;"
```

## Reset between runs

If the queue has already drained and you only want to clear table contents:

```sh
docker compose exec -T postgres psql -U postgres -d simulator -c "TRUNCATE TABLE orders RESTART IDENTITY;"
```

If you want a full clean slate, including Kafka offsets and persisted Postgres data:

```sh
docker compose down -v
docker compose up --build -d
```

## Tune the simulation

The API container exposes the main knobs as environment variables in `docker-compose.yml`:

- `MAX_OPEN_CONNS=10`: smaller pool makes direct-write saturation happen sooner
- `CONNECT_TIMEOUT=2s`: tighter timeout makes direct failures show up faster
- `BATCH_SIZE=100`: larger batches increase worker throughput
- `FLUSH_INTERVAL=500ms`: shorter interval reduces queue drain latency

Useful experiments:

- give the DB more headroom: set `MAX_OPEN_CONNS=50`
- drain the queue faster: set `BATCH_SIZE=500` and `FLUSH_INTERVAL=100ms`
- make direct writes more fragile: lower `CONNECT_TIMEOUT`

After changing values, rebuild and restart:

```sh
docker compose up --build -d
```

## Expected outcome

At the default settings:

- the direct path should show visible latency growth and eventually failures as load ramps
- the queue path should keep accepting requests while PostgreSQL drains at its own steady rate
- the queue path trades immediate consistency for stability and eventual catch-up

For the theory behind converting this miniature setup into real-world sizing guidance, see `THEORY.md`.
