# From this simulator to real design recommendations

This project is useful when you treat it as a **compressed model** of a production system, not as a miniature that magically converts `300 RPS on my laptop` into `30,000 RPS in prod`.

What transfers is not the raw throughput number. What transfers are:

- where the bottleneck is
- how close the system is to saturation
- whether failure shows up as timeouts, backlog growth, or outright rejects
- how much headroom the design needs for bursts

## The core symbols

| Symbol | Meaning | In this project |
| --- | --- | --- |
| `lambda` | arrival rate, in requests or writes per second | approximated by `/status.arrival_rate_rps` |
| `mu` | service rate per DB connection, in writes per second | `/status.service_rate_per_conn` |
| `c` | parallel database capacity | `MAX_OPEN_CONNS` |
| `rho` | utilization, `lambda / (c * mu)` | `/status.utilization_rho` |

If `rho` stays well below `1`, the database tier has headroom. If `rho` approaches `1`, latency and queueing rise sharply. If `rho` stays above `1`, backlog grows without bound unless the system rejects work.

## What the simulator can teach you

It can teach you:

- whether the direct path fails as **timeouts under contention**
- whether the queue-backed path converts those same bursts into **backlog plus delayed drain**
- how sensitive the design is to smaller pools, shorter timeouts, slower writes, or faster batching
- roughly where your architecture transitions from stable to unstable

It cannot tell you:

- the exact production RPS your real hardware will sustain
- network, disk, replication, or multi-node effects you did not model
- tail behavior caused by real query plans, locks, indexes, or cross-service fan-out

The simulator is best for **choosing architecture and sizing formulas**. Production benchmarking is still required for the final constants.

## How to interpret `/status`

During a run, poll:

```sh
curl -s http://localhost:8080/status
```

Important fields:

- `db_rows`: how much work has actually reached PostgreSQL
- `queue_depth`: approximate backlog still waiting to drain
- `avg_write_latency_ms`: average per-row DB write time seen by the simulator
- `service_rate_per_conn`: measured `mu`
- `max_connections`: current `c`
- `utilization_rho`: current estimate of `lambda / (c * mu)`

Read the endpoints differently:

- **Direct path:** rising latency and `500`s mean callers are paying for DB contention immediately
- **Queue path:** `202`s plus rising `queue_depth` mean callers are protected, but the system is spending that pressure as eventual-consistency lag instead

## The workflow: simulation to production

### 1. Use the simulator to find the failure mode

Run the same load profile against both endpoints and note:

- the `rho` range where direct writes begin to fail
- whether failures show up before `rho` gets near `1` because of tight timeouts
- how quickly `queue_depth` grows during the queue-backed run
- how long drain takes after the load stops

This gives you the **shape** of the system:

- timeout-sensitive
- capacity-sensitive
- burst-sensitive
- backlog-tolerant or not

### 2. Extract the transferable rule

You are not learning “this design fails at 300 RPS.”

You are learning rules like:

- “Direct writes become unreliable once caller timeouts overlap with DB contention.”
- “The queue-backed design tolerates bursty ingress as long as average drain capacity exceeds average arrival rate.”
- “A shorter flush interval improves drain latency, but a larger batch improves throughput.”

Those rules survive scaling.

### 3. Measure the real production constant: `mu_prod`

For your real system, benchmark the actual write unit on real infrastructure:

- pick a narrow operation, such as one order insert or one worker write batch
- measure the time the database connection is actually busy
- compute `mu_prod = 1 / avg_service_time`

Example:

- one production write takes `4 ms` on average
- `mu_prod = 1 / 0.004 = 250 writes/sec per connection`

That is the number the simulator cannot invent for you. You must measure it.

### 4. Convert business requirements into `lambda`

Decide what load you are sizing for:

- `lambda_avg`: average write rate
- `lambda_peak`: sustained peak write rate
- `lambda_burst`: temporary burst rate
- `burst_duration`: how long the burst lasts

Those are product and traffic assumptions, not simulator outputs.

### 5. Turn the simulator insight into recommended parameters

#### For a direct-write design

Choose a target safe utilization, `rho_target`, based on what the simulation showed.

If the direct path became unstable once `rho` drifted into the high range, size production so it stays below that point:

```text
c_recommended = ceil(lambda_peak / (rho_target * mu_prod))
```

Example:

- `lambda_peak = 20,000 writes/sec`
- `mu_prod = 250 writes/sec per connection`
- choose `rho_target = 0.70`

Then:

```text
c_recommended = ceil(20000 / (0.70 * 250)) = ceil(114.29) = 115
```

That means a direct-write design should budget roughly `115` concurrently usable DB connections to stay near 70% utilization at peak.

#### For a queue-backed design

You usually size the worker/database tier for **average or sustained** load, then size the queue for bursts.

Worker-side connection budget:

```text
c_worker = ceil(lambda_avg / (rho_target * mu_prod))
```

Burst backlog to absorb:

```text
backlog_growth_per_sec = max(0, lambda_burst - (c_worker * mu_prod))
required_queue_capacity = backlog_growth_per_sec * burst_duration
```

Example:

- `lambda_avg = 8,000 writes/sec`
- `lambda_burst = 20,000 writes/sec`
- `mu_prod = 250 writes/sec per connection`
- `rho_target = 0.80`
- `burst_duration = 120 seconds`

First size the worker tier:

```text
c_worker = ceil(8000 / (0.80 * 250)) = 40
```

Drain capacity at that size:

```text
c_worker * mu_prod = 40 * 250 = 10,000 writes/sec
```

Backlog growth during the burst:

```text
20,000 - 10,000 = 10,000 writes/sec
```

Required queue capacity for a two-minute burst:

```text
10,000 * 120 = 1,200,000 queued writes
```

This is the real value of the queue path: it lets you trade immediate consistency for a finite backlog budget instead of forcing the DB tier to handle burst peak inline.

## How to form a recommendation

A useful recommendation from this project should look like:

1. **State the observed architecture behavior.**
   Example: “Direct writes timed out once contention rose, while the queue-backed path stayed available and converted overload into backlog.”

2. **State the governing formula.**
   Example: “The DB tier must satisfy `c * mu > lambda` with headroom; the queue must hold `(lambda_burst - c * mu) * burst_duration` when burst exceeds drain.”

3. **Plug in real measurements.**
   Example: “Our production benchmark shows `mu_prod = 250 writes/sec/connection`.”

4. **Recommend parameters.**
   Example: “Use a queue-backed design, provision about `40` worker-side DB connections for sustained load, and reserve queue capacity for at least `1.2M` writes.”

That is the right level of inference. It is specific enough to size a system, but honest about where the numbers came from.

## Final rule of thumb

This simulator helps an agent or engineer answer:

- where should work wait
- what should fail first
- how much headroom do we need
- how big must the backlog buffer be

It should not be used to claim:

- “300 local RPS means 30,000 production RPS”

Use the simulator to learn the **constraints and formulas**. Use production measurements to supply the **real constants**.
