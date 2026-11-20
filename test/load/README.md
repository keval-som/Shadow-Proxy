# Load testing Shadow Proxy

This runner uses PostgreSQL's `pgbench` from a disposable `postgres:16-alpine`
container. No new Java dependencies or local PostgreSQL installation are needed.
Use Windows PowerShell 5.1 or PowerShell 7 and a running Docker Desktop with the
Shadow Proxy stack started.

Each simulated user holds one database connection and sends one SELECT at a time
as quickly as it can. These are concurrent database clients, not HTTP users or a
fixed requests-per-second schedule. Each script iteration has exactly one query,
so the reported transactions per second are queries per second for these workloads.
There are no explicit BEGIN/COMMIT blocks or prepared-protocol messages.

## Start small

From the repository root:

```powershell
# Start the already-built proxy and monitoring stack.
docker compose --profile proxy --profile monitoring up -d

# A short connectivity/load smoke test.
.\test\load\run-load-test.ps1 -Users 1 -Durations '5s' -WarmupSeconds 0

# One comparison: 10, 50, then 100 users, each for 30 seconds.
.\test\load\run-load-test.ps1 -Users 10,50,100 -Durations '30s'

# Full matrix: each user count for 30 seconds, then each for 1 minute.
.\test\load\run-load-test.ps1 -Users 1,10,50,100 -Durations '30s','1m'

# Preview stages without Docker or sending any traffic.
.\test\load\run-load-test.ps1 -Users 1,10,50,100 -Durations '30s','1m' -PlanOnly
```

The defaults add a separate five-second warmup before each stage and a five-second
cooldown between stages. Warmup uses separate pgbench connections; it warms caches
and services but does not measure connection reuse from the warmup phase.
Image pulls and setup happen before benchmarking. Each measured stage establishes
its own client connections, with their initial setup time reported separately.
Use `-WarmupSeconds 0` or `-CooldownSeconds 0` to change these settings.

## Workloads

| Workload | Command | What it measures |
|---|---|---|
| `hot` (default) | `-Workload hot` | Repeated reads of the seeded user with ID 1; normally mostly L1 hits after warmup. |
| `miss` | `-Workload miss` | The same row with a random harmless predicate, producing mostly distinct cache keys; exercises the replica/pool and cache population. |

```powershell
.\test\load\run-load-test.ps1 -Users 10,50,100 -Durations '30s' -Workload miss
```

Both built-in workloads issue SELECTs against `public.users`; they do not modify
tables or flush Redis. The existing database seed supplies the row. If that row was
removed, the runner's read-only preflight check will refuse to start the matrix.
This tiny dataset is an infrastructure smoke benchmark, not a
representative production dataset.

`miss` is **not a cache-disabled test**: the proxy still stores the results in both
cache levels, random values can repeat, and cache writes/evictions contribute to
the cost. It does not establish a consistency guarantee. Check actual cache hit
ratios in Grafana. Cache contents persist between stages and invocations; a zero
warmup does not guarantee a cold cache.

## Reports and errors

Every invocation creates an ignored directory under `test/load/results/` containing:

- `summary.csv` and `summary.json`, with one row per attempted measured stage.
- Stage configuration, UTC start/end timestamps, overall wall time, status, and
  pgbench exit code. UTC timestamps bracket Docker execution, including connection
  setup; use them to find the matching period in Grafana.
- Reported QPS, successful-query count, mean/stddev latency, and initial connection time.
- p50/p95/p99 **sampled successful-query latency**, sample count, and invalid-log count.
- Raw stdout, stderr (including five-second progress reports), and sampled per-query logs.

By default, 10% of transaction latencies are logged to reduce generator disk I/O.
Percentiles are nearest-rank estimates from this sample; QPS and mean latency come
from pgbench's full report, not extrapolated samples. Use `-SampleRate 1` to log every
transaction, at the cost of larger files and more logging overhead. Small samples
are unreliable for tail percentiles; a warning appears below 1000 successful samples.
The files have no row data or database password.

`ReportedFailedTransactions` is pgbench's reported transaction failure count. It
does not count every possible connection/protocol/aborted-client error. Inspect
`Status`, `ExitCode`, and stderr; never interpret a missing count or latency as zero.
Failed/aborted query timings are excluded from the successful latency percentiles.
Client-measured latency includes the proxy round trip and response handling; the
Grafana backend timers measure a different, narrower part of the request.

The matrix stops after an unsuccessful warmup, error, or timeout instead of comparing
partial stages as valid results. The runner exits with code 2 for these stage failures.
The watchdog allows `duration + GraceSeconds` (default 30 extra seconds), then removes
only its own uniquely named load-generator container. This bounds stalled tests;
stopping a client does not necessarily cancel work already running on PostgreSQL.
Ctrl+C also cleans up the active load container when PowerShell executes its finally block.

## Compare against PostgreSQL directly

Run the same workload against the replica's published port to isolate proxy overhead:

```powershell
.\test\load\run-load-test.ps1 -Users 10,50,100 -Durations '30s' -Port 5434 -Workload miss
```

Port `5433` targets the primary instead. Compare the same backend, workload,
client count, logging rate, and duration. Repeated cached reads vs direct database
reads answer a different question from cache-miss overhead. Repeat runs to see
variation; do not compare a failed stage with a completed one.

The client container uses `host.docker.internal` to reach published ports on Docker
Desktop. Use `-Server <reachable-host>` for another host. Database credentials default
to the local Compose values; set `$env:PGPASSWORD` for another password, and use
`-Username` / `-Database` as appropriate. Passwords are passed through the environment,
not command-line arguments or reports.

## Watch Grafana during the test

Open http://localhost:3000 and the **Shadow Proxy Overview** dashboard. Watch process
CPU, JVM memory/direct buffers, open frontend connections, pool waiters, backend
latency/errors, and cache hit ratios. Do not expect backend pool connections to equal
user count: many frontend clients share each pool, and cache hits avoid the database.

Increasing users should increase completed work until a bottleneck is reached.
If QPS flattens while p95/p99 and pool waits rise, the system is becoming saturated.
Error spikes and dropped clients also matter, even when successful-query latency is low.
Existing protocol, cache consistency, and circuit-recovery bugs may surface under load;
this harness measures the current implementation without fixing or hiding them.

The load generator shares the local machine/Docker VM with the proxy and databases.
It can become the bottleneck and affect CPU/RAM readings. Use another machine for
credible capacity measurements. Current DEBUG SQL logging also affects throughput;
keep logging settings consistent when comparing runs.

Validate report parsing without Docker:

```powershell
.\test\load\test-load-report.ps1
```

Reference: https://www.postgresql.org/docs/16/pgbench.html
