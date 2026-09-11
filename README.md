# Observable Job Queue

A durable, Postgres-backed job queue in Go. Multiple worker processes poll
the same `jobs` table concurrently; the storage layer guarantees that no
two workers ever claim the same job, and that a worker crashing mid-job
doesn't strand that job forever. Job submission and status are exposed
over a gRPC API; the CLI is a thin client over that API, not a separate
path into the store. Job lifecycle events are shipped asynchronously to
ClickHouse for analytics, without that shipping ever being able to block
claiming or executing a job.

Metrics, traces, and a provisioned Grafana dashboard are included; a
Kubernetes deployment is not yet.

## Setup

```
docker compose up -d      # start Postgres, ClickHouse, Prometheus, Tempo, Grafana
make migrate               # create the jobs/dead_letters/job_events tables
make ch-migrate             # create ClickHouse's job_events table
make build                 # build ./bin/jobqueue
```

Regenerating the gRPC code (only needed if you change `proto/`) requires
protoc plus two plugins:

```
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
make proto
```

Generated code under `internal/pb/` is checked in, so this step is not
needed just to build and run the project.

## Usage

Two long-running processes, plus a CLI that talks to them:

```
./bin/jobqueue serve             # gRPC API on :50051 — submission/status
./bin/jobqueue work -workers=4   # claims and executes jobs directly against Postgres
```

```
./bin/jobqueue enqueue -type=sum_numbers -payload='{"numbers":[1,2,3]}'   # via gRPC Submit
./bin/jobqueue status -id=123                                             # via gRPC GetStatus
./bin/jobqueue watch -id=123                                              # via gRPC WatchStatus (streams until terminal)
./bin/jobqueue dead-letters -limit=20                                     # inspect terminally-failed jobs (direct DB read)
./bin/jobqueue analytics -report=drain-time -window=5m                    # ClickHouse-backed reports — see Analytical queries below
```

`serve` and `work` are independent processes with direct database
access; `enqueue`/`status`/`watch` are pure gRPC clients and never touch
Postgres — any other client of the API would submit/query jobs the same
way. Both `serve` and `work` stop accepting new work on SIGINT/SIGTERM
but let anything already in progress finish before exiting. A
background reaper (inside `work`) reclaims jobs left `running` past
their lease — the recovery path when a worker is killed mid-job.

### Job types

`work` dispatches a claimed job to a handler by its `type`, via the
registry in `internal/handlers`:

| Type          | Payload                                    | Notes |
|---------------|---------------------------------------------|-------|
| `http_check`  | `{"url": "...", "timeout_seconds": 5}`       | Real outbound HTTP GET; non-2xx or a network error fails the job |
| `write_file`  | `{"name": "...", "content": "..."}`          | Writes under `WRITE_FILE_DIR`; naturally idempotent (rewriting the same content is safe under at-least-once retries) |
| `sum_numbers` | `{"numbers": [1, 2, 3]}`                     | Pure computation, no side effects |

A job whose `type` isn't registered is **not** failed immediately — it
goes through the same retry-with-backoff path as any other failure,
and only reaches the terminal `failed` state after exhausting
`max_attempts`. This is deliberate: in a rolling deploy, an older
worker might momentarily not have a type registered that a newer one
does, and failing outright would permanently lose a job a worker
seconds away from existing could have handled. See the comment on
`Registry.Dispatch` in `internal/handlers/registry.go` for the full
reasoning.

### Retries: exponential backoff with jitter, and dead letters

A failed job's next `run_after` is computed as exponential backoff
(doubling per attempt, capped at `MAX_RETRY_DELAY`) with **full
jitter**: the actual delay used is a random value between 0 and that
computed cap, not the cap itself. This isn't about spacing out any one
job's own retries (that's what the backoff alone does) — it's about
preventing many jobs that fail at the same moment (e.g. a downstream
dependency has a brief outage) from all computing the *identical*
deterministic delay and retrying in a synchronized burst that can knock
a just-recovering dependency back over. See the doc comment on
`calculateRetryDelay` in `internal/worker/worker.go` for the full
reasoning, and `internal/worker/worker_test.go` for tests that verify
the jitter is actually happening (not just bounded).

Once a job exhausts `max_attempts` — whether via `Worker.Run` giving up
or the reaper reclaiming an exhausted lease — it's moved to terminal
`failed` and, in the same atomic statement, a copy of it (type,
payload, attempts, error) is written to the `dead_letters` table. This
is what makes a terminal failure inspectable via `jobqueue dead-letters`
rather than just a status value buried in a growing history table.
There's no redrive/requeue-from-dead-letter capability yet — inspecting
is all this stage does.

### Analytics: job events shipped to ClickHouse

Every state-changing `Store` method (`Enqueue`, `Claim`, `Complete`,
`Retry`, `Fail`, `ReapExpiredLeases`) writes a row to a `job_events`
outbox table in Postgres, atomically, in the same statement as its own
update — the same CTE pattern `dead_letters` uses. A background
shipper goroutine inside `work` (`internal/analytics`) polls that
outbox on a ticker, batches unshipped rows into ClickHouse, and marks
them shipped in Postgres.

Postgres stays authoritative regardless of ClickHouse's availability:
if ClickHouse is unreachable at `work` startup, that's logged as a
warning, not a fatal error — events simply accumulate unshipped until
a connection succeeds. Shipping is at-least-once, not exactly-once (a
crash between inserting into ClickHouse and marking rows shipped
re-sends the batch next tick), so ClickHouse's `job_events` table uses
`ReplacingMergeTree` keyed on the Postgres outbox row's own id —
duplicate inserts merge away in the background. A query that needs a
guaranteed-deduped read before a merge has happened should add `FINAL`;
aggregate queries over a real time range usually don't need to bother.

This is what makes questions like "throughput over time" or "failure
rate by job type" answerable without competing with the live claim
query for Postgres's attention — see `clickhouse/schema.sql` and
`internal/analytics/shipper.go` for the full design notes.

### Analytical queries: why ClickHouse, not Postgres

```
jobqueue analytics -report=arrivals   -window=1h  -bucket=1m   # arrivals vs. completions per bucket
jobqueue analytics -report=duration   -window=1h                # p99 job duration by job type
jobqueue analytics -report=backlog    -window=1h  -bucket=1m   # backlog trend vs. active worker count
jobqueue analytics -report=drain-time -window=5m                # ETA to drain the current backlog, by job type
```

All four read from ClickHouse (`internal/analytics/reports.go`); `-report=drain-time` also reads Postgres directly, for a reason spelled out below. The short version of why these are ClickHouse queries at all: **Postgres's `jobs` table only ever holds each job's current row**, overwritten in place on every claim, retry, or completion. It cannot answer "how many jobs arrived per minute over the last hour" or "what was p99 duration during last Tuesday's incident" — that history is gone the moment a job's status moves on. Only the immutable, append-only `job_events` log — shipped to ClickHouse precisely so it never competes with the live claim path — has enough history to answer these:

- **Arrivals vs. completions** buckets `job_events` by time and counts `enqueued` against jobs reaching a terminal state. Postgres has no historical count of what happened in past buckets, only the current queue depth *right now* (which is what the Grafana `queue_depth` panel already shows, pulled straight from Postgres). This report answers the different question of whether backlog is growing because arrivals are spiking or because completions are falling behind — over time, not right now.
- **p99 duration by job type** pairs each attempt's `claimed` and terminal event (matched by `job_id, attempts`, since a retried job's next attempt gets a new pair) and runs `quantile(0.99)` over the difference, grouped by type. This is deliberately a *second*, complementary way to see duration alongside the Grafana `jobqueue_job_duration_seconds` histogram (`internal/metrics`): the Prometheus histogram is a live, in-process instrument that resets on every `work` restart and only reflects recent buckets. This ClickHouse query can recompute p99 for *any* past window, long after the fact, because `job_events` is durable, queryable history rather than a live counter.
- **Backlog vs. worker count** tracks, per bucket, how many distinct `worker_id`s claimed a job alongside the running net change in backlog (arrivals minus departures) — so scaling workers up or down can be visually correlated with backlog actually draining. Postgres's `jobs.claimed_by` is overwritten on every reclaim, so the past is gone the instant a job is retried; only the event log remembers who was working when.
- **Drain-time estimation** is the one report that deliberately reads *both* stores, because neither can answer it alone: Postgres gives the live, exact backlog count (`Store.CountByStatusAndType`, the same query the Grafana `queue_depth` panel uses), and ClickHouse gives the recent completion *rate* — a derived quantity from historical throughput that Postgres has no record of, since a job that already left the queue leaves no trace in `jobs`. `internal/analytics.EstimateDrainTime` is a pure function over the two already-fetched values (backlog ÷ rate), kept separate from the queries themselves so it's unit-testable without either database. One caveat worth knowing: the completion rate is only as fresh as the shipper, so a very short window (a few seconds) right after a burst can under-count until the next shipper tick lands — the same lag the `shipper_lag` panel already tracks. Windows of a minute or more make this negligible.

### Observability: metrics, traces, and Grafana

`docker compose up -d` starts Prometheus, Grafana Tempo, and Grafana
alongside Postgres and ClickHouse — `docker compose up` alone gives a
working dashboard, not an empty one. Open **http://localhost:3000**
(anonymous viewer access, no login needed) and the "Observable Job
Queue" dashboard is already there, provisioned from
`observability/grafana/provisioning/dashboards/jobqueue.json`.

`work` (not `serve` — there's nothing to poll on a process with no
queue-depth state) exposes Prometheus metrics on `METRICS_ADDR`
(`:9464` by default):

| Metric                          | Type      | What it answers |
|----------------------------------|-----------|------------------|
| `jobqueue_queue_depth`           | gauge, by `job_type` | How many jobs are waiting right now |
| `jobqueue_in_flight`             | gauge, by `job_type` | How many jobs are currently claimed/running |
| `jobqueue_claim_duration_seconds`| histogram | How long a `Claim` call takes — the thing that gets slow under contention |
| `jobqueue_job_duration_seconds`  | histogram, by `job_type` | How long a handler actually takes to run |
| `jobqueue_retries_total`         | counter, by `job_type` | Retry rate |
| `jobqueue_dead_letters_total`    | counter, by `job_type` | Dead-letter rate |
| `jobqueue_shipper_lag_seconds`   | gauge     | Age of the oldest unshipped `job_events` row — how far ClickHouse is behind Postgres |

`queue_depth`, `in_flight`, and `shipper_lag` are implemented as a
Prometheus `Collector` (`internal/metrics/collector.go`) that queries
Postgres fresh on every `/metrics` scrape, rather than a `Gauge` kept
updated in memory — this state lives in the database, is shared across
every `work` process, and can change from a claim happening in a
*different* process, so polling the source of truth on scrape is more
correct than any one process trying to track it itself. The other four
are ordinary event-driven `Histogram`/`CounterVec` updates at the point
each event happens, inside `internal/worker/worker.go`.

Every job also carries a trace spanning **submit → claim → execute →
complete**, even though those steps run in different processes
(`serve` for submit, `work` for the rest) and are separated by however
long the job sits queued. A trace normally propagates through request
headers, but there's no live request connecting these steps — the job
sits in Postgres between them, sometimes for a while. So `Enqueue`
stores the submitting span's context as a W3C traceparent string in a
new `trace_context` column, and `Claim` reads it back and starts the
worker's spans as children of it (`internal/tracing/tracing.go`,
`Inject`/`Extract`). A retried job keeps reusing the *original*
submission's trace context, so every attempt shows up as a sibling
span under the same trace rather than starting a disconnected one.
Traces export via OTLP to Tempo at `OTLP_ENDPOINT` (`localhost:4317`
by default); if the collector is unreachable, `tracing.Setup` logs a
warning and the binary runs without tracing — same non-fatal treatment
as ClickHouse being unreachable, for the same reason (observability
being down must never take job processing down with it).

## Configuration

All via environment variables; every one has a default suitable for the
`docker-compose.yml` in this repo.

| Variable            | Default                                                    | Meaning                                          |
|---------------------|-------------------------------------------------------------|---------------------------------------------------|
| `DATABASE_URL`       | `postgres://jobqueue:jobqueue@localhost:5433/jobqueue`       | Postgres connection string (used by `serve`, `work`) |
| `DB_MAX_CONNS`       | `20`                                                           | Pool size for `serve`/`dead-letters`; `work` instead sizes its own pool to `-workers`+4 (see below) |
| `GRPC_ADDR`          | `localhost:50051`                                             | Address `serve` listens on, and clients dial      |
| `WRITE_FILE_DIR`     | `./data/writes`                                               | Sandbox directory the `write_file` handler is allowed to write into |
| `WORKER_COUNT`       | `4`                                                           | Default `-workers` for `work` if not overridden   |
| `POLL_INTERVAL`      | `500ms`                                                       | How often an idle worker checks for new jobs      |
| `MAX_POLL_INTERVAL`  | `5s`                                                          | Cap on the poll backoff when the queue stays empty|
| `LEASE_DURATION`     | `30s`                                                         | How long a claim is valid before the reaper can reclaim it |
| `RETRY_BASE_DELAY`   | `2s`                                                           | Base for exponential retry backoff (doubles per attempt) |
| `MAX_RETRY_DELAY`    | `5m`                                                           | Cap on the exponential term before jitter is applied |
| `REAP_INTERVAL`      | `10s`                                                         | How often the reaper checks for expired leases    |
| `CLICKHOUSE_ADDR`    | `localhost:9001`                                              | ClickHouse native-protocol address                |
| `CLICKHOUSE_DATABASE`| `jobqueue`                                                    | Must be named explicitly — see `make ch-migrate`'s note |
| `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` | `jobqueue` / `jobqueue`                  | ClickHouse credentials                            |
| `SHIP_INTERVAL`      | `5s`                                                          | How often `work`'s shipper polls for unshipped events |
| `SHIP_BATCH_SIZE`    | `1000`                                                        | Max events shipped to ClickHouse per tick          |
| `OTLP_ENDPOINT`      | `localhost:4317`                                              | Where `serve`/`work` export traces (Tempo's OTLP gRPC port) |
| `METRICS_ADDR`       | `:9464`                                                       | Where `work` serves `/metrics` for Prometheus to scrape |
| `LOG_LEVEL`          | `info`                                                        | `debug`, `info`, `warn`, or `error`               |

`work`'s own connection pool is sized to `-workers`+4, not
`DB_MAX_CONNS` — it needs one connection per worker plus headroom for
its always-on reaper and shipper goroutines, and that number depends
on `-workers`, which `DB_MAX_CONNS` alone can't express. This was a
real bug, not a hypothetical: `pgxpool`'s CPU-based default pool size
can be smaller than a `work` process's actual concurrent demand on a
CPU-constrained machine, and this project's own test suite hit exactly
that starvation — see `internal/store/jobs_test.go`'s
`newTestStoreWithPoolSize` for the full writeup.

## Kubernetes

Manifests for Postgres, ClickHouse, a one-shot migration Job, the gRPC
`serve` Deployment, and the `worker` Deployment live under `k8s/`,
composed via a `kustomization.yaml` at the repo root (Kustomize
sandboxes file references to at-or-below its own root, which is why
the kustomization isn't inside `k8s/` itself — it needs to reach
`migrations/*.sql` and `clickhouse/schema.sql` to generate the
migration Job's ConfigMaps from the same files `make migrate`/
`make ch-migrate` use locally, rather than duplicating that SQL into
the manifests). No Prometheus/Tempo/Grafana here — this pass is scoped
to what the prompt asked for (worker, Postgres, ClickHouse), not the
whole observability stack.

```
docker build -t jobqueue:local .     # image both serve and worker Deployments use
kubectl apply -k .                    # namespace, secrets, config, both databases, migrate Job, serve, worker
kubectl get pods -n jobqueue -w       # watch it come up
kubectl port-forward -n jobqueue svc/jobqueue-serve 50051:50051 &
GRPC_ADDR=localhost:50051 ./bin/jobqueue enqueue -type=sum_numbers -payload='{"numbers":[1,2,3]}'
```

Any local Kubernetes works — this was built and tested against both
`kind` and Docker Desktop's built-in Kubernetes. `kind` needed several
attempts on the machine this was built on (a 4-CPU/4GB-RAM Docker
Desktop VM shared with an unrelated project's containers): each
`kindest/node` is itself a nested Docker-in-Docker VM running its own
systemd and kubeadm, and under real host contention `kubeadm init`'s
control-plane bootstrap has its own fixed ~60s timeout that a
sufficiently starved host can blow through even while individual API
requests are still succeeding. Docker Desktop's built-in Kubernetes
came up reliably in comparison, because it shares the host's existing
VM rather than nesting a second one inside it — worth knowing if `kind`
ever seems to be hanging or timing out on a similarly constrained
machine: it may not be `kind` at fault so much as the host.

**A real gotcha this deployment surfaced, worth its own callout:**
Kubernetes' exec/httpGet/tcpSocket probes default `timeoutSeconds` to
**1**. On a contended host, `pg_isready` (or any probe command) can
legitimately take longer than that to schedule and respond even though
the thing being checked is perfectly healthy — and a liveness probe
that times out gets its container killed and restarted, which is
strictly worse than the transient slowness it was reacting to: a
healthy Postgres gets bounced, loses its warm state, and briefly stops
serving anything at all. This was caught live on this exact cluster —
`postgres-0` was crash-looping purely from `pg_isready` timing out at
1s under load, not from any real database problem. Every probe in
`k8s/*.yaml` now sets `timeoutSeconds: 5` and `failureThreshold: 6`
explicitly, with a comment pointing back here. The same class of lesson
as this repo's own concurrency-test patience (see Development, below)
and the `pgxpool` sizing note above: a tight default that assumes a
quiet host doesn't catch problems sooner on a busy one, it just
manufactures new ones.

Secrets (`k8s/secrets.yaml`) hold the same throwaway `jobqueue`/
`jobqueue` credentials already committed in `docker-compose.yml` — fine
for a cluster that never leaves this laptop, not a pattern to copy for
anything real. `write_file`'s output directory is an `emptyDir` per
worker replica rather than a shared PVC, since it's a demo artifact,
not state the system depends on. `jobqueue-serve` has no
`grpc.health.v1` service implemented, so its probes are a plain TCP
check on the gRPC port — an honest, if coarse, signal ("is anything
listening") rather than a full "am I actually healthy" check.

## Development

```
make test     # needs docker compose up -d, make migrate, make ch-migrate first
make lint     # gofmt + go vet
make fmt      # gofmt -w .
```

`make test` runs `go test -p 1 -count=1 ./...` deliberately, not plain
`go test ./...`: `internal/store` and `internal/analytics` both mutate
the same live, shared Postgres tables (not mocks — e.g. `SKIP LOCKED`'s
behavior is a property of Postgres's actual lock manager), and Go runs
different packages' tests concurrently by default, which caused real
cross-package interference before `-p 1` was added.

`internal/store/jobs_test.go` includes a concurrency test that races 20
goroutines against 2,000 seeded jobs and asserts none is ever claimed
twice. Its own connection pool is explicitly sized to that concurrency
(see `newTestStoreWithPoolSize`) — worth reading if this test is ever
slow or flaky again, since undersizing it was a real, previously-hit
bug, not a hypothetical.

This test has since flaked again on the same machine for a second,
distinct reason, worth telling apart from the pool-sizing bug above:
adding Prometheus/Tempo/Grafana plus an unrelated project's containers
pushed this 4-CPU host to ~10 total containers. A run that stalled at
"claimed 20 of 2000" was checked live against `pg_stat_activity` mid-run
— every connection sat `idle`/`ClientRead`, meaning Postgres was
waiting on the Go client, not the other way around. The bottleneck was
host CPU scheduling starving the test's own goroutines, not a database
lock. Re-running the identical test in isolation (or the full suite at
a quieter moment) passed in ~20s with no code change. If this test
stalls, check `docker stats`/`uptime` before suspecting the claim query
— a saturated host can make a correct test look broken.

**If jobs look "stuck" in `running` while manually testing `work`,
check host load before assuming a bug.** On a heavily loaded or
CPU-constrained machine (this project was debugged on one reporting
`load average` past 20 on 4 CPUs, with several unrelated Docker
containers also running), individual claims can take many seconds
under contention — a job that looks permanently stuck at the 15-20
second mark can still reach `completed`/`failed` correctly by 60
seconds. The lease/reaper mechanism and the retry/backoff path are
what actually matter for correctness here, not how long any single
run happens to take; don't mistake a slow host for a hang.

## Layout

```
proto/                        gRPC API definition (source of truth)
internal/pb/jobqueuepb/       generated from proto/ — do not hand-edit
internal/grpcserver/          gRPC service implementation (thin: proto <-> store)
internal/store/               all SQL; Claim/Complete/Retry/Fail/ReapExpiredLeases
internal/worker/              claim/execute/complete loop
internal/handlers/            job-type registry + real handler implementations
internal/analytics/           ClickHouse client, the job_events shipper, and the analytical report queries
internal/metrics/             Prometheus metrics + the DB-backed queue-depth/in-flight/shipper-lag collector
internal/tracing/             OpenTelemetry setup + trace-context inject/extract across the Postgres handoff
internal/job/                 domain types shared across the above
clickhouse/                   ClickHouse schema (source of truth for job_events there)
observability/                Prometheus, Tempo, and Grafana provisioning (datasources + the checked-in dashboard)
cmd/jobqueue/                 serve / work / enqueue / status / watch / dead-letters
k8s/                           Postgres, ClickHouse, migrate Job, serve/worker Deployments
kustomization.yaml             kubectl apply -k . — composes k8s/ with migrations/ and clickhouse/ directly
Dockerfile                     builds jobqueue:local, the image k8s/ deploys
```
