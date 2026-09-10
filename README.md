# Observable Job Queue

A durable, Postgres-backed job queue in Go. Multiple worker processes poll
the same `jobs` table concurrently; the storage layer guarantees that no
two workers ever claim the same job, and that a worker crashing mid-job
doesn't strand that job forever. Job submission and status are exposed
over a gRPC API; the CLI is a thin client over that API, not a separate
path into the store. Job lifecycle events are shipped asynchronously to
ClickHouse for analytics, without that shipping ever being able to block
claiming or executing a job.

No Kubernetes, no Prometheus yet — those are later weeks.

## Setup

```
docker compose up -d      # start Postgres and ClickHouse
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

## Development

```
make test     # needs docker compose up -d, make migrate, make ch-migrate first
make lint     # gofmt + go vet
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

## Layout

```
proto/                        gRPC API definition (source of truth)
internal/pb/jobqueuepb/       generated from proto/ — do not hand-edit
internal/grpcserver/          gRPC service implementation (thin: proto <-> store)
internal/store/               all SQL; Claim/Complete/Retry/Fail/ReapExpiredLeases
internal/worker/              claim/execute/complete loop
internal/handlers/            job-type registry + real handler implementations
internal/analytics/           ClickHouse client + the job_events shipper
internal/job/                 domain types shared across the above
clickhouse/                   ClickHouse schema (source of truth for job_events there)
cmd/jobqueue/                 serve / work / enqueue / status / watch / dead-letters
```
