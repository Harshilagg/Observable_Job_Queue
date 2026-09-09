# Observable Job Queue

A durable, Postgres-backed job queue in Go. Multiple worker processes poll
the same `jobs` table concurrently; the storage layer guarantees that no
two workers ever claim the same job, and that a worker crashing mid-job
doesn't strand that job forever.

Scope for this stage: the storage layer and a working claim loop only —
enqueue jobs from the CLI, run N concurrent workers, and verify no job is
ever double-claimed under real concurrency. No gRPC, no ClickHouse, no
Kubernetes.

## Setup

```
docker compose up -d      # start Postgres
make migrate              # create the jobs table and indexes
make build                # build ./bin/jobqueue
```

## Usage

```
./bin/jobqueue enqueue -type=demo_job -payload='{"n":1}'
./bin/jobqueue work -workers=4
```

`work` runs until it receives SIGINT/SIGTERM, at which point it stops
claiming new jobs but lets any job already in progress finish before
exiting. A background reaper reclaims jobs left `running` past their
lease — the recovery path when a worker is killed mid-job.

## Configuration

All via environment variables; every one has a default suitable for the
`docker-compose.yml` in this repo.

| Variable            | Default                                                    | Meaning                                          |
|---------------------|-------------------------------------------------------------|---------------------------------------------------|
| `DATABASE_URL`       | `postgres://jobqueue:jobqueue@localhost:5433/jobqueue`       | Postgres connection string                        |
| `WORKER_COUNT`       | `4`                                                           | Default `-workers` for `work` if not overridden   |
| `POLL_INTERVAL`      | `500ms`                                                       | How often an idle worker checks for new jobs      |
| `MAX_POLL_INTERVAL`  | `5s`                                                          | Cap on the poll backoff when the queue stays empty|
| `LEASE_DURATION`     | `30s`                                                         | How long a claim is valid before the reaper can reclaim it |
| `RETRY_BASE_DELAY`   | `2s`                                                           | Multiplied by attempt count for retry backoff     |
| `REAP_INTERVAL`      | `10s`                                                         | How often the reaper checks for expired leases    |
| `LOG_LEVEL`          | `info`                                                        | `debug`, `info`, `warn`, or `error`               |

## Development

```
make test     # needs docker compose up -d && make migrate first
make lint     # gofmt + go vet
```

`internal/store/jobs_test.go` runs against the real Postgres container
(not a mock) — including a concurrency test that races 20 goroutines
against 2,000 seeded jobs and asserts none is ever claimed twice.
