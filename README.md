# Observable Job Queue

A durable, Postgres-backed job queue in Go. Multiple worker processes poll
the same `jobs` table concurrently; the storage layer guarantees that no
two workers ever claim the same job, and that a worker crashing mid-job
doesn't strand that job forever.

Scope for this stage: the storage layer and a working claim loop only —
enqueue jobs from the CLI, run N concurrent workers, and verify no job is
ever double-claimed under real concurrency. No gRPC, no ClickHouse, no
Kubernetes.

## Development

```
docker compose up -d      # start Postgres
make build                # build ./bin/jobqueue
make test
make lint
```
