BINARY := jobqueue
CMD    := ./cmd/jobqueue
MODULE := github.com/Harshilagg/Observable_Job_Queue
GOBIN  := $(shell go env GOPATH)/bin

.PHONY: build test run lint fmt migrate ch-migrate proto

# Applies every migration in order, for a fresh database. This does
# NOT track which migrations already ran, so re-running it against a
# database that already has some of them applied will fail on the
# first one that already exists (e.g. "relation already exists") — a
# real migration tool (golang-migrate, etc.) would be the right call
# the moment this needs to run against a partially-migrated database
# rather than only a fresh one.
migrate:
	for f in migrations/*.sql; do \
		echo "applying $$f"; \
		docker compose exec -T postgres psql -U jobqueue -d jobqueue < $$f || exit 1; \
	done

# Applies the ClickHouse schema. The database must be named explicitly
# in the query string — CLICKHOUSE_DB only sets the *default* user's
# default database at container init, it is not implied by every later
# HTTP request, and an unqualified request otherwise silently lands in
# "default" instead of erroring.
ch-migrate:
	curl -s --user jobqueue:jobqueue "http://localhost:8124/?database=jobqueue" --data-binary @clickhouse/schema.sql

# Needs protoc-gen-go and protoc-gen-go-grpc on PATH:
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
proto:
	PATH="$(GOBIN):$$PATH" protoc \
		--proto_path=proto \
		--go_out=. --go_opt=module=$(MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
		proto/jobqueue/v1/jobqueue.proto

build:
	go build -o bin/$(BINARY) $(CMD)

# Needs Postgres running with the schema applied (docker compose up -d
# && make migrate) — these hit the real database, not a mock.
test:
	go test ./...

run: build
	./bin/$(BINARY) $(ARGS)

lint:
	@if [ -n "$$(gofmt -l .)" ]; then echo "gofmt needs to be run on:"; gofmt -l .; exit 1; fi
	go vet ./...

fmt:
	gofmt -w .
