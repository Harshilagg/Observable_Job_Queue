BINARY := jobqueue
CMD    := ./cmd/jobqueue

.PHONY: build test run lint fmt migrate

migrate:
	docker compose exec -T postgres psql -U jobqueue -d jobqueue < migrations/0001_create_jobs.sql

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
