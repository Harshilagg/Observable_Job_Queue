BINARY := jobqueue
CMD    := ./cmd/jobqueue
MODULE := github.com/Harshilagg/Observable_Job_Queue
GOBIN  := $(shell go env GOPATH)/bin

.PHONY: build test run lint fmt migrate proto

migrate:
	docker compose exec -T postgres psql -U jobqueue -d jobqueue < migrations/0001_create_jobs.sql

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
