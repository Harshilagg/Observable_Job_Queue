BINARY := jobqueue
CMD    := ./cmd/jobqueue

.PHONY: build test run lint fmt

build:
	go build -o bin/$(BINARY) $(CMD)

test:
	go test ./...

run: build
	./bin/$(BINARY) $(ARGS)

lint:
	@if [ -n "$$(gofmt -l .)" ]; then echo "gofmt needs to be run on:"; gofmt -l .; exit 1; fi
	go vet ./...

fmt:
	gofmt -w .
