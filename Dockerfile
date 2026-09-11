# Builder: CGO_ENABLED=0 gives a statically linked binary so the
# runtime stage needs nothing from the Go toolchain's libc assumptions
# — pgx, clickhouse-go, and grpc-go are all pure Go here, so this
# imposes no real cost.
FROM golang:1.26.8-alpine3.24 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /jobqueue ./cmd/jobqueue

# Runtime: plain alpine, not scratch/distroless, because the
# http_check handler makes real outbound HTTPS calls and needs a CA
# bundle to verify certificates — ca-certificates is the one thing
# worth the extra few MB over a fully empty base.
FROM alpine:3.24.1
RUN apk add --no-cache ca-certificates
COPY --from=builder /jobqueue /usr/local/bin/jobqueue
ENTRYPOINT ["/usr/local/bin/jobqueue"]
