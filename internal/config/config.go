// Package config loads runtime configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds everything the binary needs to connect to Postgres and run workers.
type Config struct {
	DatabaseURL     string
	GRPCAddr        string
	WriteFileDir    string
	PollInterval    time.Duration
	MaxPollInterval time.Duration
	LeaseDuration   time.Duration
	RetryBaseDelay  time.Duration
	MaxRetryDelay   time.Duration
	ReapInterval    time.Duration
	WorkerCount     int

	ClickHouseAddr     string
	ClickHouseDatabase string
	ClickHouseUser     string
	ClickHousePassword string
	ShipInterval       time.Duration
	ShipBatchSize      int
}

// Load reads configuration from environment variables, applying defaults
// for anything not set. It fails fast on malformed (not missing) values.
func Load() (Config, error) {
	cfg := Config{
		DatabaseURL:     getEnv("DATABASE_URL", "postgres://jobqueue:jobqueue@localhost:5433/jobqueue"),
		GRPCAddr:        getEnv("GRPC_ADDR", "localhost:50051"),
		WriteFileDir:    getEnv("WRITE_FILE_DIR", "./data/writes"),
		PollInterval:    500 * time.Millisecond,
		MaxPollInterval: 5 * time.Second,
		LeaseDuration:   30 * time.Second,
		RetryBaseDelay:  2 * time.Second,
		MaxRetryDelay:   5 * time.Minute,
		ReapInterval:    10 * time.Second,
		WorkerCount:     4,

		ClickHouseAddr:     getEnv("CLICKHOUSE_ADDR", "localhost:9001"),
		ClickHouseDatabase: getEnv("CLICKHOUSE_DATABASE", "jobqueue"),
		ClickHouseUser:     getEnv("CLICKHOUSE_USER", "jobqueue"),
		ClickHousePassword: getEnv("CLICKHOUSE_PASSWORD", "jobqueue"),
		ShipInterval:       5 * time.Second,
		ShipBatchSize:      1000,
	}

	durations := []struct {
		env string
		dst *time.Duration
	}{
		{"POLL_INTERVAL", &cfg.PollInterval},
		{"MAX_POLL_INTERVAL", &cfg.MaxPollInterval},
		{"LEASE_DURATION", &cfg.LeaseDuration},
		{"RETRY_BASE_DELAY", &cfg.RetryBaseDelay},
		{"MAX_RETRY_DELAY", &cfg.MaxRetryDelay},
		{"REAP_INTERVAL", &cfg.ReapInterval},
		{"SHIP_INTERVAL", &cfg.ShipInterval},
	}
	for _, d := range durations {
		if v, ok := os.LookupEnv(d.env); ok {
			parsed, err := time.ParseDuration(v)
			if err != nil {
				return Config{}, fmt.Errorf("config: invalid %s %q: %w", d.env, v, err)
			}
			*d.dst = parsed
		}
	}

	if v, ok := os.LookupEnv("WORKER_COUNT"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, fmt.Errorf("config: invalid WORKER_COUNT %q: %w", v, err)
		}
		cfg.WorkerCount = n
	}

	if v, ok := os.LookupEnv("SHIP_BATCH_SIZE"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, fmt.Errorf("config: invalid SHIP_BATCH_SIZE %q: %w", v, err)
		}
		cfg.ShipBatchSize = n
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}
