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
	DatabaseURL   string
	PollInterval  time.Duration
	LeaseDuration time.Duration
	WorkerCount   int
}

// Load reads configuration from environment variables, applying defaults
// for anything not set. It fails fast on malformed (not missing) values.
func Load() (Config, error) {
	cfg := Config{
		DatabaseURL:   getEnv("DATABASE_URL", "postgres://jobqueue:jobqueue@localhost:5433/jobqueue"),
		PollInterval:  500 * time.Millisecond,
		LeaseDuration: 30 * time.Second,
		WorkerCount:   4,
	}

	if v, ok := os.LookupEnv("POLL_INTERVAL"); ok {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, fmt.Errorf("config: invalid POLL_INTERVAL %q: %w", v, err)
		}
		cfg.PollInterval = d
	}

	if v, ok := os.LookupEnv("LEASE_DURATION"); ok {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, fmt.Errorf("config: invalid LEASE_DURATION %q: %w", v, err)
		}
		cfg.LeaseDuration = d
	}

	if v, ok := os.LookupEnv("WORKER_COUNT"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, fmt.Errorf("config: invalid WORKER_COUNT %q: %w", v, err)
		}
		cfg.WorkerCount = n
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}
