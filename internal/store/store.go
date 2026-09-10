// Package store is the storage layer: all SQL against the jobs table
// lives here. Nothing outside this package should import pgx directly.
package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound is returned by methods that look up a single job by id
// when no such job exists. Callers outside this package should check
// against this, not pgx.ErrNoRows directly — that keeps pgx an
// implementation detail of the storage layer.
var ErrNotFound = pgx.ErrNoRows

// Store wraps a pgx connection pool. Methods for enqueueing, claiming,
// and completing jobs are added on top of this in later steps.
type Store struct {
	pool *pgxpool.Pool
}

// New connects to Postgres using connString and returns a Store backed
// by a connection pool sized to maxConns (0 keeps pgxpool's own
// default, max(4, runtime.NumCPU())). It does not verify connectivity
// beyond what pgxpool itself checks; call Ping to confirm the database
// is actually reachable.
//
// Explicit sizing matters here for a concrete, previously-hit reason:
// pgxpool's CPU-based default has no idea how many concurrent workers,
// plus a reaper, plus a shipper, will actually be sharing this one
// pool — on a machine with few CPUs, that default can be smaller than
// the real concurrent demand, and callers waiting on a saturated pool
// isn't a hypothetical, it was reproduced and fixed in this project's
// own test suite (see newTestStoreWithPoolSize in internal/store).
func New(ctx context.Context, connString string, maxConns int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("store: parsing connection string: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("store: creating pool: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Ping verifies the database is reachable.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Close releases all pooled connections.
func (s *Store) Close() {
	s.pool.Close()
}
