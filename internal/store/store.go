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
// by a connection pool. It does not verify connectivity beyond what
// pgxpool.New itself checks; call Ping to confirm the database is
// actually reachable.
func New(ctx context.Context, connString string) (*Store, error) {
	pool, err := pgxpool.New(ctx, connString)
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
