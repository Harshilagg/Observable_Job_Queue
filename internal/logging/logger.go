// Package logging provides a single, consistent way to construct the
// application's structured logger.
package logging

import (
	"log/slog"
	"os"
	"strings"
)

// New builds a structured logger writing JSON to stdout. level is one of
// "debug", "info", "warn", "error" (case-insensitive); anything else
// falls back to "info".
func New(level string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: parseLevel(level)}
	handler := slog.NewJSONHandler(os.Stdout, opts)
	return slog.New(handler)
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
