package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Harshilagg/Observable_Job_Queue/internal/job"
	"github.com/Harshilagg/Observable_Job_Queue/internal/worker"
)

type writeFilePayload struct {
	Name    string `json:"name"`
	Content string `json:"content"`
}

// NewWriteFileHandler returns a handler that writes payload.content to
// baseDir/payload.name.
//
// Note on at-least-once execution: os.WriteFile truncates and
// rewrites the whole file, so running this handler twice with the same
// payload — which is exactly what happens if a worker claims it,
// crashes, and the reaper hands it to another worker — leaves the same
// final file on disk either way. That's what "idempotent" means in
// practice for a handler: not that it only ever runs once, but that
// running it more than once has the same observable result as running
// it once. Contrast this with a handler that appends to a file, or
// calls a non-idempotent external API (e.g. "charge this card") —
// those need their own dedup strategy (e.g. an idempotency key derived
// from job.ID) to be safe under at-least-once execution; this one
// doesn't need to think about it at all, by construction.
func NewWriteFileHandler(baseDir string) worker.Handler {
	return func(ctx context.Context, j job.Job) error {
		var p writeFilePayload
		if err := json.Unmarshal(j.Payload, &p); err != nil {
			return fmt.Errorf("write_file: invalid payload: %w", err)
		}
		if p.Name == "" {
			return fmt.Errorf("write_file: payload.name is required")
		}

		// Join against the real baseDir so any ".." resolves against its
		// actual path components, then verify the result still has baseDir
		// as a prefix. Rejecting a mismatch here — rather than trying to
		// neutralize ".." before joining — is what actually catches a
		// traversal attempt instead of silently redirecting it somewhere
		// else still-technically-contained.
		cleaned := filepath.Join(baseDir, p.Name)
		if !strings.HasPrefix(cleaned, filepath.Clean(baseDir)+string(filepath.Separator)) {
			return fmt.Errorf("write_file: %q escapes the allowed directory", p.Name)
		}

		if err := os.MkdirAll(filepath.Dir(cleaned), 0o755); err != nil {
			return fmt.Errorf("write_file: creating directory: %w", err)
		}
		if err := os.WriteFile(cleaned, []byte(p.Content), 0o644); err != nil {
			return fmt.Errorf("write_file: %w", err)
		}
		return nil
	}
}
