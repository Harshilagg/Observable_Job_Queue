package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/Harshilagg/Observable_Job_Queue/internal/job"
	"github.com/Harshilagg/Observable_Job_Queue/internal/worker"
)

type sumNumbersPayload struct {
	Numbers []float64 `json:"numbers"`
}

// NewSumNumbersHandler returns a handler that sums payload.numbers and
// logs the result. Pure computation, no I/O and no side effects at all
// — the simplest possible "real work" example, and trivially safe
// under at-least-once execution since running it twice writes nothing
// anywhere and just logs the same number twice.
func NewSumNumbersHandler(logger *slog.Logger) worker.Handler {
	return func(ctx context.Context, j job.Job) error {
		var p sumNumbersPayload
		if err := json.Unmarshal(j.Payload, &p); err != nil {
			return fmt.Errorf("sum_numbers: invalid payload: %w", err)
		}
		if len(p.Numbers) == 0 {
			return fmt.Errorf("sum_numbers: payload.numbers must be non-empty")
		}

		var sum float64
		for _, n := range p.Numbers {
			sum += n
		}
		logger.Info("sum_numbers result", "job_id", j.ID, "sum", sum, "count", len(p.Numbers))
		return nil
	}
}
