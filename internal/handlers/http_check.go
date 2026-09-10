package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/Harshilagg/Observable_Job_Queue/internal/job"
)

type httpCheckPayload struct {
	URL            string `json:"url"`
	TimeoutSeconds int    `json:"timeout_seconds"`
}

// HTTPCheck fetches payload.url and fails (triggering the normal
// retry/backoff path) unless the response status is 2xx. This is a
// real outbound HTTP request — a slow or down URL is a genuine
// failure, not a simulated one.
func HTTPCheck(ctx context.Context, j job.Job) error {
	var p httpCheckPayload
	if err := json.Unmarshal(j.Payload, &p); err != nil {
		return fmt.Errorf("http_check: invalid payload: %w", err)
	}
	if p.URL == "" {
		return fmt.Errorf("http_check: payload.url is required")
	}

	timeout := 5 * time.Second
	if p.TimeoutSeconds > 0 {
		timeout = time.Duration(p.TimeoutSeconds) * time.Second
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, p.URL, nil)
	if err != nil {
		return fmt.Errorf("http_check: building request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("http_check: request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("http_check: %s returned status %d", p.URL, resp.StatusCode)
	}
	return nil
}
