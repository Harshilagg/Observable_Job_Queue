package handlers

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/Harshilagg/Observable_Job_Queue/internal/job"
)

func TestDispatchRoutesToRegisteredHandler(t *testing.T) {
	r := NewRegistry()
	called := false
	r.Register("demo", func(ctx context.Context, j job.Job) error {
		called = true
		return nil
	})

	err := r.Dispatch(context.Background(), job.Job{Type: "demo"})
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if !called {
		t.Error("registered handler was not called")
	}
}

func TestDispatchUnregisteredTypeReturnsError(t *testing.T) {
	r := NewRegistry()
	err := r.Dispatch(context.Background(), job.Job{Type: "does_not_exist"})
	if err == nil {
		t.Fatal("expected an error for an unregistered type, got nil")
	}
}

func TestHTTPCheckSucceedsOn2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	payload, _ := json.Marshal(map[string]any{"url": srv.URL})
	err := HTTPCheck(context.Background(), job.Job{Payload: payload})
	if err != nil {
		t.Fatalf("HTTPCheck: %v", err)
	}
}

func TestHTTPCheckFailsOnNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	payload, _ := json.Marshal(map[string]any{"url": srv.URL})
	err := HTTPCheck(context.Background(), job.Job{Payload: payload})
	if err == nil {
		t.Fatal("expected an error for a 500 response, got nil")
	}
}

func TestHTTPCheckRejectsMissingURL(t *testing.T) {
	payload, _ := json.Marshal(map[string]any{})
	err := HTTPCheck(context.Background(), job.Job{Payload: payload})
	if err == nil {
		t.Fatal("expected an error for a missing url, got nil")
	}
}

func TestWriteFileWritesContent(t *testing.T) {
	dir := t.TempDir()
	h := NewWriteFileHandler(dir)

	payload, _ := json.Marshal(map[string]string{"name": "out.txt", "content": "hello"})
	if err := h(context.Background(), job.Job{Payload: payload}); err != nil {
		t.Fatalf("handler: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(dir, "out.txt"))
	if err != nil {
		t.Fatalf("reading written file: %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("file content = %q, want %q", got, "hello")
	}
}

func TestWriteFileIsIdempotentOnReplay(t *testing.T) {
	dir := t.TempDir()
	h := NewWriteFileHandler(dir)
	payload, _ := json.Marshal(map[string]string{"name": "out.txt", "content": "hello"})

	// Simulate at-least-once execution: run the same job twice.
	if err := h(context.Background(), job.Job{Payload: payload}); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if err := h(context.Background(), job.Job{Payload: payload}); err != nil {
		t.Fatalf("second run: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(dir, "out.txt"))
	if err != nil {
		t.Fatalf("reading written file: %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("file content after replay = %q, want %q (should be unchanged, not doubled)", got, "hello")
	}
}

func TestWriteFileRejectsPathTraversal(t *testing.T) {
	dir := t.TempDir()
	h := NewWriteFileHandler(dir)

	payload, _ := json.Marshal(map[string]string{"name": "../../etc/passwd", "content": "pwned"})
	err := h(context.Background(), job.Job{Payload: payload})
	if err == nil {
		t.Fatal("expected an error for a path-traversal attempt, got nil")
	}

	// Confirm nothing was written anywhere in the parent of the sandbox
	// dir — the traversal must have been rejected outright, not merely
	// clamped to some other unintended location.
	entries, err := os.ReadDir(filepath.Dir(dir))
	if err != nil {
		t.Fatalf("reading parent dir: %v", err)
	}
	for _, e := range entries {
		if e.Name() == "passwd" {
			t.Error("traversal attempt created a file outside the sandbox directory")
		}
	}
}

func TestSumNumbersComputesSum(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	h := NewSumNumbersHandler(logger)

	payload, _ := json.Marshal(map[string]any{"numbers": []float64{1, 2, 3.5}})
	if err := h(context.Background(), job.Job{ID: 1, Payload: payload}); err != nil {
		t.Fatalf("handler: %v", err)
	}
}

func TestSumNumbersRejectsEmpty(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	h := NewSumNumbersHandler(logger)

	payload, _ := json.Marshal(map[string]any{"numbers": []float64{}})
	err := h(context.Background(), job.Job{Payload: payload})
	if err == nil {
		t.Fatal("expected an error for empty numbers, got nil")
	}
}
