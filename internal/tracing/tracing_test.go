package tracing

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// TestInjectExtractRoundTripsTraceID is the round trip that makes the
// submit -> claim -> execute -> complete span all belong to one trace
// even though Submit and Claim run in different processes: whatever
// Inject serializes, Extract must reconstruct into the same trace, not
// a disconnected new one.
func TestInjectExtractRoundTripsTraceID(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	defer tp.Shutdown(context.Background())
	otel.SetTracerProvider(tp)

	ctx, submitSpan := Tracer("test").Start(context.Background(), "submit")
	wantTraceID := submitSpan.SpanContext().TraceID()
	traceContext := Inject(ctx)
	submitSpan.End()

	if traceContext == "" {
		t.Fatal("Inject returned an empty traceparent for an active span")
	}

	claimCtx := Extract(context.Background(), traceContext)
	_, claimSpan := Tracer("test").Start(claimCtx, "claim")
	defer claimSpan.End()

	gotTraceID := claimSpan.SpanContext().TraceID()
	if gotTraceID != wantTraceID {
		t.Errorf("claim span TraceID = %s, want %s (same trace as submit)", gotTraceID, wantTraceID)
	}
}

// TestExtractWithEmptyStringLeavesContextUnchanged covers rows written
// before trace_context existed (default ”), and jobs enqueued outside
// any active span (e.g. from a test) — both cases must not panic and
// must not fabricate a bogus parent trace.
func TestExtractWithEmptyStringLeavesContextUnchanged(t *testing.T) {
	type key struct{}
	ctx := context.WithValue(context.Background(), key{}, "marker")

	got := Extract(ctx, "")

	if got.Value(key{}) != "marker" {
		t.Error("Extract with an empty traceContext returned a different context than the one passed in")
	}
}
