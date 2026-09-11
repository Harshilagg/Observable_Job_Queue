// Package tracing wires up OpenTelemetry so a trace can span Submit
// (in the serve process) through Claim, Execute, and Complete (in a
// work process) — two different processes, and potentially seconds or
// minutes apart, since a job can sit queued for a while before a
// worker claims it.
//
// Standard OTel context propagation carries a trace across an HTTP or
// gRPC call by injecting a "traceparent" header the receiving side
// extracts. There's no live call between Submit and Claim to carry a
// header over — the job sits in Postgres in between. So the same W3C
// TraceContext propagator format is used, but the carrier is a column
// on the jobs row (jobs.trace_context) instead of a header: Inject at
// Submit time writes it into the row, Extract at Claim time reads it
// back out and continues the same trace.
package tracing

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.28.0"
	"go.opentelemetry.io/otel/trace"
)

// Setup configures the global TracerProvider to export spans to
// otlpEndpoint (Tempo's OTLP/gRPC receiver, e.g. "localhost:4317") and
// registers the W3C TraceContext propagator globally. Call the
// returned shutdown func before the process exits, so buffered spans
// get flushed rather than dropped.
func Setup(ctx context.Context, serviceName, otlpEndpoint string) (shutdown func(context.Context) error, err error) {
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(otlpEndpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("tracing: creating exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewSchemaless(semconv.ServiceName(serviceName))),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp.Shutdown, nil
}

// Inject serializes the trace context carried by ctx (i.e. the span
// active in ctx, if any) into a string suitable for storing in
// jobs.trace_context.
func Inject(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// Extract rebuilds a context carrying the remote trace described by
// traceContext (as produced by Inject), so a span started from the
// returned context continues that trace instead of starting a new,
// disconnected one. Returns ctx unchanged if traceContext is empty
// (e.g. a row written before this feature existed).
func Extract(ctx context.Context, traceContext string) context.Context {
	if traceContext == "" {
		return ctx
	}
	carrier := propagation.MapCarrier{"traceparent": traceContext}
	return propagation.TraceContext{}.Extract(ctx, carrier)
}

// Tracer returns the named tracer from the global TracerProvider.
func Tracer(name string) trace.Tracer {
	return otel.Tracer(name)
}
