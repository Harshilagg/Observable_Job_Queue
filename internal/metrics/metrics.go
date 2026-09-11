// Package metrics defines this project's Prometheus instrumentation.
// Two kinds live here, deliberately kept separate:
//
//   - Metrics: instruments recorded directly, at the call site, the
//     moment something happens (a claim call returns, a job finishes,
//     a retry or dead-letter occurs). These are event-driven.
//   - the storeCollector (collector.go): point-in-time state (queue
//     depth, in-flight count, shipper lag) that isn't tied to any
//     single event — it's queried fresh from Postgres every time
//     Prometheus scrapes /metrics, rather than cached by a background
//     goroutine. Pull, not poll: one less thing to keep in sync.
package metrics

import "github.com/prometheus/client_golang/prometheus"

// Metrics holds the event-driven instruments the worker loop records.
type Metrics struct {
	ClaimDuration prometheus.Histogram
	JobDuration   *prometheus.HistogramVec
	Retries       *prometheus.CounterVec
	DeadLetters   *prometheus.CounterVec
}

// New creates the worker-side metrics and registers them on reg.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		ClaimDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "jobqueue",
			Name:      "claim_duration_seconds",
			Help:      "Time taken by a single Claim call, whether or not it found a job.",
			Buckets:   prometheus.DefBuckets,
		}),
		JobDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "jobqueue",
			Name:      "job_duration_seconds",
			Help:      "Time spent executing a claimed job's handler, by job type.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"job_type"}),
		Retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "jobqueue",
			Name:      "retries_total",
			Help:      "Jobs sent back to the queue after a failed attempt, by job type.",
		}, []string{"job_type"}),
		DeadLetters: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "jobqueue",
			Name:      "dead_letters_total",
			Help:      "Jobs that reached terminal failure, by job type.",
		}, []string{"job_type"}),
	}
	reg.MustRegister(m.ClaimDuration, m.JobDuration, m.Retries, m.DeadLetters)
	return m
}
