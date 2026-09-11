package metrics

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Harshilagg/Observable_Job_Queue/internal/store"
)

// storeCollector queries Postgres directly every time /metrics is
// scraped, rather than caching state via a background goroutine. These
// are already cheap, indexed queries — CountByStatusAndType('queued')
// and ('running') hit the same partial indexes the claim query and
// reaper already use — so there's no real cost to computing them fresh
// on every scrape, and doing so avoids a second source of staleness to
// reason about.
type storeCollector struct {
	store  *store.Store
	logger *slog.Logger

	queueDepth *prometheus.Desc
	inFlight   *prometheus.Desc
	shipperLag *prometheus.Desc
}

// RegisterStoreCollector builds and registers the DB-driven gauges
// (queue depth and in-flight count by job type, and shipper lag) on reg.
func RegisterStoreCollector(reg prometheus.Registerer, st *store.Store, logger *slog.Logger) {
	reg.MustRegister(&storeCollector{
		store:  st,
		logger: logger,
		queueDepth: prometheus.NewDesc(
			"jobqueue_queue_depth",
			"Number of jobs currently queued, by job type.",
			[]string{"job_type"}, nil,
		),
		inFlight: prometheus.NewDesc(
			"jobqueue_in_flight",
			"Number of jobs currently running, by job type.",
			[]string{"job_type"}, nil,
		),
		shipperLag: prometheus.NewDesc(
			"jobqueue_shipper_lag_seconds",
			"Age of the oldest unshipped job_events row, in seconds (0 if none).",
			nil, nil,
		),
	})
}

func (c *storeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.queueDepth
	ch <- c.inFlight
	ch <- c.shipperLag
}

func (c *storeCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if depths, err := c.store.CountByStatusAndType(ctx, "queued"); err != nil {
		c.logger.Error("metrics: queue depth query failed", "error", err)
	} else {
		for jobType, n := range depths {
			ch <- prometheus.MustNewConstMetric(c.queueDepth, prometheus.GaugeValue, float64(n), jobType)
		}
	}

	if inFlight, err := c.store.CountByStatusAndType(ctx, "running"); err != nil {
		c.logger.Error("metrics: in-flight query failed", "error", err)
	} else {
		for jobType, n := range inFlight {
			ch <- prometheus.MustNewConstMetric(c.inFlight, prometheus.GaugeValue, float64(n), jobType)
		}
	}

	if lag, err := c.store.ShipperLag(ctx); err != nil {
		c.logger.Error("metrics: shipper lag query failed", "error", err)
	} else {
		ch <- prometheus.MustNewConstMetric(c.shipperLag, prometheus.GaugeValue, lag.Seconds())
	}
}
