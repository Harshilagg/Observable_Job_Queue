package worker

import (
	"testing"
	"time"

	"github.com/Harshilagg/Observable_Job_Queue/internal/job"
)

func TestCalculateRetryDelayStaysWithinBoundsAndVaries(t *testing.T) {
	w := &Worker{
		retryBaseDelay: 1 * time.Second,
		maxRetryDelay:  10 * time.Second,
	}

	// attempts=1: no doubling has happened yet, so the exponential term
	// is exactly retryBaseDelay — full jitter means the actual delay
	// must land in [0, 1s).
	for i := 0; i < 100; i++ {
		d := w.calculateRetryDelay(job.Job{Attempts: 1})
		if d < 0 || d >= 1*time.Second {
			t.Fatalf("attempts=1: delay %v out of expected range [0, 1s)", d)
		}
	}

	// attempts=10: the exponential term has long since hit maxRetryDelay,
	// so the bound is the cap, not a huge computed exponential.
	seen := map[time.Duration]bool{}
	for i := 0; i < 200; i++ {
		d := w.calculateRetryDelay(job.Job{Attempts: 10})
		if d < 0 || d >= 10*time.Second {
			t.Fatalf("attempts=10: delay %v out of expected range [0, maxRetryDelay=10s)", d)
		}
		seen[d] = true
	}
	// If jitter weren't actually being applied (e.g. a bug always
	// returning the cap itself, or always 0), every sample would be
	// identical — this is what would catch that, not just "looks right".
	if len(seen) < 2 {
		t.Error("calculateRetryDelay returned the same value on every call — jitter doesn't appear to be applied")
	}
}

func TestCalculateRetryDelayGrowsWithAttempts(t *testing.T) {
	w := &Worker{
		retryBaseDelay: 100 * time.Millisecond,
		maxRetryDelay:  time.Hour, // effectively uncapped for this test
	}

	// With full jitter, any single sample could be near zero regardless
	// of attempts, so "grows with attempts" has to be checked on the
	// max observed over many samples, not any one delay.
	maxOf := func(attempts, samples int) time.Duration {
		var max time.Duration
		for i := 0; i < samples; i++ {
			if d := w.calculateRetryDelay(job.Job{Attempts: attempts}); d > max {
				max = d
			}
		}
		return max
	}

	low := maxOf(1, 50)
	high := maxOf(5, 50)
	if high <= low {
		t.Errorf("expected max delay at attempts=5 (%v) to exceed attempts=1 (%v)", high, low)
	}
}

func TestCalculateRetryDelayNeverPanicsOnZeroConfig(t *testing.T) {
	w := &Worker{retryBaseDelay: 0, maxRetryDelay: 0}
	if d := w.calculateRetryDelay(job.Job{Attempts: 3}); d != 0 {
		t.Errorf("expected 0 delay with zero config, got %v", d)
	}
}
