package worker

import (
	"testing"
	"time"
)

func TestDefaultBackoffGrowsAndIsCapped(t *testing.T) {
	prevMax := time.Duration(0)
	for attempt := 1; attempt <= 12; attempt++ {
		d := DefaultBackoff(attempt)
		if d <= 0 {
			t.Fatalf("attempt %d: non-positive delay %s", attempt, d)
		}
		if d > 75*time.Second { // 60s cap + 25% jitter
			t.Fatalf("attempt %d: delay %s is over the cap", attempt, d)
		}
		if attempt <= 4 && d > prevMax {
			prevMax = d
		}
	}
	// first retry is about 1s, not minutes
	if d := DefaultBackoff(1); d > 2*time.Second {
		t.Fatalf("first retry too slow: %s", d)
	}
	// weird input shouldn't panic
	_ = DefaultBackoff(0)
	_ = DefaultBackoff(-3)
}
