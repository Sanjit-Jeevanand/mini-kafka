package eval

import "testing"

func TestFailoverMeasurement(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping failover measurement: use -short to skip")
	}

	const trials = 10
	repl, err := MeasureFailover(trials)
	if err != nil {
		t.Fatalf("failover measurement failed: %v", err)
	}

	for i, d := range repl.FailoverDurations {
		t.Logf("trial %2d: %v", i+1, d)
	}
	t.Logf("min %.0fms  max %.0fms", repl.MinFailoverMs(), repl.MaxFailoverMs())

	// Detection cannot be faster than the heartbeat timeout: a leader is only
	// declared dead once its last heartbeat is older than that. Anything below
	// it would mean the controller evicted a live leader.
	floorMs := float64(FailoverHeartbeatTimeout.Milliseconds())
	if repl.MinFailoverMs() < floorMs {
		t.Errorf("failover %.0fms is below the heartbeat timeout floor %.0fms",
			repl.MinFailoverMs(), floorMs)
	}
}
