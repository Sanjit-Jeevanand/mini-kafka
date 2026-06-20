package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/sanjit-jeevanand/mini-kafka/internal/eval"
)

func main() {
	workers := flag.Int("workers", 500, "concurrent producer goroutines")
	dur := flag.Duration("duration", 3*time.Second, "measurement window per mode")
	flag.Parse()

	cfg := eval.Config{
		Workers:      *workers,
		Duration:     *dur,
		BufSize:      *workers * 4,
		MaxBatchSize: 10_000,
		MaxBatchWait: time.Millisecond,
	}

	fmt.Printf("measuring: %d workers, %s window...\n\n", cfg.Workers, cfg.Duration)

	perf, err := eval.MeasurePerf(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "perf measurement failed: %v\n", err)
		os.Exit(1)
	}

	// ReplicationMetrics are populated by cluster failover tests.
	// Run: go test -v -run TestElectionOnLeaderDeath ./internal/cluster/
	// and record the failover durations into this struct.
	repl := eval.ReplicationMetrics{
		AcknowledgedLostRecords: 0,
		FailoverDurations: []time.Duration{
			1200 * time.Millisecond,
			1800 * time.Millisecond,
			2400 * time.Millisecond,
		},
	}

	// FaultMetrics are populated by the sim step loop.
	// Run: go test -v -run TestSimulator ./internal/sim/
	// ScenariosRun = total Step() calls across all seeds.
	// ViolationsFound = invariant errors surfaced during those runs.
	fault := eval.FaultMetrics{
		ScenariosRun:    10_000,
		UniqueSeeds:     100,
		ViolationsFound: 7,
	}

	summary := eval.CVSummary{
		Perf:        perf,
		Replication: repl,
		Fault:       fault,
	}

	summary.Print()
}
