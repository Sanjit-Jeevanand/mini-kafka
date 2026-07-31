package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/sanjit-jeevanand/mini-kafka/internal/eval"
)

func main() {
	workers := flag.Int("workers", 2000, "concurrent producer goroutines")
	dur := flag.Duration("duration", 3*time.Second, "measurement window per mode")
	faultSeeds := flag.Int("fault-seeds", 100, "number of seeded fault-injection scenarios to run")
	failoverTrials := flag.Int("failover-trials", 10, "number of leader-failover trials to time")
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

	// ReplicationMetrics come from driving the real cluster.Controller loop
	// (internal/eval/failover.go): each trial kills the partition leader and
	// times how long the controller takes to detect it, bump the epoch, and
	// install a successor. Trials sweep the death moment across one poll
	// interval so min/max bracket the true best and worst case.
	fmt.Printf("running %d failover trials...\n", *failoverTrials)
	repl, err := eval.MeasureFailover(*failoverTrials)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failover measurement failed: %v\n", err)
		os.Exit(1)
	}

	// FaultMetrics come from real seeded scenarios against the ISR /
	// HighWatermark / Fetcher replication primitives (internal/eval/fault.go),
	// not a placeholder: each scenario randomly crashes and recovers a
	// follower mid-stream and checks that no record acknowledged via
	// isr.WaitAll is ever missing from a replica that was in the ISR at
	// acknowledgment time, plus high-watermark monotonicity.
	fmt.Printf("running %d fault-injection scenarios...\n", *faultSeeds)
	fault, err := eval.MeasureFaultInjection(*faultSeeds)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fault injection sweep failed: %v\n", err)
		os.Exit(1)
	}

	summary := eval.CVSummary{
		Perf:        perf,
		Replication: repl,
		Fault:       fault,
	}

	summary.Print()
}
