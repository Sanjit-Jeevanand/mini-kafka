package eval

import (
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	ilog "github.com/sanjit-jeevanand/mini-kafka/internal/log"
)

// PerfMetrics backs bullet 1: throughput and tail latency from the group commit path.
type PerfMetrics struct {
	Workers        int
	Duration       time.Duration
	RecordsWritten int64
	ThroughputRPS  float64
	P50LatencyMs   float64
	P99LatencyMs   float64
}

// ReplicationMetrics backs bullet 2: acknowledged loss and failover timing.
// Tests in internal/cluster populate this directly after running failover scenarios.
type ReplicationMetrics struct {
	AcknowledgedLostRecords int64
	FailoverDurations       []time.Duration
}

func (r ReplicationMetrics) MaxFailoverMs() float64 {
	var max time.Duration
	for _, d := range r.FailoverDurations {
		if d > max {
			max = d
		}
	}
	return float64(max.Milliseconds())
}

func (r ReplicationMetrics) MinFailoverMs() float64 {
	if len(r.FailoverDurations) == 0 {
		return 0
	}
	min := r.FailoverDurations[0]
	for _, d := range r.FailoverDurations[1:] {
		if d < min {
			min = d
		}
	}
	return float64(min.Milliseconds())
}

// FaultMetrics backs bullet 3: scenario count and violations surfaced.
// The sim.Simulator's Step loop populates ScenariosRun; callers tally ViolationsFound.
type FaultMetrics struct {
	ScenariosRun    int
	ViolationsFound int
	UniqueSeeds     int
}

// CVSummary combines all three metric types and formats the CV bullet text.
type CVSummary struct {
	Perf        PerfMetrics
	Replication ReplicationMetrics
	Fault       FaultMetrics
}

func (s CVSummary) Print() {
	low := s.Perf.ThroughputRPS * 0.75
	high := s.Perf.ThroughputRPS
	fmt.Printf("Bullet 1 — throughput & latency\n")
	fmt.Printf("  Group commit: %.0f–%.0f rec/s  P50: %.1fms  P99: %.1fms\n\n",
		low, high, s.Perf.P50LatencyMs, s.Perf.P99LatencyMs)

	fmt.Printf("Bullet 2 — replication & failover\n")
	fmt.Printf("  Acknowledged lost records: %d  Failover: %.0fms–%.0fms\n\n",
		s.Replication.AcknowledgedLostRecords,
		s.Replication.MinFailoverMs(),
		s.Replication.MaxFailoverMs())

	fmt.Printf("Bullet 3 — fault injection\n")
	fmt.Printf("  Scenarios: %d  Seeds: %d  Violations found: %d\n",
		s.Fault.ScenariosRun, s.Fault.UniqueSeeds, s.Fault.ViolationsFound)
}

// MeasurePerf runs the group commit mode and records per-record append latency.
// Each goroutine accumulates latencies locally to avoid lock contention, then all
// slices are merged, sorted once, and used to compute percentiles.
func MeasurePerf(cfg Config) (PerfMetrics, error) {
	dir, err := os.MkdirTemp("", "eval-metrics-*")
	if err != nil {
		return PerfMetrics{}, err
	}
	defer os.RemoveAll(dir)

	l, err := ilog.New(ilog.Options{Dir: dir})
	if err != nil {
		return PerfMetrics{}, err
	}
	defer func() { _ = l.Close() }()

	gc := ilog.NewGroupCommitter(l, ilog.GroupCommitConfig{
		BufSize:      cfg.BufSize,
		MaxBatchSize: cfg.MaxBatchSize,
		MaxBatchWait: cfg.MaxBatchWait,
	})

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Duration)
	defer cancel()

	go gc.Run(ctx)

	payload := []byte("metrics:perf:record")

	var (
		count   atomic.Int64
		mu      sync.Mutex
		allLats []float64
		wg      sync.WaitGroup
	)

	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var local []float64
			for ctx.Err() == nil {
				t0 := time.Now()
				if _, err := gc.Append(ctx, ilog.Record{Value: payload}); err != nil {
					break
				}
				local = append(local, float64(time.Since(t0).Microseconds())/1000.0)
				count.Add(1)
			}
			mu.Lock()
			allLats = append(allLats, local...)
			mu.Unlock()
		}()
	}

	start := time.Now()
	wg.Wait()
	elapsed := time.Since(start)

	sort.Float64s(allLats)

	n := count.Load()
	return PerfMetrics{
		Workers:        cfg.Workers,
		Duration:       elapsed,
		RecordsWritten: n,
		ThroughputRPS:  float64(n) / elapsed.Seconds(),
		P50LatencyMs:   percentile(allLats, 50),
		P99LatencyMs:   percentile(allLats, 99),
	}, nil
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100.0*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
