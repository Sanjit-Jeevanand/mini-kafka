package eval

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	ilog "github.com/sanjit-jeevanand/mini-kafka/internal/log"
)

type Config struct {
	Workers      int
	Duration     time.Duration
	BufSize      int
	MaxBatchSize int
	MaxBatchWait time.Duration
}

func DefaultConfig() Config {
	return Config{
		Workers:      500,
		Duration:     3 * time.Second,
		BufSize:      2000,
		MaxBatchSize: 10_000,
		MaxBatchWait: time.Millisecond,
	}
}

type Result struct {
	Workers        int           `json:"workers"`
	Duration       time.Duration `json:"duration_ns"`
	PerRecordRPS   float64       `json:"per_record_rps"`
	GroupCommitRPS float64       `json:"group_commit_rps"`
	Speedup        float64       `json:"speedup"`
}

func Run(cfg Config) (Result, error) {
	payload := []byte("eval:perf:record")

	perRPS, err := runPerRecord(payload, cfg)
	if err != nil {
		return Result{}, fmt.Errorf("per-record run: %w", err)
	}

	gcRPS, err := runGroupCommit(payload, cfg)
	if err != nil {
		return Result{}, fmt.Errorf("group-commit run: %w", err)
	}

	speedup := 0.0
	if perRPS > 0 {
		speedup = gcRPS / perRPS
	}

	return Result{
		Workers:        cfg.Workers,
		Duration:       cfg.Duration,
		PerRecordRPS:   perRPS,
		GroupCommitRPS: gcRPS,
		Speedup:        speedup,
	}, nil
}

func CheckRegression(current, baseline float64, maxDropFraction float64) error {
	floor := baseline * (1 - maxDropFraction)
	if current < floor {
		pct := (1 - current/baseline) * 100
		return fmt.Errorf("throughput regression: %.0f rec/s is %.1f%% below baseline %.0f rec/s (limit %.0f%%)",
			current, pct, baseline, maxDropFraction*100)
	}
	return nil
}

type Baseline struct {
	GroupCommitRPS float64 `json:"group_commit_rps"`
	Workers        int     `json:"workers"`
	RecordedAt     string  `json:"recorded_at"`
}

func LoadBaseline(path string) (Baseline, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Baseline{}, err
	}
	var b Baseline
	if err := json.Unmarshal(data, &b); err != nil {
		return Baseline{}, fmt.Errorf("baseline parse: %w", err)
	}
	return b, nil
}

func SaveBaseline(path string, r Result) error {
	b := Baseline{
		GroupCommitRPS: r.GroupCommitRPS,
		Workers:        r.Workers,
		RecordedAt:     time.Now().UTC().Format(time.RFC3339),
	}
	data, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(pathDir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func pathDir(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			return p[:i]
		}
	}
	return "."
}

func runPerRecord(payload []byte, cfg Config) (float64, error) {
	dir, err := os.MkdirTemp("", "eval-per-*")
	if err != nil {
		return 0, err
	}
	defer os.RemoveAll(dir)

	l, err := ilog.New(ilog.Options{Dir: dir})
	if err != nil {
		return 0, err
	}
	defer func() { _ = l.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Duration)
	defer cancel()

	var count atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				if _, err := l.Append(ctx, ilog.Record{Value: payload}); err != nil {
					return
				}
				count.Add(1)
			}
		}()
	}

	start := time.Now()
	wg.Wait()
	return float64(count.Load()) / time.Since(start).Seconds(), nil
}

func runGroupCommit(payload []byte, cfg Config) (float64, error) {
	dir, err := os.MkdirTemp("", "eval-gc-*")
	if err != nil {
		return 0, err
	}
	defer os.RemoveAll(dir)

	l, err := ilog.New(ilog.Options{Dir: dir})
	if err != nil {
		return 0, err
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

	var count atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				if _, err := gc.Append(ctx, ilog.Record{Value: payload}); err != nil {
					return
				}
				count.Add(1)
			}
		}()
	}

	start := time.Now()
	wg.Wait()
	return float64(count.Load()) / time.Since(start).Seconds(), nil
}
