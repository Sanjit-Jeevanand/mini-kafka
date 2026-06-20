package eval

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"testing"
)

const baselinePath = "testdata/baseline.json"
const regressionThreshold = 0.10

var update = flag.Bool("update", false, "record current throughput as the new baseline")

func TestPerfGate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping perf gate: use -short to skip slow benchmarks")
	}

	cfg := DefaultConfig()
	t.Logf("running eval: %d workers, %s per mode", cfg.Workers, cfg.Duration)

	result, err := Run(cfg)
	if err != nil {
		t.Fatalf("eval run failed: %v", err)
	}

	t.Logf("per-record fsync : %.0f rec/s", result.PerRecordRPS)
	t.Logf("group commit     : %.0f rec/s", result.GroupCommitRPS)
	t.Logf("speedup          : %.1fx", result.Speedup)

	if *update {
		if err := SaveBaseline(baselinePath, result); err != nil {
			t.Fatalf("saving baseline: %v", err)
		}
		t.Logf("baseline saved to %s (%.0f rec/s)", baselinePath, result.GroupCommitRPS)
		return
	}

	baseline, err := LoadBaseline(baselinePath)
	if errors.Is(err, os.ErrNotExist) {
		t.Logf("no baseline found at %s — run with -update to record one", baselinePath)
		t.Skip("skipping regression check: no baseline")
	}
	if err != nil {
		t.Fatalf("loading baseline: %v", err)
	}

	t.Logf("baseline (recorded %s): %.0f rec/s at %d workers",
		baseline.RecordedAt, baseline.GroupCommitRPS, baseline.Workers)

	if err := CheckRegression(result.GroupCommitRPS, baseline.GroupCommitRPS, regressionThreshold); err != nil {
		t.Fatal(err)
	}

	t.Logf("perf gate passed: %.0f rec/s >= %.0f rec/s floor (90%% of baseline)",
		result.GroupCommitRPS, baseline.GroupCommitRPS*0.9)
}

func TestCheckRegressionUnit(t *testing.T) {
	cases := []struct {
		name    string
		current float64
		base    float64
		wantErr bool
	}{
		{"exactly at baseline", 100_000, 100_000, false},
		{"9% drop — within limit", 91_000, 100_000, false},
		{"exactly at floor (10%)", 90_000, 100_000, false},
		{"11% drop — regression", 89_000, 100_000, true},
		{"50% drop — regression", 50_000, 100_000, true},
		{"improvement — not a regression", 120_000, 100_000, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckRegression(tc.current, tc.base, regressionThreshold)
			if tc.wantErr && err == nil {
				t.Errorf("expected regression error, got nil (current=%.0f baseline=%.0f)", tc.current, tc.base)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected regression error: %v", err)
			}
		})
	}
}

func TestCheckRegressionMessage(t *testing.T) {
	err := CheckRegression(80_000, 100_000, 0.10)
	if err == nil {
		t.Fatal("expected error")
	}
	msg := err.Error()
	want := "throughput regression"
	if len(msg) < len(want) || msg[:len(want)] != want {
		t.Errorf("error message format unexpected: %q", msg)
	}
	fmt.Println("regression message:", msg)
}
