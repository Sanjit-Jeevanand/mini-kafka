package sim

import (
	"errors"
	"testing"
	"time"
)

func TestSameSeedSameFaults(t *testing.T) {
	cfg := FaultConfig{DropRate: 0.3, DelayRate: 0.2, MaxDelay: 10 * time.Millisecond}
	clock := NewSimClock(time.Now())

	n1 := NewNetwork(42, cfg, clock)
	n2 := NewNetwork(42, cfg, clock)

	var drops1, drops2 []int
	noop := func() error { return nil }

	for i := 0; i < 50; i++ {
		if err := n1.Deliver(noop); errors.Is(err, ErrDropped) {
			drops1 = append(drops1, i)
		}
		if err := n2.Deliver(noop); errors.Is(err, ErrDropped) {
			drops2 = append(drops2, i)
		}
	}

	if len(drops1) != len(drops2) {
		t.Fatalf("seed 42: drop counts differ: %v vs %v", drops1, drops2)
	}
	for i := range drops1 {
		if drops1[i] != drops2[i] {
			t.Fatalf("seed 42: drop at index %d differs: %d vs %d", i, drops1[i], drops2[i])
		}
	}
}

func TestDifferentSeedsDifferentFaults(t *testing.T) {
	cfg := FaultConfig{DropRate: 0.3}
	clock := NewSimClock(time.Now())

	n1 := NewNetwork(1, cfg, clock)
	n2 := NewNetwork(2, cfg, clock)

	noop := func() error { return nil }
	var drops1, drops2 int
	for i := 0; i < 100; i++ {
		if errors.Is(n1.Deliver(noop), ErrDropped) {
			drops1++
		}
		if errors.Is(n2.Deliver(noop), ErrDropped) {
			drops2++
		}
	}

	if drops1 == drops2 {
		t.Logf("different seeds produced same drop count (%d) — unlikely but possible", drops1)
	}
}

func TestSimClockAdvance(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := NewSimClock(start)

	if !clock.Now().Equal(start) {
		t.Fatalf("want %v, got %v", start, clock.Now())
	}

	clock.Advance(5 * time.Minute)
	want := start.Add(5 * time.Minute)
	if !clock.Now().Equal(want) {
		t.Fatalf("after Advance: want %v, got %v", want, clock.Now())
	}

	clock.Sleep(30 * time.Second)
	want = want.Add(30 * time.Second)
	if !clock.Now().Equal(want) {
		t.Fatalf("after Sleep: want %v, got %v", want, clock.Now())
	}
}

func TestStepPassesOnSuccess(t *testing.T) {
	s := New(1, FaultConfig{})
	err := s.Step("ok",
		func() error { return nil },
		func() error { return nil },
		func() error { return nil },
	)
	if err != nil {
		t.Fatalf("expected nil, got: %v", err)
	}
}

func TestStepReportsInvariantViolation(t *testing.T) {
	s := New(1, FaultConfig{})
	err := s.Step("bad",
		func() error { return nil },
		func() error { return errors.New("hw regressed") },
	)
	if err == nil {
		t.Fatal("expected invariant violation, got nil")
	}
}

func TestStepIgnoresErrDropped(t *testing.T) {
	s := New(1, FaultConfig{})
	err := s.Step("dropped",
		func() error { return ErrDropped },
		func() error { return nil },
	)
	if err != nil {
		t.Fatalf("ErrDropped should not fail a step, got: %v", err)
	}
}

func TestCheckAllOwned(t *testing.T) {
	owned := map[int]bool{0: true, 1: true, 2: true}
	if err := CheckAllOwned("partitions", 3, owned); err != nil {
		t.Fatalf("all owned: unexpected error: %v", err)
	}

	delete(owned, 1)
	if err := CheckAllOwned("partitions", 3, owned); err == nil {
		t.Fatal("missing partition 1: expected error, got nil")
	}
}

func TestCheckMonotonic(t *testing.T) {
	if err := CheckMonotonic("hw", 5, 6); err != nil {
		t.Fatalf("6 >= 5: unexpected error: %v", err)
	}
	if err := CheckMonotonic("hw", 5, 5); err != nil {
		t.Fatalf("5 == 5: unexpected error: %v", err)
	}
	if err := CheckMonotonic("hw", 5, 4); err == nil {
		t.Fatal("4 < 5: expected error, got nil")
	}
}

func TestCheckNoRegression(t *testing.T) {
	prev := map[string]uint64{"g1:orders:0": 100}
	curr := map[string]uint64{"g1:orders:0": 110}
	if err := CheckNoRegression("offsets", prev, curr); err != nil {
		t.Fatalf("110 >= 100: unexpected error: %v", err)
	}

	curr["g1:orders:0"] = 90
	if err := CheckNoRegression("offsets", prev, curr); err == nil {
		t.Fatal("90 < 100: expected regression error, got nil")
	}
}

func TestViolationsAsError(t *testing.T) {
	var vs Violations
	if vs.AsError() != nil {
		t.Fatal("empty Violations should return nil")
	}
	vs = append(vs, errors.New("first"), errors.New("second"))
	if vs.AsError() == nil {
		t.Fatal("non-empty Violations should return error")
	}
}
