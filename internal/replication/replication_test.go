package replication

import (
	"context"
	"fmt"
	"testing"
	"time"

	ilog "github.com/sanjit-jeevanand/mini-kafka/internal/log"
)

func newTestLog(t *testing.T) *ilog.Log {
	t.Helper()
	l, err := ilog.New(ilog.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("newTestLog: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(msg)
}

func TestAcksAll(t *testing.T) {
	leaderLog := newTestLog(t)
	f1Log := newTestLog(t)
	f2Log := newTestLog(t)

	r1 := NewReplica(1)
	r2 := NewReplica(2)
	isr := NewISR(r1, r2)
	hw := NewHighWatermark()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go NewFetcher(r1, leaderLog, f1Log, isr, hw, 100, time.Millisecond).Run(ctx)
	go NewFetcher(r2, leaderLog, f2Log, isr, hw, 100, time.Millisecond).Run(ctx)

	for i := 0; i < 10; i++ {
		offset, err := leaderLog.Append(ctx, ilog.Record{
			Value: []byte(fmt.Sprintf("record-%d", i)),
		})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if err := isr.WaitAll(ctx, offset+1); err != nil {
			t.Fatalf("WaitAll offset %d: %v", offset, err)
		}

		for fi, fl := range []*ilog.Log{f1Log, f2Log} {
			rec, err := fl.Read(ctx, offset)
			if err != nil {
				t.Fatalf("follower%d missing record at offset %d: %v", fi+1, offset, err)
			}
			want := fmt.Sprintf("record-%d", i)
			if string(rec.Value) != want {
				t.Errorf("follower%d offset %d: got %q want %q", fi+1, offset, rec.Value, want)
			}
		}
	}
}

func TestLeaderFailover(t *testing.T) {
	leaderLog := newTestLog(t)
	followerLog := newTestLog(t)

	r := NewReplica(1)
	isr := NewISR(r)
	hw := NewHighWatermark()

	ctx, cancel := context.WithCancel(context.Background())

	go NewFetcher(r, leaderLog, followerLog, isr, hw, 100, time.Millisecond).Run(ctx)

	const n = 10
	for i := 0; i < n; i++ {
		offset, err := leaderLog.Append(ctx, ilog.Record{
			Value: []byte(fmt.Sprintf("msg-%d", i)),
		})
		if err != nil {
			cancel()
			t.Fatalf("append %d: %v", i, err)
		}
		if err := isr.WaitAll(ctx, offset+1); err != nil {
			cancel()
			t.Fatalf("WaitAll: %v", err)
		}
	}

	cancel()

	for i := uint64(0); i < n; i++ {
		rec, err := followerLog.Read(context.Background(), i)
		if err != nil {
			t.Fatalf("new leader missing record at offset %d: %v", i, err)
		}
		want := fmt.Sprintf("msg-%d", i)
		if string(rec.Value) != want {
			t.Errorf("offset %d: got %q want %q", i, rec.Value, want)
		}
	}
}

func TestISREviction(t *testing.T) {
	leaderLog := newTestLog(t)
	followerLog := newTestLog(t)

	const lagThreshold = uint64(3)

	for i := uint64(0); i <= lagThreshold+1; i++ {
		if _, err := leaderLog.Append(context.Background(), ilog.Record{
			Value: []byte(fmt.Sprintf("r%d", i)),
		}); err != nil {
			t.Fatal(err)
		}
	}

	r := NewReplica(1)
	isr := NewISR(r)
	hw := NewHighWatermark()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go NewFetcher(r, leaderLog, followerLog, isr, hw, lagThreshold, time.Millisecond).Run(ctx)

	time.Sleep(20 * time.Millisecond)

	if isr.Contains(r.ID()) {
		t.Fatal("replica should have been evicted from ISR due to lag")
	}
}

func TestHighWatermark(t *testing.T) {
	leaderLog := newTestLog(t)
	f1Log := newTestLog(t)
	f2Log := newTestLog(t)

	r1 := NewReplica(1)
	r2 := NewReplica(2)
	isr := NewISR(r1, r2)
	hw := NewHighWatermark()

	if hw.Get() != 0 {
		t.Fatalf("initial HW want 0, got %d", hw.Get())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go NewFetcher(r1, leaderLog, f1Log, isr, hw, 100, time.Millisecond).Run(ctx)

	const n = 5
	for i := 0; i < n; i++ {
		if _, err := leaderLog.Append(ctx, ilog.Record{
			Value: []byte(fmt.Sprintf("r%d", i)),
		}); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(50 * time.Millisecond)

	if hw.Get() != 0 {
		t.Fatalf("HW should be 0 while r2 is stalled, got %d", hw.Get())
	}

	go NewFetcher(r2, leaderLog, f2Log, isr, hw, 100, time.Millisecond).Run(ctx)

	waitFor(t, func() bool { return hw.Get() == n }, 2*time.Second,
		fmt.Sprintf("HW should reach %d after both fetchers caught up, got %d", n, hw.Get()))
}
