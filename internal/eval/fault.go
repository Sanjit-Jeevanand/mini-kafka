package eval

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"

	ilog "github.com/sanjit-jeevanand/mini-kafka/internal/log"
	"github.com/sanjit-jeevanand/mini-kafka/internal/replication"
)

// fetcherHandle lets a scenario stop a fetcher and know it has actually
// exited before starting a replacement — without this, an old fetcher
// goroutine racing a newly started one could double-append to the same
// follower log and desync its offsets from the leader's, producing a false
// "missing record" violation that reflects a bug in the scenario driver,
// not in replication.
type fetcherHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func startFetcher(
	r *replication.Replica,
	leaderLog, followerLog *ilog.Log,
	isr *replication.ISR,
	hw *replication.HighWatermark,
	lagThreshold uint64,
	backoff time.Duration,
) *fetcherHandle {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = replication.NewFetcher(r, leaderLog, followerLog, isr, hw, lagThreshold, backoff).Run(ctx)
		close(done)
	}()
	return &fetcherHandle{cancel: cancel, done: done}
}

func (h *fetcherHandle) stop() {
	h.cancel()
	<-h.done
}

// runFaultScenario runs one seeded fault-injection scenario against the real
// ISR / HighWatermark / Fetcher replication primitives: a leader and two
// followers, with follower 2 randomly crashed and recovered mid-stream. It
// checks two invariants after every acknowledged append:
//
//  1. durability: every replica that was in the ISR at the moment a record
//     was acknowledged (isr.WaitAll succeeded) must actually have that
//     record on disk.
//  2. monotonicity: the high-watermark never regresses.
//
// Same seed always produces the same crash/recovery schedule and the same
// record count, so any violation found is reproducible by re-running the
// seed.
func runFaultScenario(seed int64) ([]string, error) {
	rng := rand.New(rand.NewSource(seed))
	numRecords := 5 + rng.Intn(10)

	dir, err := os.MkdirTemp("", "fault-*")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(dir) }()

	leaderLog, err := ilog.New(ilog.Options{Dir: dir + "/leader"})
	if err != nil {
		return nil, err
	}
	defer func() { _ = leaderLog.Close() }()

	f1Log, err := ilog.New(ilog.Options{Dir: dir + "/f1"})
	if err != nil {
		return nil, err
	}
	defer func() { _ = f1Log.Close() }()

	f2Log, err := ilog.New(ilog.Options{Dir: dir + "/f2"})
	if err != nil {
		return nil, err
	}
	defer func() { _ = f2Log.Close() }()

	const lagThreshold = 5
	const backoff = time.Millisecond

	r1 := replication.NewReplica(1)
	r2 := replication.NewReplica(2)
	isr := replication.NewISR(r1, r2)
	hw := replication.NewHighWatermark()
	bg := context.Background()

	h1 := startFetcher(r1, leaderLog, f1Log, isr, hw, lagThreshold, backoff)
	h2 := startFetcher(r2, leaderLog, f2Log, isr, hw, lagThreshold, backoff)
	f2Down := false
	defer func() {
		h1.stop()
		if !f2Down {
			h2.stop()
		}
	}()

	var violations []string
	var lastHW uint64

	for i := 0; i < numRecords; i++ {
		switch {
		case !f2Down && rng.Intn(5) == 0:
			h2.stop()
			isr.Remove(2)
			f2Down = true
		case f2Down && rng.Intn(3) == 0:
			h2 = startFetcher(r2, leaderLog, f2Log, isr, hw, lagThreshold, backoff)
			deadline := time.Now().Add(2 * time.Second)
			for r2.FetchOffset() < leaderLog.HighestOffset() && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			isr.Add(r2)
			f2Down = false
		}

		val := fmt.Sprintf("rec-%d", i)
		offset, err := leaderLog.Append(bg, ilog.Record{Value: []byte(val)})
		if err != nil {
			return violations, fmt.Errorf("seed %d: append %d: %w", seed, i, err)
		}

		ackMembers := isr.Members()
		ackCtx, ackCancel := context.WithTimeout(bg, 2*time.Second)
		ackErr := isr.WaitAll(ackCtx, offset+1)
		ackCancel()
		if ackErr != nil {
			continue // not acknowledged in time — no durability claim to check
		}

		for _, r := range ackMembers {
			flog := f1Log
			if r.ID() == 2 {
				flog = f2Log
			}
			if _, rerr := flog.Read(bg, offset); rerr != nil {
				violations = append(violations, fmt.Sprintf(
					"seed %d: acknowledged offset %d missing from replica %d: %v",
					seed, offset, r.ID(), rerr))
			}
		}

		if cur := hw.Get(); cur < lastHW {
			violations = append(violations, fmt.Sprintf(
				"seed %d: high-watermark regressed from %d to %d", seed, lastHW, cur))
		} else {
			lastHW = cur
		}
	}

	return violations, nil
}

// MeasureFaultInjection runs numSeeds independent seeded scenarios against
// the real replication primitives (not a mock) and tallies invariant
// violations actually found. Unlike PerfMetrics this is a correctness sweep:
// the honest result is whatever count comes out, including zero.
func MeasureFaultInjection(numSeeds int) (FaultMetrics, error) {
	total := 0
	for seed := 0; seed < numSeeds; seed++ {
		vs, err := runFaultScenario(int64(seed))
		if err != nil {
			return FaultMetrics{}, err
		}
		total += len(vs)
	}
	return FaultMetrics{
		ScenariosRun:    numSeeds,
		UniqueSeeds:     numSeeds,
		ViolationsFound: total,
	}, nil
}
