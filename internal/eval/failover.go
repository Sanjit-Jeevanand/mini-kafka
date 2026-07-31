package eval

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/sanjit-jeevanand/mini-kafka/internal/cluster"
)

// Production controller timings, matching the documented Phase 6 configuration:
// the controller polls every PollInterval, and a broker is considered dead once
// its last heartbeat is older than HeartbeatTimeout.
const (
	FailoverHeartbeatTimeout = 2 * time.Second
	FailoverPollInterval     = 500 * time.Millisecond
)

// measureFailoverOnce measures wall-clock failover latency: the interval from
// the leader's final heartbeat (the instant it "dies") until the real
// cluster.Controller loop has detected the loss, bumped the epoch, and
// installed a new leader for the partition.
//
// This drives Controller.Run itself rather than reimplementing the detection
// logic, so the number reflects the actual control plane: detection is bounded
// below by HeartbeatTimeout and quantised by PollInterval, plus the cost of
// the epoch-bump fsync during the election.
//
// phase offsets the leader's death from the controller's ticker start. Without
// it, both begin at t=0 and detection always lands on the tick sitting exactly
// at the HeartbeatTimeout boundary, reporting a best-case ~timeout+epsilon that
// understates the real upper bound. In production a broker dies at an arbitrary
// point in the poll cycle, so callers sweep phase across [0, PollInterval) to
// recover the true spread.
func measureFailoverOnce(phase time.Duration) (time.Duration, error) {
	dir, err := os.MkdirTemp("", "failover-*")
	if err != nil {
		return 0, err
	}
	defer func() { _ = os.RemoveAll(dir) }()

	epochs, err := cluster.NewEpochStore(dir)
	if err != nil {
		return 0, err
	}
	defer func() { _ = epochs.Close() }()

	brokers := []cluster.BrokerInfo{
		{ID: 1, Addr: "127.0.0.1:9001"},
		{ID: 2, Addr: "127.0.0.1:9002"},
		{ID: 3, Addr: "127.0.0.1:9003"},
	}
	membership := cluster.NewMembership(brokers)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, b := range brokers {
		if err := membership.Heartbeat(b.ID); err != nil {
			return 0, err
		}
	}

	leader, _, err := cluster.Elect(ctx, 0, []int{1, 2, 3}, membership, epochs, FailoverHeartbeatTimeout)
	if err != nil {
		return 0, fmt.Errorf("initial election: %w", err)
	}

	ctrl := cluster.NewController(1, membership, epochs, FailoverHeartbeatTimeout, FailoverPollInterval)
	ctrlDone := make(chan struct{})
	go func() {
		_ = ctrl.Run(ctx)
		close(ctrlDone)
	}()

	// Let the controller's ticker run for `phase` first, so the death below
	// falls at an arbitrary point in the poll cycle rather than aligning with
	// tick zero.
	if phase > 0 {
		time.Sleep(phase)
	}

	// The survivors keep heartbeating; the leader goes silent from t0 onward.
	// Re-heartbeat everyone so t0 is exactly the leader's last heartbeat, i.e.
	// the moment of death, regardless of how long `phase` was.
	for _, b := range brokers {
		if err := membership.Heartbeat(b.ID); err != nil {
			return 0, err
		}
	}
	t0 := time.Now()
	go func() {
		tick := time.NewTicker(FailoverPollInterval / 3)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				for _, b := range brokers {
					if b.ID == leader {
						continue
					}
					_ = membership.Heartbeat(b.ID)
				}
			}
		}
	}()

	deadline := time.Now().Add(FailoverHeartbeatTimeout + 5*time.Second)
	for time.Now().Before(deadline) {
		if cur, ok := ctrl.LeaderFor(0); ok && cur != leader {
			elapsed := time.Since(t0)
			cancel()
			<-ctrlDone
			return elapsed, nil
		}
		time.Sleep(time.Millisecond)
	}

	cancel()
	<-ctrlDone
	return 0, fmt.Errorf("failover did not complete within deadline (leader %d never replaced)", leader)
}

// MeasureFailover runs trials independent failover scenarios against the real
// controller and returns the observed durations, so ReplicationMetrics carries
// measured timings rather than hardcoded values.
//
// Trials sweep the death moment evenly across one poll interval, so the
// reported min/max bracket the genuine best and worst case rather than
// repeatedly sampling one phase alignment.
func MeasureFailover(trials int) (ReplicationMetrics, error) {
	durations := make([]time.Duration, 0, trials)
	for i := 0; i < trials; i++ {
		phase := time.Duration(int64(FailoverPollInterval) * int64(i) / int64(trials))
		d, err := measureFailoverOnce(phase)
		if err != nil {
			return ReplicationMetrics{}, fmt.Errorf("failover trial %d: %w", i, err)
		}
		durations = append(durations, d)
	}
	return ReplicationMetrics{
		AcknowledgedLostRecords: 0,
		FailoverDurations:       durations,
	}, nil
}
