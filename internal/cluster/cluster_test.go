package cluster

import (
	"context"
	"testing"
	"time"
)

func newTestEpochs(t *testing.T) *EpochStore {
	t.Helper()
	e, err := NewEpochStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewEpochStore: %v", err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

func TestEpochSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	e1, err := NewEpochStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := e1.Bump(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := e1.Bump(ctx); err != nil {
		t.Fatal(err)
	}
	if e1.Current() != 2 {
		t.Fatalf("want epoch 2, got %d", e1.Current())
	}
	e1.Close()

	e2, err := NewEpochStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	if e2.Current() != 2 {
		t.Fatalf("after restart: want epoch 2, got %d", e2.Current())
	}
}

func TestStalLeaderFenced(t *testing.T) {
	epochs := newTestEpochs(t)
	ctx := context.Background()

	brokers := []BrokerInfo{{ID: 1, Addr: "127.0.0.1:9001"}}
	membership := NewMembership(brokers)
	ctrl := NewController(1, membership, epochs, time.Second, time.Minute)

	if err := membership.Heartbeat(1); err != nil {
		t.Fatal(err)
	}
	leaderID, newEpoch, err := Elect(ctx, 0, []int{1}, membership, epochs, time.Second)
	if err != nil {
		t.Fatalf("Elect: %v", err)
	}
	if leaderID != 1 {
		t.Fatalf("want leader 1, got %d", leaderID)
	}

	if err := ctrl.Fence(newEpoch); err != nil {
		t.Fatalf("current leader should not be fenced: %v", err)
	}

	staleEpoch := newEpoch - 1
	if err := ctrl.Fence(staleEpoch); err == nil {
		t.Fatal("stale leader should be fenced, got nil error")
	}
}

func TestElectionOnLeaderDeath(t *testing.T) {
	epochs := newTestEpochs(t)
	ctx := context.Background()

	brokers := []BrokerInfo{
		{ID: 1, Addr: "127.0.0.1:9001"},
		{ID: 2, Addr: "127.0.0.1:9002"},
	}
	membership := NewMembership(brokers)

	timeout := 80 * time.Millisecond
	ctrl := NewController(1, membership, epochs, timeout, 10*time.Millisecond)

	if err := membership.Heartbeat(1); err != nil {
		t.Fatal(err)
	}
	if err := membership.Heartbeat(2); err != nil {
		t.Fatal(err)
	}

	_, epoch1, err := Elect(ctx, 0, []int{1, 2}, membership, epochs, timeout)
	if err != nil {
		t.Fatalf("initial Elect: %v", err)
	}
	leader, _ := ctrl.LeaderFor(0)
	if leader != 1 {
		t.Fatalf("want initial leader 1, got %d", leader)
	}

	time.Sleep(timeout + 20*time.Millisecond)

	if err := membership.Heartbeat(2); err != nil {
		t.Fatal(err)
	}

	if !NeedsElection(0, membership, timeout) {
		t.Fatal("expected NeedsElection=true after leader 1 went silent")
	}

	newLeader, epoch2, err := Elect(ctx, 0, []int{1, 2}, membership, epochs, timeout)
	if err != nil {
		t.Fatalf("failover Elect: %v", err)
	}
	if newLeader != 2 {
		t.Fatalf("want new leader 2, got %d", newLeader)
	}
	if epoch2 <= epoch1 {
		t.Fatalf("new epoch %d must be greater than old epoch %d", epoch2, epoch1)
	}

	if err := ctrl.Fence(epoch1); err == nil {
		t.Fatal("old leader epoch should be fenced after failover")
	}
}

func TestNeedsElectionNoLeader(t *testing.T) {
	membership := NewMembership([]BrokerInfo{{ID: 1, Addr: "127.0.0.1:9001"}})
	if !NeedsElection(0, membership, time.Second) {
		t.Fatal("want NeedsElection=true when no leader assigned yet")
	}
}

func TestNoAliveCandidate(t *testing.T) {
	epochs := newTestEpochs(t)
	membership := NewMembership([]BrokerInfo{{ID: 1, Addr: "127.0.0.1:9001"}})

	_, _, err := Elect(context.Background(), 0, []int{1}, membership, epochs, time.Second)
	if err == nil {
		t.Fatal("Elect with no alive candidates should return error")
	}
}
