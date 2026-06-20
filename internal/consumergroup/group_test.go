package consumergroup

import (
	"sort"
	"testing"
	"time"
)

func TestStickyAssign(t *testing.T) {
	members := map[string]struct{}{"a": {}}
	got := stickyAssign(3, members, nil)
	assertPartitions(t, got["a"], []int{0, 1, 2})

	members = map[string]struct{}{"a": {}, "b": {}}
	got = stickyAssign(3, members, nil)
	total := len(got["a"]) + len(got["b"])
	if total != 3 {
		t.Fatalf("expected 3 total partitions, got %d", total)
	}
	if abs(len(got["a"])-len(got["b"])) > 1 {
		t.Fatalf("unbalanced: a=%v b=%v", got["a"], got["b"])
	}
}

func TestStickyPreservation(t *testing.T) {
	members := map[string]struct{}{"a": {}}
	round1 := stickyAssign(4, members, nil)

	members["b"] = struct{}{}
	round2 := stickyAssign(4, members, round1)

	if len(round2["a"]) != 2 || len(round2["b"]) != 2 {
		t.Fatalf("expected 2 partitions each after rebalance, got a=%v b=%v", round2["a"], round2["b"])
	}
	for _, p := range round2["a"] {
		if !contains(round1["a"], p) {
			t.Errorf("partition %d was not previously owned by a (not sticky)", p)
		}
	}
}

func TestGroupJoinLeave(t *testing.T) {
	store := newTestStore(t)
	defer store.Close()

	g := NewGroup("test-group", 4, store)

	parts, err := g.Join("m1")
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 4 {
		t.Fatalf("sole member should own all 4 partitions, got %v", parts)
	}

	_, err = g.Join("m1")
	if err == nil {
		t.Fatal("expected error on duplicate join")
	}

	parts2, err := g.Join("m2")
	if err != nil {
		t.Fatal(err)
	}
	if len(parts2) == 0 {
		t.Fatal("second member should receive some partitions")
	}

	g.Leave("m1")
	partsAfter, ok := g.Assignments("m2")
	if !ok {
		t.Fatal("m2 should still be in group after m1 leaves")
	}
	assertPartitions(t, partsAfter, []int{0, 1, 2, 3})
}

func TestOffsetStoreRoundTrip(t *testing.T) {
	store := newTestStore(t)
	defer store.Close()

	if err := store.Commit("g1", "topic", 0, 42); err != nil {
		t.Fatal(err)
	}
	if err := store.Commit("g1", "topic", 1, 99); err != nil {
		t.Fatal(err)
	}
	if err := store.Commit("g2", "topic", 0, 7); err != nil {
		t.Fatal(err)
	}

	check := func(group, topic string, partition int, want uint64) {
		t.Helper()
		got, ok := store.Committed(group, topic, partition)
		if !ok {
			t.Fatalf("Committed(%q,%q,%d): not found", group, topic, partition)
		}
		if got != want {
			t.Fatalf("Committed(%q,%q,%d): got %d, want %d", group, topic, partition, got, want)
		}
	}
	check("g1", "topic", 0, 42)
	check("g1", "topic", 1, 99)
	check("g2", "topic", 0, 7)

	_, ok := store.Committed("g1", "topic", 99)
	if ok {
		t.Fatal("expected not found for uncommitted partition")
	}
}

func TestOffsetStoreReplay(t *testing.T) {
	dir := t.TempDir()

	s1, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.Commit("grp", "orders", 0, 100); err != nil {
		t.Fatal(err)
	}
	if err := s1.Commit("grp", "orders", 0, 200); err != nil {
		t.Fatal(err)
	}
	s1.Close()

	s2, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	got, ok := s2.Committed("grp", "orders", 0)
	if !ok {
		t.Fatal("offset not found after replay")
	}
	if got != 200 {
		t.Fatalf("expected 200 after replay, got %d", got)
	}
}

func TestCoordinatorHeartbeat(t *testing.T) {
	store := newTestStore(t)
	defer store.Close()

	timeout := 80 * time.Millisecond
	c := NewCoordinator(4, store, timeout)
	defer c.Close()

	if _, err := c.Join("grp", "m1"); err != nil {
		t.Fatal(err)
	}

	time.Sleep(40 * time.Millisecond)
	if err := c.Heartbeat("grp", "m1"); err != nil {
		t.Fatalf("heartbeat should succeed while member is alive: %v", err)
	}

	time.Sleep(40 * time.Millisecond)
	if err := c.Heartbeat("grp", "m1"); err != nil {
		t.Fatalf("heartbeat should succeed: %v", err)
	}

	time.Sleep(timeout + 20*time.Millisecond)
	if err := c.Heartbeat("grp", "m1"); err == nil {
		t.Fatal("expected error: member should have been evicted after timeout")
	}
}

func TestCoordinatorMultiMember(t *testing.T) {
	store := newTestStore(t)
	defer store.Close()

	c := NewCoordinator(4, store, 5*time.Second)
	defer c.Close()

	c.Join("grp", "m1")
	c.Join("grp", "m2")

	p1, _ := c.Assignments("grp", "m1")
	p2, _ := c.Assignments("grp", "m2")
	if len(p1)+len(p2) != 4 {
		t.Fatalf("expected 4 total partitions after rebalance, got m1=%v m2=%v", p1, p2)
	}

	c.Leave("grp", "m1")
	p2After, ok := c.Assignments("grp", "m2")
	if !ok {
		t.Fatal("m2 should still be assigned")
	}
	assertPartitions(t, p2After, []int{0, 1, 2, 3})
}

func newTestStore(t *testing.T) *OffsetStore {
	t.Helper()
	s, err := NewOffsetStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewOffsetStore: %v", err)
	}
	return s
}

func assertPartitions(t *testing.T, got, want []int) {
	t.Helper()
	sort.Ints(got)
	sort.Ints(want)
	if len(got) != len(want) {
		t.Fatalf("partitions: got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("partitions: got %v, want %v", got, want)
		}
	}
}

func contains(s []int, v int) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
