package topic

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"

	ilog "github.com/sanjit-jeevanand/mini-kafka/internal/log"
)

// openTestTopic creates a topic with numPartitions backed by a temp directory.
func openTestTopic(t *testing.T, numPartitions int) *Topic {
	t.Helper()
	tp, err := Open("test", Options{Dir: t.TempDir(), NumPartitions: numPartitions})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { tp.Close() })
	return tp
}

// TestTopicAppendRead is the golden path: append records, read them back.
func TestTopicAppendRead(t *testing.T) {
	ctx := context.Background()
	tp := openTestTopic(t, 1)

	const n = 50
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		val := []byte(fmt.Sprintf("val-%d", i))
		_, _, err := tp.Append(ctx, key, ilog.Record{Key: key, Value: val})
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	// All keys are distinct but hash to the same partition (only 1 partition).
	for i := uint64(0); i < n; i++ {
		rec, err := tp.Read(ctx, 0, i)
		if err != nil {
			t.Fatalf("Read offset %d: %v", i, err)
		}
		if rec.Offset != i {
			t.Errorf("offset: got %d want %d", rec.Offset, i)
		}
	}
}

// TestPartitionerSameKeyConsistent verifies that the same key always maps
// to the same partition, regardless of how many times it is called.
func TestPartitionerSameKeyConsistent(t *testing.T) {
	p := NewPartitioner(8)
	key := []byte("user-42")

	first := p.Partition(key)
	for i := 0; i < 1000; i++ {
		got := p.Partition(key)
		if got != first {
			t.Fatalf("iteration %d: key mapped to partition %d, want %d", i, got, first)
		}
	}
}

// TestPartitionerNullKeyRotates verifies that null keys rotate across partitions,
// spreading load rather than pinning to a single partition.
func TestPartitionerNullKeyRotates(t *testing.T) {
	const n = 3
	p := NewPartitioner(n)
	seen := make(map[int]bool)

	for i := 0; i < n*4; i++ {
		seen[p.Partition(nil)] = true
	}

	if len(seen) < n {
		t.Errorf("null-key partitioner visited %d/%d partitions in %d calls", len(seen), n, n*4)
	}
}

// TestPerPartitionOrdering is the core Phase 3 invariant:
// under many concurrent producers all writing to the same partition,
// the offsets within that partition must be gap-free and monotonically increasing.
func TestPerPartitionOrdering(t *testing.T) {
	ctx := context.Background()
	// One partition so all records compete on the same log.
	tp := openTestTopic(t, 1)

	const goroutines = 8
	const recordsEach = 200

	var wg sync.WaitGroup
	offsets := make([][]uint64, goroutines)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			key := []byte("shared-key") // same key → same partition
			for i := 0; i < recordsEach; i++ {
				_, off, err := tp.Append(ctx, key, ilog.Record{
					Key:   key,
					Value: []byte(fmt.Sprintf("g%d-r%d", g, i)),
				})
				if err != nil {
					t.Errorf("goroutine %d Append %d: %v", g, i, err)
					return
				}
				offsets[g] = append(offsets[g], off)
			}
		}(g)
	}
	wg.Wait()

	// Collect all offsets from all goroutines into one sorted slice.
	var all []uint64
	for _, offs := range offsets {
		all = append(all, offs...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })

	total := goroutines * recordsEach
	if len(all) != total {
		t.Fatalf("got %d offsets, want %d", len(all), total)
	}

	// Offsets must be exactly 0, 1, 2, ..., total-1 — no gaps, no duplicates.
	for i, off := range all {
		if off != uint64(i) {
			t.Fatalf("offset[%d] = %d, want %d (gap or duplicate)", i, off, i)
		}
	}
}

// TestPartitionIsolation verifies that writes to different partitions
// have independent offset sequences — partition 0 at offset 5 and
// partition 1 at offset 5 are completely unrelated records.
func TestPartitionIsolation(t *testing.T) {
	ctx := context.Background()
	tp := openTestTopic(t, 2)

	const n = 20
	var wg sync.WaitGroup

	// Force records to partition 0 and partition 1 using known keys.
	// We find keys that hash to each partition by brute force.
	p := NewPartitioner(2)
	var key0, key1 []byte
	for i := 0; ; i++ {
		k := []byte(fmt.Sprintf("k%d", i))
		if p.Partition(k) == 0 && key0 == nil {
			key0 = k
		}
		if p.Partition(k) == 1 && key1 == nil {
			key1 = k
		}
		if key0 != nil && key1 != nil {
			break
		}
	}

	offsets := [2][]uint64{}
	var mu sync.Mutex

	for part, key := range [][]byte{key0, key1} {
		wg.Add(1)
		go func(part int, key []byte) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				_, off, err := tp.Append(ctx, key, ilog.Record{
					Key:   key,
					Value: []byte(fmt.Sprintf("p%d-r%d", part, i)),
				})
				if err != nil {
					t.Errorf("partition %d Append %d: %v", part, i, err)
					return
				}
				mu.Lock()
				offsets[part] = append(offsets[part], off)
				mu.Unlock()
			}
		}(part, key)
	}
	wg.Wait()

	// Each partition must have exactly n offsets in [0, n).
	for part := 0; part < 2; part++ {
		offs := offsets[part]
		sort.Slice(offs, func(i, j int) bool { return offs[i] < offs[j] })
		if len(offs) != n {
			t.Errorf("partition %d: got %d offsets, want %d", part, len(offs), n)
			continue
		}
		for i, off := range offs {
			if off != uint64(i) {
				t.Errorf("partition %d: offset[%d] = %d, want %d", part, i, off, i)
			}
		}
	}
}
