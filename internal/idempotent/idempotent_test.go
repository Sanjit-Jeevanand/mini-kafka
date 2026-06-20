package idempotent

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	ilog "github.com/sanjit-jeevanand/mini-kafka/internal/log"
)

func TestSequenceTrackerNewProducer(t *testing.T) {
	tr := NewSequenceTracker()

	dup, _, err := tr.Check("p1", 0)
	if err != nil {
		t.Fatalf("first message seq=0: unexpected error: %v", err)
	}
	if dup {
		t.Fatal("first message should not be a duplicate")
	}
}

func TestSequenceTrackerAdvanceAndNext(t *testing.T) {
	tr := NewSequenceTracker()

	tr.Advance("p1", 0, 42)

	dup, _, err := tr.Check("p1", 1)
	if err != nil {
		t.Fatalf("seq=1 after advance: unexpected error: %v", err)
	}
	if dup {
		t.Fatal("seq=1 should not be a duplicate")
	}
}

func TestSequenceTrackerDuplicate(t *testing.T) {
	tr := NewSequenceTracker()
	tr.Advance("p1", 3, 99)

	dup, offset, err := tr.Check("p1", 3)
	if err != nil {
		t.Fatalf("duplicate check: unexpected error: %v", err)
	}
	if !dup {
		t.Fatal("same seq should be detected as duplicate")
	}
	if offset != 99 {
		t.Fatalf("duplicate should return last offset 99, got %d", offset)
	}
}

func TestSequenceTrackerTooOld(t *testing.T) {
	tr := NewSequenceTracker()
	tr.Advance("p1", 5, 10)

	_, _, err := tr.Check("p1", 3)
	if err == nil {
		t.Fatal("seq too old: expected error, got nil")
	}
}

func TestSequenceTrackerGap(t *testing.T) {
	tr := NewSequenceTracker()
	tr.Advance("p1", 2, 10)

	_, _, err := tr.Check("p1", 5)
	if err == nil {
		t.Fatal("seq gap: expected error, got nil")
	}
}

func TestSequenceTrackerMultipleProducers(t *testing.T) {
	tr := NewSequenceTracker()

	tr.Advance("p1", 0, 10)
	tr.Advance("p2", 0, 11)

	dup1, _, _ := tr.Check("p1", 0)
	dup2, _, _ := tr.Check("p2", 0)
	if !dup1 || !dup2 {
		t.Fatal("both producers seq=0 should be duplicates after Advance")
	}

	ok1, _, err := tr.Check("p1", 1)
	if err != nil || ok1 {
		t.Fatalf("p1 seq=1 should be new: dup=%v err=%v", ok1, err)
	}
	ok2, _, err := tr.Check("p2", 1)
	if err != nil || ok2 {
		t.Fatalf("p2 seq=1 should be new: dup=%v err=%v", ok2, err)
	}
}

func TestEncodeDecodeKeyRoundTrip(t *testing.T) {
	cases := []struct {
		id  string
		seq int64
		key []byte
	}{
		{"producer-abc", 0, []byte("user-123")},
		{"producer-abc", 42, []byte("user-456")},
		{"x", 9999, nil},
		{"long-producer-id-string", 1, []byte("k")},
	}

	for _, tc := range cases {
		encoded := EncodeKey(tc.id, tc.seq, tc.key)
		gotID, gotSeq, gotKey, err := DecodeKey(encoded)
		if err != nil {
			t.Fatalf("DecodeKey(%q, %d): %v", tc.id, tc.seq, err)
		}
		if gotID != tc.id {
			t.Errorf("id: got %q want %q", gotID, tc.id)
		}
		if gotSeq != tc.seq {
			t.Errorf("seq: got %d want %d", gotSeq, tc.seq)
		}
		if !bytes.Equal(gotKey, tc.key) {
			t.Errorf("key: got %v want %v", gotKey, tc.key)
		}
	}
}

func TestGroupCommitterAllOffsetsUnique(t *testing.T) {
	l, err := ilog.New(ilog.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	const workers = 20
	const perWorker = 50

	gc := ilog.NewGroupCommitter(l, ilog.GroupCommitConfig{
		BufSize:      workers * 4,
		MaxBatchSize: workers * perWorker,
		MaxBatchWait: time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go gc.Run(ctx)

	var mu sync.Mutex
	var offsets []uint64
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				off, err := gc.Append(ctx, ilog.Record{Value: []byte("x")})
				if err != nil {
					t.Errorf("Append: %v", err)
					return
				}
				mu.Lock()
				offsets = append(offsets, off)
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	total := workers * perWorker
	if len(offsets) != total {
		t.Fatalf("expected %d offsets, got %d", total, len(offsets))
	}

	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
	for i, off := range offsets {
		if off != uint64(i) {
			t.Fatalf("offset[%d] = %d, want %d (gap or duplicate)", i, off, i)
		}
	}
}

func TestGroupCommitterCancelReturnsError(t *testing.T) {
	l, err := ilog.New(ilog.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	gc := ilog.NewGroupCommitter(l, ilog.GroupCommitConfig{
		BufSize:      1,
		MaxBatchSize: 100,
		MaxBatchWait: time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())

	go gc.Run(ctx)
	cancel()

	_, err = gc.Append(ctx, ilog.Record{Value: []byte("x")})
	if err == nil {
		t.Fatal("expected error after context cancel, got nil")
	}
}
