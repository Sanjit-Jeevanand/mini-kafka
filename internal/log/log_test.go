package log

import (
	"context"
	"math/rand"
	"os"
	"testing"
)

func newTestLog(t *testing.T, maxBytes uint32) *Log {
	t.Helper()
	dir := t.TempDir()
	l, err := New(Options{Dir: dir, MaxBytes: maxBytes})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func appendN(t *testing.T, l *Log, n int) []uint64 {
	t.Helper()
	ctx := context.Background()
	offsets := make([]uint64, n)
	for i := 0; i < n; i++ {
		off, err := l.Append(ctx, Record{Key: []byte("k"), Value: []byte("v")})
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		offsets[i] = off
	}
	return offsets
}

// TestAppendReadBasic: write N records, read them back, check offsets 0..N-1.
func TestAppendReadBasic(t *testing.T) {
	l := newTestLog(t, 0)
	ctx := context.Background()
	const n = 100

	offsets := appendN(t, l, n)

	for i, want := range offsets {
		if want != uint64(i) {
			t.Fatalf("offset[%d] = %d, want %d", i, want, i)
		}
		rec, err := l.Read(ctx, want)
		if err != nil {
			t.Fatalf("Read(%d): %v", want, err)
		}
		if rec.Offset != want {
			t.Fatalf("record offset = %d, want %d", rec.Offset, want)
		}
	}
}

// TestMonotonicGapFree: offsets must be exactly 0,1,2,...,N-1 with no gaps.
func TestMonotonicGapFree(t *testing.T) {
	l := newTestLog(t, 0)
	const n = 500
	offsets := appendN(t, l, n)

	for i, off := range offsets {
		if off != uint64(i) {
			t.Fatalf("gap or non-monotonic at position %d: got offset %d", i, off)
		}
	}
}

// TestRestart: close and reopen the log, assert nextOffset continues correctly.
func TestRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	l, err := New(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	const n = 50
	appendN(t, l, n)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l2, err := New(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()

	// Next append should get offset n, not 0.
	off, err := l2.Append(ctx, Record{Key: []byte("k"), Value: []byte("v")})
	if err != nil {
		t.Fatal(err)
	}
	if off != n {
		t.Fatalf("after restart, first append offset = %d, want %d", off, n)
	}

	// All original records must still be readable.
	for i := 0; i < n; i++ {
		rec, err := l2.Read(ctx, uint64(i))
		if err != nil {
			t.Fatalf("Read(%d) after restart: %v", i, err)
		}
		if rec.Offset != uint64(i) {
			t.Fatalf("record offset = %d, want %d", rec.Offset, i)
		}
	}
}

// TestTornWriteRecovery: corrupt the tail of the active segment file, reopen,
// assert the torn record is gone and existing records are still readable.
func TestTornWriteRecovery(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	l, err := New(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	const n = 10
	appendN(t, l, n)

	// Grab the active segment file path before closing.
	segFile := l.active.file.Name()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	// Append garbage bytes to simulate a torn write.
	f, err := os.OpenFile(segFile, os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte("garbage torn write data"))
	_ = f.Close()

	// Reopen — recovery should truncate the garbage.
	l2, err := New(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()

	// All original records readable.
	for i := 0; i < n; i++ {
		rec, err := l2.Read(ctx, uint64(i))
		if err != nil {
			t.Fatalf("Read(%d) after torn write recovery: %v", i, err)
		}
		if rec.Offset != uint64(i) {
			t.Fatalf("record offset = %d, want %d", rec.Offset, i)
		}
	}

	// Next append continues from n, not garbage.
	off, err := l2.Append(ctx, Record{Key: []byte("k"), Value: []byte("v")})
	if err != nil {
		t.Fatal(err)
	}
	if off != n {
		t.Fatalf("post-recovery append offset = %d, want %d", off, n)
	}
}

// TestSegmentRoll: set MaxBytes tiny, append enough to force a roll, assert
// reads work across the segment boundary.
func TestSegmentRoll(t *testing.T) {
	// Each record is roughly headerSize(24) + 2 key + 2 val + 4 CRC = ~32 bytes.
	// Set MaxBytes to 100 so we roll after a few records.
	l := newTestLog(t, 100)
	ctx := context.Background()

	const n = 30
	offsets := appendN(t, l, n)

	if len(l.segments) < 2 {
		t.Fatal("expected segment roll but only one segment exists")
	}

	for _, off := range offsets {
		rec, err := l.Read(ctx, off)
		if err != nil {
			t.Fatalf("Read(%d) across segment boundary: %v", off, err)
		}
		if rec.Offset != off {
			t.Fatalf("record offset = %d, want %d", rec.Offset, off)
		}
	}
}

// TestPropertyMonotonicOffsets: random append counts, always gap-free monotonic.
func TestPropertyMonotonicOffsets(t *testing.T) {
	const iterations = 20
	ctx := context.Background()

	for i := 0; i < iterations; i++ {
		l := newTestLog(t, 0)
		n := rand.Intn(200) + 1
		offsets := appendN(t, l, n)

		for j, off := range offsets {
			if off != uint64(j) {
				t.Fatalf("iter %d: offset[%d] = %d, want %d", i, j, off, j)
			}
		}

		// All readable.
		for _, off := range offsets {
			if _, err := l.Read(ctx, off); err != nil {
				t.Fatalf("iter %d: Read(%d): %v", i, off, err)
			}
		}
	}
}

// TestReadOutOfRange: reading a non-existent offset returns an error.
func TestReadOutOfRange(t *testing.T) {
	l := newTestLog(t, 0)
	ctx := context.Background()
	appendN(t, l, 5)

	if _, err := l.Read(ctx, 9999); err == nil {
		t.Fatal("expected error reading out-of-range offset, got nil")
	}
}
