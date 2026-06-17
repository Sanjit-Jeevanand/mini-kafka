package log

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const defaultMaxBytes = 64 * 1024 * 1024

type Options struct {
	Dir      string
	MaxBytes uint32
}

type Log struct {
	mu       sync.RWMutex
	opts     Options
	segments []*Segment
	active   *Segment
}

func New(opts Options) (*Log, error) {
	if opts.MaxBytes == 0 {
		opts.MaxBytes = defaultMaxBytes
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}

	bases, err := scanBaseOffsets(opts.Dir)
	if err != nil {
		return nil, err
	}
	if len(bases) == 0 {
		bases = []uint64{0}
	}

	l := &Log{opts: opts}
	for _, base := range bases {
		seg, err := openSegment(opts.Dir, base, opts.MaxBytes)
		if err != nil {
			l.Close()
			return nil, err
		}
		l.segments = append(l.segments, seg)
	}

	active := l.segments[len(l.segments)-1]
	nextOffset, err := Recover(active.file, active.index, active.baseOffset)
	if err != nil {
		l.Close()
		return nil, err
	}
	active.nextOffset = nextOffset
	active.size = currentSize(active.file)
	l.active = active

	return l, nil
}

func (l *Log) Append(_ context.Context, r Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.active.IsFull() {
		if err := l.roll(); err != nil {
			return 0, err
		}
	}
	return l.active.Append(r)
}

func (l *Log) Read(_ context.Context, offset uint64) (Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	seg := l.findSegment(offset)
	if seg == nil {
		return Record{}, fmt.Errorf("log: offset %d out of range", offset)
	}
	return seg.Read(offset)
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var firstErr error
	for _, seg := range l.segments {
		if err := seg.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (l *Log) roll() error {
	nextBase := l.active.nextOffset
	seg, err := openSegment(l.opts.Dir, nextBase, l.opts.MaxBytes)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, seg)
	l.active = seg
	return nil
}

func (l *Log) findSegment(offset uint64) *Segment {
	n := len(l.segments)
	i := sort.Search(n, func(i int) bool {
		return l.segments[i].baseOffset > offset
	}) - 1
	if i < 0 {
		return nil
	}
	return l.segments[i]
}

func scanBaseOffsets(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var bases []uint64
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		stem := strings.TrimSuffix(name, ".log")
		base, err := strconv.ParseUint(stem, 10, 64)
		if err != nil {
			continue
		}
		bases = append(bases, base)
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	return bases, nil
}

func currentSize(f *os.File) uint32 {
	info, err := f.Stat()
	if err != nil {
		return 0
	}
	return uint32(info.Size())
}

func (l *Log) LowestOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset
}

func (l *Log) HighestOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	next := l.active.nextOffset
	if next == l.active.baseOffset {
		return 0
	}
	return next - 1
}

func (l *Log) DeleteSegmentsBefore(offset uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var keep []*Segment
	for _, seg := range l.segments {
		if seg == l.active || seg.baseOffset >= offset {
			keep = append(keep, seg)
			continue
		}
		logPath := filepath.Join(l.opts.Dir, fmt.Sprintf("%020d.log", seg.baseOffset))
		idxPath := filepath.Join(l.opts.Dir, fmt.Sprintf("%020d.index", seg.baseOffset))
		_ = seg.Close()
		_ = os.Remove(logPath)
		_ = os.Remove(idxPath)
	}
	l.segments = keep
	return nil
}
