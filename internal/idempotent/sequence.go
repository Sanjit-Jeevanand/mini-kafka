package idempotent

import (
	"fmt"
	"sync"
)

type producerState struct {
	lastSeq    int64
	lastOffset uint64
}

type SequenceTracker struct {
	mu    sync.Mutex
	state map[string]producerState
}

func NewSequenceTracker() *SequenceTracker {
	return &SequenceTracker{
		state: make(map[string]producerState),
	}
}

func (t *SequenceTracker) Check(producerID string, seq int64) (isDuplicate bool, lastOffset uint64, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	s, seen := t.state[producerID]
	if !seen {
		s = producerState{lastSeq: -1}
	}

	switch {
	case seq == s.lastSeq+1:
		return false, 0, nil
	case seq == s.lastSeq:
		return true, s.lastOffset, nil
	case seq < s.lastSeq:
		return false, 0, fmt.Errorf("idempotent: producer %q seq %d is too old (last accepted: %d)",
			producerID, seq, s.lastSeq)
	default:
		return false, 0, fmt.Errorf("idempotent: producer %q seq %d skips ahead (expected: %d)",
			producerID, seq, s.lastSeq+1)
	}
}

func (t *SequenceTracker) Advance(producerID string, seq int64, offset uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.state[producerID] = producerState{lastSeq: seq, lastOffset: offset}
}
