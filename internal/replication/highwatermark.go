package replication

import "sync"

type HighWatermark struct {
	mu    sync.RWMutex
	value uint64
}

func NewHighWatermark() *HighWatermark {
	return &HighWatermark{}
}

func (hw *HighWatermark) Advance(isr *ISR) {
	members := isr.Members()
	if len(members) == 0 {
		return
	}

	min := members[0].FetchOffset()
	for _, r := range members[1:] {
		if f := r.FetchOffset(); f < min {
			min = f
		}
	}

	hw.mu.Lock()
	if min > hw.value {
		hw.value = min
	}
	hw.mu.Unlock()
}

func (hw *HighWatermark) Get() uint64 {
	hw.mu.RLock()
	defer hw.mu.RUnlock()
	return hw.value
}
