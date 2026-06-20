package replication

import (
	"context"
	"sync"
)

type ISR struct {
	mu      sync.Mutex
	members map[int]*Replica
	notify  chan struct{}
}

func NewISR(replicas ...*Replica) *ISR {
	isr := &ISR{
		members: make(map[int]*Replica, len(replicas)),
		notify:  make(chan struct{}),
	}
	for _, r := range replicas {
		isr.members[r.id] = r
	}
	return isr
}

func (isr *ISR) Add(r *Replica) {
	isr.mu.Lock()
	isr.members[r.id] = r
	isr.mu.Unlock()
}

func (isr *ISR) Remove(id int) {
	isr.mu.Lock()
	delete(isr.members, id)
	isr.mu.Unlock()
}

func (isr *ISR) Contains(id int) bool {
	isr.mu.Lock()
	defer isr.mu.Unlock()
	_, ok := isr.members[id]
	return ok
}

func (isr *ISR) Members() []*Replica {
	isr.mu.Lock()
	defer isr.mu.Unlock()
	out := make([]*Replica, 0, len(isr.members))
	for _, r := range isr.members {
		out = append(out, r)
	}
	return out
}

func (isr *ISR) Notify() {
	isr.mu.Lock()
	ch := isr.notify
	isr.notify = make(chan struct{})
	isr.mu.Unlock()
	close(ch)
}

func (isr *ISR) WaitAll(ctx context.Context, target uint64) error {
	for {
		isr.mu.Lock()
		ok := isr.allCaughtLocked(target)
		ch := isr.notify
		isr.mu.Unlock()

		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
	}
}

func (isr *ISR) allCaughtLocked(target uint64) bool {
	for _, r := range isr.members {
		if r.FetchOffset() < target {
			return false
		}
	}
	return true
}
