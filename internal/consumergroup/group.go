package consumergroup

import (
	"fmt"
	"sync"
)

type Group struct {
	mu            sync.Mutex
	name          string
	numPartitions int
	members       map[string]struct{}
	assignments   map[string][]int
	store         *OffsetStore
}

func NewGroup(name string, numPartitions int, store *OffsetStore) *Group {
	return &Group{
		name:          name,
		numPartitions: numPartitions,
		members:       make(map[string]struct{}),
		assignments:   make(map[string][]int),
		store:         store,
	}
}

func (g *Group) Join(memberID string) ([]int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, ok := g.members[memberID]; ok {
		return nil, fmt.Errorf("group %q: member %q already joined", g.name, memberID)
	}
	g.members[memberID] = struct{}{}
	g.rebalance()
	return append([]int(nil), g.assignments[memberID]...), nil
}

func (g *Group) Leave(memberID string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, ok := g.members[memberID]; !ok {
		return
	}
	delete(g.members, memberID)
	delete(g.assignments, memberID)
	if len(g.members) > 0 {
		g.rebalance()
	}
}

func (g *Group) Assignments(memberID string) ([]int, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	parts, ok := g.assignments[memberID]
	if !ok {
		return nil, false
	}
	return append([]int(nil), parts...), true
}

func (g *Group) Members() []string {
	g.mu.Lock()
	defer g.mu.Unlock()

	out := make([]string, 0, len(g.members))
	for id := range g.members {
		out = append(out, id)
	}
	return out
}

func (g *Group) rebalance() {
	g.assignments = stickyAssign(g.numPartitions, g.members, g.assignments)
}
