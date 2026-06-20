package consumergroup

import (
	"fmt"
	"sync"
	"time"
)

type Coordinator struct {
	mu             sync.Mutex
	numPartitions  int
	store          *OffsetStore
	groups         map[string]*Group
	timers         map[string]*time.Timer
	sessionTimeout time.Duration
}

func NewCoordinator(numPartitions int, store *OffsetStore, sessionTimeout time.Duration) *Coordinator {
	return &Coordinator{
		numPartitions:  numPartitions,
		store:          store,
		groups:         make(map[string]*Group),
		timers:         make(map[string]*time.Timer),
		sessionTimeout: sessionTimeout,
	}
}

func (c *Coordinator) Join(groupID, memberID string) ([]int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	g := c.groupLocked(groupID)
	parts, err := g.Join(memberID)
	if err != nil {
		return nil, err
	}
	c.resetTimerLocked(groupID, memberID)
	return parts, nil
}

func (c *Coordinator) Leave(groupID, memberID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.stopTimerLocked(groupID, memberID)
	if g, ok := c.groups[groupID]; ok {
		g.Leave(memberID)
	}
}

func (c *Coordinator) Heartbeat(groupID, memberID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	g, ok := c.groups[groupID]
	if !ok {
		return fmt.Errorf("coordinator: group %q not found", groupID)
	}
	if _, ok := g.assignments[memberID]; !ok {
		return fmt.Errorf("coordinator: member %q not in group %q", memberID, groupID)
	}
	c.resetTimerLocked(groupID, memberID)
	return nil
}

func (c *Coordinator) Assignments(groupID, memberID string) ([]int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	g, ok := c.groups[groupID]
	if !ok {
		return nil, false
	}
	return g.Assignments(memberID)
}

func (c *Coordinator) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key, t := range c.timers {
		t.Stop()
		delete(c.timers, key)
	}
}

func (c *Coordinator) groupLocked(groupID string) *Group {
	g, ok := c.groups[groupID]
	if !ok {
		g = NewGroup(groupID, c.numPartitions, c.store)
		c.groups[groupID] = g
	}
	return g
}

func (c *Coordinator) timerKey(groupID, memberID string) string {
	return groupID + "\x00" + memberID
}

func (c *Coordinator) resetTimerLocked(groupID, memberID string) {
	key := c.timerKey(groupID, memberID)
	if t, ok := c.timers[key]; ok {
		t.Stop()
	}
	c.timers[key] = time.AfterFunc(c.sessionTimeout, func() {
		c.Leave(groupID, memberID)
	})
}

func (c *Coordinator) stopTimerLocked(groupID, memberID string) {
	key := c.timerKey(groupID, memberID)
	if t, ok := c.timers[key]; ok {
		t.Stop()
		delete(c.timers, key)
	}
}
