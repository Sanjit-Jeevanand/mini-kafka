package sim

import (
	"sync"
	"time"
)

type Clock interface {
	Now() time.Time
	Sleep(d time.Duration)
}

type RealClock struct{}

func (RealClock) Now() time.Time          { return time.Now() }
func (RealClock) Sleep(d time.Duration)   { time.Sleep(d) }

type SimClock struct {
	mu  sync.Mutex
	now time.Time
}

func NewSimClock(start time.Time) *SimClock {
	return &SimClock{now: start}
}

func (c *SimClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *SimClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func (c *SimClock) Sleep(d time.Duration) { c.Advance(d) }
