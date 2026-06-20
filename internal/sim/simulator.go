package sim

import (
	"fmt"
	"time"
)

type Simulator struct {
	Seed    int64
	Clock   *SimClock
	Network *Network
}

func New(seed int64, config FaultConfig) *Simulator {
	clock := NewSimClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	return &Simulator{
		Seed:    seed,
		Clock:   clock,
		Network: NewNetwork(seed, config, clock),
	}
}

func (s *Simulator) Step(name string, action func() error, checks ...func() error) error {
	if err := action(); err != nil && err != ErrDropped {
		return fmt.Errorf("sim step %q: %w", name, err)
	}
	var vs Violations
	for _, check := range checks {
		if err := check(); err != nil {
			vs = append(vs, fmt.Errorf("step %q: %w", name, err))
		}
	}
	return vs.AsError()
}

func (s *Simulator) Advance(d time.Duration) {
	s.Clock.Advance(d)
}

func (s *Simulator) Reproduce() string {
	return fmt.Sprintf("re-run with seed %d to reproduce", s.Seed)
}
