package sim

import (
	"errors"
	"math/rand"
	"sync"
	"time"
)

var ErrDropped = errors.New("sim: message dropped by fault injector")

type FaultConfig struct {
	DropRate  float64
	DelayRate float64
	MaxDelay  time.Duration
}

type Network struct {
	mu     sync.Mutex
	rng    *rand.Rand
	config FaultConfig
	clock  Clock
}

func NewNetwork(seed int64, config FaultConfig, clock Clock) *Network {
	return &Network{
		rng:    rand.New(rand.NewSource(seed)),
		config: config,
		clock:  clock,
	}
}

func (n *Network) Deliver(fn func() error) error {
	n.mu.Lock()
	drop := n.rng.Float64() < n.config.DropRate
	delay := n.rng.Float64() < n.config.DelayRate
	var delayDur time.Duration
	if delay && n.config.MaxDelay > 0 {
		delayDur = time.Duration(n.rng.Int63n(int64(n.config.MaxDelay) + 1))
	}
	n.mu.Unlock()

	if drop {
		return ErrDropped
	}
	if delay && delayDur > 0 {
		n.clock.Sleep(delayDur)
	}
	return fn()
}

func (n *Network) DropRate() float64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.config.DropRate
}

func (n *Network) SetDropRate(r float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.config.DropRate = r
}
