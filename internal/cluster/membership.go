package cluster

import (
	"fmt"
	"sync"
	"time"
)

type BrokerInfo struct {
	ID   int
	Addr string
}

type Membership struct {
	mu       sync.RWMutex
	brokers  map[int]BrokerInfo
	lastSeen map[int]time.Time
	leaders  map[int]int
}

func NewMembership(brokers []BrokerInfo) *Membership {
	m := &Membership{
		brokers:  make(map[int]BrokerInfo, len(brokers)),
		lastSeen: make(map[int]time.Time, len(brokers)),
		leaders:  make(map[int]int),
	}
	for _, b := range brokers {
		m.brokers[b.ID] = b
	}
	return m
}

func (m *Membership) Heartbeat(brokerID int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.brokers[brokerID]; !ok {
		return fmt.Errorf("membership: unknown broker %d", brokerID)
	}
	m.lastSeen[brokerID] = time.Now()
	return nil
}

func (m *Membership) IsAlive(brokerID int, timeout time.Duration) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.lastSeen[brokerID]
	if !ok {
		return false
	}
	return time.Since(t) < timeout
}

func (m *Membership) SetLeader(partition, brokerID int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.brokers[brokerID]; !ok {
		return fmt.Errorf("membership: unknown broker %d", brokerID)
	}
	m.leaders[partition] = brokerID
	return nil
}

func (m *Membership) Leader(partition int) (int, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	id, ok := m.leaders[partition]
	return id, ok
}

func (m *Membership) Brokers() []BrokerInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]BrokerInfo, 0, len(m.brokers))
	for _, b := range m.brokers {
		out = append(out, b)
	}
	return out
}
