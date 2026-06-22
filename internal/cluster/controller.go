package cluster

import (
	"context"
	"fmt"
	"log"
	"sort"
	"time"
)

type Controller struct {
	numPartitions    int
	membership       *Membership
	epochs           *EpochStore
	heartbeatTimeout time.Duration
	pollInterval     time.Duration
}

func NewController(
	numPartitions int,
	membership *Membership,
	epochs *EpochStore,
	heartbeatTimeout time.Duration,
	pollInterval time.Duration,
) *Controller {
	return &Controller{
		numPartitions:    numPartitions,
		membership:       membership,
		epochs:           epochs,
		heartbeatTimeout: heartbeatTimeout,
		pollInterval:     pollInterval,
	}
}

func (c *Controller) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			c.checkPartitions(ctx)
		}
	}
}

func (c *Controller) Heartbeat(brokerID int) error {
	return c.membership.Heartbeat(brokerID)
}

func (c *Controller) LeaderFor(partition int) (int, bool) {
	return c.membership.Leader(partition)
}

func (c *Controller) Fence(senderEpoch uint64) error {
	current := c.epochs.Current()
	if senderEpoch < current {
		return fmt.Errorf("cluster: stale epoch %d, current epoch is %d", senderEpoch, current)
	}
	return nil
}

func (c *Controller) CurrentEpoch() uint64 {
	return c.epochs.Current()
}

func (c *Controller) checkPartitions(ctx context.Context) {
	candidates := c.sortedBrokerIDs()
	for p := 0; p < c.numPartitions; p++ {
		if NeedsElection(p, c.membership, c.heartbeatTimeout) {
			if _, _, err := Elect(ctx, p, candidates, c.membership, c.epochs, c.heartbeatTimeout); err != nil {
				log.Printf("cluster: election failed for partition %d: %v", p, err)
			}
		}
	}
}

func (c *Controller) sortedBrokerIDs() []int {
	brokers := c.membership.Brokers()
	ids := make([]int, len(brokers))
	for i, b := range brokers {
		ids[i] = b.ID
	}
	sort.Ints(ids)
	return ids
}
