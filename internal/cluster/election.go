package cluster

import (
	"context"
	"fmt"
	"time"
)

func NeedsElection(partition int, membership *Membership, timeout time.Duration) bool {
	leaderID, ok := membership.Leader(partition)
	if !ok {
		return true
	}
	return !membership.IsAlive(leaderID, timeout)
}

func Elect(
	ctx context.Context,
	partition int,
	candidates []int,
	membership *Membership,
	epochs *EpochStore,
	timeout time.Duration,
) (leaderID int, epoch uint64, err error) {
	for _, id := range candidates {
		if membership.IsAlive(id, timeout) {
			newEpoch, err := epochs.Bump(ctx)
			if err != nil {
				return 0, 0, fmt.Errorf("election: partition %d: bump epoch: %w", partition, err)
			}
			if err := membership.SetLeader(partition, id); err != nil {
				return 0, 0, fmt.Errorf("election: partition %d: set leader: %w", partition, err)
			}
			return id, newEpoch, nil
		}
	}
	return 0, 0, fmt.Errorf("election: partition %d: no alive candidates", partition)
}
