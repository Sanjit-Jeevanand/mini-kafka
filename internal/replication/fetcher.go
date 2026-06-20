package replication

import (
	"context"
	"fmt"
	"time"

	ilog "github.com/sanjit-jeevanand/mini-kafka/internal/log"
)

type Fetcher struct {
	replica      *Replica
	leaderLog    *ilog.Log
	followerLog  *ilog.Log
	isr          *ISR
	hw           *HighWatermark
	lagThreshold uint64
	backoff      time.Duration
}

func NewFetcher(
	replica *Replica,
	leaderLog *ilog.Log,
	followerLog *ilog.Log,
	isr *ISR,
	hw *HighWatermark,
	lagThreshold uint64,
	backoff time.Duration,
) *Fetcher {
	return &Fetcher{
		replica:      replica,
		leaderLog:    leaderLog,
		followerLog:  followerLog,
		isr:          isr,
		hw:           hw,
		lagThreshold: lagThreshold,
		backoff:      backoff,
	}
}

func (f *Fetcher) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fetchOffset := f.replica.FetchOffset()

		leaderHO := int64(f.leaderLog.HighestOffset())
		lag := leaderHO - int64(fetchOffset)
		if lag > int64(f.lagThreshold) {
			f.isr.Remove(f.replica.id)
		}

		rec, err := f.leaderLog.Read(ctx, fetchOffset)
		if err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(f.backoff):
			}
			continue
		}

		if _, err := f.followerLog.Append(ctx, rec); err != nil {
			return fmt.Errorf("fetcher %d: append: %w", f.replica.id, err)
		}

		f.replica.SetFetchOffset(fetchOffset + 1)
		f.isr.Notify()
		f.hw.Advance(f.isr)
	}
}
