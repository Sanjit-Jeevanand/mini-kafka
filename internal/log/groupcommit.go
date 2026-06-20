package log

import (
	"context"
	"time"
)

type commitRequest struct {
	record Record
	result chan commitResult
}

type commitResult struct {
	offset uint64
	err    error
}

type GroupCommitConfig struct {
	BufSize      int
	MaxBatchSize int
	MaxBatchWait time.Duration
}

type GroupCommitter struct {
	log     *Log
	pending chan commitRequest
	cfg     GroupCommitConfig
}

func NewGroupCommitter(l *Log, cfg GroupCommitConfig) *GroupCommitter {
	if cfg.MaxBatchSize <= 0 {
		cfg.MaxBatchSize = 1<<31 - 1
	}
	if cfg.MaxBatchWait <= 0 {
		cfg.MaxBatchWait = time.Millisecond
	}
	return &GroupCommitter{
		log:     l,
		pending: make(chan commitRequest, cfg.BufSize),
		cfg:     cfg,
	}
}

func (gc *GroupCommitter) Append(ctx context.Context, r Record) (uint64, error) {
	result := make(chan commitResult, 1)
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case gc.pending <- commitRequest{record: r, result: result}:
	}
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case res := <-result:
		return res.offset, res.err
	}
}

func (gc *GroupCommitter) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case first := <-gc.pending:
			batch := []commitRequest{first}
			timer := time.NewTimer(gc.cfg.MaxBatchWait)
		drain:
			for len(batch) < gc.cfg.MaxBatchSize {
				select {
				case next := <-gc.pending:
					batch = append(batch, next)
				case <-timer.C:
					break drain
				}
			}
			timer.Stop()
			gc.flush(batch)
		}
	}
}

func (gc *GroupCommitter) flush(batch []commitRequest) {
	gc.log.mu.Lock()

	results := make([]commitResult, len(batch))
	for i, req := range batch {
		if gc.log.active.IsFull() {
			if err := gc.log.roll(); err != nil {
				for j := i; j < len(batch); j++ {
					results[j] = commitResult{err: err}
				}
				gc.log.mu.Unlock()
				gc.fanOut(batch, results)
				return
			}
		}
		offset, err := gc.log.active.appendNoSync(req.record)
		if err != nil {
			for j := i; j < len(batch); j++ {
				results[j] = commitResult{err: err}
			}
			gc.log.mu.Unlock()
			gc.fanOut(batch, results)
			return
		}
		results[i] = commitResult{offset: offset}
	}

	if err := gc.log.active.sync(); err != nil {
		for i := range results {
			results[i] = commitResult{err: err}
		}
	}

	gc.log.mu.Unlock()
	gc.fanOut(batch, results)
}

func (gc *GroupCommitter) fanOut(batch []commitRequest, results []commitResult) {
	for i, req := range batch {
		req.result <- results[i]
	}
}
