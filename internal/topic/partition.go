package topic

import (
	"context"
	"fmt"
	"path/filepath"

	ilog "github.com/sanjit-jeevanand/mini-kafka/internal/log"
)

type Partition struct {
	id  int
	log *ilog.Log
}

func openPartition(dir string, id int, maxBytes uint32) (*Partition, error) {
	partDir := filepath.Join(dir, fmt.Sprintf("partition-%d", id))
	l, err := ilog.New(ilog.Options{Dir: partDir, MaxBytes: maxBytes})
	if err != nil {
		return nil, fmt.Errorf("topic: open partition %d: %w", id, err)
	}
	return &Partition{id: id, log: l}, nil
}

func (p *Partition) Append(ctx context.Context, r ilog.Record) (uint64, error) {
	return p.log.Append(ctx, r)
}

func (p *Partition) Read(ctx context.Context, offset uint64) (ilog.Record, error) {
	return p.log.Read(ctx, offset)
}

func (p *Partition) HighestOffset() uint64 {
	return p.log.HighestOffset()
}

func (p *Partition) Close() error {
	return p.log.Close()
}
