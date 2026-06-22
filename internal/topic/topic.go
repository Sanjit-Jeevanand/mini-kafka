package topic

import (
	"context"
	"fmt"

	ilog "github.com/sanjit-jeevanand/mini-kafka/internal/log"
)

const defaultMaxBytes = 64 * 1024 * 1024 // 64 MiB per segment

// Topic owns N partitions, each backed by an independent ilog.Log.
// Append routes records to a partition via the Partitioner.
// Read fetches a record from a specific partition by offset.
type Topic struct {
	name       string
	partitions []*Partition
	partioner  *Partitioner
}

// Options configures a Topic on open.
type Options struct {
	Dir           string // base directory — topic creates a subdirectory inside
	NumPartitions int
	MaxBytes      uint32 // max segment size per partition (0 → 64 MiB)
}

// Open opens (or creates) a topic with its partitions on disk.
// Crash recovery runs automatically for each partition.
func Open(name string, opts Options) (*Topic, error) {
	if opts.NumPartitions <= 0 {
		opts.NumPartitions = 1
	}
	if opts.MaxBytes == 0 {
		opts.MaxBytes = defaultMaxBytes
	}

	topicDir := fmt.Sprintf("%s/%s", opts.Dir, name)

	t := &Topic{
		name:      name,
		partitions:  make([]*Partition, opts.NumPartitions),
		partioner: NewPartitioner(opts.NumPartitions),
	}

	for i := 0; i < opts.NumPartitions; i++ {
		p, err := openPartition(topicDir, i, opts.MaxBytes)
		if err != nil {
			_ = t.Close()
			return nil, err
		}
		t.partitions[i] = p
	}
	return t, nil
}

// Append routes the record to the partition determined by key,
// appends it, and returns (partitionID, offset).
func (t *Topic) Append(ctx context.Context, key []byte, r ilog.Record) (partition int, offset uint64, err error) {
	partition = t.partioner.Partition(key)
	offset, err = t.partitions[partition].Append(ctx, r)
	return partition, offset, err
}

// Read fetches a record from a specific partition at the given offset.
func (t *Topic) Read(ctx context.Context, partition int, offset uint64) (ilog.Record, error) {
	if partition < 0 || partition >= len(t.partitions) {
		return ilog.Record{}, fmt.Errorf("topic %s: partition %d out of range [0,%d)", t.name, partition, len(t.partitions))
	}
	return t.partitions[partition].Read(ctx, offset)
}

// NumPartitions returns the number of partitions this topic has.
func (t *Topic) NumPartitions() int {
	return len(t.partitions)
}

// HighestOffset returns the highest offset written to a specific partition.
func (t *Topic) HighestOffset(partition int) uint64 {
	return t.partitions[partition].HighestOffset()
}

// Close closes all partitions.
func (t *Topic) Close() error {
	var firstErr error
	for _, p := range t.partitions {
		if p == nil {
			continue
		}
		if err := p.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
