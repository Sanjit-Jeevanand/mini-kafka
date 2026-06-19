package topic

import (
	"hash/fnv"
	"sync/atomic"
)

// Partitioner maps a record key to a partition index in [0, numPartitions).
//
// Keyed records: fnv1a(key) % numPartitions — same key always lands on the
// same partition, preserving per-key ordering.
//
// Null-key records: sticky round-robin — all null-key records in the same
// batch go to the same partition. The partition rotates between batches.
// This avoids scattering a batch across N partitions, which would defeat
// the throughput benefit of batching.
type Partitioner struct {
	n       int           // numPartitions
	counter atomic.Uint64 // for sticky null-key rotation
}

func NewPartitioner(numPartitions int) *Partitioner {
	return &Partitioner{n: numPartitions}
}

// Partition returns the partition index for the given key.
// Pass nil key for null-key (sticky round-robin) behaviour.
func (p *Partitioner) Partition(key []byte) int {
	if len(key) == 0 {
		return int(p.counter.Add(1)-1) % p.n
	}
	h := fnv.New32a()
	h.Write(key)
	return int(h.Sum32()) % p.n
}
