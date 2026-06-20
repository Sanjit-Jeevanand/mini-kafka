package replication

import "sync"

type Replica struct {
	mu          sync.Mutex
	id          int
	fetchOffset uint64
}

func NewReplica(id int) *Replica {
	return &Replica{id: id}
}

func (r *Replica) ID() int {
	return r.id
}

func (r *Replica) FetchOffset() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.fetchOffset
}

func (r *Replica) SetFetchOffset(offset uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fetchOffset = offset
}
