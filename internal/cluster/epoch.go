package cluster

import (
	"context"
	"encoding/binary"
	"sync"

	ilog "github.com/sanjit-jeevanand/mini-kafka/internal/log"
)

type EpochStore struct {
	mu      sync.RWMutex
	log     *ilog.Log
	current uint64
}

func NewEpochStore(dir string) (*EpochStore, error) {
	l, err := ilog.New(ilog.Options{Dir: dir})
	if err != nil {
		return nil, err
	}
	e := &EpochStore{log: l}
	if err := e.replay(); err != nil {
		l.Close()
		return nil, err
	}
	return e, nil
}

func (e *EpochStore) Current() uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.current
}

func (e *EpochStore) Bump(ctx context.Context) (uint64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	next := e.current + 1
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], next)
	if _, err := e.log.Append(ctx, ilog.Record{Value: buf[:]}); err != nil {
		return 0, err
	}
	e.current = next
	return next, nil
}

func (e *EpochStore) Close() error {
	return e.log.Close()
}

func (e *EpochStore) replay() error {
	ctx := context.Background()
	for off := uint64(0); ; off++ {
		rec, err := e.log.Read(ctx, off)
		if err != nil {
			break
		}
		if len(rec.Value) == 8 {
			v := binary.BigEndian.Uint64(rec.Value)
			if v > e.current {
				e.current = v
			}
		}
	}
	return nil
}
