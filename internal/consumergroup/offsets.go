package consumergroup

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	ilog "github.com/sanjit-jeevanand/mini-kafka/internal/log"
)

type offsetKey struct {
	group     string
	topic     string
	partition int
}

type OffsetStore struct {
	mu      sync.RWMutex
	log     *ilog.Log
	offsets map[offsetKey]uint64
}

func NewOffsetStore(dir string) (*OffsetStore, error) {
	l, err := ilog.New(ilog.Options{Dir: dir})
	if err != nil {
		return nil, fmt.Errorf("offsets: open log: %w", err)
	}
	s := &OffsetStore{
		log:     l,
		offsets: make(map[offsetKey]uint64),
	}
	if err := s.replay(); err != nil {
		_ = l.Close()
		return nil, err
	}
	return s, nil
}

func (s *OffsetStore) Commit(group, topic string, partition int, offset uint64) error {
	key := encodeOffsetKey(group, topic, partition)
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, offset)

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.log.Append(context.Background(), ilog.Record{
		Key:   key,
		Value: val,
	}); err != nil {
		return fmt.Errorf("offsets: commit: %w", err)
	}
	s.offsets[offsetKey{group, topic, partition}] = offset
	return nil
}

func (s *OffsetStore) Committed(group, topic string, partition int) (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	off, ok := s.offsets[offsetKey{group, topic, partition}]
	return off, ok
}

func (s *OffsetStore) Close() error {
	return s.log.Close()
}

func (s *OffsetStore) replay() error {
	ctx := context.Background()
	var off uint64
	for {
		rec, err := s.log.Read(ctx, off)
		if err != nil {
			break // end of log
		}
		k, err := decodeOffsetKey(rec.Key)
		if err != nil {
			return fmt.Errorf("offsets: replay record %d: %w", off, err)
		}
		if len(rec.Value) < 8 {
			return fmt.Errorf("offsets: replay record %d: value too short", off)
		}
		s.offsets[k] = binary.BigEndian.Uint64(rec.Value)
		off++
	}
	return nil
}

// Key wire layout: [2 group_len][group][2 topic_len][topic][4 partition]

func encodeOffsetKey(group, topic string, partition int) []byte {
	buf := make([]byte, 2+len(group)+2+len(topic)+4)
	pos := 0
	binary.BigEndian.PutUint16(buf[pos:], uint16(len(group)))
	pos += 2
	copy(buf[pos:], group)
	pos += len(group)
	binary.BigEndian.PutUint16(buf[pos:], uint16(len(topic)))
	pos += 2
	copy(buf[pos:], topic)
	pos += len(topic)
	binary.BigEndian.PutUint32(buf[pos:], uint32(partition))
	return buf
}

func decodeOffsetKey(b []byte) (offsetKey, error) {
	var k offsetKey
	if len(b) < 2 {
		return k, fmt.Errorf("truncated group length")
	}
	gl := int(binary.BigEndian.Uint16(b[0:2]))
	pos := 2
	if len(b[pos:]) < gl+2 {
		return k, fmt.Errorf("truncated group")
	}
	k.group = string(b[pos : pos+gl])
	pos += gl
	tl := int(binary.BigEndian.Uint16(b[pos:]))
	pos += 2
	if len(b[pos:]) < tl+4 {
		return k, fmt.Errorf("truncated topic")
	}
	k.topic = string(b[pos : pos+tl])
	pos += tl
	k.partition = int(binary.BigEndian.Uint32(b[pos:]))
	return k, nil
}
