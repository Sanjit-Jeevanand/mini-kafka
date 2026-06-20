package idempotent

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"

	"github.com/sanjit-jeevanand/mini-kafka/internal/client"
)

type Producer struct {
	mu      sync.Mutex
	id      string
	inner   *client.Producer
	nextSeq int64
}

func NewProducer(p *client.Producer) *Producer {
	return &Producer{
		id:    generateID(),
		inner: p,
	}
}

func (p *Producer) ID() string { return p.id }

func (p *Producer) Send(key, value []byte) (uint64, error) {
	p.mu.Lock()
	seq := p.nextSeq
	encodedKey := EncodeKey(p.id, seq, key)
	p.mu.Unlock()

	offset, err := p.inner.Send(encodedKey, value)
	if err != nil {
		return 0, err
	}

	p.mu.Lock()
	p.nextSeq++
	p.mu.Unlock()

	return offset, nil
}

// EncodeKey prepends producerID and seq to key.
// Layout: [4 id_len][id bytes][8 seq big-endian][original key]
func EncodeKey(producerID string, seq int64, key []byte) []byte {
	idBytes := []byte(producerID)
	buf := make([]byte, 4+len(idBytes)+8+len(key))
	pos := 0
	binary.BigEndian.PutUint32(buf[pos:], uint32(len(idBytes)))
	pos += 4
	copy(buf[pos:], idBytes)
	pos += len(idBytes)
	binary.BigEndian.PutUint64(buf[pos:], uint64(seq))
	pos += 8
	copy(buf[pos:], key)
	return buf
}

// DecodeKey extracts producerID, seq, and the original key from an encoded key.
func DecodeKey(encoded []byte) (producerID string, seq int64, key []byte, err error) {
	if len(encoded) < 4 {
		return "", 0, nil, fmt.Errorf("idempotent: encoded key too short")
	}
	idLen := int(binary.BigEndian.Uint32(encoded[0:4]))
	pos := 4
	if len(encoded) < pos+idLen+8 {
		return "", 0, nil, fmt.Errorf("idempotent: encoded key truncated")
	}
	producerID = string(encoded[pos : pos+idLen])
	pos += idLen
	seq = int64(binary.BigEndian.Uint64(encoded[pos:]))
	pos += 8
	key = encoded[pos:]
	return producerID, seq, key, nil
}

func generateID() string {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, rand.Uint64())
	return fmt.Sprintf("%x", b)
}
