package client

import (
	"fmt"

	"github.com/sanjit-jeevanand/mini-kafka/internal/proto"
)

const defaultFetchMaxBytes = 256 * 1024 // 256 KB per fetch

// Message is a single record returned by Poll.
type Message struct {
	Topic     string
	Offset    uint64
	Timestamp int64
	Key       []byte
	Value     []byte
}

// Consumer fetches records from a topic partition starting at a given offset.
// It tracks the next offset internally so callers just call Poll repeatedly.
type Consumer struct {
	client     *Client
	topic      string
	partition  int32
	nextOffset uint64
	maxBytes   uint32
}

func NewConsumer(c *Client, topic string, partition int32, startOffset uint64) *Consumer {
	return &Consumer{
		client:     c,
		topic:      topic,
		partition:  partition,
		nextOffset: startOffset,
		maxBytes:   defaultFetchMaxBytes,
	}
}

// Poll fetches the next batch of messages from the broker.
// Returns an empty slice (not an error) when there are no new records yet.
func (p *Consumer) Poll() ([]Message, error) {
	req := proto.FetchRequest{
		Topic:     p.topic,
		Partition: p.partition,
		Offset:    p.nextOffset,
		MaxBytes:  p.maxBytes,
	}

	p.client.mu.Lock()
	_, payload, err := p.client.send(proto.TypeFetchRequest, proto.EncodeFetchRequest(req))
	p.client.mu.Unlock()

	if err != nil {
		return nil, fmt.Errorf("consumer: fetch: %w", err)
	}

	resp, err := proto.DecodeFetchResponse(payload)
	if err != nil {
		return nil, fmt.Errorf("consumer: decode: %w", err)
	}
	if resp.Err != "" {
		return nil, fmt.Errorf("consumer: broker error: %s", resp.Err)
	}

	if len(resp.Records) == 0 {
		return nil, nil
	}

	msgs := make([]Message, len(resp.Records))
	for i, r := range resp.Records {
		msgs[i] = Message{
			Topic:     p.topic,
			Offset:    r.Offset,
			Timestamp: r.Timestamp,
			Key:       r.Key,
			Value:     r.Value,
		}
	}

	p.nextOffset = resp.NextOffset
	return msgs, nil
}

// Seek moves the consumer to a specific offset.
func (p *Consumer) Seek(offset uint64) {
	p.nextOffset = offset
}

// NextOffset returns the offset Poll will ask for next.
func (p *Consumer) NextOffset() uint64 {
	return p.nextOffset
}
