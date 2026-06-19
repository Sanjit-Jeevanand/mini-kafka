package client

import (
	"fmt"
	"sync"
	"time"

	"github.com/sanjit-jeevanand/mini-kafka/internal/proto"
)

const (
	defaultBatchSize  = 100
	defaultLingerTime = 5 * time.Millisecond
)

// Producer batches records and sends them to the broker in one RPC.
// Two flush triggers: batch reaches MaxBatch records, or LingerTime elapses.
//
// Send() is non-blocking — it adds the record to the batch and returns
// immediately. Flush() forces an immediate send of whatever is buffered.
type Producer struct {
	client    *Client
	topic     string
	maxBatch  int
	linger    time.Duration

	mu      sync.Mutex
	batch   []proto.ProduceRecord
	timer   *time.Timer
	pending []pendingRecord // Send() callers waiting for their offset
}

type pendingRecord struct {
	indexInBatch int
	result       chan sendResult
}

type sendResult struct {
	offset uint64
	err    error
}

func NewProducer(c *Client, topic string, maxBatch int, linger time.Duration) *Producer {
	if maxBatch <= 0 {
		maxBatch = defaultBatchSize
	}
	if linger <= 0 {
		linger = defaultLingerTime
	}
	return &Producer{
		client:   c,
		topic:    topic,
		maxBatch: maxBatch,
		linger:   linger,
	}
}

// Send adds a record to the batch and returns the assigned offset once the
// batch is flushed to the broker. It blocks until the broker acknowledges.
func (p *Producer) Send(key, value []byte) (uint64, error) {
	p.mu.Lock()

	idx := len(p.batch)
	p.batch = append(p.batch, proto.ProduceRecord{Key: key, Value: value})

	result := make(chan sendResult, 1)
	p.pending = append(p.pending, pendingRecord{indexInBatch: idx, result: result})

	// Start the linger timer on the first record in a new batch.
	if len(p.batch) == 1 {
		p.timer = time.AfterFunc(p.linger, func() {
			p.mu.Lock()
			p.flushLocked()
			p.mu.Unlock()
		})
	}

	// If we've hit the batch size cap, flush immediately.
	shouldFlush := len(p.batch) >= p.maxBatch
	if shouldFlush {
		if p.timer != nil {
			p.timer.Stop()
		}
		p.flushLocked()
	}

	p.mu.Unlock()

	res := <-result
	return res.offset, res.err
}

// Flush forces an immediate send of any buffered records.
func (p *Producer) Flush() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.batch) == 0 {
		return nil
	}
	if p.timer != nil {
		p.timer.Stop()
	}
	p.flushLocked()
	return nil
}

// flushLocked sends the current batch and notifies all pending Send() callers.
// Must be called with p.mu held.
func (p *Producer) flushLocked() {
	if len(p.batch) == 0 {
		return
	}

	batch := p.batch
	pending := p.pending
	p.batch = nil
	p.pending = nil

	p.client.mu.Lock()
	_, payload, err := p.client.send(
		proto.TypeProduceRequest,
		proto.EncodeProduceRequest(proto.ProduceRequest{
			Topic:   p.topic,
			Records: batch,
		}),
	)
	p.client.mu.Unlock()

	if err != nil {
		for _, pr := range pending {
			pr.result <- sendResult{err: fmt.Errorf("producer: flush: %w", err)}
		}
		return
	}

	resp, err := proto.DecodeProduceResponse(payload)
	if err != nil {
		for _, pr := range pending {
			pr.result <- sendResult{err: fmt.Errorf("producer: decode response: %w", err)}
		}
		return
	}
	if resp.Err != "" {
		for _, pr := range pending {
			pr.result <- sendResult{err: fmt.Errorf("producer: broker error: %s", resp.Err)}
		}
		return
	}

	// Each record in the batch got baseOffset + its index.
	for _, pr := range pending {
		pr.result <- sendResult{offset: resp.BaseOffset + uint64(pr.indexInBatch)}
	}
}
