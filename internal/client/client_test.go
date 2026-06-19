package client

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sanjit-jeevanand/mini-kafka/internal/server"
	"github.com/sanjit-jeevanand/mini-kafka/internal/topic"
)

// newTestBroker spins up a real broker on a random port backed by a real topic.
// Returns the broker address and a cancel func that shuts everything down.
func newTestBroker(t *testing.T) (addr string, cancel context.CancelFunc) {
	t.Helper()

	tp, err := topic.Open("test", topic.Options{Dir: t.TempDir(), NumPartitions: 1})
	if err != nil {
		t.Fatalf("topic.Open: %v", err)
	}
	t.Cleanup(func() { tp.Close() })

	h := server.NewHandler(tp, "127.0.0.1:0")
	srv := server.NewServer("127.0.0.1:0", h, 64)

	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	go func() {
		if err := srv.ListenAndServe(ctx, ready); err != nil {
			t.Logf("broker stopped: %v", err)
		}
	}()
	<-ready

	return srv.Addr().String(), cancel
}

// TestProducerConsumerRoundTrip is the golden-path test:
// produce N records, consume them all back in order.
func TestProducerConsumerRoundTrip(t *testing.T) {
	addr, cancel := newTestBroker(t)
	defer cancel()

	c, err := NewClient(addr)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	const n = 200
	topic := "events"

	p := NewProducer(c, topic, 50, 10*time.Millisecond)

	// Produce n records concurrently to exercise batching.
	var wg sync.WaitGroup
	offsets := make([]uint64, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			off, err := p.Send(
				[]byte(fmt.Sprintf("key-%d", i)),
				[]byte(fmt.Sprintf("val-%d", i)),
			)
			if err != nil {
				t.Errorf("Send %d: %v", i, err)
				return
			}
			offsets[i] = off
		}(i)
	}
	wg.Wait()

	// All offsets must be in [0, n).
	seen := make(map[uint64]bool, n)
	for i, off := range offsets {
		if off >= n {
			t.Errorf("record %d: offset %d out of range [0,%d)", i, off, n)
		}
		if seen[off] {
			t.Errorf("duplicate offset %d", off)
		}
		seen[off] = true
	}

	// Consume all records back.
	cons := NewConsumer(c, topic, 0, 0)
	var msgs []Message
	for len(msgs) < n {
		batch, err := cons.Poll()
		if err != nil {
			t.Fatalf("Poll: %v", err)
		}
		msgs = append(msgs, batch...)
	}

	if len(msgs) != n {
		t.Fatalf("got %d messages, want %d", len(msgs), n)
	}
	for i, m := range msgs {
		if m.Offset != uint64(i) {
			t.Errorf("msg %d: offset %d want %d", i, m.Offset, i)
		}
	}
}

// TestConsumerSeek verifies that Seek repositions the consumer correctly.
func TestConsumerSeek(t *testing.T) {
	addr, cancel := newTestBroker(t)
	defer cancel()

	c, err := NewClient(addr)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	const n = 20
	p := NewProducer(c, "seek-topic", 1, 0)
	for i := 0; i < n; i++ {
		if _, sendErr := p.Send(nil, []byte(fmt.Sprintf("v%d", i))); sendErr != nil {
			t.Fatalf("Send %d: %v", i, sendErr)
		}
	}

	cons := NewConsumer(c, "seek-topic", 0, 0)

	// Drain 10 messages.
	var got int
	for got < 10 {
		var msgs []Message
		msgs, err = cons.Poll()
		if err != nil {
			t.Fatalf("Poll: %v", err)
		}
		got += len(msgs)
	}

	// Seek back to 5 and re-read from there.
	cons.Seek(5)
	if cons.NextOffset() != 5 {
		t.Fatalf("NextOffset: got %d want 5", cons.NextOffset())
	}

	msgs, err := cons.Poll()
	if err != nil {
		t.Fatalf("Poll after seek: %v", err)
	}
	if len(msgs) == 0 {
		t.Fatal("expected messages after seek, got none")
	}
	if msgs[0].Offset != 5 {
		t.Errorf("first msg after seek: offset %d want 5", msgs[0].Offset)
	}
}

// TestMetadataCache verifies BrokerFor caches and returns the broker address.
func TestMetadataCache(t *testing.T) {
	addr, cancel := newTestBroker(t)
	defer cancel()

	c, err := NewClient(addr)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	got, err := c.BrokerFor("any-topic")
	if err != nil {
		t.Fatalf("BrokerFor: %v", err)
	}
	if got == "" {
		t.Fatal("BrokerFor returned empty address")
	}

	// Second call must hit the cache (no new network round-trip needed).
	got2, err := c.BrokerFor("any-topic")
	if err != nil {
		t.Fatalf("BrokerFor (cached): %v", err)
	}
	if got != got2 {
		t.Errorf("cached addr differs: %q vs %q", got, got2)
	}
}

// TestProducerFlush verifies that Flush() sends buffered records immediately.
func TestProducerFlush(t *testing.T) {
	addr, cancel := newTestBroker(t)
	defer cancel()

	c, err := NewClient(addr)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	// Large linger so the timer won't fire during the test.
	p := NewProducer(c, "flush-topic", 1000, 10*time.Second)

	// Send 3 records but don't wait for linger — call Flush() explicitly.
	type result struct {
		off uint64
		err error
	}
	results := make(chan result, 3)
	for i := 0; i < 3; i++ {
		go func(i int) {
			off, err := p.Send(nil, []byte(fmt.Sprintf("flush-%d", i)))
			results <- result{off, err}
		}(i)
	}

	// Give the goroutines time to enqueue before we flush.
	time.Sleep(5 * time.Millisecond)
	if err := p.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	for i := 0; i < 3; i++ {
		r := <-results
		if r.err != nil {
			t.Errorf("Send %d: %v", i, r.err)
		}
	}
}
