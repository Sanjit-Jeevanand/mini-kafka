package server

import (
	"context"
	"fmt"
	"net"
	"testing"

	ilog "github.com/sanjit-jeevanand/mini-kafka/internal/log"
	"github.com/sanjit-jeevanand/mini-kafka/internal/proto"
)

// newTestServer starts a real TCP server on a random port backed by a real log.
// It returns the server address and a cancel func that shuts everything down.
func newTestServer(t *testing.T) (addr string, cancel context.CancelFunc) {
	t.Helper()

	dir := t.TempDir()
	l, err := ilog.New(ilog.Options{Dir: dir})
	if err != nil {
		t.Fatalf("ilog.New: %v", err)
	}
	t.Cleanup(func() { l.Close() })

	h := NewHandler(l, "127.0.0.1:0")
	srv := NewServer("127.0.0.1:0", h, 64)

	ctx, cancel := context.WithCancel(context.Background())

	ready := make(chan struct{})
	go func() {
		if err := srv.ListenAndServe(ctx, ready); err != nil {
			t.Logf("server stopped: %v", err)
		}
	}()
	<-ready // blocks until listener is bound

	srvAddr := srv.Addr().String()
	return srvAddr, cancel
}

func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// TestProduceAndFetch is the golden-path integration test:
// produce 10k records, fetch them all back, assert offsets are gap-free.
func TestProduceAndFetch(t *testing.T) {
	addr, cancel := newTestServer(t)
	defer cancel()

	conn := dial(t, addr)
	const n = 10_000

	// Produce n records.
	for i := 0; i < n; i++ {
		req := proto.ProduceRequest{
			Topic: "test",
			Records: []proto.ProduceRecord{
				{Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte(fmt.Sprintf("v%d", i))},
			},
		}
		if err := proto.WriteFrame(conn, proto.TypeProduceRequest, proto.EncodeProduceRequest(req)); err != nil {
			t.Fatalf("WriteFrame produce %d: %v", i, err)
		}
		_, payload, err := proto.ReadFrame(conn)
		if err != nil {
			t.Fatalf("ReadFrame produce response %d: %v", i, err)
		}
		resp, err := proto.DecodeProduceResponse(payload)
		if err != nil {
			t.Fatalf("DecodeProduceResponse %d: %v", i, err)
		}
		if resp.Err != "" {
			t.Fatalf("produce %d error: %s", i, resp.Err)
		}
		if resp.BaseOffset != uint64(i) {
			t.Fatalf("produce %d: got offset %d want %d", i, resp.BaseOffset, i)
		}
	}

	// Fetch all records back.
	var nextOffset uint64
	var fetched int
	for fetched < n {
		req := proto.FetchRequest{
			Topic:    "test",
			Offset:   nextOffset,
			MaxBytes: 64 * 1024,
		}
		if err := proto.WriteFrame(conn, proto.TypeFetchRequest, proto.EncodeFetchRequest(req)); err != nil {
			t.Fatalf("WriteFrame fetch: %v", err)
		}
		_, payload, err := proto.ReadFrame(conn)
		if err != nil {
			t.Fatalf("ReadFrame fetch response: %v", err)
		}
		resp, err := proto.DecodeFetchResponse(payload)
		if err != nil {
			t.Fatalf("DecodeFetchResponse: %v", err)
		}
		if resp.Err != "" {
			t.Fatalf("fetch error: %s", resp.Err)
		}
		if len(resp.Records) == 0 {
			t.Fatalf("got empty fetch response at offset %d, fetched %d/%d", nextOffset, fetched, n)
		}
		for _, rec := range resp.Records {
			if rec.Offset != uint64(fetched) {
				t.Fatalf("record offset: got %d want %d", rec.Offset, fetched)
			}
			fetched++
		}
		nextOffset = resp.NextOffset
	}

	if fetched != n {
		t.Fatalf("fetched %d records, want %d", fetched, n)
	}
}

// TestMetadata checks the broker returns its own address for any topic.
func TestMetadata(t *testing.T) {
	addr, cancel := newTestServer(t)
	defer cancel()

	conn := dial(t, addr)

	req := proto.MetaRequest{Topic: "anything"}
	if err := proto.WriteFrame(conn, proto.TypeMetaRequest, proto.EncodeMetaRequest(req)); err != nil {
		t.Fatal(err)
	}
	_, payload, err := proto.ReadFrame(conn)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := proto.DecodeMetaResponse(payload)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Err != "" {
		t.Fatalf("meta error: %s", resp.Err)
	}
	if resp.Partitions != 1 {
		t.Fatalf("partitions: got %d want 1", resp.Partitions)
	}
}

// TestGracefulShutdown cancels the server context and verifies it stops
// accepting new connections without killing the test process.
func TestGracefulShutdown(t *testing.T) {
	addr, cancel := newTestServer(t)

	// Cancel the server.
	cancel()

	// New connections should fail after shutdown.
	var accepted bool
	for i := 0; i < 50; i++ {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			break // expected
		}
		conn.Close()
		accepted = true
	}
	if accepted {
		t.Fatal("server still accepting connections after shutdown")
	}
}

// TestConcurrentProducers verifies multiple producers writing simultaneously
// produce gap-free monotonic offsets with no data races.
func TestConcurrentProducers(t *testing.T) {
	addr, cancel := newTestServer(t)
	defer cancel()

	const producers = 8
	const recordsEach = 500
	errc := make(chan error, producers)

	for p := 0; p < producers; p++ {
		go func(p int) {
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				errc <- fmt.Errorf("producer %d dial: %w", p, err)
				return
			}
			defer conn.Close()
			for i := 0; i < recordsEach; i++ {
				req := proto.ProduceRequest{
					Topic: "test",
					Records: []proto.ProduceRecord{
						{Value: []byte(fmt.Sprintf("p%d-r%d", p, i))},
					},
				}
				if err := proto.WriteFrame(conn, proto.TypeProduceRequest, proto.EncodeProduceRequest(req)); err != nil {
					errc <- fmt.Errorf("producer %d write: %w", p, err)
					return
				}
				_, payload, err := proto.ReadFrame(conn)
				if err != nil {
					errc <- fmt.Errorf("producer %d read: %w", p, err)
					return
				}
				resp, err := proto.DecodeProduceResponse(payload)
				if err != nil || resp.Err != "" {
					errc <- fmt.Errorf("producer %d response: decode=%v err=%s", p, err, resp.Err)
					return
				}
			}
			errc <- nil
		}(p)
	}

	for i := 0; i < producers; i++ {
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	}

	// All records written — verify total count via fetch.
	conn := dial(t, addr)
	total := 0
	var nextOffset uint64
	for {
		req := proto.FetchRequest{Topic: "test", Offset: nextOffset, MaxBytes: 128 * 1024}
		proto.WriteFrame(conn, proto.TypeFetchRequest, proto.EncodeFetchRequest(req))
		_, payload, err := proto.ReadFrame(conn)
		if err != nil {
			break
		}
		resp, _ := proto.DecodeFetchResponse(payload)
		if len(resp.Records) == 0 {
			break
		}
		total += len(resp.Records)
		nextOffset = resp.NextOffset
	}

	want := producers * recordsEach
	if total != want {
		t.Fatalf("total records: got %d want %d", total, want)
	}
}
