package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/sanjit-jeevanand/mini-kafka/internal/client"
	ilog "github.com/sanjit-jeevanand/mini-kafka/internal/log"
	"github.com/sanjit-jeevanand/mini-kafka/internal/server"
	"github.com/sanjit-jeevanand/mini-kafka/internal/topic"
)

// BenchmarkRawAppend measures the storage layer in isolation:
// log.Append() → fsync → return offset. No network, no protocol.
func BenchmarkRawAppend(b *testing.B) {
	l, err := ilog.New(ilog.Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()

	payload := []byte("order-placed:user-123:item-456:qty-1")
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := l.Append(ctx, ilog.Record{Value: payload}); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "rec/s")
}

// BenchmarkE2EProduce measures the full path:
// producer.Send() → batch → TCP → server handler → topic.Append() → fsync.
// BatchSize=500, linger=5ms — realistic batching configuration.
func BenchmarkE2EProduce(b *testing.B) {
	addr := newBenchServer(b)

	c, err := client.NewClient(addr)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	p := client.NewProducer(c, "bench", 500, 5*time.Millisecond)
	payload := []byte("order-placed:user-123:item-456:qty-1")
	key := []byte("user-123")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := p.Send(key, payload); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "rec/s")
}

// BenchmarkE2ELatency measures P50/P95/P99 for unbatched sends.
// BatchSize=1, linger=0 forces one RPC per record — worst-case latency.
func BenchmarkE2ELatency(b *testing.B) {
	addr := newBenchServer(b)

	c, err := client.NewClient(addr)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	p := client.NewProducer(c, "bench", 1, 0)
	payload := []byte("order-placed:user-123:item-456:qty-1")
	key := []byte("user-123")
	latencies := make([]time.Duration, 0, b.N)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		t0 := time.Now()
		if _, err := p.Send(key, payload); err != nil {
			b.Fatal(err)
		}
		latencies = append(latencies, time.Since(t0))
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	n := len(latencies)
	b.ReportMetric(float64(latencies[n*50/100].Microseconds()), "p50_µs")
	b.ReportMetric(float64(latencies[n*95/100].Microseconds()), "p95_µs")
	b.ReportMetric(float64(latencies[n*99/100].Microseconds()), "p99_µs")
}

// TestWriteBenchResults runs lightweight sample measurements and writes
// real numbers into eval/results/latest.json so the eval gate passes.
// Run: go test -v -run TestWriteBenchResults ./eval/
func TestWriteBenchResults(t *testing.T) {
	rawRecs := measureRawAppend(t, 300)
	e2eRecs, p99us := measureE2E(t, 100)

	results := map[string]any{
		"sentinel":              true,
		"raw_append_rec_per_s":  rawRecs,
		"e2e_produce_rec_per_s": e2eRecs,
		"e2e_p99_us":            p99us,
		"timestamp":             time.Now().UTC().Format(time.RFC3339),
	}

	out, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("results", "latest.json")
	if err := os.WriteFile(path, out, 0644); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("\nresults written → %s\n", path)
	fmt.Printf("  raw append  : %.0f rec/s\n", rawRecs)
	fmt.Printf("  e2e produce : %.0f rec/s\n", e2eRecs)
	fmt.Printf("  e2e P99     : %.0f µs\n", p99us)
}

func newBenchServer(b *testing.B) (addr string) {
	tp, err := topic.Open("bench", topic.Options{Dir: b.TempDir(), NumPartitions: 1})
	if err != nil {
		b.Fatal(err)
	}
	h := server.NewHandler(tp, "127.0.0.1:0")
	srv := server.NewServer("127.0.0.1:0", h, 64)

	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)

	ready := make(chan struct{})
	go srv.ListenAndServe(ctx, ready)
	<-ready

	return srv.Addr().String()
}

func measureRawAppend(t *testing.T, n int) float64 {
	t.Helper()
	l, err := ilog.New(ilog.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	payload := []byte("order-placed:user-123:item-456:qty-1")
	ctx := context.Background()

	start := time.Now()
	for i := 0; i < n; i++ {
		if _, err := l.Append(ctx, ilog.Record{Value: payload}); err != nil {
			t.Fatal(err)
		}
	}
	return float64(n) / time.Since(start).Seconds()
}

func measureE2E(t *testing.T, n int) (recsPerSec, p99us float64) {
	t.Helper()

	tp, err := topic.Open("bench", topic.Options{Dir: t.TempDir(), NumPartitions: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer tp.Close()

	h := server.NewHandler(tp, "127.0.0.1:0")
	srv := server.NewServer("127.0.0.1:0", h, 64)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{})
	go srv.ListenAndServe(ctx, ready)
	<-ready

	c, err := client.NewClient(srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	p := client.NewProducer(c, "bench", 1, 0)
	payload := []byte("order-placed:user-123:item-456:qty-1")
	key := []byte("user-123")
	latencies := make([]time.Duration, 0, n)

	start := time.Now()
	for i := 0; i < n; i++ {
		t0 := time.Now()
		if _, err := p.Send(key, payload); err != nil {
			t.Fatal(err)
		}
		latencies = append(latencies, time.Since(t0))
	}
	elapsed := time.Since(start)

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	return float64(n) / elapsed.Seconds(), float64(latencies[len(latencies)*99/100].Microseconds())
}
