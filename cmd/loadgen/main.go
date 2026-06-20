package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	ilog "github.com/sanjit-jeevanand/mini-kafka/internal/log"
)

func main() {
	workers := flag.Int("workers", 50, "number of concurrent producer goroutines")
	dur := flag.Duration("duration", 3*time.Second, "how long to run each mode")
	flag.Parse()

	payload := []byte("order-placed:user-123:item-456:qty-1")

	fmt.Printf("loadgen: %d workers, %s per mode\n\n", *workers, *dur)

	base, err := runPerRecord(payload, *workers, *dur)
	if err != nil {
		fmt.Fprintf(os.Stderr, "per-record mode: %v\n", err)
		os.Exit(1)
	}

	gc, err := runGroupCommit(payload, *workers, *dur)
	if err != nil {
		fmt.Fprintf(os.Stderr, "group-commit mode: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("per-record fsync : %8.0f rec/s\n", base)
	fmt.Printf("group commit     : %8.0f rec/s\n", gc)
	if base > 0 {
		fmt.Printf("speedup          : %8.1fx\n", gc/base)
	}
}

func runPerRecord(payload []byte, workers int, dur time.Duration) (float64, error) {
	dir, err := os.MkdirTemp("", "loadgen-per-*")
	if err != nil {
		return 0, err
	}
	defer os.RemoveAll(dir)

	l, err := ilog.New(ilog.Options{Dir: dir})
	if err != nil {
		return 0, err
	}
	defer l.Close()

	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()

	var count atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				if _, err := l.Append(ctx, ilog.Record{Value: payload}); err != nil {
					return
				}
				count.Add(1)
			}
		}()
	}

	start := time.Now()
	wg.Wait()
	elapsed := time.Since(start)

	return float64(count.Load()) / elapsed.Seconds(), nil
}

func runGroupCommit(payload []byte, workers int, dur time.Duration) (float64, error) {
	dir, err := os.MkdirTemp("", "loadgen-gc-*")
	if err != nil {
		return 0, err
	}
	defer os.RemoveAll(dir)

	l, err := ilog.New(ilog.Options{Dir: dir})
	if err != nil {
		return 0, err
	}
	defer l.Close()

	gc := ilog.NewGroupCommitter(l, ilog.GroupCommitConfig{
		BufSize:      workers * 4,
		MaxBatchSize: 10_000,
		MaxBatchWait: time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()

	go gc.Run(ctx)

	var count atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				if _, err := gc.Append(ctx, ilog.Record{Value: payload}); err != nil {
					return
				}
				count.Add(1)
			}
		}()
	}

	start := time.Now()
	wg.Wait()
	elapsed := time.Since(start)

	return float64(count.Load()) / elapsed.Seconds(), nil
}
