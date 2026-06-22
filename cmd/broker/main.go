package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/sanjit-jeevanand/mini-kafka/internal/server"
	"github.com/sanjit-jeevanand/mini-kafka/internal/topic"
)

func main() {
	addr := flag.String("addr", ":9092", "TCP address to listen on")
	dir := flag.String("dir", "/tmp/mini-kafka", "Directory to store log segments")
	topicName := flag.String("topic", "default", "Topic name")
	numPartitions := flag.Int("partitions", 4, "Number of partitions")
	maxConns := flag.Int("max-conns", 1024, "Maximum concurrent connections")
	flag.Parse()

	t, err := topic.Open(*topicName, topic.Options{
		Dir:           *dir,
		NumPartitions: *numPartitions,
	})
	if err != nil {
		log.Fatalf("open topic: %v", err)
	}
	defer func() { _ = t.Close() }()

	h := server.NewHandler(t, *addr)
	srv := server.NewServer(*addr, h, *maxConns)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Printf("broker listening on %s (topic: %s, partitions: %d, dir: %s)",
		*addr, *topicName, *numPartitions, *dir)

	ready := make(chan struct{})
	go func() {
		<-ready
		log.Printf("broker ready at %s", srv.Addr())
	}()

	if err := srv.ListenAndServe(ctx, ready); err != nil {
		log.Fatalf("server: %v", err)
	}

	log.Println("broker shut down cleanly")
}
