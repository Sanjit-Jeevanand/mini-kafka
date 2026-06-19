package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	ilog "github.com/sanjit-jeevanand/mini-kafka/internal/log"
	"github.com/sanjit-jeevanand/mini-kafka/internal/server"
)

func main() {
	addr := flag.String("addr", ":9092", "TCP address to listen on")
	dir := flag.String("dir", "/tmp/mini-kafka", "Directory to store log segments")
	maxConns := flag.Int("max-conns", 1024, "Maximum concurrent connections")
	flag.Parse()

	l, err := ilog.New(ilog.Options{Dir: *dir})
	if err != nil {
		log.Fatalf("open log: %v", err)
	}
	defer l.Close()

	h := server.NewHandler(l, *addr)
	srv := server.NewServer(*addr, h, *maxConns)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Printf("broker listening on %s (data dir: %s)", *addr, *dir)

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
