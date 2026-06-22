package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/sanjit-jeevanand/mini-kafka/internal/client"
)

const usage = `mini-kafka CLI

Usage:
  cli -broker <addr> produce <topic> <key> <value>
  cli -broker <addr> fetch   <topic> [start-offset]
  cli -broker <addr> meta    <topic>

Examples:
  cli produce orders k1 "hello world"
  cli fetch orders
  cli fetch orders 100
  cli meta orders
`

func main() {
	broker := flag.String("broker", "localhost:9092", "Broker address")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 {
		flag.Usage()
		os.Exit(1)
	}

	c, err := client.NewClient(*broker)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer func() { _ = c.Close() }()

	cmd, topic := args[0], args[1]

	switch cmd {
	case "produce":
		if len(args) < 4 {
			log.Fatal("produce requires <topic> <key> <value>")
		}
		p := client.NewProducer(c, topic, 1, 0)
		var offset uint64
		offset, err = p.Send([]byte(args[2]), []byte(args[3]))
		if err != nil {
			log.Fatalf("produce: %v", err)
		}
		fmt.Printf("written at offset %d\n", offset)

	case "fetch":
		var startOffset uint64
		if len(args) >= 3 {
			startOffset, err = strconv.ParseUint(args[2], 10, 64)
			if err != nil {
				log.Fatalf("invalid offset: %v", err)
			}
		}
		cons := client.NewConsumer(c, topic, 0, startOffset)
		msgs, err := cons.Poll()
		if err != nil {
			log.Fatalf("fetch: %v", err)
		}
		if len(msgs) == 0 {
			fmt.Println("no messages")
			return
		}
		for _, m := range msgs {
			fmt.Printf("offset=%-6d  ts=%s  key=%s  value=%s\n",
				m.Offset,
				time.UnixMicro(m.Timestamp).Format(time.RFC3339),
				m.Key,
				m.Value,
			)
		}

	case "meta":
		addr, err := c.BrokerFor(topic)
		if err != nil {
			log.Fatalf("meta: %v", err)
		}
		fmt.Printf("topic=%s  broker=%s\n", topic, addr)

	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
		flag.Usage()
		os.Exit(1)
	}
}
