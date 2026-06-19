package client

import (
	"fmt"
	"net"
	"sync"

	"github.com/sanjit-jeevanand/mini-kafka/internal/proto"
)

type Client struct {
	mu       sync.Mutex
	conn     net.Conn
	addr     string
	metaCache map[string]string
}

func NewClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("client: dial %s: %w", addr, err)
	}
	return &Client{
		conn:      conn,
		addr:      addr,
		metaCache: make(map[string]string),
	}, nil
}

func (c *Client) BrokerFor(topic string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if addr, ok := c.metaCache[topic]; ok {
		return addr, nil
	}

	req := proto.MetaRequest{Topic: topic}
	if err := proto.WriteFrame(c.conn, proto.TypeMetaRequest, proto.EncodeMetaRequest(req)); err != nil {
		return "", fmt.Errorf("client: meta request: %w", err)
	}
	_, payload, err := proto.ReadFrame(c.conn)
	if err != nil {
		return "", fmt.Errorf("client: meta response: %w", err)
	}
	resp, err := proto.DecodeMetaResponse(payload)
	if err != nil {
		return "", fmt.Errorf("client: decode meta: %w", err)
	}
	if resp.Err != "" {
		return "", fmt.Errorf("client: broker meta error: %s", resp.Err)
	}

	c.metaCache[topic] = resp.Addr
	return resp.Addr, nil
}

func (c *Client) send(msgType uint16, payload []byte) (uint16, []byte, error) {
	if err := proto.WriteFrame(c.conn, msgType, payload); err != nil {
		return 0, nil, err
	}
	return proto.ReadFrame(c.conn)
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.Close()
}
