package server

import (
	"context"
	"net"
	"sync"
)

const defaultMaxConns = 1024

type Server struct {
	addr     string
	handler  *Handler
	maxConns int

	sem chan struct{}
	wg  sync.WaitGroup

	mu       sync.Mutex
	listener net.Listener
}

func NewServer(addr string, h *Handler, maxConns int) *Server {
	if maxConns <= 0 {
		maxConns = defaultMaxConns
	}
	return &Server{
		addr:     addr,
		handler:  h,
		maxConns: maxConns,
		sem:      make(chan struct{}, maxConns),
	}
}

// ListenAndServe starts the TCP listener and blocks until ctx is cancelled.
// If ready is non-nil, it is closed once the listener is bound and accepting —
// callers can use this to know the exact address when addr was ":0".
func (s *Server) ListenAndServe(ctx context.Context, ready chan<- struct{}) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()

	if ready != nil {
		close(ready)
	}

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				s.wg.Wait()
				return nil
			default:
				return err
			}
		}

		s.sem <- struct{}{}
		s.wg.Add(1)
		go func(c net.Conn) {
			defer func() {
				_ = c.Close()
				<-s.sem
				s.wg.Done()
			}()
			s.handler.Handle(ctx, c)
		}(conn)
	}
}

// Addr returns the address the server is listening on.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}
