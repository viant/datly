package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"
)

func (s *Server) Serve(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("MCP server is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if s.config.Transport.Kind == TransportStdio {
		return s.serveStdio(ctx)
	}
	return s.serveHTTP(ctx)
}

func (s *Server) serveStdio(ctx context.Context) error {
	serveCtx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	if s.stdioCancel != nil {
		s.mu.Unlock()
		cancel()
		return fmt.Errorf("MCP stdio server is already running")
	}
	s.stdioCancel = cancel
	s.mu.Unlock()
	defer func() {
		cancel()
		s.mu.Lock()
		s.stdioCancel = nil
		s.mu.Unlock()
	}()
	transport, err := s.Stdio(serveCtx)
	if err != nil {
		return err
	}
	err = transport.ListenAndServe()
	if errors.Is(err, context.Canceled) && serveCtx.Err() != nil {
		return nil
	}
	return err
}

func (s *Server) serveHTTP(ctx context.Context) error {
	transport, err := s.HTTP()
	if err != nil {
		return err
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = transport.Shutdown(shutdownCtx)
			cancel()
		case <-done:
		}
	}()
	err = transport.ListenAndServe()
	close(done)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	cancel := s.stdioCancel
	httpServer := s.httpServer
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if httpServer != nil {
		return httpServer.Shutdown(ctx)
	}
	return nil
}
