package testharness

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"time"
)

// Output captures concurrent command output and waits for complete readiness lines.
type Output struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (o *Output) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.buffer.Write(p)
}
func (o *Output) String() string { o.mu.Lock(); defer o.mu.Unlock(); return o.buffer.String() }
func (o *Output) WaitLine(ctx context.Context, prefix string) (string, error) {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		lines := strings.Split(o.String(), "\n")
		for _, line := range lines[:len(lines)-1] {
			if strings.HasPrefix(line, prefix) {
				return strings.TrimPrefix(line, prefix), nil
			}
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
		}
	}
}
