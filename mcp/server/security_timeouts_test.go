package server

import (
	"testing"
	"time"
)

func TestSecurityHTTPTimeoutPolicy(t *testing.T) {
	for _, kind := range []TransportKind{TransportSSE, TransportStreamable} {
		for _, d := range []time.Duration{0, 30 * time.Minute, -time.Millisecond} {
			s, err := New(Config{Service: newTransportTestService(nil), Transport: TransportConfig{Kind: kind, ReadHeaderTimeout: d, IdleTimeout: d}})
			if err != nil {
				t.Fatal(err)
			}
			h, err := s.HTTP()
			if err != nil {
				t.Fatal(err)
			}
			header, idle := d, d
			if d == 0 {
				header = 10 * time.Second
				idle = 120 * time.Second
			}
			if h.ReadHeaderTimeout != header || h.IdleTimeout != idle || h.WriteTimeout != 0 || h.ReadTimeout != 0 {
				t.Fatalf("%s server=%+v", kind, h)
			}
			// HTTP() returns the same caller-owned server; custom streaming policies survive.
			h.WriteTimeout = 45 * time.Minute
			again, _ := s.HTTP()
			if again.WriteTimeout != 45*time.Minute {
				t.Fatal("explicit write policy replaced")
			}
		}
	}
}
