package httpserver

import (
	"net/http"
	"testing"
	"time"
)

func TestDefaultsPreserveActiveResponsePolicies(t *testing.T) {
	for _, explicit := range []time.Duration{0, -time.Millisecond, 30 * time.Minute} {
		server := &http.Server{ReadHeaderTimeout: explicit, IdleTimeout: explicit, ReadTimeout: explicit, WriteTimeout: explicit}
		Defaults(server)
		header, idle := explicit, explicit
		if explicit == 0 {
			header = 10 * time.Second
			idle = 120 * time.Second
		}
		if server.ReadHeaderTimeout != header || server.IdleTimeout != idle || server.ReadTimeout != explicit || server.WriteTimeout != explicit {
			t.Fatalf("changed explicit policy: %+v", server)
		}
	}
}
