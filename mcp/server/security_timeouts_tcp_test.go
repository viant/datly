package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestSecurityTimeoutsOnTCPServers(t *testing.T) {
	for _, kind := range []TransportKind{TransportSSE, TransportStreamable} {
		t.Run(string(kind), func(t *testing.T) {
			const timeout = 40 * time.Millisecond
			wire := startConfiguredWireServer(t, newTransportTestService(nil), TransportConfig{Kind: kind, ReadHeaderTimeout: timeout, IdleTimeout: timeout}, func(server *http.Server) {
				original := server.Handler
				// Exercise the actual factory-created server's transport deadlines with a
				// deterministic active response, independently of MCP session lifetimes.
				server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/timeout-probe" {
						original.ServeHTTP(w, r)
						return
					}
					if r.URL.RawQuery == "slow" {
						w.Header().Set("Content-Type", "text/event-stream")
						fmt.Fprint(w, "first\n")
						w.(http.Flusher).Flush()
						select {
						case <-time.After(3 * timeout):
							fmt.Fprint(w, "last\n")
						case <-r.Context().Done():
						}
						return
					}
					fmt.Fprint(w, "ok")
				})
			})
			address := strings.TrimPrefix(wire.baseURL, "http://")
			t.Run("slow header", func(t *testing.T) {
				conn, err := net.Dial("tcp", address)
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(2 * time.Second))
				if _, err = fmt.Fprint(conn, "GET /timeout-probe HTTP/1.1\r\nHost: localhost\r\nX-Incomplete:"); err != nil {
					t.Fatal(err)
				}
				_, err = io.ReadAll(conn)
				if timed, ok := err.(net.Error); ok && timed.Timeout() {
					t.Fatalf("server did not close slow headers: %v", err)
				}
			})
			t.Run("idle keepalive", func(t *testing.T) {
				conn, err := net.Dial("tcp", address)
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(2 * time.Second))
				if _, err = fmt.Fprint(conn, "GET /timeout-probe HTTP/1.1\r\nHost: localhost\r\n\r\n"); err != nil {
					t.Fatal(err)
				}
				reader := bufio.NewReader(conn)
				response, err := http.ReadResponse(reader, nil)
				if err != nil {
					t.Fatal(err)
				}
				data, err := io.ReadAll(response.Body)
				response.Body.Close()
				if err != nil || string(data) != "ok" || response.Close {
					t.Fatalf("not a reusable response: %q close=%v err=%v", data, response.Close, err)
				}
				_, err = reader.ReadByte()
				if err == nil {
					t.Fatal("unexpected bytes on idle connection")
				}
				if timed, ok := err.(net.Error); ok && timed.Timeout() {
					t.Fatalf("server did not close idle connection: %v", err)
				}
			})
			t.Run("active response survives idle period", func(t *testing.T) {
				client := &http.Client{Timeout: 2 * time.Second}
				defer client.CloseIdleConnections()
				response, err := client.Get(wire.baseURL + "/timeout-probe?slow")
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				data, err := io.ReadAll(response.Body)
				if err != nil || string(data) != "first\nlast\n" {
					t.Fatalf("response cut short: %q %v", data, err)
				}
			})
		})
	}
}
