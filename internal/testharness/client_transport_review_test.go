package testharness

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	clienthttp "github.com/viant/datly/internal/client/http"
	xhttp "github.com/viant/xdatly/client/http"
)

func TestHTTPClientPreservesTransportFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	endpoint := server.URL
	server.Close()
	client, err := clienthttp.New(xhttp.Options{URL: endpoint, Method: http.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Do(request)
	if err == nil {
		t.Fatal("closed endpoint unexpectedly accepted a connection")
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("transport failure was incorrectly replaced with cancellation: %v", err)
	}
	var transportErr *net.OpError
	if !errors.As(err, &transportErr) {
		t.Fatalf("original transport error not available to caller: %v", err)
	}
}

func TestHTTPClientZeroResponseLimitIsUnlimited(t *testing.T) {
	want := strings.Repeat("x", (1<<20)+1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, want) }))
	defer server.Close()
	c, err := clienthttp.New(xhttp.Options{URL: server.URL, Method: http.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	request, _ := http.NewRequest(http.MethodGet, server.URL, nil)
	response, err := c.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	actual, err := io.ReadAll(response.Body)
	if err != nil || string(actual) != want {
		t.Fatalf("zero response limit imposed a hidden bound: bytes=%d err=%v", len(actual), err)
	}
}
