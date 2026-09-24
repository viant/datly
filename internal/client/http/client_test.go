package http

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/viant/xdatly/client"
	xhttp "github.com/viant/xdatly/client/http"
)

func TestClientAppliesStaticHeadersWithoutOverridingExplicitOnes(t *testing.T) {
	var seen http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Clone()
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	c, err := New(xhttp.Options{URL: server.URL + "/ctx", Method: http.MethodGet, Header: map[string]string{"x-client": "static", "Authorization": "static-token"}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	request, _ := http.NewRequest(http.MethodGet, server.URL+"/ctx", nil)
	request.Header.Set("Authorization", "explicit")
	response, err := c.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(response.Body)
	_ = response.Body.Close()
	if seen.Get("X-Client") != "static" || seen.Get("Authorization") != "explicit" {
		t.Fatalf("header = %v", seen)
	}
	if c.Identity() == "" {
		t.Fatal("identity is empty")
	}
}

func TestClientRefusesForeignHostsAndBoundsBodyAndDeadline(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/slow":
			select {
			case <-r.Context().Done():
			case <-release:
			}
		case "/big":
			_, _ = w.Write([]byte(strings.Repeat("x", 100)))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	defer server.Close()
	c, err := New(xhttp.Options{URL: server.URL + "/ctx", Method: http.MethodGet, Limits: client.Limits{Timeout: "50ms", MaxResponseBytes: 16}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	foreign, _ := http.NewRequest(http.MethodGet, "http://other.test/ctx", nil)
	if _, err := c.Do(foreign); err == nil || !strings.Contains(err.Error(), "does not target the configured endpoint") {
		t.Fatalf("foreign host accepted: %v", err)
	}
	big, _ := http.NewRequest(http.MethodGet, server.URL+"/big", nil)
	response, err := c.Do(big)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err == nil || !strings.Contains(err.Error(), "exceeds 16 bytes") {
		t.Fatalf("oversized body accepted: %v", err)
	}
	slow, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL+"/slow", nil)
	started := time.Now()
	if _, err := c.Do(slow); !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
		t.Fatalf("deadline not enforced: %v after %s", err, time.Since(started))
	}
	if _, err := c.Do(nil); err == nil {
		t.Fatal("nil request accepted")
	}
}

func TestClientDoesNotFollowRedirects(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ctx" {
			http.Redirect(w, r, "/elsewhere", http.StatusFound)
			return
		}
		t.Errorf("redirect target reached: %s", r.URL.Path)
	}))
	defer server.Close()
	c, err := New(xhttp.Options{URL: server.URL + "/ctx", Method: http.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequest(http.MethodGet, server.URL+"/ctx", nil)
	response, err := c.Do(request)
	if err != nil || response.StatusCode != http.StatusFound {
		t.Fatalf("response = %v err = %v", response, err)
	}
	_ = response.Body.Close()
}
