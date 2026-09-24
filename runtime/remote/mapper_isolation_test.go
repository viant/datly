package remote

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	xhttp "github.com/viant/xdatly/client/http"
)

func TestMapperCacheSeparatesOutputContracts(t *testing.T) {
	config := Config{Client: ClientOptions{Transport: TransportHTTP, HTTP: &xhttp.Options{URL: "https://example.test/context", Method: http.MethodGet}}, Response: Response{Mappings: []ResponseMapping{{Path: "/id", Output: "ID"}}}, Cache: &Cache{Name: testCacheName, TTL: "5m"}}
	cacheProvider := newTestCache(t, 0)
	provider := func(document string) *regressionHTTPProvider {
		return &regressionHTTPProvider{client: &regressionHTTPClient{do: func(*http.Request) (*http.Response, error) {
			return regressionHTTPResponse(http.StatusOK, document), nil
		}}}
	}
	mapper := NewMapper()
	input := &struct{}{}
	var numeric struct{ ID int }
	if err := mapper.HTTP(context.Background(), &config, provider(`{"id":7}`), cacheProvider, input, &numeric); err != nil {
		t.Fatal(err)
	}
	var text struct{ ID string }
	if err := mapper.HTTP(context.Background(), &config, provider(`{"id":"seven"}`), cacheProvider, input, &text); err != nil {
		t.Fatalf("another output contract reused the wrong cached document: %v", err)
	}
	if text.ID != "seven" {
		t.Fatalf("text ID=%q", text.ID)
	}
}

func TestCacheDoesNotExtendExpiredResponseValidity(t *testing.T) {
	var tick atomic.Int64
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return start.Add(time.Duration(tick.Load()) * time.Minute) }
	config := regressionHTTPConfig()
	config.Cache = &Cache{Name: testCacheName, TTL: "5m"}
	config.Response.Validity = &Validity{Path: "/expires", Format: ValidityUnix}
	cacheProvider := newTestCache(t, 0)
	var calls atomic.Int64
	provider := &regressionHTTPProvider{client: &regressionHTTPClient{do: func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		tick.Store(2) // Remote response arrives after its declared absolute expiry.
		return regressionHTTPResponse(http.StatusOK, fmt.Sprintf(`{"value":7,"expires":%d}`, start.Add(time.Minute).Unix())), nil
	}}}
	mapper := NewMapper(WithClock(clock))
	for i := 0; i < 2; i++ {
		var output struct{ Value int }
		if err := mapper.HTTP(context.Background(), &config, provider, cacheProvider, &struct{}{}, &output); err != nil || output.Value != 7 {
			t.Fatalf("output=%#v err=%v", output, err)
		}
	}
	if calls.Load() != 2 || testEntries(t, mapper, &config, cacheProvider) != 0 {
		t.Fatalf("expired response cached: calls=%d", calls.Load())
	}
}
