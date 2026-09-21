package locator

import (
	"net/http"
	"net/url"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithQueryOwnsValues(t *testing.T) {
	query := url.Values{"first": {"one"}}
	option := WithQuery(query)
	query.Set("first", "changed")

	options := NewOptions([]Option{
		option,
		WithQuery(url.Values{"second": {"two"}}),
	})
	options.Query.Set("first", "result changed")

	require.Equal(t, "changed", query.Get("first"))
	require.Empty(t, query.Get("second"))
	require.Equal(t, "result changed", options.Query.Get("first"))
	require.Equal(t, "two", options.Query.Get("second"))

	replayed := NewOptions([]Option{option})
	require.Equal(t, "one", replayed.Query.Get("first"))
}

func TestWithQueryConcurrentReplay(t *testing.T) {
	query := url.Values{"first": {"one"}}
	options := []Option{
		WithQuery(query),
		WithQuery(url.Values{"second": {"two"}}),
	}

	for _, actual := range concurrentlyReplayOptions(options) {
		require.Equal(t, "one", actual.Query.Get("first"))
		require.Equal(t, "two", actual.Query.Get("second"))
	}

	require.Equal(t, url.Values{"first": {"one"}}, query)
}

func TestWithHeadersConcurrentReplay(t *testing.T) {
	header := http.Header{"First": {"one"}}
	options := []Option{
		WithHeaders(header),
		WithHeaders(http.Header{"Second": {"two"}}),
	}

	for _, actual := range concurrentlyReplayOptions(options) {
		require.Equal(t, "one", actual.Header.Get("First"))
		require.Equal(t, "two", actual.Header.Get("Second"))
	}

	require.Equal(t, http.Header{"First": {"one"}}, header)
}

func concurrentlyReplayOptions(options []Option) []*Options {
	const concurrency = 100
	result := make([]*Options, concurrency)
	start := make(chan struct{})
	var waitGroup sync.WaitGroup
	waitGroup.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(index int) {
			defer waitGroup.Done()
			<-start
			result[index] = NewOptions(options)
		}(i)
	}
	close(start)
	waitGroup.Wait()
	return result
}
