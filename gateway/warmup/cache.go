package warmup

import (
	"context"
	"fmt"
	"github.com/viant/datly/view"
	"github.com/viant/datly/warmup"
	"net/http"
	"strings"
	"sync"
	"time"
)

type PreCachables func(ctx context.Context, method, matchingURI string) ([]*view.View, error)
type PreCached struct {
	URI       string
	View      string
	Column    string
	Params    string
	Elapsed   string
	TimeTaken time.Duration
	Rows      int
}

type Response struct {
	Error     string       `json:"error,omitempty"`
	Status    string       `json:"status"`
	PreCached []*PreCached `json:"preCached"`
}

func PreCache(ctx context.Context, lookup PreCachables, warmupURIs ...string) *Response {
	started := time.Now()
	fmt.Printf("[INFO] cache warmup request start start_time=%s uris=%v\n", started.Format(time.RFC3339), warmupURIs)
	group := sync.WaitGroup{}
	var err error
	var mux = sync.Mutex{}
	var response = &Response{Status: "ok"}
	setErr := func(e error) {
		if e == nil {
			return
		}
		mux.Lock()
		defer mux.Unlock()
		err = e
	}

	for _, URI := range warmupURIs {
		group.Add(1)
		go func(URI string) {
			defer group.Done()
			startTime := time.Now()
			fmt.Printf("[INFO] cache warmup uri start uri=%s start_time=%s\n", URI, startTime.Format(time.RFC3339))
			views, e := lookup(ctx, http.MethodGet, URI)
			if e != nil {
				fmt.Printf("[INFO] cache warmup uri lookup error uri=%s elapsed=%s error=%v\n", URI, time.Since(startTime), e)
				setErr(e)
			}
			fmt.Printf("[INFO] cache warmup uri views uri=%s count=%d views=%s elapsed=%s\n", URI, len(views), viewNames(views), time.Since(startTime))
			var result *warmup.Result
			if result, e = warmup.PopulateCacheWithDetails(views); e != nil {
				fmt.Printf("[INFO] cache warmup uri populate error uri=%s elapsed=%s error=%v\n", URI, time.Since(startTime), e)
				setErr(e)
			}
			elapsed := time.Now().Sub(startTime)
			rows := 0
			if result != nil {
				rows = result.Rows
			}
			fmt.Printf("[INFO] cache warmup uri done uri=%s rows=%d elapsed=%s\n", URI, rows, elapsed)
			if result == nil {
				return
			}
			mux.Lock()
			appendPreCached(response, URI, result)
			mux.Unlock()
		}(URI)
	}
	group.Wait()
	if err != nil {
		response.Error = err.Error()
		response.Status = "error"
	}
	fmt.Printf("[INFO] cache warmup request done status=%s elapsed=%s\n", response.Status, time.Since(started))
	return response
}

func appendPreCached(response *Response, URI string, result *warmup.Result) {
	if response == nil || result == nil {
		return
	}
	for _, entry := range result.Entries {
		if entry == nil {
			continue
		}
		response.PreCached = append(response.PreCached, &PreCached{
			URI:       URI,
			View:      entry.View,
			Column:    entry.Column,
			Params:    entry.Params,
			Elapsed:   entry.Elapsed,
			TimeTaken: entry.TimeTaken,
			Rows:      entry.Rows,
		})
	}
}

func viewNames(views []*view.View) string {
	if len(views) == 0 {
		return ""
	}
	names := make([]string, 0, len(views))
	for _, candidate := range views {
		if candidate == nil {
			continue
		}
		names = append(names, candidate.Name)
	}
	return strings.Join(names, ",")
}
