package warmup

import (
	"github.com/viant/datly/view"
	"github.com/viant/datly/warmup"
	"net/http"
	"sync"
	"time"
)

type PreCachables func(method, matchingURI string) ([]*view.View, error)
type PreCached struct {
	View      string
	Elapsed   string
	TimeTaken time.Duration
	Rows      int
}

type Response struct {
	Error     string       `json:"error,omitempty"`
	Status    string       `json:"status"`
	PreCached []*PreCached `json:"preCached"`
}

func PreCache(lookup PreCachables, warmupURIs ...string) *Response {
	group := sync.WaitGroup{}
	var err error
	var mux = sync.Mutex{}
	var response = &Response{Status: "ok"}

	for _, URI := range warmupURIs {
		group.Add(1)
		go func(URI string) {
			defer group.Done()
			startTime := time.Now()
			views, e := lookup(http.MethodGet, URI)
			if e != nil {
				err = e
			}
			var added int
			if added, e = warmup.PopulateCache(views); e != nil {
				err = e
			}
			elapsed := time.Now().Sub(startTime)
			for _, v := range views {
				mux.Lock()
				response.PreCached = append(response.PreCached, &PreCached{View: v.Name, Elapsed: elapsed.String(), TimeTaken: elapsed, Rows: added})
				mux.Unlock()
			}
		}(URI)
	}
	group.Wait()
	if err != nil {
		response.Error = err.Error()
		response.Status = "error"
	}
	return response
}
