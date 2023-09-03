package handler

import (
	"github.com/viant/xdatly/handler/async"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

type (
	RecordWithHttp struct {
		Record  *async.Job
		Body    string
		Method  string
		URL     string
		Headers http.Header
	}

	Records struct {
		sync.Mutex
		items []*async.Job
	}
)

func NewRecords() *Records {
	return &Records{items: make([]*async.Job, 0)}
}

func (r *Records) Result() []*async.Job {
	return r.items
}

func (r *Records) Add(records ...*async.Job) {
	r.Lock()
	defer r.Unlock()
	r.items = append(r.items, records...)
}

func (r *RecordWithHttp) Request() (*http.Request, error) {
	URL, err := url.Parse(r.URL)
	if err != nil {
		return nil, err
	}

	h := &http.Request{
		Method:     r.Method,
		URL:        URL,
		Header:     r.Headers,
		Body:       io.NopCloser(strings.NewReader(r.Body)),
		Host:       URL.Host,
		RequestURI: r.URL,
	}

	return h, nil
}
