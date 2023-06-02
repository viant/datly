package async

import (
	"github.com/viant/toolbox"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	StateRunning = "RUNNING"
	StateDone    = "DONE"

	CreateDispositionIfNeeded = "CREATE_IF_NEEDED"
	CreateDispositionNever    = "CREATE_NEVER"
	WriteDispositionTruncate  = "WRITE_TRUNCATE"
	WriteDispositionEmpty     = "WRITE_EMPTY"
	WriteDispositionAppend    = "WRITE_APPEND"
)

type (
	RecordWithHttp struct {
		Record  *Record
		Body    string
		Method  string
		URL     string
		Headers http.Header
	}

	Record struct {
		JobID   string `sqlx:"primaryKey=true,name=JobID" json:",omitempty"`
		State   string `json:",omitempty"`
		Metrics string `json:",omitempty"`
		RecordRequest
		RecordPrincipal
		RecordDestination
		MainView     string     `json:",omitempty" sqlx:"MainView"`
		Labels       string     `json:",omitempty"`
		JobType      string     `json:",omitempty"`
		Error        *string    `json:",omitempty"`
		CreationTime time.Time  `json:",omitempty"`
		EndTime      *time.Time `json:",omitempty"`
	}

	RecordRequest struct {
		RequestRouteURI string `json:",omitempty"`
		RequestURI      string `json:",omitempty"`
		RequestHeader   string `json:",omitempty"`
		RequestMethod   string `json:",omitempty"`
	}

	RecordPrincipal struct {
		PrincipalUserEmail *string `json:",omitempty"`
		PrincipalSubject   *string `json:",omitempty"`
	}

	RecordDestination struct {
		DestinationTable             string  `json:",omitempty"`
		DestinationCreateDisposition string  `json:",omitempty"`
		DestinationSchema            *string `json:",omitempty"`
		DestinationTemplate          *string `json:",omitempty"`
		DestinationWriteDesposition  *string `json:",omitempty"`
	}

	Records struct {
		sync.Mutex
		items []*Record
	}
)

func NewRecords() *Records {
	return &Records{items: make([]*Record, 0)}
}

func (r *Records) Result() []*Record {
	return r.items
}

func (r *Records) Add(records ...*Record) {
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

	toolbox.Dump(h)
	return h, nil
}
