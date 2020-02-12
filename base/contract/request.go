package contract

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/datly/shared"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
)

//Request represents base request
type Request struct {
	TraceID     string
	Path        string
	Headers     http.Header
	PathParams  url.Values
	QueryParams url.Values
	EventTime   time.Time
	Data        map[string]interface{}
	Metrics     string `json:",omitempty"`
	CaseFormat  string `json:",omitempty"` //source data case format
}

// BasicAuth returns the username and password provided in the request's
// Authorization header, if the request uses HTTP Basic Authentication.
// See RFC 2617, Section 2.
func (r *Request) BasicAuth() (username, password string, ok bool) {
	request := &http.Request{Header: r.Headers}
	return request.BasicAuth()
}

//Init initialises request
func (r *Request) Init(request *http.Request) error {
	if request.URL != nil {
		r.Path = request.RequestURI
	}
	if URL := request.URL; URL != nil {
		r.QueryParams = URL.Query()
		r.Path = URL.Path
	}

	fmt.Printf("PATH: %v\n", r.Path)

	r.EventTime = time.Now()
	if len(r.Headers) > 0 {
		if created, ok := r.Headers[shared.EventCreateTimeHeader]; ok {
			r.EventTime, _ = time.Parse(time.RFC3339, created[0])
		}
	}
	r.Headers = request.Header
	if request.Body != nil {
		data, err := ioutil.ReadAll(request.Body)
		if err != nil {
			return err
		}
		_ = request.Body.Close()
		if len(data) > 0 && json.Valid(data) {
			err = json.Unmarshal(data, &r.Data)
			if err != nil {
				return errors.Wrapf(err, "failed to decode body: '%s'", data)
			}
		}
	}
	return nil
}

//Validate checks if request is valid
func (r *Request) Validate() error {
	if r.Path == "" {
		return errors.Errorf("Path was empty")
	}
	return nil
}
