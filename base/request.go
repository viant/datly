package base

import (
	"encoding/json"
	"github.com/pkg/errors"
	"io/ioutil"
	"net/http"
	"net/url"
)

//Request represents base request
type Request struct {
	TraceID     string
	Path        string
	Headers     http.Header
	PathParams  url.Values
	QueryParams url.Values
	Data        map[string]interface{}
	CaseFormat  string `json:",omitempty"` //source data case format
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
