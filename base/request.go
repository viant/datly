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
	TraceID string
	URI string
	Headers http.Header
	URIParams url.Values
	QueryParams url.Values
	Data map[string]interface{}
}
//Init initialises reqeust
func (r *Request) Init(request *http.Request) error {
	if request.URL != nil {
		r.URI = request.RequestURI
	}
	r.Headers = request.Header
	if request.Body != nil {
		data, err := ioutil.ReadAll(request.Body)
		if err != nil {
			return err
		}
		_ = request.Body.Close()
		err = json.Unmarshal(data, &r.Data)
		if err != nil {
			return errors.Wrapf(err, "failed to decode body: %s", data)
		}
	}
	return nil
}

//Validate checks if request is valid
func (r *Request) Validate() error {
	if r.URI == "" {
		return errors.Errorf("URI was empty")
	}
	return nil
}
