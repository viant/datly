package expand

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
)

type (
	Http struct {
	}

	HttpValidation struct {
		Invalid bool
		Message string
	}

	HttpResponse struct {
		HttpValidation
		StatusCode int
		Body       string
	}
)

func (h *Http) Do(method string, URL string, input interface{}) (HttpResponse, error) {
	result := HttpResponse{}
	result.Invalid = true
	var reader io.Reader
	if input != nil {
		data, err := json.Marshal(input)
		if err != nil {
			return result, err
		}
		reader = bytes.NewReader(data)
	}
	request, err := http.NewRequest(method, URL, reader)
	if err != nil {
		return result, err
	}
	response, err := (&http.Client{}).Do(request)
	if err != nil {
		return result, err
	}
	result.Invalid = response.StatusCode < 200 || response.StatusCode > 299
	result.StatusCode = response.StatusCode
	data, err := ioutil.ReadAll(response.Body)
	defer response.Body.Close()
	if err != nil {
		return result, err
	}
	result.Body = string(data)
	if len(data) > 0 && json.Valid(data) {
		if err = json.Unmarshal(data, &result.HttpValidation); err != nil {
			return result, err
		}
	}
	return result, err
}

func (h *Http) Post(url string, body interface{}) (HttpResponse, error) {
	return h.Do(http.MethodPost, url, body)
}

func (h *Http) Get(url string) (HttpResponse, error) {
	return h.Do(http.MethodGet, url, nil)
}
