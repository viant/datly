package expand

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
)

type (
	Http struct {
	}

	HttpResponse struct {
		Invalid bool
		Message string
	}
)

func (h *Http) Do(method string, URL string, body interface{}) (HttpResponse, error) {
	result := HttpResponse{
		Invalid: true,
	}

	marshal, err := json.Marshal(body)
	if err != nil {
		return result, err
	}

	request, err := http.NewRequest(method, URL, bytes.NewReader(marshal))
	if err != nil {
		return result, err
	}

	response, err := (&http.Client{}).Do(request)
	if err != nil {
		return result, err
	}

	result.Invalid = response.StatusCode < 200 || response.StatusCode > 299

	data, err := ioutil.ReadAll(response.Body)
	defer response.Body.Close()

	if err != nil {
		return result, err
	}

	if len(data) > 0 {
		if err = json.Unmarshal(data, &result); err != nil {
			return result, err
		}
	}

	return result, err
}

func (h *Http) Post(url string, body interface{}) (HttpResponse, error) {
	return h.Do(http.MethodPost, url, body)
}
