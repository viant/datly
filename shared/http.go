package shared

import (
	"bytes"
	"io"
	"net/http"
)

// CloneHTTPRequest clones http request
func CloneHTTPRequest(request *http.Request) (*http.Request, error) {
	var data []byte
	var err error
	ret := *request
	if request.Body != nil {
		if data, err = readRequestBody(request); err != nil {
			return nil, err
		}
		ret.Body = io.NopCloser(bytes.NewReader(data))
	}
	return &ret, err
}

func readRequestBody(request *http.Request) ([]byte, error) {
	data, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	_ = request.Body.Close()
	request.Body = io.NopCloser(bytes.NewReader(data))
	return data, err
}
