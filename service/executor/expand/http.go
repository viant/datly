package expand

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
)

const (
	StatusUnspecified HTTPStatus = iota
	StatusDefault
	StatusFailure
	StatusSuccess
)

type (
	HTTPStatus int
	Flushable  struct {
		Handler  func() error
		OnStatus HTTPStatus
	}

	Callback     func(object interface{}, err error) error
	ErrorHandler func(err error) error

	Http struct {
		flushables []*Flushable `velty:"-"`
	}

	HttpValidation struct {
		Invalid bool
		Message string
	}

	HttpResponse struct {
		HttpValidation
		IsPostponed bool
		StatusCode  int
		Body        string
	}

	StatusCode int
)

func (h *Http) Do(method string, URL string, input interface{}, args ...interface{}) (HttpResponse, error) {
	httpStatus := StatusUnspecified
	var callbacks []Callback
	var errorHandlers []ErrorHandler
	for _, arg := range args {
		switch actual := arg.(type) {
		case HTTPStatus:
			httpStatus = actual
		case Callback:
			callbacks = append(callbacks, actual)
		case ErrorHandler:
			errorHandlers = append(errorHandlers, actual)
		}
	}

	switch httpStatus {
	case StatusSuccess, StatusFailure, StatusDefault:
		h.flushables = append(h.flushables, &Flushable{
			Handler: func() error {
				response, err := h.do(method, URL, copyValue(input))
				for _, callback := range callbacks {
					callbackErr := callback(response, err)
					if callbackErr != nil {
						return callbackErr
					}
				}

				for _, handler := range errorHandlers {
					if handlerErr := handler(err); handlerErr != nil {
						return handlerErr
					}
				}

				return nil
			},
			OnStatus: httpStatus,
		})

		return HttpResponse{IsPostponed: true}, nil
	}

	return h.doAndHandle(method, URL, input, errorHandlers)
}

func (h *Http) doAndHandle(method string, URL string, input interface{}, errorHandlers []ErrorHandler) (HttpResponse, error) {
	do, err := h.do(method, URL, input)
	for _, handler := range errorHandlers {
		if err := handler(err); err != nil {
			return do, err
		}
	}

	return do, err
}

func (h *Http) do(method string, URL string, input interface{}) (HttpResponse, error) {
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

func (h *Http) StatusCode(code int) StatusCode {
	return StatusCode(code)
}

func (h *Http) DeferOnSuccess() HTTPStatus {
	return StatusSuccess
}

func (h *Http) DeferOnFailure() HTTPStatus {
	return StatusFailure
}

func (h *Http) Defer() HTTPStatus {
	return StatusDefault
}

func (h *Http) OnError(callback Callback) ErrorHandler {
	return func(err error) error {
		if err != nil {
			return callback(nil, err)
		}

		return nil
	}
}

func (h *Http) OnSuccess(callback Callback) ErrorHandler {
	return func(err error) error {
		if err == nil {
			return callback(nil, err)
		}

		return nil
	}
}

func (h *Http) Flush(status HTTPStatus) error {
	for _, flushable := range h.flushables {
		if flushable.OnStatus != status && flushable.OnStatus != StatusDefault {
			continue
		}

		if err := flushable.Handler(); err != nil {
			return err
		}
	}

	return nil
}
