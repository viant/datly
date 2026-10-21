package openapi3

import (
	"context"
	"encoding/json"
	"fmt"
)

// Responses is specified by OpenAPI/Swagger 3.0 standard.
type (
	Responses map[string]*Response

	// Response is specified by OpenAPI/Swagger 3.0 standard.
	Response struct {
		Extension   `json:",omitempty" yaml:",inline"`
		Ref         string  `json:"$ref,omitempty" yaml:"$ref,omitempty"`
		Description *string `json:"description,omitempty" yaml:"description,omitempty"`
		Headers     Headers `json:"headers,omitempty" yaml:"headers,omitempty"`
		Content     Content `json:"content,omitempty" yaml:"content,omitempty"`
		Links       Links   `json:"links,omitempty" yaml:"links,omitempty"`
	}
)

const (
	ResponseContinue           ResponseCode = "100"
	ResponseOK                 ResponseCode = "200"
	ResponseCreated            ResponseCode = "201"
	ResponseAccepted           ResponseCode = "202"
	ResponseNoContent          ResponseCode = "204"
	ResponseBadRequest         ResponseCode = "400"
	ResponseUnauthorized       ResponseCode = "401"
	ResponseForbidden          ResponseCode = "403"
	ResponseNotFound           ResponseCode = "404"
	ResponseConflict           ResponseCode = "409"
	ResponseUnprocessable      ResponseCode = "422"
	ResponseInternalServerErr  ResponseCode = "500"
	ResponseBadGateway         ResponseCode = "502"
	ResponseServiceUnavailable ResponseCode = "503"
	ResponseDefault            ResponseCode = "default"
)

type (
	ResponseCode        string
	ResponseCodeLiteral string

	ResponseKey interface {
		~string | ~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64
	}
)

func NormalizeResponseCode[T ResponseKey](code T) ResponseCode {
	return ResponseCode(fmt.Sprintf("%v", code))
}

func SetResponse[T ResponseKey](r Responses, code T, response *Response) {
	key := NormalizeResponseCode(code)
	if key == "" {
		return
	}
	r[string(key)] = response
}

func GetResponse[T ResponseKey](r Responses, code T) (*Response, bool) {
	key := NormalizeResponseCode(code)
	if key == "" {
		return nil, false
	}
	value, ok := r[string(key)]
	return value, ok
}

func (r *Response) UnmarshalJSON(b []byte) error {
	type temp Response
	var tmp = temp{}
	err := json.Unmarshal(b, &tmp)
	if err != nil {
		return err
	}
	*r = Response(tmp)
	return r.Extension.UnmarshalJSON(b)
}

func (r *Response) MarshalJSON() ([]byte, error) {
	type temp Response
	tmp := temp(*r)
	tmp.Extension = nil
	data, err := json.Marshal(tmp)
	if err != nil {
		return nil, err
	}
	if len(r.Extension) == 0 {
		return data, nil
	}
	extData, err := json.Marshal(r.Extension)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return extData, nil
	}
	res := mergeJSON(data, extData)
	return res, nil
}

func (s *Response) UnmarshalYAML(ctx context.Context, fn func(dest interface{}) error) error {
	type temp Response
	tmp := temp(*s)
	err := fn(&tmp)
	if err != nil {
		return err
	}
	ext := CustomExtension{}
	err = fn(&ext)
	if err != nil {
		return err
	}
	tmp.Extension = Extension(ext)
	*s = Response(tmp)
	if tmp.Ref == "" {
		return nil
	}
	session := LookupSession(ctx)
	param, err := session.LookupResponse(session.Location, tmp.Ref)
	if err == nil {
		*s = *param
	}
	return err
}
