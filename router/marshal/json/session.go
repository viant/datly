package json

import (
	"bytes"
	"github.com/francoispqt/gojay"
)

type (
	MarshallSession struct {
		Filters *Filters
		Options []interface{}
		*bytes.Buffer
	}

	UnmarshalerInterceptors map[string]UnmarshalInterceptor
	UnmarshalSession        struct {
		Interceptors UnmarshalerInterceptors
		Options      []interface{}
	}

	UnmarshalInterceptor func(dst interface{}, decoder *gojay.Decoder, options ...interface{}) error
)
