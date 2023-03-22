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

	Interceptors      map[string]UnmarshallInterceptor
	UnmarshallSession struct {
		Interceptors Interceptors
		Options      []interface{}
	}

	UnmarshallInterceptor func(dst interface{}, decoder *gojay.Decoder, options ...interface{}) error
)
