package xdatly

import (
	"context"
	"net/http"
	"reflect"
)

type (
	Valuer interface {
		Value(ctx context.Context, raw interface{}, options ...interface{}) (interface{}, error)
	}

	CodecDef interface {
		BasicCodec
		Typer
	}

	Typer interface {
		ResultType(paramType reflect.Type) (reflect.Type, error)
	}

	BasicCodec interface {
		Valuer() Valuer
		Name() string
	}

	CodecFactory interface {
		New(codecConfig *CodecConfig, paramType reflect.Type) (Valuer, error)
	}

	BeforeFetcher interface {
		BeforeFetch(response http.ResponseWriter, request *http.Request) error
	}

	AfterFetcher interface {
		AfterFetch(dest interface{}, response http.ResponseWriter, req *http.Request) error
	}
)
