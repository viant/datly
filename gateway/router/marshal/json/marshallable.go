package json

import "github.com/francoispqt/gojay"

type UnmarshalerInto interface {
	UnmarshalJSONWithOptions(dst interface{}, decoder *gojay.Decoder, options ...interface{}) error
}
