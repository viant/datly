package config

import (
	"github.com/viant/xdatly/codec"
	"github.com/viant/xdatly/handler/parameter"
	"github.com/viant/xreflect"
)

// Options represents internal codec option
type Options struct {
	xreflect.LookupType
	parameter.ColumnsSource
	parameter.Selector
	*ParentValue
}

// NewOptions creates options
func NewOptions(opts *codec.Options) *Options {
	ret := &Options{}
	for _, option := range opts.Options {
		switch actual := option.(type) {
		case xreflect.LookupType:
			ret.LookupType = actual
		case parameter.ColumnsSource:
			ret.ColumnsSource = actual
		case parameter.Selector:
			ret.Selector = actual
		case *ParentValue:
			ret.ParentValue = actual
		case ParentValue:
			ret.ParentValue = &actual
		}
	}

	return ret
}
