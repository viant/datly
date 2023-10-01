package codec

import (
	"github.com/viant/xdatly/codec"
	"github.com/viant/xreflect"
	"reflect"
)

// Options represents internal codec option
type Options struct {
	xreflect.LookupType
	codec.ColumnsSource
	codec.Selector
	codec.ValueGetter
	*ParentValue
}

// NewOptions creates options
func NewOptions(opts *codec.Options) *Options {
	ret := &Options{}

	for _, option := range opts.Options {
		switch actual := option.(type) {
		case xreflect.LookupType:
			ret.LookupType = actual
		case codec.ColumnsSource:
			ret.ColumnsSource = actual
		case codec.Selector:
			ret.Selector = actual
		case *ParentValue:
			ret.ParentValue = actual
		case ParentValue:
			ret.ParentValue = &actual
		}
	}
	if opts.ColumnsSource != nil {
		ret.ColumnsSource = opts.ColumnsSource
	}
	if opts.Selector != nil {
		ret.Selector = opts.Selector
	}
	if opts.ValueGetter != nil {
		ret.ValueGetter = opts.ValueGetter
	}
	if opts.LookupType != nil && ret.LookupType == nil {
		ret.LookupType = func(name string, option ...xreflect.Option) (reflect.Type, error) {
			return opts.LookupType(name)
		}
	}

	return ret
}
