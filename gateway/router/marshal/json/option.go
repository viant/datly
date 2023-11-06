package json

import (
	"github.com/viant/tagly/format"
)

type Option interface{}
type Options []Option

func (o Options) Tag() *Tag {
	for _, candidate := range o {
		if value, ok := candidate.(*Tag); ok {
			return value
		}
	}
	return nil
}

func (o Options) FormatTag() *format.Tag {
	for _, candidate := range o {
		if value, ok := candidate.(*format.Tag); ok {
			return value
		}
	}
	return nil
}

type cacheConfig struct {
	ignoreCustomUnmarshaller bool
	ignoreCustomMarshaller   bool
}
