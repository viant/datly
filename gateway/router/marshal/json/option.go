package json

import (
	"github.com/viant/structology/format"
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

func (o Options) DefaultTag() *format.Tag {
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
