package handler

import (
	"embed"
	"github.com/viant/datly/view/state"
)

type options struct {
	Types   []*state.Type
	embedFS *embed.FS
}

func (o *options) apply(opts []Option) *options {
	ret := *o
	for _, opt := range opts {
		opt(&ret)
	}
	return &ret
}
func newOptions(opts ...Option) *options {
	ret := &options{}
	for _, opt := range opts {
		opt(ret)
	}
	return ret
}

type Option func(o *options)

func WithTypes(types ...*state.Type) Option {
	return func(o *options) {
		o.Types = append(o.Types, types...)
	}
}

func WithEmbedFS(fs *embed.FS) Option {
	return func(o *options) {
		o.embedFS = fs
	}
}
