package handler

import (
	"embed"
	"github.com/viant/datly/service/auth"
	"github.com/viant/datly/view/state"
	"github.com/viant/xdatly/handler/logger"
)

type options struct {
	Types   []*state.Type
	embedFS *embed.FS
	opts    []Option
	auth    *auth.Service
	logger  logger.Logger
}

func (o *options) Clone(opts []Option) *options {
	return newOptions(append(o.opts, opts...)...)
}
func (o *options) Options(opts []Option) []Option {
	return append(o.opts, opts...)
}
func newOptions(opts ...Option) *options {
	ret := &options{
		opts: opts,
	}
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

func WithLogger(logger logger.Logger) Option {
	return func(o *options) {
		o.logger = logger
	}
}

func WithAuth(auth *auth.Service) Option {
	return func(o *options) {
		o.auth = auth
	}
}
func WithEmbedFS(fs *embed.FS) Option {
	return func(o *options) {
		o.embedFS = fs
	}
}
