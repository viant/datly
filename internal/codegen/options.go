package codegen

import "strings"

type (
	Options struct {
		withInsert bool
		withUpdate bool
		withDML    bool //TODO implement DML
	}

	Option func(o *Options)
)

func (o *Options) apply(opts []Option) {
	if len(opts) == 0 {
		return
	}
	for _, opt := range opts {
		opt(o)
	}
}

func WithDML() Option {
	return func(o *Options) {
		o.withDML = true
	}
}

func WithInsert() Option {
	return func(o *Options) {
		o.withInsert = true
	}
}

func WithUpdate() Option {
	return func(o *Options) {
		o.withUpdate = true
	}
}

func WithHTTPMethod(method string) Option {
	return func(o *Options) {
		switch strings.ToLower(method) {
		case "patch":
			o.withUpdate = true
			o.withInsert = true
		case "put":
			o.withUpdate = true
		case "post":
			o.withInsert = true
		}
	}
}

func WithPatch() Option {
	return func(o *Options) {
		o.withInsert = true
		o.withUpdate = true
	}
}
