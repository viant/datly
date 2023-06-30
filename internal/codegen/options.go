package codegen

import (
	"github.com/viant/datly/internal/codegen/ast"
	"strings"
)

type (
	Options struct {
		withInsert           bool
		withUpdate           bool
		withDML              bool //TODO implement DML
		withoutBusinessLogic bool
		withLowerCaseIdent   bool
		lang                 string
	}

	Option func(o *Options)
)

func (o *Options) astOption() ast.Options {
	astOptions := ast.Options{Lang: ast.LangVelty}
	astOptions.WithoutBusinessLogic = o.withoutBusinessLogic
	astOptions.WithLowerCaseIdent = o.withLowerCaseIdent

	if o.lang != "" {
		astOptions.Lang = o.lang
	}
	return astOptions
}

func (o *Options) isInsertOnly() bool {
	return o.withInsert && !o.withUpdate
}

func (o *Options) IsGoLang() bool {
	return o.lang == ast.LangGO
}

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

func WithLowerCamelIdent() Option {
	return func(o *Options) {
		o.withLowerCaseIdent = true
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

func WithoutBusinessLogic() Option {
	return func(o *Options) {
		o.withoutBusinessLogic = true
	}
}

func WithPatch() Option {
	return func(o *Options) {
		o.withInsert = true
		o.withUpdate = true
	}
}

func WithLang(lang string) Option {
	return func(o *Options) {
		o.lang = lang
	}
}
