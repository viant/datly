package dml

import (
	"context"
	"database/sql"
)

type options struct {
	Tx       *sql.Tx
	OnCommit func(context.Context)
}

// Option configures an invocation SQL DML capability.
type Option func(*options)

// WithTx attaches a parent-managed transaction. Flush writes through it but
// never commits or rolls it back.
func WithTx(tx *sql.Tx) Option {
	return func(options *options) {
		options.Tx = tx
	}
}

func WithCommitObserver(observer func(context.Context)) Option {
	return func(options *options) {
		options.OnCommit = observer
	}
}

func collectOptions(opts ...Option) options {
	options := options{}
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}
	return options
}
