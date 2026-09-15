package dml

import (
	"context"
	"database/sql"
	"github.com/viant/sqlx/metadata/info/dialect"
)

type options struct {
	Tx               *sql.Tx
	SequenceStrategy dialect.PresetIDStrategy
	OnCommit         func(context.Context)
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

// WithSequenceStrategy selects a native allocation strategy for this data owner.
// Empty/undefined retains the native product default.
func WithSequenceStrategy(strategy dialect.PresetIDStrategy) Option {
	return func(o *options) { o.SequenceStrategy = strategy }
}
