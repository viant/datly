package dml

import (
	"context"
	"database/sql"
	"fmt"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	sqlconfig "github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/option"
	xhandler "github.com/viant/xdatly/handler"
)

// Source opens a fresh SQLX-backed handler data capability for each invocation.
type Source struct {
	DB               *sql.DB
	SequenceStrategy dialect.PresetIDStrategy
	Tx               *sql.Tx
	OnCommit         func(context.Context)
}

var (
	_ dexec.DataSource   = Source{}
	_ xhandler.Data      = (*Data)(nil)
	_ xhandler.Sequencer = (*Data)(nil)
)

func (s Source) Open(ctx context.Context) (xhandler.Data, error) {
	if s.DB == nil {
		return nil, fmt.Errorf("DML data source DB is required")
	}
	strategy, err := s.resolveSequenceStrategy(ctx)
	if err != nil {
		return nil, err
	}
	options := []Option{WithSequenceStrategy(strategy)}
	if s.Tx != nil {
		options = append(options, WithTx(s.Tx))
	}
	if s.OnCommit != nil {
		options = append(options, WithCommitObserver(s.OnCommit))
	}
	return NewData(s.DB, options...), nil
}

// InvocationKey lets Datly's private invocation scope separate database units
// without exposing transaction or data-unit lifecycle through xdatly.
func (s Source) InvocationKey() any { return s.DB }

// InvocationTransactionKey supports early child/root transaction conflict checks.
func (s Source) InvocationTransactionKey() any {
	if s.Tx == nil {
		return nil
	}
	return s.Tx
}

func (s Source) WithSequenceStrategy(strategy string) (dexec.DataSource, error) {
	if err := (&spec.Settings{SequenceStrategy: strategy}).ValidateSequenceStrategy(); err != nil {
		return nil, err
	}
	s.SequenceStrategy = dialect.PresetIDStrategy(strategy)
	return s, nil
}
func (s Source) InvocationSequenceStrategy() string {
	if s.SequenceStrategy == dialect.PresetIDStrategyUndefined {
		return ""
	}
	return string(s.SequenceStrategy)
}

// ResolveSequenceStrategy freezes the effective native policy on a source copy
// before Open. It only reads SQLX dialect metadata on the source/caller owner;
// it never opens a Data capability or starts/finishes a transaction.
func (s Source) ResolveSequenceStrategy(ctx context.Context) (dexec.DataSource, string, error) {
	strategy, err := s.resolveSequenceStrategy(ctx)
	if err != nil {
		return nil, "", err
	}
	s.SequenceStrategy = strategy
	return s, string(strategy), nil
}
func (s Source) resolveSequenceStrategy(ctx context.Context) (dialect.PresetIDStrategy, error) {
	if s.SequenceStrategy != "" && s.SequenceStrategy != dialect.PresetIDStrategyUndefined {
		return s.SequenceStrategy, nil
	}
	if s.DB == nil {
		return "", fmt.Errorf("DML data source DB is required")
	}
	var opts []option.Option
	if s.Tx != nil {
		opts = append(opts, s.Tx)
	}
	product, err := sqlconfig.Dialect(ctx, s.DB, opts...)
	if err != nil {
		return "", err
	}
	return product.SequenceStrategy(s.SequenceStrategy), nil
}
