package dml

import (
	"context"
	"database/sql"
	"fmt"

	dexec "github.com/viant/datly/exec"
	xhandler "github.com/viant/xdatly/handler"
)

// Source opens a fresh SQLX-backed handler data capability for each invocation.
type Source struct {
	DB       *sql.DB
	Tx       *sql.Tx
	OnCommit func(context.Context)
}

var (
	_ dexec.DataSource   = Source{}
	_ xhandler.Data      = (*Data)(nil)
	_ xhandler.Sequencer = (*Data)(nil)
)

func (s Source) Open(context.Context) (xhandler.Data, error) {
	if s.DB == nil {
		return nil, fmt.Errorf("DML data source DB is required")
	}
	options := []Option(nil)
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
