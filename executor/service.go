package executor

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/shared"
	"strings"
	"sync"
)

type Executor struct {
	sqlBuilder *SqlBuilder
}

func New() *Executor {
	return &Executor{sqlBuilder: NewBuilder()}
}

func (e *Executor) Exec(ctx context.Context, session *Session) error {
	data, printer, err := e.sqlBuilder.Build(session.View, session.Lookup(session.View))
	if err != nil {
		return err
	}

	if err = e.exec(ctx, session, data); err != nil {
		return err
	}

	printer.Flush()
	return err
}

func (e *Executor) exec(ctx context.Context, session *Session, data []*SQLStatment) error {
	if len(data) == 0 {
		return nil
	}

	db, err := session.View.Db()
	if err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	errors := shared.NewErrors(0)
	wg := &sync.WaitGroup{}
	wg.Add(len(data))
	for i := range data {
		e.execData(ctx, wg, tx, data[i], errors, session)
	}

	wg.Wait()

	if err = errors.Error(); err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (e *Executor) execData(ctx context.Context, wg *sync.WaitGroup, tx *sql.Tx, data *SQLStatment, errors *shared.Errors, session *Session) {
	defer wg.Done()
	if strings.TrimSpace(data.SQL) == "" {
		return
	}

	err := e.executeStatement(ctx, tx, data, session)
	if err != nil {
		errors.Append(err)
	}
}

func (e *Executor) executeStatement(ctx context.Context, tx *sql.Tx, stmt *SQLStatment, session *Session) error {
	_, err := tx.ExecContext(ctx, stmt.SQL, stmt.Args...)
	if err != nil {
		session.View.Logger.LogDatabaseErr(stmt.SQL, err)
		err = fmt.Errorf("error occured while connecting to database")
	}

	return err
}
