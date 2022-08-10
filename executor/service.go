package executor

import (
	"context"
	"database/sql"
	"github.com/viant/datly/shared"
	"sync"
)

type Executor struct {
	sqlBuilder *SqlBuilder
}

func New() *Executor {
	return &Executor{sqlBuilder: NewBuilder()}
}

func (e *Executor) Exec(ctx context.Context, session *Session) error {
	data, err := e.sqlBuilder.Build(session.View, session.Lookup(session.View))
	if err != nil {
		return err
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
		go e.execData(ctx, wg, tx, data[i], errors)
	}

	wg.Wait()

	if err = errors.Error(); err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (e *Executor) execData(ctx context.Context, wg *sync.WaitGroup, tx *sql.Tx, data *SqlData, errors *shared.Errors) {
	defer wg.Done()
	err := e.execDataWithErr(ctx, tx, data)
	if err != nil {
		errors.Append(err)
	}
}

func (e *Executor) execDataWithErr(ctx context.Context, tx *sql.Tx, data *SqlData) error {
	_, err := tx.ExecContext(ctx, data.SQL, data.Args...)
	return err
}
