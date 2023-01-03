package executor

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/template/expand"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/io/update"
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
	state, data, printer, sqlCriteria, err := e.sqlBuilder.Build(session.View, session.Lookup(session.View))
	session.State = state

	if err != nil {
		return err
	}

	if err = e.exec(ctx, session, data, sqlCriteria); err != nil {
		return err
	}

	printer.Flush()
	return err
}

func (e *Executor) exec(ctx context.Context, session *Session, data []*SQLStatment, criteria *expand.SQLCriteria) error {
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
		e.execData(ctx, wg, tx, data[i], errors, session, criteria, db)
	}

	wg.Wait()

	if err = errors.Error(); err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (e *Executor) execData(ctx context.Context, wg *sync.WaitGroup, tx *sql.Tx, data *SQLStatment, errors *shared.Errors, session *Session, criteria *expand.SQLCriteria, db *sql.DB) {
	defer wg.Done()
	if strings.TrimSpace(data.SQL) == "" {
		return
	}

	err := e.tryExec(ctx, criteria, tx, data, session, db)
	if err != nil {
		errors.Append(err)
	}
}

func (e *Executor) tryExec(ctx context.Context, criteria *expand.SQLCriteria, tx *sql.Tx, data *SQLStatment, session *Session, db *sql.DB) error {
	if executable, ok := criteria.IsServiceExec(data.SQL); ok {
		if executable.Executed {
			return nil
		}

		executable.Executed = true
		switch executable.ExecType {
		case expand.ExecTypeInsert:
			service, err := insert.New(ctx, db, executable.Table)
			if err != nil {
				return err
			}

			_, _, err = service.Exec(ctx, executable.Data, tx)
			return err
		case expand.ExecTypeUpdate:
			service, err := update.New(ctx, db, executable.Table)
			if err != nil {
				return err
			}

			_, err = service.Exec(ctx, executable.Data, tx)
			return err
		default:
			return fmt.Errorf("unsupported exec type: %v\n", executable.ExecType)
		}
	}

	err := e.executeStatement(ctx, tx, data, session)
	return err
}

func (e *Executor) executeStatement(ctx context.Context, tx *sql.Tx, stmt *SQLStatment, session *Session) error {
	_, err := tx.ExecContext(ctx, stmt.SQL, stmt.Args...)
	if err != nil {
		session.View.Logger.LogDatabaseErr(stmt.SQL, err)
		err = fmt.Errorf("error occured while connecting to database")
	}

	return err
}
