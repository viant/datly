package executor

import (
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/option"
)

type lazyTx struct {
	db       *sql.DB
	tx       *sql.Tx
	isGlobal bool
}

func newLazyTx(db *sql.DB, globally bool) *lazyTx {
	return &lazyTx{
		db:       db,
		isGlobal: globally,
	}
}

func (l *lazyTx) RollbackIfNeeded() error {
	if l.tx == nil {
		return nil
	}

	return l.tx.Rollback()
}

func (l *lazyTx) CommitIfNeeded() error {
	if l.tx == nil {
		return nil
	}

	return l.tx.Commit()
}

func (l *lazyTx) Tx() (*sql.Tx, error) {
	if l.isGlobal {
		return nil, fmt.Errorf("unexpected attemp to get Tx")
	}

	if l.tx != nil {
		return l.tx, nil
	}

	tx, err := l.db.Begin()
	l.tx = tx
	return tx, err
}

func (l *lazyTx) PrepareTxOptions() ([]option.Option, error) {
	tx, err := l.Tx()
	if err != nil {
		return nil, err
	}

	return []option.Option{tx}, nil
}
