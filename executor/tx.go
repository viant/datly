package executor

import (
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/option"
)

type (
	lazyTx struct {
		db            *sql.DB
		tx            *sql.Tx
		isGlobal      bool
		isTransientTx bool
	}

	Tx interface {
		Rollback() error
		Commit() error
	}

	TxTransient struct{}
)

func (t *TxTransient) Rollback() error {
	return nil
}

func (t *TxTransient) Commit() error {
	return nil
}

func newLazyTx(db *sql.DB, globally bool, tx *sql.Tx) *lazyTx {
	return &lazyTx{
		db:            db,
		isGlobal:      globally,
		tx:            tx,
		isTransientTx: tx != nil,
	}
}

func (l *lazyTx) RollbackIfNeeded() error {
	if l.tx == nil {
		return nil
	}

	closer, err := l.TxCloser()
	if err != nil {
		return err
	}

	return closer.Rollback()
}

func (l *lazyTx) CommitIfNeeded() error {
	if l.tx == nil {
		return nil
	}

	closer, err := l.TxCloser()
	if err != nil {
		return err
	}

	return closer.Commit()
}

func (l *lazyTx) TxCloser() (Tx, error) {
	if l.isTransientTx {
		return &TxTransient{}, nil
	}

	return l.Tx()
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
