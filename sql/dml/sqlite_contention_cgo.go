//go:build cgo

package dml

import (
	"errors"
	"github.com/mattn/go-sqlite3"
)

func sqliteMutationContention(err error) bool {
	var sqliteError sqlite3.Error
	return errors.As(err, &sqliteError) && (sqliteError.Code == sqlite3.ErrBusy || sqliteError.Code == sqlite3.ErrLocked)
}
