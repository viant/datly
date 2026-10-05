package dml

import (
	"errors"
	"fmt"
	"testing"
)

type sqliteCodeError int

func (e sqliteCodeError) Error() string { return "coded database failure" }
func (e sqliteCodeError) Code() int     { return int(e) }

func TestSQLiteContentionRequiresDialectAndDriverCode(t *testing.T) {
	for _, tc := range []struct {
		dialect string
		err     error
		want    bool
	}{
		{"SQLite", sqliteCodeError(5), true},
		{"sqlite", sqliteCodeError(6), true},
		{"SQLite", sqliteCodeError(517), true},
		{"SQLite", sqliteCodeError(262), true},
		{"SQLite", sqliteCodeError(19), false},
		{"MySQL", sqliteCodeError(5), false},
		{"", sqliteCodeError(5), false},
		{"SQLite", errors.New("database is locked (5)"), false},
	} {
		if got := codedSQLiteContention(fmt.Errorf("wrapped: %w", tc.err), tc.dialect); got != tc.want {
			t.Fatalf("%s %v: %v", tc.dialect, tc.err, got)
		}
	}
}
