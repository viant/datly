package executor

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/datly/service/executor/expand"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/metadata/info"
)

type executionTestSource struct{ db *sql.DB }

func (s executionTestSource) Db(context.Context) (*sql.DB, error) { return s.db, nil }
func (s executionTestSource) Dialect(ctx context.Context) (*info.Dialect, error) {
	return config.Dialect(ctx, s.db)
}

type executionTestIterator struct {
	items []interface{}
	index int
}

func (i *executionTestIterator) HasAny() bool  { return len(i.items) > 0 }
func (i *executionTestIterator) HasNext() bool { return i.index < len(i.items) }
func (i *executionTestIterator) Next() interface{} {
	value := i.items[i.index]
	i.index++
	return value
}

func TestExecuteStmtsMarksRawSQLOnlyAfterSuccess(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	failed := &expand.SQLStatment{SQL: "INSERT INTO missing(id) VALUES (1)"}
	err = New().ExecuteStmts(context.Background(), executionTestSource{db: db}, &executionTestIterator{items: []interface{}{failed}})
	if err == nil {
		t.Fatal("expected execution failure")
	}
	if failed.Executed() {
		t.Fatal("failed raw SQL was marked executed")
	}
	if _, err = db.Exec("CREATE TABLE audit (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	success := &expand.SQLStatment{SQL: "INSERT INTO audit(id) VALUES (1)"}
	if err = New().ExecuteStmts(context.Background(), executionTestSource{db: db}, &executionTestIterator{items: []interface{}{success}}); err != nil {
		t.Fatal(err)
	}
	if !success.Executed() {
		t.Fatal("successful raw SQL was not marked executed")
	}
	if err = New().ExecuteStmts(context.Background(), executionTestSource{db: db}, &executionTestIterator{items: []interface{}{success}}); err != nil {
		t.Fatalf("executed raw SQL ran twice: %v", err)
	}
}
